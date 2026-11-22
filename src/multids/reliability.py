"""Small, dependency-free reliability helpers shared by connectors."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional, TypeVar

from .errors import ConnectorConnectionError
from .telemetry import Telemetry

T = TypeVar("T")
logger = logging.getLogger("multids.connectors")

RetryHook = Callable[[int, float, BaseException], None]


@dataclass(frozen=True)
class RetryConfig:
    """Retry policy for transient connector operations.

    ``max_retries`` is the number of retries after the initial attempt.
    ``timeout`` is the per-attempt timeout in seconds; ``None`` disables it.
    """

    max_retries: int = 3
    backoff_factor: float = 0.5
    max_backoff: float = 30.0
    timeout: float | None = None

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.backoff_factor < 0 or self.max_backoff < 0:
            raise ValueError("backoff values must be non-negative")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be positive when set")

    def delay(self, retry_number: int) -> float:
        return min(self.max_backoff, self.backoff_factor * (2 ** (retry_number - 1)))


async def with_timeout(
    operation: Callable[[], Awaitable[T]],
    *,
    timeout: float | None,
    operation_name: str,
) -> T:
    """Run ``operation`` with an optional per-attempt timeout.

    A timeout is surfaced as :class:`ConnectorConnectionError` so it is treated
    as a retryable transient failure by :func:`retry_async`.
    """
    if timeout is None:
        return await operation()
    try:
        return await asyncio.wait_for(operation(), timeout=timeout)
    except asyncio.TimeoutError as exc:
        raise ConnectorConnectionError(f"{operation_name} timed out after {timeout}s") from exc


async def retry_async(
    operation: Callable[[], Awaitable[T]],
    *,
    config: RetryConfig,
    operation_name: str,
    retry_if: Callable[[Exception], bool] | None = None,
    on_retry: RetryHook | None = None,
) -> T:
    """Run an async operation with exponential backoff and structured logs.

    Each attempt is bounded by ``config.timeout`` (when set); a timeout is
    raised as :class:`ConnectorConnectionError` and is therefore retryable.
    ``on_retry`` is invoked with ``(retry_number, delay_seconds, exception)``
    before each sleep so callers can emit retry telemetry.
    """
    for retry_number in range(config.max_retries + 1):
        try:
            return await with_timeout(operation, timeout=config.timeout, operation_name=operation_name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if retry_number >= config.max_retries or (retry_if and not retry_if(exc)):
                raise
            delay = config.delay(retry_number + 1)
            logger.warning(
                "connector_retry",
                extra={
                    "connector_operation": operation_name,
                    "retry_number": retry_number + 1,
                    "retry_delay_seconds": delay,
                    "error_type": type(exc).__name__,
                },
            )
            if on_retry is not None:
                on_retry(retry_number + 1, delay, exc)
            await asyncio.sleep(delay)

    raise AssertionError("unreachable")  # pragma: no cover


def retry_sync(
    operation: Callable[[], T],
    *,
    config: RetryConfig,
    operation_name: str,
    retry_if: Callable[[Exception], bool] | None = None,
    on_retry: RetryHook | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Synchronous counterpart to :func:`retry_async`.

    Uses exponential backoff and emits the same structured ``connector_retry``
    log record. Per-attempt timeouts are not enforced here (blocking calls are
    expected to honour the backend client's own timeout settings).
    """
    for retry_number in range(config.max_retries + 1):
        try:
            return operation()
        except Exception as exc:
            if retry_number >= config.max_retries or (retry_if and not retry_if(exc)):
                raise
            delay = config.delay(retry_number + 1)
            logger.warning(
                "connector_retry",
                extra={
                    "connector_operation": operation_name,
                    "retry_number": retry_number + 1,
                    "retry_delay_seconds": delay,
                    "error_type": type(exc).__name__,
                },
            )
            if on_retry is not None:
                on_retry(retry_number + 1, delay, exc)
            sleep(delay)

    raise AssertionError("unreachable")  # pragma: no cover


def _emit_retry(
    telemetry: Telemetry,
    backend: str,
    operation_name: str,
    resource: Optional[str],
) -> RetryHook:
    def hook(retry_number: int, _delay: float, exc: BaseException) -> None:
        telemetry.retry(
            backend,
            operation_name,
            retry_number=retry_number,
            error_type=type(exc).__name__,
            resource=resource,
        )

    return hook


async def run_operation(
    operation: Callable[[], Awaitable[T]],
    *,
    config: RetryConfig,
    backend: str,
    operation_name: str,
    telemetry: Telemetry | None = None,
    resource: str | None = None,
    retry: bool = True,
    retry_if: Callable[[Exception], bool] | None = None,
) -> T:
    """Run an async operation with unified retry, timeout and telemetry.

    This is the single entry point every async connector uses so that
    error/retry/logging behaviour is identical across backends. Non-idempotent
    operations pass ``retry=False`` to bound them by a timeout only.
    """
    telemetry = telemetry or Telemetry()
    telemetry.start(backend, operation_name, resource)
    started = time.perf_counter()
    try:
        if retry:
            result = await retry_async(
                operation,
                config=config,
                operation_name=operation_name,
                retry_if=retry_if,
                on_retry=_emit_retry(telemetry, backend, operation_name, resource),
            )
        else:
            result = await with_timeout(operation, timeout=config.timeout, operation_name=operation_name)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:
        telemetry.failure(
            backend,
            operation_name,
            duration_seconds=time.perf_counter() - started,
            error_type=type(exc).__name__,
            resource=resource,
        )
        raise
    telemetry.completion(backend, operation_name, duration_seconds=time.perf_counter() - started, resource=resource)
    return result


def run_operation_sync(
    operation: Callable[[], T],
    *,
    config: RetryConfig,
    backend: str,
    operation_name: str,
    telemetry: Telemetry | None = None,
    resource: str | None = None,
    retry: bool = True,
    retry_if: Callable[[Exception], bool] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Synchronous counterpart to :func:`run_operation`."""
    telemetry = telemetry or Telemetry()
    telemetry.start(backend, operation_name, resource)
    started = time.perf_counter()
    try:
        if retry:
            result = retry_sync(
                operation,
                config=config,
                operation_name=operation_name,
                retry_if=retry_if,
                on_retry=_emit_retry(telemetry, backend, operation_name, resource),
                sleep=sleep,
            )
        else:
            result = operation()
    except BaseException as exc:
        telemetry.failure(
            backend,
            operation_name,
            duration_seconds=time.perf_counter() - started,
            error_type=type(exc).__name__,
            resource=resource,
        )
        raise
    telemetry.completion(backend, operation_name, duration_seconds=time.perf_counter() - started, resource=resource)
    return result
