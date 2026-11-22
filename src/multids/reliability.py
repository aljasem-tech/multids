"""Small, dependency-free reliability helpers shared by connectors."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, TypeVar

from .errors import ConnectorConnectionError

T = TypeVar("T")
logger = logging.getLogger("multids.connectors")


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
) -> T:
    """Run an async operation with exponential backoff and structured logs.

    Each attempt is bounded by ``config.timeout`` (when set); a timeout is
    raised as :class:`ConnectorConnectionError` and is therefore retryable.
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
            await asyncio.sleep(delay)

    raise AssertionError("unreachable")  # pragma: no cover
