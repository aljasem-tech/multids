"""Small, dependency-free reliability helpers shared by connectors."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")
logger = logging.getLogger("multids.connectors")


@dataclass(frozen=True)
class RetryConfig:
    """Retry policy for transient connector operations.

    ``max_retries`` is the number of retries after the initial attempt.
    """

    max_retries: int = 3
    backoff_factor: float = 0.5
    max_backoff: float = 30.0

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.backoff_factor < 0 or self.max_backoff < 0:
            raise ValueError("backoff values must be non-negative")

    def delay(self, retry_number: int) -> float:
        return min(self.max_backoff, self.backoff_factor * (2 ** (retry_number - 1)))


async def retry_async(
    operation: Callable[[], Awaitable[T]],
    *,
    config: RetryConfig,
    operation_name: str,
    retry_if: Callable[[Exception], bool] | None = None,
) -> T:
    """Run an async operation with exponential backoff and structured logs."""
    for retry_number in range(config.max_retries + 1):
        try:
            return await operation()
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
