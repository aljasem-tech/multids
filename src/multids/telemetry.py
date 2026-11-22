"""Structured operational telemetry for connectors.

This module provides a small, dependency-free telemetry surface that every
remote connector uses to emit the same *shape* of operational events:

* ``start``       - an operation began
* ``completion``  - an operation finished successfully
* ``retry``       - a transient failure is being retried
* ``failure``     - an operation failed permanently

Security note
-------------
:class:`TelemetryEvent` intentionally has a **closed set of fields**. There is
no free-form payload, no SQL text, no query parameters and no credentials.
``resource`` is a *safe* operator-chosen identifier (for example an S3 bucket
name, a table name, or an index name). Callers must never pass secret values as
``resource``. This is what lets the exit criteria hold: credentials, SQL
parameters and record payloads cannot appear in emitted telemetry because there
is nowhere to put them.

Callbacks
---------
``tracer`` and ``metrics`` are optional callables that receive each event. They
are invoked *after* the structured log record and are isolated: an exception in
a callback is logged and swallowed so telemetry can never break data movement.
No observability provider is required; when both are omitted the events are
still emitted through the standard :mod:`logging` module at ``DEBUG`` level.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional

__all__ = ["TelemetryEvent", "Telemetry", "TelemetryObserver"]

#: Callback type for tracing/metrics sinks. Implementations must not raise.
TelemetryObserver = Callable[["TelemetryEvent"], None]

START = "start"
COMPLETION = "completion"
RETRY = "retry"
FAILURE = "failure"

_DEFAULT_LOGGER = "multids.connectors"


@dataclass(frozen=True)
class TelemetryEvent:
    """A single, redaction-safe operational event.

    Attributes
    ----------
    event:
        One of ``"start"``, ``"completion"``, ``"retry"`` or ``"failure"``.
    backend:
        Backend family, e.g. ``"s3"``, ``"sql"``, ``"opensearch"``.
    operation:
        Dotted operation name, e.g. ``"sql.execute"``.
    duration_seconds:
        Elapsed wall-clock seconds for completion/failure events (``0.0`` for
        ``start`` and ``retry``).
    resource:
        Optional *safe* identifier (bucket/key/table/index). Never a secret.
    retry_number:
        The retry attempt number for ``retry`` events, otherwise ``None``.
    error_type:
        Exception class name for ``retry``/``failure`` events, otherwise ``None``.
    """

    event: str
    backend: str
    operation: str
    duration_seconds: float = 0.0
    resource: Optional[str] = None
    retry_number: Optional[int] = None
    error_type: Optional[str] = None

    def as_log_extra(self) -> dict[str, object]:
        """Return the structured ``extra`` mapping for a log record."""
        extra: dict[str, object] = {
            "telemetry_event": self.event,
            "connector_backend": self.backend,
            "connector_operation": self.operation,
            "connector_duration_seconds": round(self.duration_seconds, 6),
        }
        if self.resource is not None:
            extra["connector_resource"] = self.resource
        if self.retry_number is not None:
            extra["connector_retry_number"] = self.retry_number
        if self.error_type is not None:
            extra["connector_error_type"] = self.error_type
        return extra


class Telemetry:
    """Emit :class:`TelemetryEvent` objects to logs and optional sinks.

    Parameters
    ----------
    tracer:
        Optional callback invoked with every event (for distributed tracing).
    metrics:
        Optional callback invoked with every event (for metrics aggregation).
    logger:
        Logger to emit structured records to. Defaults to ``multids.connectors``.
    """

    def __init__(
        self,
        *,
        tracer: Optional[TelemetryObserver] = None,
        metrics: Optional[TelemetryObserver] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._tracer = tracer
        self._metrics = metrics
        self._logger = logger or logging.getLogger(_DEFAULT_LOGGER)

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    def emit(self, event: TelemetryEvent) -> None:
        """Emit ``event`` to the log and any configured sinks."""
        self._logger.debug(event.event, extra=event.as_log_extra())
        self._dispatch(self._tracer, event, "tracer")
        self._dispatch(self._metrics, event, "metrics")

    @staticmethod
    def _dispatch(observer: Optional[TelemetryObserver], event: TelemetryEvent, kind: str) -> None:
        if observer is None:
            return
        try:
            observer(event)
        except Exception:  # pragma: no cover - defensive; telemetry must not break work
            logging.getLogger(_DEFAULT_LOGGER).warning(
                "telemetry_observer_failed", extra={"telemetry_observer": kind, "telemetry_event": event.event}
            )

    def start(self, backend: str, operation: str, resource: Optional[str] = None) -> None:
        self.emit(TelemetryEvent(event=START, backend=backend, operation=operation, resource=resource))

    def completion(
        self,
        backend: str,
        operation: str,
        *,
        duration_seconds: float,
        resource: Optional[str] = None,
    ) -> None:
        self.emit(
            TelemetryEvent(
                event=COMPLETION,
                backend=backend,
                operation=operation,
                duration_seconds=duration_seconds,
                resource=resource,
            )
        )

    def retry(
        self,
        backend: str,
        operation: str,
        *,
        retry_number: int,
        error_type: str,
        resource: Optional[str] = None,
    ) -> None:
        self.emit(
            TelemetryEvent(
                event=RETRY,
                backend=backend,
                operation=operation,
                resource=resource,
                retry_number=retry_number,
                error_type=error_type,
            )
        )

    def failure(
        self,
        backend: str,
        operation: str,
        *,
        duration_seconds: float,
        error_type: str,
        resource: Optional[str] = None,
    ) -> None:
        self.emit(
            TelemetryEvent(
                event=FAILURE,
                backend=backend,
                operation=operation,
                duration_seconds=duration_seconds,
                resource=resource,
                error_type=error_type,
            )
        )

    def with_defaults(self, *, backend: str, operation_prefix: str = "") -> "_ScopedTelemetry":
        """Return a lightweight binding that pre-fills backend/operation fields."""
        return _ScopedTelemetry(self, backend=backend, operation_prefix=operation_prefix)


class _ScopedTelemetry:
    """Internal helper binding a backend and operation prefix to a Telemetry."""

    def __init__(self, telemetry: Telemetry, *, backend: str, operation_prefix: str = "") -> None:
        self._telemetry = telemetry
        self._backend = backend
        prefix = operation_prefix
        if prefix and not prefix.endswith("."):
            prefix = f"{prefix}."
        self._prefix = prefix

    def _op(self, operation: str) -> str:
        return f"{self._prefix}{operation}"

    def start(self, operation: str, resource: Optional[str] = None) -> None:
        self._telemetry.start(self._backend, self._op(operation), resource)

    def completion(self, operation: str, *, duration_seconds: float, resource: Optional[str] = None) -> None:
        self._telemetry.completion(
            self._backend, self._op(operation), duration_seconds=duration_seconds, resource=resource
        )

    def retry(self, operation: str, *, retry_number: int, error_type: str, resource: Optional[str] = None) -> None:
        self._telemetry.retry(
            self._backend, self._op(operation), retry_number=retry_number, error_type=error_type, resource=resource
        )

    def failure(
        self, operation: str, *, duration_seconds: float, error_type: str, resource: Optional[str] = None
    ) -> None:
        self._telemetry.failure(
            self._backend,
            self._op(operation),
            duration_seconds=duration_seconds,
            error_type=error_type,
            resource=resource,
        )


def monotonic() -> float:
    """Monotonic clock used to measure operation duration."""
    return time.perf_counter()
