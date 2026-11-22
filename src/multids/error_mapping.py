"""Map backend-specific exceptions onto the stable multids error hierarchy.

Connectors raise normalized :mod:`multids.errors` types so callers can handle
failures without importing backend SDKs. The mappers here are deliberately
duck-typed: they inspect class names and attributes rather than importing
optional drivers, so they are safe in minimal installations.

Classification
--------------
* Missing optional dependency      -> :class:`ConnectorDependencyError`
* Unreachable / dropped connection -> :class:`ConnectorConnectionError`
* Invalid statement / schema       -> :class:`ConnectorSchemaError`
* Data-level rejection             -> :class:`ConnectorDataError`
* Anything else                    -> :class:`ConnectorOperationError`
"""

from __future__ import annotations

from typing import Optional

from .errors import (
    ConnectorConnectionError,
    ConnectorDataError,
    ConnectorDependencyError,
    ConnectorError,
    ConnectorOperationError,
    ConnectorSchemaError,
)

__all__ = [
    "is_connection_error",
    "map_botocore_error",
    "map_pyodbc_error",
    "map_sqlalchemy_error",
    "normalize_backend_error",
]

# Substrings (lower-cased class names) that indicate an unreachable backend.
_CONNECTION_MARKERS = (
    "connection",
    "connect",
    "operational",
    "interface",
    "timeout",
    "timedout",
    "network",
    "endpoint",
    "proxy",
    "ssl",
    "dns",
    "socket",
    "disconnect",
    "gone",
    "unreachable",
    "requesterror",
    "readtimeout",
    "connecttimeout",
    "connectionerror",
    "connectionrefused",
)

# Substrings that indicate a statement/schema problem.
_SCHEMA_MARKERS = (
    "programming",
    "syntax",
    "compile",
    "undefined",
    "nosuchtable",
    "unknowncolumn",
    "schemerror",
    "invalidcatalog",
)

# Substrings that indicate a data-level problem.
_DATA_MARKERS = (
    "dataerror",
    "integrity",
    "invalidinput",
    "validation",
    "notnull",
    "foreignkey",
    "unprocessable",
)


def _class_names(exc: BaseException) -> list[str]:
    names: list[str] = []
    for cls in type(exc).__mro__:
        names.append(cls.__name__.lower())
    return names


def is_connection_error(exc: BaseException) -> bool:
    """Return ``True`` when ``exc`` looks like a transient connectivity failure."""
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return True
    haystack = " ".join(_class_names(exc))
    return any(marker in haystack for marker in _CONNECTION_MARKERS)


def _has_marker(exc: BaseException, markers: tuple[str, ...]) -> bool:
    haystack = " ".join(_class_names(exc))
    return any(marker in haystack for marker in markers)


def _status_code(exc: BaseException) -> Optional[int]:
    for attr in ("status_code", "status", "http_status"):
        value = getattr(exc, attr, None)
        if isinstance(value, int):
            return value
    response = getattr(exc, "response", None)
    if response is not None:
        value = getattr(response, "status_code", None)
        if isinstance(value, int):
            return value
    return None


def normalize_backend_error(
    exc: BaseException,
    *,
    backend: str,
    operation: str,
    fallback: type[ConnectorError] = ConnectorOperationError,
) -> ConnectorError:
    """Classify ``exc`` and return a normalized :class:`ConnectorError`.

    ``backend`` and ``operation`` are safe identifiers used only to build the
    message; the original exception is preserved as ``__cause__``.
    """
    if isinstance(exc, ConnectorError):
        return exc

    if isinstance(exc, ModuleNotFoundError) or type(exc).__name__.lower() in {"importerror", "modulenotfounderror"}:
        return ConnectorDependencyError(f"{backend} dependency is missing for {operation}; install the matching extra")

    status = _status_code(exc)
    if status is not None and 500 <= status < 600:
        return ConnectorConnectionError(f"{backend} {operation} failed with server status {status}")
    if is_connection_error(exc):
        return ConnectorConnectionError(f"{backend} {operation} could not reach the backend")
    if _has_marker(exc, _SCHEMA_MARKERS):
        return ConnectorSchemaError(f"{backend} rejected the statement for {operation}")
    if _has_marker(exc, _DATA_MARKERS):
        return ConnectorDataError(f"{backend} reported a data error for {operation}")
    if status is not None and 400 <= status < 500:
        return fallback(f"{backend} rejected the request for {operation} (status {status})")
    return fallback(f"{backend} {operation} failed")


def map_sqlalchemy_error(exc: BaseException, *, operation: str) -> ConnectorError:
    """Map a SQLAlchemy/DBAPI exception to the multids hierarchy."""
    return normalize_backend_error(exc, backend="sql", operation=operation)


def map_botocore_error(exc: BaseException, *, backend: str, operation: str) -> ConnectorError:
    """Map a botocore/aioboto3 exception to the multids hierarchy."""
    return normalize_backend_error(exc, backend=backend, operation=operation)


def map_pyodbc_error(exc: BaseException, *, operation: str) -> ConnectorError:
    """Map a pyodbc/aioodbc exception to the multids hierarchy."""
    return normalize_backend_error(exc, backend="mssql", operation=operation)
