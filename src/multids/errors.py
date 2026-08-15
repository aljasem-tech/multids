"""Stable error types for callers that need to handle connector failures."""


class ConnectorError(Exception):
    """Base class for errors raised by multids connectors."""


class ConnectorDependencyError(ConnectorError):
    """A connector's optional dependency is not installed."""


class ConnectorConnectionError(ConnectorError):
    """A connector could not reach its backend after retrying."""


class ConnectorOperationError(ConnectorError):
    """A backend rejected an otherwise valid connector operation."""


class ConnectorSchemaError(ConnectorOperationError):
    """A request or result does not match the backend's expected schema."""


class ConnectorDataError(ConnectorOperationError):
    """A backend reported a data-level failure (for example, a rejected bulk row)."""
