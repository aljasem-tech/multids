from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, AsyncIterator, Iterator, Optional, Protocol, TypedDict, runtime_checkable

DataChunk = bytes
AsyncByteIterator = AsyncIterator[DataChunk]


class Readable(Protocol):
    """Protocol for objects that can stream bytes out of a data source."""

    def read_stream(self, *args: Any, **kwargs: Any) -> AsyncByteIterator:
        """Yield chunks of bytes from the source."""
        ...

    async def read_bytes(self, *args: Any, **kwargs: Any) -> bytes:
        """Convenience: read the entire payload into memory."""
        ...


class Writable(Protocol):
    """Protocol for objects that can consume async byte streams to write to a destination."""

    async def write_stream(self, stream: AsyncByteIterator, *args: Any, **kwargs: Any) -> None:
        """Consume the stream and store the content."""
        ...

    async def write_bytes(self, data: bytes, *args: Any, **kwargs: Any) -> None:
        """Convenience: write a single bytes object."""
        ...


@runtime_checkable
class Connector(Protocol):
    """
    Base connector interface.

    Connectors SHOULD implement `close()` and may provide any combination of
    the read/write convenience methods above.
    """

    @abstractmethod
    async def close(self) -> None:  # pragma: no cover - interface only
        """Release any held resources (sessions, clients)."""
        ...

    async def ping(self) -> bool:
        """Optional quick health check; return True if the connector is responsive."""
        ...


class S3Location(TypedDict):
    bucket: str
    key: str


class SQLLocation(TypedDict, total=False):
    table: str
    schema: Optional[str]


@dataclass
class RangeSpec:
    offset: Optional[int] = None
    length: Optional[int] = None


@runtime_checkable
class ContentHook(Protocol):
    """Protocol for hooks that can intercept and transform data."""

    async def pre_write(self, data: Any, **kwargs: Any) -> Any:
        """Called before writing data to storage. Return transformed data."""
        ...

    async def post_read(self, data: Any, **kwargs: Any) -> Any:
        """Called after reading data from storage. Return transformed data."""
        ...


class SyncReadable(Protocol):
    """Protocol for objects that can stream bytes synchronously."""

    def read_stream(self, *args: Any, **kwargs: Any) -> Iterator[bytes]:
        """Yield chunks of bytes from the source."""
        ...

    def read_bytes(self, *args: Any, **kwargs: Any) -> bytes:
        """Convenience: read the entire payload into memory."""
        ...


class SyncWritable(Protocol):
    """Protocol for objects that can consume synchronous byte streams."""

    def write_stream(self, stream: Iterator[bytes], *args: Any, **kwargs: Any) -> None:
        """Consume the stream and store the content."""
        ...

    def write_bytes(self, data: bytes, *args: Any, **kwargs: Any) -> None:
        """Convenience: write a single bytes object."""
        ...


@runtime_checkable
class SyncConnector(Protocol):
    """Base synchronous connector interface."""

    @abstractmethod
    def close(self) -> None:
        """Release any held resources."""
        ...

    def ping(self) -> bool:
        """Optional quick health check."""
        ...


@runtime_checkable
class SyncContentHook(Protocol):
    """Protocol for synchronous hooks."""

    def pre_write(self, data: Any, **kwargs: Any) -> Any: ...

    def post_read(self, data: Any, **kwargs: Any) -> Any: ...
