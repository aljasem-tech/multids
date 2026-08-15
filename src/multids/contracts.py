"""Typed, backend-neutral connector capability contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    runtime_checkable,
)


@dataclass(frozen=True)
class ObjectRef:
    """A portable object reference; ``container`` is required by S3 and unused locally."""

    key: str
    container: Optional[str] = None


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: Optional[int] = None
    last_modified: Optional[datetime | float] = None
    etag: Optional[str] = None


@dataclass(frozen=True)
class ObjectPage:
    items: Sequence[ObjectInfo]
    next_cursor: Optional[str] = None


@dataclass(frozen=True)
class WriteResult:
    items_written: int = 1
    bytes_written: Optional[int] = None
    checkpoint: Optional[str] = None


@dataclass(frozen=True)
class BulkWriteResult:
    items_written: int
    failed_items: Sequence[Any] = field(default_factory=tuple)
    next_cursor: Optional[str] = None


@runtime_checkable
class AsyncObjectStorage(Protocol):
    async def read_object(self, ref: ObjectRef) -> bytes: ...

    async def write_object(self, ref: ObjectRef, data: bytes) -> WriteResult: ...

    async def list_object_page(
        self, prefix: ObjectRef = ObjectRef(""), *, cursor: Optional[str] = None, page_size: int = 1_000
    ) -> ObjectPage: ...

    async def delete_object_ref(self, ref: ObjectRef) -> WriteResult: ...


@runtime_checkable
class SyncObjectStorage(Protocol):
    def read_object(self, ref: ObjectRef) -> bytes: ...

    def write_object(self, ref: ObjectRef, data: bytes) -> WriteResult: ...

    def list_object_page(
        self, prefix: ObjectRef = ObjectRef(""), *, cursor: Optional[str] = None, page_size: int = 1_000
    ) -> ObjectPage: ...

    def delete_object_ref(self, ref: ObjectRef) -> WriteResult: ...


@runtime_checkable
class AsyncRecordReader(Protocol):
    def stream_records(self, query: str, /, **params: Any) -> AsyncIterator[Mapping[str, Any]]: ...


@runtime_checkable
class AsyncRecordWriter(Protocol):
    async def write_records(
        self,
        target: str,
        records: Iterable[Mapping[str, Any]] | AsyncIterable[Mapping[str, Any]],
        *,
        batch_size: int = 1_000,
    ) -> BulkWriteResult: ...


@runtime_checkable
class SyncRecordReader(Protocol):
    def stream_records(self, query: str, /, **params: Any) -> Iterator[Mapping[str, Any]]: ...


@runtime_checkable
class SyncRecordWriter(Protocol):
    def write_records(
        self, target: str, records: Iterable[Mapping[str, Any]], *, batch_size: int = 1_000
    ) -> BulkWriteResult: ...
