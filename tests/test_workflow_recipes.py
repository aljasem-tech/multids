"""End-to-end workflow recipe tests.

These exercise the ``multids.workflows`` helpers against in-memory fakes that
behave like the real connectors, covering the four documented cross-connector
flows (local-to-S3, S3-to-OpenSearch, Athena-to-SQL, SQL-to-OpenSearch) with
explicit cancellation, partial-failure, and checkpoint semantics.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator, Iterable, Mapping

import pytest

from multids.workflows import CopyResult, RecordCopyResult, copy_records, copy_stream

# ---------------------------------------------------------------------------
# Byte-oriented fakes (local / S3)
# ---------------------------------------------------------------------------


class FakeByteSource:
    """Simulates a byte source such as LocalConnector or S3Connector."""

    def __init__(self, chunks: list[bytes], *, fail_after: int | None = None) -> None:
        self.chunks = chunks
        self.fail_after = fail_after
        self.read_args: tuple[Any, ...] = ()
        self.read_kwargs: dict[str, Any] = {}

    async def read_stream(self, *args: Any, **kwargs: Any) -> AsyncIterator[bytes]:
        self.read_args = args
        self.read_kwargs = kwargs
        for index, chunk in enumerate(self.chunks):
            if self.fail_after is not None and index >= self.fail_after:
                raise ConnectionError("simulated source read failure")
            yield chunk

    async def read_bytes(self, *args: Any, **kwargs: Any) -> bytes:
        return b"".join(self.chunks)


class FakeByteDestination:
    """Simulates a byte destination such as LocalConnector or S3Connector."""

    def __init__(self, *, fail_on_open: bool = False) -> None:
        self.chunks: list[bytes] = []
        self.fail_on_open = fail_on_open
        self.write_args: tuple[Any, ...] = ()
        self.write_kwargs: dict[str, Any] = {}

    async def write_stream(self, stream: AsyncIterator[bytes], *args: Any, **kwargs: Any) -> None:
        self.write_args = args
        self.write_kwargs = kwargs
        if self.fail_on_open:
            raise ConnectionError("simulated destination open failure")
        async for chunk in stream:
            self.chunks.append(chunk)

    async def write_bytes(self, data: bytes, *args: Any, **kwargs: Any) -> None:
        self.chunks.append(data)


# ---------------------------------------------------------------------------
# Record-oriented fakes (Athena / SQL / OpenSearch)
# ---------------------------------------------------------------------------


class FakeRecordSource:
    """Simulates a record source such as AthenaConnector or a SQL connector."""

    def __init__(
        self,
        records: Iterable[Mapping[str, Any]],
        *,
        fail_after: int | None = None,
    ) -> None:
        self._records = list(records)
        self.fail_after = fail_after

    def query_stream(self, sql: str, /, **params: Any) -> AsyncIterator[Mapping[str, Any]]:
        return self._gen()

    def fetch_rows(self, sql: str, /, **params: Any) -> AsyncIterator[Mapping[str, Any]]:
        return self._gen()

    async def _gen(self) -> AsyncIterator[Mapping[str, Any]]:
        for index, record in enumerate(self._records):
            if self.fail_after is not None and index >= self.fail_after:
                raise ConnectionError("simulated source read failure")
            yield record


class FakeRecordWriter:
    """Simulates a record destination such as OpenSearch or a SQL connector."""

    def __init__(self, *, fail_on_batch: int | None = None) -> None:
        self.written: list[Mapping[str, Any]] = []
        self.batches: list[list[Mapping[str, Any]]] = []
        self.fail_on_batch = fail_on_batch

    async def bulk_index(
        self, index: str, docs: Iterable[Mapping[str, Any]], chunk_size: int = 500
    ) -> list[dict[str, Any]]:
        batch = list(docs)
        self.batches.append(batch)
        if self.fail_on_batch is not None and len(self.batches) - 1 == self.fail_on_batch:
            raise ConnectionError("simulated bulk write failure")
        self.written.extend(batch)
        return [{"errors": False, "items": len(batch)}]

    async def bulk_insert(self, table: str, rows: Iterable[Mapping[str, Any]]) -> Any:
        batch = list(rows)
        self.batches.append(batch)
        if self.fail_on_batch is not None and len(self.batches) - 1 == self.fail_on_batch:
            raise ConnectionError("simulated bulk insert failure")
        self.written.extend(batch)
        return type("Result", (), {"items_written": len(batch)})()


# ---------------------------------------------------------------------------
# 1. local-to-S3: byte copy with progress, checkpoint, and resume kwargs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_to_s3_stream_copy_with_progress_and_checkpoint():
    source = FakeByteSource([b"chunk-1", b"chunk-2", b"chunk-3"])
    destination = FakeByteDestination()

    progress: list[CopyResult] = []
    checkpoints: list[CopyResult] = []

    result = await copy_stream(
        source,
        destination,
        source_args=("daily/events.jsonl",),
        destination_args=("my-bucket", "exports/events.jsonl"),
        destination_kwargs={"checkpoint_path": "ckpt.json", "resume": True},
        progress=progress.append,
        checkpoint=checkpoints.append,
    )

    # Streaming preserved chunk-by-chunk.
    assert destination.chunks == [b"chunk-1", b"chunk-2", b"chunk-3"]
    assert result.bytes_copied == len(b"chunk-1chunk-2chunk-3")
    assert result.chunks_copied == 3

    # Progress fired after each chunk with cumulative byte counts.
    assert [update.chunks_copied for update in progress] == [1, 2, 3]
    assert [update.bytes_copied for update in progress] == [7, 14, 21]

    # Checkpoint fired alongside progress.
    assert checkpoints == progress

    # Connector-specific args/kwargs are forwarded untouched.
    assert source.read_args == ("daily/events.jsonl",)
    assert destination.write_args == ("my-bucket", "exports/events.jsonl")
    assert destination.write_kwargs == {"checkpoint_path": "ckpt.json", "resume": True}


# ---------------------------------------------------------------------------
# 2. S3-to-OpenSearch: JSONL stream -> bulk index
# ---------------------------------------------------------------------------


def _jsonl_records(chunks: Iterable[bytes]) -> list[dict[str, Any]]:
    remainder = b""
    records: list[dict[str, Any]] = []
    for chunk in chunks:
        lines = (remainder + chunk).split(b"\n")
        remainder = lines.pop()
        for line in lines:
            if line:
                records.append(json.loads(line))
    if remainder:
        records.append(json.loads(remainder))
    return records


@pytest.mark.asyncio
async def test_s3_to_opensearch_jsonl_flow():
    payload = b'{"id": 1, "name": "alice"}\n{"id": 2, "name": "bob"}\n{"id": 3, "name": "carol"}\n'
    # Split mid-line to prove the remainder handling works.
    source = FakeByteSource([payload[:20], payload[20:]])
    writer = FakeRecordWriter()

    async def index_rows(batch: list[dict[str, Any]]) -> None:
        await writer.bulk_index("events", batch, chunk_size=len(batch))

    result = await copy_records(_jsonl_records(source.chunks), index_rows, batch_size=2)

    assert [r["id"] for r in writer.written] == [1, 2, 3]
    assert result.records_copied == 3
    assert result.batches_written == 2


# ---------------------------------------------------------------------------
# 3. Athena-to-SQL: query stream -> bulk insert
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_athena_to_sql_flow():
    rows = [{"id": i, "value": f"row-{i}"} for i in range(250)]
    source = FakeRecordSource(rows)
    writer = FakeRecordWriter()

    async def insert_rows(batch: list[Mapping[str, Any]]) -> None:
        await writer.bulk_insert("events", batch)

    result = await copy_records(source.query_stream("SELECT * FROM events"), insert_rows, batch_size=100)

    assert result.records_copied == 250
    assert result.batches_written == 3
    assert [len(b) for b in writer.batches] == [100, 100, 50]
    assert writer.written[0] == {"id": 0, "value": "row-0"}
    assert writer.written[-1] == {"id": 249, "value": "row-249"}


# ---------------------------------------------------------------------------
# 4. SQL-to-OpenSearch: fetch rows -> bulk index
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sql_to_opensearch_flow():
    rows = [{"id": i, "name": f"name-{i}"} for i in range(120)]
    source = FakeRecordSource(rows)
    writer = FakeRecordWriter()

    async def index_rows(batch: list[Mapping[str, Any]]) -> None:
        await writer.bulk_index("events", batch)

    result = await copy_records(source.fetch_rows("SELECT * FROM events"), index_rows, batch_size=50)

    assert result.records_copied == 120
    assert result.batches_written == 3
    assert [len(b) for b in writer.batches] == [50, 50, 20]


# ---------------------------------------------------------------------------
# 5. Checkpoint receives cumulative results
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_copy_records_checkpoint_receives_cumulative_results():
    rows = [{"id": i} for i in range(5)]
    source = FakeRecordSource(rows)
    writer = FakeRecordWriter()

    checkpoints: list[RecordCopyResult] = []

    async def write_batch(batch: list[Mapping[str, Any]]) -> None:
        await writer.bulk_insert("events", batch)

    result = await copy_records(
        source.query_stream("SELECT * FROM events"),
        write_batch,
        batch_size=2,
        checkpoint=checkpoints.append,
    )

    # Checkpoints fire after each successful batch with cumulative counts.
    assert [(c.records_copied, c.batches_written) for c in checkpoints] == [
        (2, 1),
        (4, 2),
        (5, 3),
    ]
    assert result.records_copied == 5
    assert result.batches_written == 3


# ---------------------------------------------------------------------------
# 6. Partial failure: copy_records keeps already-written batches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_copy_records_partial_failure_keeps_prior_batches():
    rows = [{"id": i} for i in range(6)]
    source = FakeRecordSource(rows)
    # Fail while writing the second batch.
    writer = FakeRecordWriter(fail_on_batch=1)

    async def write_batch(batch: list[Mapping[str, Any]]) -> None:
        await writer.bulk_insert("events", batch)

    with pytest.raises(ConnectionError):
        await copy_records(source.query_stream("SELECT * FROM events"), write_batch, batch_size=2)

    # First batch was committed; second was not.
    assert writer.written == [{"id": 0}, {"id": 1}]
    assert len(writer.batches) == 2


# ---------------------------------------------------------------------------
# 7. Partial failure: copy_stream leaves a partial destination
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_copy_stream_partial_failure_leaves_partial_destination():
    source = FakeByteSource([b"aaa", b"bbb", b"ccc"], fail_after=2)
    destination = FakeByteDestination()

    with pytest.raises(ConnectionError):
        await copy_stream(source, destination)

    # Two chunks made it through before the source failed.
    assert destination.chunks == [b"aaa", b"bbb"]


@pytest.mark.asyncio
async def test_copy_stream_destination_open_failure_propagates():
    source = FakeByteSource([b"aaa"])
    destination = FakeByteDestination(fail_on_open=True)

    # The destination fails before consuming the stream.
    with pytest.raises(ConnectionError):
        await copy_stream(source, destination)


# ---------------------------------------------------------------------------
# 8. Cancellation propagates and preserves partial progress
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_copy_stream_cancellation_propagates():
    class CancellingSource:
        def __init__(self) -> None:
            self.cancelled = False

        async def read_stream(self, *args: Any, **kwargs: Any) -> AsyncIterator[bytes]:
            yield b"first"
            raise asyncio.CancelledError()

        async def read_bytes(self, *args: Any, **kwargs: Any) -> bytes:
            return b"first"

    destination = FakeByteDestination()
    with pytest.raises(asyncio.CancelledError):
        await copy_stream(CancellingSource(), destination)

    assert destination.chunks == [b"first"]


@pytest.mark.asyncio
async def test_copy_records_cancellation_propagates():
    class CancellingSource:
        def query_stream(self, sql: str, /, **params: Any) -> AsyncIterator[Mapping[str, Any]]:
            return self._gen()

        async def _gen(self) -> AsyncIterator[Mapping[str, Any]]:
            yield {"id": 0}
            yield {"id": 1}
            # Cancel while the second batch is being assembled.
            raise asyncio.CancelledError()

    writer = FakeRecordWriter()

    async def write_batch(batch: list[Mapping[str, Any]]) -> None:
        await writer.bulk_insert("events", batch)

    with pytest.raises(asyncio.CancelledError):
        await copy_records(
            CancellingSource().query_stream("SELECT 1"),
            write_batch,
            batch_size=2,
        )

    # The first full batch was committed before cancellation.
    assert writer.written == [{"id": 0}, {"id": 1}]
    assert len(writer.batches) == 1
