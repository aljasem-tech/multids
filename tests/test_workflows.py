import asyncio
from typing import AsyncIterator

import pytest

from multids.workflows import CopyResult, batch_records, copy_records, copy_stream, filter_records, map_records


class MemorySource:
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks

    async def read_stream(self, *args: object, **kwargs: object) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk

    async def read_bytes(self, *args: object, **kwargs: object) -> bytes:
        return b"".join(self.chunks)


class MemoryDestination:
    def __init__(self) -> None:
        self.chunks: list[bytes] = []

    async def write_stream(self, stream: AsyncIterator[bytes], *args: object, **kwargs: object) -> None:
        async for chunk in stream:
            self.chunks.append(chunk)

    async def write_bytes(self, data: bytes, *args: object, **kwargs: object) -> None:
        self.chunks.append(data)


@pytest.mark.asyncio
async def test_copy_stream_preserves_streaming_and_reports_progress():
    destination = MemoryDestination()
    progress: list[CopyResult] = []

    result = await copy_stream(MemorySource([b"one", b"two"]), destination, progress=progress.append)

    assert destination.chunks == [b"one", b"two"]
    assert result.bytes_copied == 6
    assert result.chunks_copied == 2
    assert [update.bytes_copied for update in progress] == [3, 6]


@pytest.mark.asyncio
async def test_record_pipeline_preserves_order_with_bounded_concurrency():
    active = 0
    peak_active = 0

    async def mapper(value):
        nonlocal active, peak_active
        active += 1
        peak_active = max(peak_active, active)
        await asyncio.sleep(0)
        active -= 1
        return value * 2

    mapped: AsyncIterator[int] = map_records([1, 2, 3, 4], mapper, concurrency=2)
    filtered: AsyncIterator[int] = filter_records(mapped, lambda value: value > 4, concurrency=2)

    assert [record async for record in filtered] == [6, 8]
    assert peak_active <= 2
    assert [batch async for batch in batch_records([1, 2, 3], 2)] == [[1, 2], [3]]


@pytest.mark.asyncio
async def test_copy_records_writes_batches_and_returns_summary():
    received = []

    async def write_batch(batch):
        received.append(batch)

    result = await copy_records([{"id": 1}, {"id": 2}, {"id": 3}], write_batch, batch_size=2)

    assert received == [[{"id": 1}, {"id": 2}], [{"id": 3}]]
    assert result.records_copied == 3
    assert result.batches_written == 2
