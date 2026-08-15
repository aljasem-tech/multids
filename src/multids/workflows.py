"""Composable helpers for moving bytes and records between connectors.

These helpers deliberately do not own connector lifecycles. Callers should use
their connector's context manager (when available) or close it in ``finally``.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Any, AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, TypeVar, cast

from .interfaces import Readable, Writable

T = TypeVar("T")
R = TypeVar("R")


@dataclass(frozen=True)
class CopyResult:
    """The amount of data successfully passed to the destination writer."""

    bytes_copied: int
    chunks_copied: int


@dataclass(frozen=True)
class RecordCopyResult:
    """Summary of records successfully submitted to a batch writer."""

    records_copied: int
    batches_written: int


async def iter_records(records: Iterable[T] | AsyncIterable[T]) -> AsyncIterator[T]:
    """Normalize synchronous and asynchronous record sources."""
    if hasattr(records, "__aiter__"):
        async for record in records:  # type: ignore[union-attr]
            yield record
    else:
        for record in records:  # type: ignore[union-attr]
            yield record


async def _resolve(value: T | Awaitable[T]) -> T:
    if inspect.isawaitable(value):
        return cast(T, await value)
    return cast(T, value)


async def copy_stream(
    source: Readable,
    destination: Writable,
    *,
    source_args: tuple[Any, ...] = (),
    source_kwargs: dict[str, Any] | None = None,
    destination_args: tuple[Any, ...] = (),
    destination_kwargs: dict[str, Any] | None = None,
    progress: Callable[[CopyResult], Any | Awaitable[Any]] | None = None,
) -> CopyResult:
    """Copy bytes between connectors without buffering the complete payload.

    ``source_args``/``source_kwargs`` are passed to ``source.read_stream`` and
    destination equivalents are passed to ``destination.write_stream``. The
    progress callback runs after each chunk has been read and may be async.

    Cancellation and errors propagate to the caller. A destination may contain
    a partial object after a failure; cleanup and resume semantics remain the
    destination connector's responsibility (for example, S3 checkpoints).
    """
    bytes_copied = 0
    chunks_copied = 0
    source_kwargs = source_kwargs or {}
    destination_kwargs = destination_kwargs or {}

    async def counted_stream() -> AsyncIterator[bytes]:
        nonlocal bytes_copied, chunks_copied
        async for chunk in source.read_stream(*source_args, **source_kwargs):
            if not isinstance(chunk, bytes):
                raise TypeError("read_stream must yield bytes")
            bytes_copied += len(chunk)
            chunks_copied += 1
            result = CopyResult(bytes_copied=bytes_copied, chunks_copied=chunks_copied)
            if progress is not None:
                await _resolve(progress(result))
            yield chunk

    await destination.write_stream(counted_stream(), *destination_args, **destination_kwargs)
    return CopyResult(bytes_copied=bytes_copied, chunks_copied=chunks_copied)


async def batch_records(records: Iterable[T] | AsyncIterable[T], size: int) -> AsyncIterator[list[T]]:
    """Yield non-empty record batches of at most ``size`` records."""
    if size <= 0:
        raise ValueError("size must be greater than zero")
    batch: list[T] = []
    async for record in iter_records(records):
        batch.append(record)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


async def _apply_concurrently(
    records: Iterable[T] | AsyncIterable[T],
    function: Callable[[T], Awaitable[R]],
    concurrency: int,
) -> AsyncIterator[R]:
    if concurrency <= 0:
        raise ValueError("concurrency must be greater than zero")
    if concurrency == 1:
        async for record in iter_records(records):
            yield await function(record)
        return

    async for group in batch_records(records, concurrency):

        async def invoke(record: T) -> R:
            return await function(record)

        tasks: list[asyncio.Task[R]] = [asyncio.create_task(invoke(record)) for record in group]
        try:
            # gather preserves input order while bounding in-flight work.
            for result in await asyncio.gather(*tasks):
                yield result
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise


async def map_records(
    records: Iterable[T] | AsyncIterable[T],
    mapper: Callable[[T], R | Awaitable[R]],
    *,
    concurrency: int = 1,
) -> AsyncIterator[R]:
    """Transform records, preserving order with bounded async concurrency."""

    async def wrapped_mapper(record: T) -> R:
        return await _resolve(mapper(record))

    async for record in _apply_concurrently(records, wrapped_mapper, concurrency):
        yield record


async def filter_records(
    records: Iterable[T] | AsyncIterable[T],
    predicate: Callable[[T], bool | Awaitable[bool]],
    *,
    concurrency: int = 1,
) -> AsyncIterator[T]:
    """Yield records accepted by a synchronous or asynchronous predicate."""

    async def evaluate(record: T) -> tuple[T, bool]:
        accepted = await _resolve(predicate(record))
        return record, accepted

    async for pair in _apply_concurrently(records, evaluate, concurrency):
        record, accepted = pair
        if accepted:
            yield record


async def copy_records(
    records: Iterable[T] | AsyncIterable[T],
    write_batch: Callable[[list[T]], Any | Awaitable[Any]],
    *,
    batch_size: int = 1_000,
) -> RecordCopyResult:
    """Batch records into a destination-specific writer.

    Errors and cancellation propagate. Previously submitted batches are not
    rolled back, so destination writes should be idempotent or checkpointed
    when retries are required.
    """
    records_copied = 0
    batches_written = 0
    async for batch in batch_records(records, batch_size):
        await _resolve(write_batch(batch))
        records_copied += len(batch)
        batches_written += 1
    return RecordCopyResult(records_copied=records_copied, batches_written=batches_written)
