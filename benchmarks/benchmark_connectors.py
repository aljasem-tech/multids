"""Reusable real-service benchmark helpers; do not run these against production data."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class BenchmarkResult:
    operation: str
    items: int
    elapsed_seconds: float
    bytes_processed: int = 0


async def benchmark_s3_transfer(connector: Any, bucket: str, key: str, payload: bytes) -> BenchmarkResult:
    started = time.perf_counter()
    await connector.write_bytes(payload, bucket, key)
    received = await connector.read_bytes(bucket, key)
    elapsed = time.perf_counter() - started
    assert received == payload
    return BenchmarkResult("s3_transfer", 1, elapsed, bytes_processed=len(payload) * 2)


async def benchmark_sql_batch(connector: Any, table: str, rows: Iterable[Mapping[str, Any]]) -> BenchmarkResult:
    rows = list(rows)
    started = time.perf_counter()
    await connector.write_records(table, rows)
    return BenchmarkResult("sql_batch", len(rows), time.perf_counter() - started)


async def benchmark_opensearch_bulk(connector: Any, index: str, docs: Iterable[Mapping[str, Any]]) -> BenchmarkResult:
    docs = list(docs)
    started = time.perf_counter()
    await connector.write_records(index, docs)
    return BenchmarkResult("opensearch_bulk", len(docs), time.perf_counter() - started)
