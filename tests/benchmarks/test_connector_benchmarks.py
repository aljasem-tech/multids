"""Benchmark smoke tests.

The SQL/SQLite benchmark runs here because it is fully reproducible offline.
S3 and OpenSearch benchmarks are opt-in and skip without a configured service.
"""

from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import pytest

_TOOLS = pathlib.Path(__file__).resolve().parents[2] / "tools"
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))

_spec = importlib.util.spec_from_file_location("multids_bench_tools", _TOOLS / "benchmarks.py")
assert _spec and _spec.loader
bench = importlib.util.module_from_spec(_spec)
# Register before exec_module so module-level @dataclass can resolve cls.__module__.
sys.modules["multids_bench_tools"] = bench
_spec.loader.exec_module(bench)

_helpers_spec = importlib.util.spec_from_file_location(
    "multids_bench_helpers", _REPO_ROOT / "benchmarks" / "benchmark_connectors.py"
)
assert _helpers_spec and _helpers_spec.loader
helpers = importlib.util.module_from_spec(_helpers_spec)
sys.modules["multids_bench_helpers"] = helpers
_helpers_spec.loader.exec_module(helpers)


@pytest.mark.benchmark
def test_sql_batch_benchmark_is_reproducible():
    measurement = bench.bench_sql_sqlite(rows=1000, repeats=1)
    assert measurement is not None
    assert measurement.name == "sql_batch_sqlite"
    assert measurement.items == 1000
    assert measurement.throughput > 0


@pytest.mark.benchmark
@pytest.mark.asyncio
async def test_s3_benchmark_skips_without_configuration(monkeypatch):
    for key in ("MULTIDS_S3_TEST_BUCKET", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(key, raising=False)
    assert await bench.bench_s3_transfer(mib=1, repeats=1) is None


@pytest.mark.benchmark
@pytest.mark.asyncio
async def test_opensearch_benchmark_skips_without_configuration(monkeypatch):
    monkeypatch.delenv("OPENSEARCH_URL", raising=False)
    assert await bench.bench_opensearch_bulk(docs=10, repeats=1) is None


@pytest.mark.benchmark
@pytest.mark.real_integration
@pytest.mark.asyncio
async def test_s3_benchmark_runs_when_configured():
    if not os.environ.get("MULTIDS_S3_TEST_BUCKET"):
        pytest.skip("Set MULTIDS_S3_TEST_BUCKET and AWS credentials to run the S3 benchmark")
    measurement = await bench.bench_s3_transfer(mib=8, repeats=1)
    assert measurement is not None and measurement.throughput > 0


@pytest.mark.benchmark
@pytest.mark.real_integration
@pytest.mark.asyncio
async def test_opensearch_benchmark_runs_when_configured():
    if not os.environ.get("OPENSEARCH_URL"):
        pytest.skip("Set OPENSEARCH_URL to run the OpenSearch benchmark")
    measurement = await bench.bench_opensearch_bulk(docs=500, repeats=1)
    assert measurement is not None and measurement.throughput > 0


# ---------------------------------------------------------------------------
# Reusable helper coverage (benchmark_connectors.py)
# ---------------------------------------------------------------------------


class _FakeObjectConnector:
    def __init__(self) -> None:
        self.store: dict[tuple[str, str], bytes] = {}

    async def write_bytes(self, data, bucket, key):
        self.store[(bucket, key)] = data

    async def read_bytes(self, bucket, key):
        return self.store[(bucket, key)]


class _FakeRecordConnector:
    def __init__(self) -> None:
        self.written: list = []

    async def write_records(self, target, records):
        records = list(records)
        self.written.append((target, records))
        from multids.contracts import BulkWriteResult

        return BulkWriteResult(items_written=len(records))


@pytest.mark.benchmark
@pytest.mark.asyncio
async def test_benchmark_s3_transfer_helper():
    connector = _FakeObjectConnector()
    payload = b"payload-\xc3\xa9"
    result = await helpers.benchmark_s3_transfer(connector, "bucket", "key", payload)
    assert result.operation == "s3_transfer"
    assert result.items == 1
    assert result.bytes_processed == len(payload) * 2
    assert result.elapsed_seconds >= 0


@pytest.mark.benchmark
@pytest.mark.asyncio
async def test_benchmark_sql_batch_helper():
    connector = _FakeRecordConnector()
    rows = [{"id": i} for i in range(10)]
    result = await helpers.benchmark_sql_batch(connector, "t", rows)
    assert result.operation == "sql_batch"
    assert result.items == 10
    assert connector.written[0][0] == "t"


@pytest.mark.benchmark
@pytest.mark.asyncio
async def test_benchmark_opensearch_bulk_helper():
    connector = _FakeRecordConnector()
    docs = [{"id": i} for i in range(5)]
    result = await helpers.benchmark_opensearch_bulk(connector, "idx", docs)
    assert result.operation == "opensearch_bulk"
    assert result.items == 5
    assert connector.written[0][0] == "idx"
