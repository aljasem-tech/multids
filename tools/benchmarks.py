"""Reproducible connector benchmarks.

Usage
-----
Run every benchmark that is configured for the current environment::

    python tools/benchmarks.py --output benchmarks/results.json

Compare against the committed baseline and fail on material regressions::

    python tools/benchmarks.py --compare benchmarks/baseline.json --tolerance 0.25

Environment
-----------
* SQL batch (SQLite): always available (stdlib sqlite3 via SQLAlchemy).
* S3 transfer: ``MULTIDS_S3_TEST_BUCKET`` + AWS credentials (+ optional
  ``AWS_ENDPOINT_URL`` for MinIO/LocalStack).
* OpenSearch bulk: ``OPENSEARCH_URL`` (+ optional credentials).

Each measurement records the minimum wall time over ``--repeats`` runs to reduce
scheduler noise. Throughput is ``items / seconds``. A regression is reported
when throughput drops by more than ``--tolerance`` relative to the baseline.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Callable

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(REPO_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(REPO_ROOT, "src"))


@dataclass
class Measurement:
    name: str
    backend: str
    items: int
    unit: str
    seconds: float
    throughput: float
    details: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["seconds"] = round(self.seconds, 6)
        data["throughput"] = round(self.throughput, 3)
        return data


def _time_sync(fn: Callable[[], Any], repeats: int) -> tuple[float, Any]:
    times: list[float] = []
    result: Any = None
    for _ in range(repeats):
        started = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - started)
    return min(times), result


async def _time_async(fn: Callable[[], Any], repeats: int) -> tuple[float, Any]:
    times: list[float] = []
    result: Any = None
    for _ in range(repeats):
        started = time.perf_counter()
        result = await fn()
        times.append(time.perf_counter() - started)
    return min(times), result


def bench_sql_sqlite(rows: int, repeats: int) -> Measurement | None:
    """SQL batch insert benchmark against SQLite (always reproducible)."""
    from multids.connectors.sync.sql import SyncSQLConnectorBase

    connector = SyncSQLConnectorBase("sqlite:///:memory:")
    connector.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, grp INTEGER, payload TEXT)")
    batch = [{"id": i, "grp": i % 100, "payload": "x" * 32} for i in range(rows)]

    def run() -> int:
        # Fresh table each repetition so inserts are not rejected as duplicates.
        connector.execute("DELETE FROM bench")
        return connector.bulk_insert("bench", batch, chunk_size=500).items_written

    seconds, written = _time_sync(run, repeats)
    connector.close()
    return Measurement(
        name="sql_batch_sqlite",
        backend="sql",
        items=written,
        unit="rows",
        seconds=seconds,
        throughput=written / seconds if seconds else 0.0,
        details={"driver": "sqlite", "chunk_size": 500},
    )


async def bench_s3_transfer(mib: int, repeats: int) -> Measurement | None:
    """S3 multipart transfer benchmark (requires configured S3)."""
    bucket = os.environ.get("MULTIDS_S3_TEST_BUCKET")
    if not bucket or not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return None

    from multids.connectors.s3 import S3Connector

    payload = os.urandom(mib * 1024 * 1024)
    key = f"multids-bench/{uuid.uuid4()}.bin"
    connector = S3Connector(
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None,
        part_size=8 * 1024 * 1024,
        timeout=600.0,
    )

    async def gen():
        step = 8 * 1024 * 1024
        for offset in range(0, len(payload), step):
            yield payload[offset : offset + step]

    async def run() -> int:
        await connector.write_stream(gen(), bucket=bucket, key=key, force_multipart=True)
        data = await connector.read_bytes(bucket, key)
        await connector.delete_object(bucket, key)
        return len(data)

    try:
        seconds, written = await _time_async(run, repeats)
    finally:
        await connector.close()

    return Measurement(
        name="s3_transfer",
        backend="s3",
        items=written,
        unit="bytes",
        seconds=seconds,
        throughput=written / seconds if seconds else 0.0,
        details={"mib": mib, "part_size": 8 * 1024 * 1024},
    )


async def bench_opensearch_bulk(docs: int, repeats: int) -> Measurement | None:
    """OpenSearch bulk index benchmark (requires a running cluster)."""
    url = os.environ.get("OPENSEARCH_URL")
    if not url:
        return None

    try:
        from multids.connectors.opensearch import OpenSearchConnector
    except Exception:
        return None

    username = os.environ.get("OPENSEARCH_USERNAME")
    password = os.environ.get("OPENSEARCH_PASSWORD")
    if username and password:
        connector = OpenSearchConnector(url, basic_auth=(username, password), timeout=120.0)
    else:
        connector = OpenSearchConnector(url, api_key=os.environ.get("OPENSEARCH_API_KEY"), timeout=120.0)

    index = f"multids-bench-{uuid.uuid4().hex[:12]}"

    async def run() -> int:
        await connector._request("DELETE", f"/{index}")  # best effort
        await connector._request("PUT", f"/{index}", json={"settings": {"number_of_shards": 1}})
        batch = ({"id": i, "payload": "x" * 64} for i in range(docs))
        await connector.bulk_index(index, batch, chunk_size=500, refresh=True)
        return docs

    try:
        seconds, written = await _time_async(run, repeats)
        await connector._request("DELETE", f"/{index}")
    finally:
        await connector.close()

    return Measurement(
        name="opensearch_bulk",
        backend="opensearch",
        items=written,
        unit="docs",
        seconds=seconds,
        throughput=written / seconds if seconds else 0.0,
        details={"chunk_size": 500},
    )


def _compare(results: list[Measurement], baseline_path: str, tolerance: float) -> int:
    with open(baseline_path, encoding="utf-8") as handle:
        baseline = {m["name"]: m for m in json.load(handle)["measurements"]}

    failures = 0
    for measurement in results:
        base = baseline.get(measurement.name)
        if not base or not base.get("throughput"):
            continue
        ratio = measurement.throughput / base["throughput"]
        status = "ok"
        if ratio < (1.0 - tolerance):
            status = "REGRESSION"
            failures += 1
        print(
            f"{status:10} {measurement.name:22} "
            f"{measurement.throughput:12.1f} {measurement.unit}/s "
            f"(baseline {base['throughput']:.1f}, {ratio * 100:.0f}%)"
        )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=20_000, help="SQL rows per run")
    parser.add_argument("--docs", type=int, default=5_000, help="OpenSearch docs per run")
    parser.add_argument("--mib", type=int, default=64, help="S3 payload size in MiB")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", default=os.path.join(REPO_ROOT, "benchmarks", "results.json"))
    parser.add_argument("--compare", default=None, help="Baseline JSON to compare against")
    parser.add_argument("--tolerance", type=float, default=0.25, help="Allowed throughput regression (0..1)")
    args = parser.parse_args()

    results: list[Measurement] = []
    for measurement in (
        bench_sql_sqlite(args.rows, args.repeats),
        asyncio.run(bench_s3_transfer(args.mib, args.repeats)),
        asyncio.run(bench_opensearch_bulk(args.docs, args.repeats)),
    ):
        if measurement is not None:
            results.append(measurement)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    payload = {
        "python": sys.version.split()[0],
        "measurements": [m.as_dict() for m in results],
    }
    with open(args.output, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)

    print(f"Wrote {len(results)} measurement(s) to {args.output}")
    for measurement in results:
        print(
            f"  {measurement.name:22} {measurement.throughput:12.1f} {measurement.unit}/s "
            f"({measurement.seconds:.3f}s, n={measurement.items})"
        )

    if args.compare:
        failures = _compare(results, args.compare, args.tolerance)
        if failures:
            print(f"{failures} benchmark regression(s) exceeded tolerance {args.tolerance:.0%}")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
