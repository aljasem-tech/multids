# Connector benchmarks

Two entry points are provided:

* `benchmark_connectors.py` - reusable helpers that measure S3 transfer, SQL
  batch insertion, and OpenSearch bulk indexing against connectors you
  configure. They are intentionally not run in normal CI: each benchmark writes
  data and must be pointed at disposable buckets, tables, and indices.
* `../tools/benchmarks.py` - a reproducible runner that records results to JSON
  and can compare against a committed baseline.

## Reusable helpers

Run opt-in checks with:

```bash
RUN_MULTIDS_BENCHMARKS=1 pytest -m benchmark
```

Use a dedicated benchmark environment and record the returned elapsed time, item
count, and bytes processed in CI artifacts or your metrics system.

## Reproducible runner and baselines

```bash
# Record results (SQL/SQLite always runs; S3 and OpenSearch run when configured)
python tools/benchmarks.py --output benchmarks/results.json

# Compare against the committed baseline and fail on material regressions
python tools/benchmarks.py --compare benchmarks/baseline.json --tolerance 0.25
```

`baseline.json` holds recorded results. The SQL/SQLite baseline is reproducible
on any machine with SQLAlchemy. S3 and OpenSearch entries are populated by the
`Benchmarks` workflow (`.github/workflows/benchmarks.yml`) when those services
are available; a throughput of `0` means "not measured here".

### Service configuration

| Benchmark          | Requires                                                                 |
| ------------------ | ------------------------------------------------------------------------ |
| `sql_batch_sqlite` | nothing (stdlib `sqlite3` via SQLAlchemy)                                |
| `s3_transfer`      | `MULTIDS_S3_TEST_BUCKET`, AWS credentials, optional `AWS_ENDPOINT_URL`   |
| `opensearch_bulk`  | `OPENSEARCH_URL`, optional `OPENSEARCH_USERNAME`/`OPENSEARCH_PASSWORD`   |

### Reproducibility notes

* Each measurement is the minimum wall time over `--repeats` runs.
* Throughput is `items / seconds` (rows/s, bytes/s or docs/s).
* The S3 benchmark uses 8 MiB parts to exercise the multipart path.
* The OpenSearch benchmark uses 500-document bulk chunks with a single shard.
* A regression is reported when throughput falls by more than `--tolerance`
  (relative) compared with the baseline for the same benchmark name.
