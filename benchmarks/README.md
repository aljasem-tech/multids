# Connector benchmarks

The helpers in `benchmark_connectors.py` measure S3 transfer, SQL batch
insertion, and OpenSearch bulk indexing against connectors you configure.
They are intentionally not run in normal CI: each benchmark writes data and
must be pointed at disposable buckets, tables, and indices.

Run opt-in checks with:

```bash
RUN_MULTIDS_BENCHMARKS=1 pytest -m benchmark
```

Use a dedicated benchmark environment and record the returned elapsed time,
item count, and bytes processed in CI artifacts or your metrics system.
