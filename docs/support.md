# Support matrix and compatibility

## Supported Python versions

`multids` supports Python 3.11, 3.12, and 3.13. The project follows semantic versioning: breaking public API changes are
released only in a new major version; supported APIs receive a deprecation notice for at least one minor release before
removal.

## Connector support

| Backend     | Async | Sync | Install extra    | Contracts                                                              | Notes                                                    |
|-------------|-------|------|------------------|------------------------------------------------------------------------|----------------------------------------------------------|
| Local files | Yes   | Yes  | `local`          | `Async/SyncObjectStorage`                                              | Async mode uses `aiofiles`.                              |
| Amazon S3   | Yes   | Yes  | `s3`             | `Async/SyncObjectStorage`                                              | Async multipart/resume support.                          |
| OpenSearch  | Yes   | Yes  | `opensearch`     | `Async/SyncRecordReader`, `Async/SyncRecordWriter`                     | Compatible with Elasticsearch-style APIs.                |
| Athena      | Yes   | Yes  | `s3` / `sync`    | `Async/SyncRecordReader`                                               | Read-only; requires AWS credentials and an output location. |
| MySQL       | Yes   | Yes  | `mysql` / `sync` | `Async/SyncRecordReader`, `Async/SyncRecordWriter`                     | Uses SQLAlchemy plus asyncmy or PyMySQL.                 |
| PostgreSQL  | Yes   | Yes  | `postgres`       | `Async/SyncRecordReader`, `Async/SyncRecordWriter`                     | Uses asyncpg (async) / psycopg2 (sync); native COPY bulk insert. |
| SQL Server  | Yes   | Yes  | `sqlserver`      | `Async/SyncRecordReader`, `Async/SyncRecordWriter`                     | Requires an OS ODBC driver; unavailable on Python 3.12+. |

Every connector conforms to the capability protocols in `multids.contracts`: object-storage connectors implement
`read_object`, `write_object`, `list_object_page`, and `delete_object_ref`; record connectors implement `stream_records`
and/or `write_records`. All connectors support `async with` / `with` as appropriate and expose idempotent `close()`
methods. See `docs/api.md` for the full contract reference.

## CLI

The built-in CLI deliberately starts with safe local-file operations:

```bash
multids ping --path ./data
multids copy ./data/input.jsonl ./out/input.jsonl
multids validate workflow.json
```

`validate` accepts a JSON object with `command` set to `ping` or `copy`. Copy configurations must declare local source
and destination paths. Remote CLI profiles will be added only after credential handling is designed safely.

## Integration tests and benchmarks

Normal CI runs deterministic unit tests (including contract-conformance tests that verify every connector against the
capability protocols), documentation-example validation, and formatting/lint checks.

The `Real Service Integration` workflow runs a backend matrix against disposable
services:

| Backend    | Service                                   | Python versions | Notes                                    |
| ---------- | ----------------------------------------- | --------------- | ---------------------------------------- |
| S3         | MinIO (S3-compatible)                     | 3.11–3.13       | Auth, Unicode, multipart, pagination, cleanup, resume |
| MySQL      | `mysql:8.0` container                     | 3.11–3.13       | Unicode, pagination, large streams, failure recovery |
| PostgreSQL | `postgres:16` container                    | 3.11–3.13       | Unicode, COPY bulk insert, NULL handling, streaming |
| SQL Server | `mssql/server:2022-latest` container      | 3.11            | ODBC driver installed in CI; pyodbc is <3.12 only |
| OpenSearch | `opensearchproject/opensearch:2.9.0`      | 3.11–3.13       | Bulk, scroll pagination, large loads, failure recovery |
| Athena     | AWS (repository secrets)                  | 3.11            | Opt-in; skips when secrets are absent     |

Integration tests live in `tests/integration/` and are marked `real_integration`.
They skip cleanly when the required environment variables are not set, so they
never fail a local or PR run.

### Benchmarks

Reproducible benchmarks for S3 transfer, SQL batch inserts and OpenSearch bulk
indexing live in `benchmarks/`, driven by `tools/benchmarks.py`:

```bash
# Record results (SQL/SQLite runs offline; S3/OpenSearch run when configured)
python tools/benchmarks.py --output benchmarks/results.json

# Compare against the committed baseline and fail on material regressions
python tools/benchmarks.py --compare benchmarks/baseline.json --tolerance 0.25
```

`benchmarks/baseline.json` records the committed results:

| Benchmark          | Throughput     | Service            |
| ------------------ | -------------- | ------------------ |
| `sql_batch_sqlite` | 408,313 rows/s | SQLite (stdlib)    |
| `s3_transfer`      | ~96.3 MiB/s    | MinIO (pgsty/silo) |
| `opensearch_bulk`  | 11,407 docs/s  | OpenSearch 2.9.0   |

The SQL/SQLite baseline is reproducible on any machine with SQLAlchemy; the S3 and OpenSearch entries are recorded by
the `Benchmarks` workflow when those services are available. A throughput regression beyond `--tolerance` (default
25%) fails the benchmark job.
