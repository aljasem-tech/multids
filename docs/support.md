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
| OpenSearch  | Yes   | No   | `opensearch`     | `AsyncRecordReader`, `AsyncRecordWriter`                               | Compatible with Elasticsearch-style APIs.                |
| Athena      | Yes   | Yes  | `s3` / `sync`    | `Async/SyncRecordReader`                                               | Read-only; requires AWS credentials and an output location. |
| MySQL       | Yes   | Yes  | `mysql` / `sync` | `Async/SyncRecordReader`, `Async/SyncRecordWriter`                     | Uses SQLAlchemy plus asyncmy or PyMySQL.                 |
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
capability protocols), a local OpenSearch container, documentation-example validation, and formatting/lint checks. The
`Real Service Integration` workflow is manually dispatched and uses repository secrets/variables for a disposable S3
bucket. Benchmark helpers for S3 transfer, SQL batching, and OpenSearch bulk indexing are in `benchmarks/`; they are
opt-in because they write real data.
