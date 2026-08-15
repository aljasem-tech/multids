# Support matrix and compatibility

## Supported Python versions

`multids` supports Python 3.11, 3.12, and 3.13. The project follows semantic
versioning: breaking public API changes are released only in a new major
version; supported APIs receive a deprecation notice for at least one minor
release before removal.

## Connector support

| Backend | Async | Sync | Install extra | Notes |
| --- | --- | --- | --- | --- |
| Local files | Yes | Yes | `local` | Async mode uses `aiofiles`. |
| Amazon S3 | Yes | Yes | `s3` | Async multipart/resume support. |
| OpenSearch | Yes | No | `opensearch` | Compatible with Elasticsearch-style APIs. |
| Athena | Yes | Yes | `s3` / `sync` | Requires AWS credentials and an output location. |
| MySQL | Yes | Yes | `mysql` / `sync` | Uses SQLAlchemy plus asyncmy or PyMySQL. |
| SQL Server | Yes | Yes | `sqlserver` | Requires an OS ODBC driver; unavailable on Python 3.12+. |

## CLI

The built-in CLI deliberately starts with safe local-file operations:

```bash
multids ping --path ./data
multids copy ./data/input.jsonl ./out/input.jsonl
multids validate workflow.json
```

`validate` accepts a JSON object with `command` set to `ping` or `copy`. Copy
configurations must declare local source and destination paths. Remote CLI
profiles will be added only after credential handling is designed safely.

## Integration tests and benchmarks

Normal CI runs deterministic unit tests and a local OpenSearch container.
The `Real Service Integration` workflow is manually dispatched and uses
repository secrets/variables for a disposable S3 bucket. Benchmark helpers
for S3 transfer, SQL batching, and OpenSearch bulk indexing are in
`benchmarks/`; they are opt-in because they write real data.
