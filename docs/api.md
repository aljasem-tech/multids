# API reference

## Core interfaces

- `multids.interfaces.Readable` provides `read_stream(...)` and `read_bytes(...)`.
- `multids.interfaces.Writable` provides `write_stream(stream, ...)` and `write_bytes(data, ...)`.
- `multids.interfaces.Connector` provides lifecycle methods: `close()` and `ping()`.

## Workflows

- `multids.workflows.copy_stream` streams bytes between a readable and a writable connector.
- `map_records`, `filter_records`, and `batch_records` compose record sources with bounded concurrency.
- `copy_records` submits batches to a destination-specific bulk writer and returns a summary.

## Typed contracts

- `ObjectRef`, `ObjectInfo`, and `ObjectPage` provide portable object metadata and cursor pagination.
- `AsyncObjectStorage` / `SyncObjectStorage` define `read_object`, `write_object`, `list_object_page`, and
  `delete_object_ref`.
- `AsyncRecordReader` / `AsyncRecordWriter` and synchronous equivalents define `stream_records` and `write_records`.
- `WriteResult` and `BulkWriteResult` report successful items, bytes, failed items, and checkpoint/continuation values.

All built-in connectors support `async with` or `with` as appropriate. Their `close()` methods are idempotent.

## Common connector methods

| Connector             | Key methods                                                                                            |
|-----------------------|--------------------------------------------------------------------------------------------------------|
| `LocalConnector`      | `read_stream(path)`, `write_stream(stream, path)`, `read_bytes(path)`, `write_bytes(data, path)`       |
| `S3Connector`         | `read_stream(bucket, key)`, `write_stream(stream, bucket, key, ...)`, `write_bytes(data, bucket, key)` |
| `OpenSearchConnector` | `bulk_index(index, docs, chunk_size=500)`, `search(index, query)`, `scroll(index, query, scroll="1m")` |
| `MySQLConnector`      | `fetch_rows(query)`, `execute(statement)`, `bulk_insert(table, rows, chunk_size=1000)`                 |

`SQLConnectorBase` (async) and `SyncSQLConnectorBase` implement the record-reader/record-writer contracts through
`stream_records`, `write_records`, and `bulk_insert`; `execute` and `execute_statement` return a `WriteResult` whose
`items_written` reflects the affected-row count. `MySQLConnector` / `SyncMySQLConnector` inherit this behaviour.

## AI integration

`multids.ai.AIClientInterface` is a pluggable asynchronous text-generation protocol. `multids.ai.OpenAIClient` adapts an
OpenAI-compatible async client.
