# API reference

## Errors

All connectors raise exceptions from `multids.errors`. Catch the base class to handle any connector failure, or a
specific subclass for targeted handling.

| Exception                    | Base                    | When it is raised                                             |
|------------------------------|-------------------------|---------------------------------------------------------------|
| `ConnectorError`             | `Exception`             | Base class for every connector error                          |
| `ConnectorDependencyError`   | `ConnectorError`        | An optional dependency (driver) is not installed              |
| `ConnectorConnectionError`   | `ConnectorError`        | Backend unreachable or timed out after retries (safe to retry)|
| `ConnectorOperationError`    | `ConnectorError`        | Backend rejected a valid request                              |
| `ConnectorSchemaError`       | `ConnectorOperationError`| Request or result shape is wrong                             |
| `ConnectorDataError`         | `ConnectorOperationError`| A row or document was rejected by the backend                |

```python
from multids.errors import ConnectorError, ConnectorConnectionError

try:
    ...
except ConnectorConnectionError:
    ...  # transient; safe to retry
except ConnectorError:
    ...  # any other connector failure
```

## Core interfaces

Every connector implements one or more of these protocols from `multids.interfaces`:

| Protocol            | Methods                                              |
|---------------------|------------------------------------------------------|
| `Readable`          | `read_stream(...)`, `read_bytes(...)`                |
| `Writable`          | `write_stream(stream, ...)`, `write_bytes(data, ...)`|
| `Connector`         | `close()`, `ping()`                                  |
| `SyncReadable`      | `read_stream(...)`, `read_bytes(...)` (sync)         |
| `SyncWritable`      | `write_stream(...)`, `write_bytes(...)` (sync)       |
| `SyncConnector`     | `close()`, `ping()` (sync)                           |

All connectors support `async with` (async) or `with` (sync) as appropriate. `close()` is idempotent.

## Typed contracts

`multids.contracts` defines the capability protocols and result types:

| Contract / type                          | Purpose                                                            |
|------------------------------------------|--------------------------------------------------------------------|
| `ObjectRef`, `ObjectInfo`, `ObjectPage`  | Portable object metadata and cursor pagination                     |
| `AsyncObjectStorage` / `SyncObjectStorage`| `read_object`, `write_object`, `list_object_page`, `delete_object_ref` |
| `AsyncRecordReader` / `AsyncRecordWriter`| `stream_records`, `write_records` (and sync equivalents)           |
| `WriteResult`                            | Reports `items_written`, `bytes_written`, `failed_items`, checkpoint |
| `BulkWriteResult`                        | Bulk-operation summary with per-item failures and continuation     |

## Connector constructors and key methods

### LocalConnector (async)

```text
LocalConnector(base_path=None)
```

| Method                                                        | Description                              |
|---------------------------------------------------------------|------------------------------------------|
| `read_stream(path, chunk_size=65536)`                         | Yield file bytes in chunks               |
| `read_bytes(path)`                                            | Read a whole file                        |
| `write_stream(stream, path)`                                  | Write a byte stream to a file            |
| `write_bytes(data, path)`                                     | Write bytes to a file                    |
| `read_json(path)` / `write_json(data, path, indent=2)`        | JSON helpers (Unicode preserved)         |
| `list_keys(prefix="", recursive=True)`                        | Yield file paths under a prefix          |
| `delete_object(path, missing_ok=False)`                       | Delete a file                            |

### S3Connector (async)

```text
S3Connector(
    aws_region=None, region_name=None, endpoint_url=None,
    part_size=8388608, max_concurrency=4,
    min_multipart_upload_size=5242880,
    enforce_min_part_size=False, spill_to_disk_threshold=33554432,
    max_retries=3, backoff_factor=0.5, max_backoff=30.0, timeout=300.0,
    telemetry=None,
)
```

| Method                                                                     | Description                                     |
|----------------------------------------------------------------------------|-------------------------------------------------|
| `read_stream(bucket, key)` / `read_bytes(bucket, key)`                     | Streaming / full reads                          |
| `write_stream(stream, bucket, key, *, checkpoint_path=None, resume=False, force_multipart=False, progress_callback=None)` | Multipart upload with optional resume |
| `write_bytes(data, bucket, key)`                                           | Single PUT or multipart (automatic)             |
| `write_json(data, bucket, key)` / `read_json(bucket, key)`                 | JSON helpers                                    |
| `list_keys(bucket, prefix="")`                                             | Paginated key listing                           |
| `delete_object(bucket, key)`                                               | Delete an object                                |

### OpenSearchConnector (async)

```text
OpenSearchConnector(
    base_url, api_key=None, basic_auth=None,
    timeout=30.0, max_retries=3, backoff_factor=0.5, max_backoff=30.0,
    telemetry=None,
)
```

| Method                                              | Description                              |
|-----------------------------------------------------|------------------------------------------|
| `index_doc(index, doc)`                             | Index a single document                  |
| `bulk_index(index, docs, chunk_size=500)`           | Bulk index with per-item failure reporting|
| `search(index, query, size=10)`                     | Search; returns the raw response         |
| `scroll(index, query, scroll="1m")`                 | Async-iterate over large result sets     |

### SQLConnectorBase / MySQLConnector (async)

```text
MySQLConnector(url, *, max_retries=3, backoff_factor=0.5, max_backoff=30.0, timeout=30.0, telemetry=None)
```

| Method                                              | Description                              |
|-----------------------------------------------------|------------------------------------------|
| `execute(statement)`                                | Run a statement; returns `WriteResult`   |
| `fetch_rows(query)`                                 | Fetch rows as dicts                      |
| `bulk_insert(table, rows, chunk_size=1000)`         | Bulk insert; returns `BulkWriteResult`   |
| `stream_records(query)` / `write_records(table, records)` | Record-reader/writer contract     |

### MSSQLConnector (async)

```text
MSSQLConnector(dsn_or_url, *, max_retries=3, ..., telemetry=None)
```

| Method              | Description                          |
|---------------------|--------------------------------------|
| `connect_pool()`    | Create the ODBC connection pool      |
| `fetch_rows(query)` | Fetch rows as dicts                  |
| `execute(statement)`| Run a statement                      |
| `close()`           | Close the pool                       |

### AthenaConnector (async, read-only)

```text
AthenaConnector(region_name=None, workgroup=None, poll_interval=1.0, max_wait=300.0, ...)
```

| Method                                              | Description                              |
|-----------------------------------------------------|------------------------------------------|
| `query_stream(sql, database=None)`                  | Async-iterate over result rows           |

## Sync connectors

Sync twins live under `multids.connectors.sync` and mirror the async API with blocking calls:

| Sync connector          | Async counterpart     |
|-------------------------|-----------------------|
| `SyncLocalConnector`    | `LocalConnector`      |
| `SyncS3Connector`       | `S3Connector`         |
| `SyncMySQLConnector`    | `MySQLConnector`      |
| `SyncMSSQLConnector`    | `MSSQLConnector`      |
| `SyncAthenaConnector`   | `AthenaConnector`     |

OpenSearch has no sync variant.

## Workflows

`multids.workflows` provides connector-agnostic orchestration:

| Helper                                        | Purpose                                                        |
|-----------------------------------------------|----------------------------------------------------------------|
| `copy_stream(source, dest, ...)`              | Stream bytes between two connectors without buffering          |
| `copy_records(records, write_batch, ...)`     | Batch records to a destination writer with a summary           |
| `map_records(records, fn, concurrency=N)`     | Transform records (sync or async fn), order-preserving         |
| `filter_records(records, predicate)`          | Filter records                                                 |
| `batch_records(records, size=N)`              | Group records into batches                                     |

## AI integration

| Class / protocol        | Purpose                                                        |
|-------------------------|----------------------------------------------------------------|
| `AIClientInterface`     | Protocol: `async def generate(prompt, **kwargs) -> dict`       |
| `OpenAIClient(client)`  | Adapter for any OpenAI-compatible async client                 |

## Telemetry

`multids.telemetry.Telemetry` provides structured events. Inject it into any connector via `telemetry=`:

```python
from multids.telemetry import Telemetry, TelemetryEvent

def tracer(event: TelemetryEvent) -> None:
    ...  # forward to your tracing backend

conn = S3Connector(telemetry=Telemetry(tracer=tracer))
```

See [Reliability & Observability](observability.md) for the event schema and redaction guarantees.
