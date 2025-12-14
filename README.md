# multids

<!-- Docs status badge: replace OWNER and REPO with your GitHub owner and repository name -->
[![Docs Status](https://github.com/aljasem-tech/multids/actions/workflows/docs.yml/badge.svg)](https://aljasem-tech.github.io/multids/)
[![CI](https://github.com/aljasem-tech/multids/actions/workflows/ci.yml/badge.svg)](https://github.com/aljasem-tech/multids/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/multids)](https://pypi.org/project/multids/)
[![GitHub](https://img.shields.io/github/stars/aljasem-tech/multids?style=social)](https://github.com/aljasem-tech/multids)

Async multi-data-source connectors for S3, OpenSearch, Athena, MySQL, SQL Server and local files. Provides async
read/write primitives and pluggable AI hooks.

Quick start

1. Create a virtualenv and install:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install multids[ai]
```

2. See `examples/basic_usage.py` for a quick demonstration.

JSON Support
------------

`LocalConnector` and `S3Connector` include helper methods for reading and writing JSON with automatic Unicode support (
no escaping of characters like `你好`).

### Local JSON

```python
from multids.connectors.local import LocalConnector

local = LocalConnector()
data = {"message": "Hello", "lang": "你好"}

# Write JSON (un-escaped unicode)
await local.write_json(data, "path/to/data.json")

# Read JSON
result = await local.read_json("path/to/data.json")
print(result)
```

### S3 JSON

```python
from multids.connectors.s3 import S3Connector

s3 = S3Connector(aws_region="eu-central-1")
data = {"status": "ok", "info": "مرحبا"}

# Upload JSON object
await s3.write_json(data, bucket="my-bucket", key="status.json")

# Download and parse JSON
result = await s3.read_json(bucket="my-bucket", key="status.json")
print(result)
```

S3 multipart example

```powershell
python examples/s3_multipart_example.py
```

MySQL bulk insert example

```powershell
python examples/mysql_bulk_example.py
```

S3 upload behavior and options

- `min_multipart_upload_size` (default: 5 MiB): overall object size threshold before the connector switches from a
  single `PUT` to a multipart upload. The connector buffers the incoming stream and only creates a multipart upload when
  the accumulated bytes reach this threshold. This is useful when you're uploading many small JSON files — they will use
  a single `put_object` unless you exceed the threshold.
- `part_size` (default: 8 MiB): size of each multipart part once multipart is created.
- `enforce_min_part_size` (default: False in this library): when enabled, the connector will enforce the AWS minimum
  part size of 5 MiB for `part_size` (prevents creating parts smaller than AWS requires). For typical workloads of many
  small files, the default `False` makes the connector flexible.
- `force_multipart` (write-time flag, default `False`): pass `force_multipart=True` to `S3Connector.write_stream(...)`to
  force multipart upload even if the total size is below `min_multipart_upload_size`. Use this when you need multipart
  semantics (e.g., resumable uploads across process restarts) for small objects.

Examples

- Force multipart for a small object (useful for resuming):

```python
await s3.write_stream(my_small_stream(), bucket="b", key="k", force_multipart=True)
```

- Normal (default) path: small objects use a single `put_object` and large objects use multipart as needed.

Resumable uploads & checkpoints

The S3 connector supports simple checkpointing to make multipart uploads resumable across process restarts. When you
provide a `checkpoint_path` to `write_stream`, the connector writes a small JSON file containing the upload metadata as
parts complete. If a failure or process stop occurs you can call `write_stream(..., resume=True, checkpoint_path=...)`
to attempt a resume.

Checkpoint format (JSON):

```json
{
  "bucket": "my-bucket",
  "key": "path/to/object",
  "upload_id": "<aws-upload-id>",
  "parts": [
    {
      "PartNumber": 1,
      "ETag": "...",
      "size": 5242880
    },
    {
      "PartNumber": 2,
      "ETag": "...",
      "size": 5242880
    }
  ]
}
```

How it works:

- While uploading parts, the connector will save the checkpoint file (if `checkpoint_path` is provided) after each part
  completes. The checkpoint includes the `upload_id` and the parts uploaded so far.
- If an upload is interrupted, re-running `write_stream` with `resume=True` and the same `checkpoint_path` will attempt
  to list existing parts from S3 using the `upload_id` and continue uploading remaining parts. If listing parts fails,
  the connector will fall back to the parts recorded in the checkpoint file.
- On successful completion the checkpoint file is removed.

Resume example (simplified):

```python
async def upload_with_resume():
    conn = S3Connector(part_size=5 * 1024 * 1024)
    chk = "/tmp/uploads/my-object.chk"

    async def small_stream():
        # imagine this yields many small chunks and we want resumable semantics
        for _ in range(100):
            yield b"{" + b' ' * 1024 + b"}\n"

    # First attempt: force multipart so we have a resumable upload
    await conn.write_stream(
        small_stream(),
        bucket="my-bucket",
        key="my-object.json",
        checkpoint_path=chk,
        force_multipart=True,
    )

    # If the process were interrupted before completion, re-run and resume:
    await conn.write_stream(
        small_stream(), bucket="my-bucket", key="my-object.json", checkpoint_path=chk, resume=True
    )

    await conn.close()


asyncio.run(upload_with_resume())
```

SQL Server connector

The project includes an async `MSSQLConnector` implemented with `aioodbc` (ODBC). Notes:

- System requirement: install a SQL Server ODBC driver on your host (e.g. Microsoft ODBC Driver for SQL Server).
- Install the optional `sqlserver` extras to get the Python runtime dependency:

```powershell
pip install multids[sqlserver]
```

Quick example (using a pool):

```python
from multids.connectors.mssql import MSSQLConnector


async def example():
    conn = MSSQLConnector()
    # either set connection options on the connector and call connect_pool(), or
    # configure a DSN and call connect_pool()
    await conn.connect_pool()
    rows = await conn.fetch_rows("SELECT id, name FROM users")
    await conn.close()

```

OpenSearch connector

`OpenSearchConnector` is an async, httpx-based helper for indexing and searching documents in OpenSearch/Elasticsearch.

Examples

  ```python
  from multids.connectors.opensearch import OpenSearchConnector

oc = OpenSearchConnector("http://localhost:9200", api_key="<api-key>")

# Index a single document
await oc.index_doc("my-index", {"name": "alice"})

# Bulk index (sync iterable)
docs = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
await oc.bulk_index("my-index", docs, chunk_size=100)

# Bulk index with id field + routing field
await oc.bulk_index("my-index", docs, chunk_size=100)

# Search / scroll
res = await oc.search("my-index", {"query": {"match_all": {}}}, size=10)
async for hit in oc.scroll("my-index", {"query": {"match_all": {}}}):
    print(hit)

await oc.close()
  ```

Advanced OpenSearch examples

Indexing with explicit IDs and routing:

  ```python
  docs = [
    {"id": "user-1", "name": "alice", "org": "org1"},
    {"id": "user-2", "name": "bob", "org": "org2"},
]
await oc.bulk_index("my-index", docs, chunk_size=100)

# If you want to use a field as _id and also set routing based on another field:
await oc.bulk_index("my-index", docs, chunk_size=100)

# Or use build_bulk_ndjson directly with id_field/routing_field support
async for chunk in OpenSearchConnector.build_bulk_ndjson(docs, index="my-index", id_field="id", routing_field="org"):
    await oc._request("POST", "/_bulk", content=chunk)
  ```

Authentication examples

API key (preferred for service-to-service):

  ```python
  oc = OpenSearchConnector("https://es.example.com", api_key="BASE64_APIKEY")
await oc.index_doc("idx", {"name": "s1"})
  ```

HTTP Basic (username/password):

  ```python
  oc = OpenSearchConnector("https://es.example.com", basic_auth=("user", "pass"))
await oc.search("idx", {"query": {"match_all": {}}})
  ```

Installation note

If you only need the OpenSearch functionality (and want to avoid installing `httpx` by default), install the optional
extra:

  ```powershell
  pip install multids[opensearch]
  ```

Integration tests

We include an optional integration test suite which can run against a real OpenSearch instance. In CI, we start a local
OpenSearch service and run tests under `tests/integration/`. Locally you can run integration tests by setting
`OPENSEARCH_URL` to your instance and running:

  ```pwsh
  $env:OPENSEARCH_URL = 'http://localhost:9200'
  pytest tests/integration -q
  ```
