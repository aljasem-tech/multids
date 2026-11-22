# Quickstart

This guide gets you from `pip install` to your first working connector in a few minutes. Each example is
self-contained and runnable.

## 1. Install

```bash
pip install multids
```

Add the extras for the backends you need:

```bash
pip install 'multids[local]'          # local files (aiofiles)
pip install 'multids[s3]'             # Amazon S3 + local files
pip install 'multids[opensearch]'     # OpenSearch / Elasticsearch
pip install 'multids[mysql]'          # MySQL (asyncmy + SQLAlchemy)
pip install 'multids[sqlserver]'      # SQL Server (requires an ODBC driver)
pip install 'multids[ai]'             # OpenAI-compatible AI hooks
pip install 'multids[all]'            # everything
```

## 2. Your first connector (async)

Every connector exposes the same core methods: `read_bytes`, `write_bytes`, `read_stream`, `write_stream`,
`read_json`, `write_json`.

```python
import asyncio
from multids.connectors.local import LocalConnector


async def main():
    local = LocalConnector()

    # Write JSON (Unicode is preserved, not escaped)
    await local.write_json({"message": "Hello", "lang": "你好"}, "data.json")

    # Read it back
    result = await local.read_json("data.json")
    print(result)


asyncio.run(main())
```

## 3. Upload to S3

S3 uses the same API as local files, plus automatic multipart for large payloads:

```python
import asyncio
from multids.connectors.s3 import S3Connector


async def main():
    s3 = S3Connector(region_name="eu-central-1")

    # Small payloads: single PUT
    await s3.write_bytes(b"hello world", "my-bucket", "greeting.txt")

    # Large streams: multipart upload (automatic above 5 MiB)
    async def gen():
        for _ in range(100):
            yield b"x" * (1024 * 1024)

    await s3.write_stream(gen(), "my-bucket", "large-file.bin")

    # Read back
    print(await s3.read_bytes("my-bucket", "greeting.txt"))

    await s3.close()


asyncio.run(main())
```

## 4. Index documents in OpenSearch

```python
import asyncio
from multids.connectors.opensearch import OpenSearchConnector


async def main():
    oc = OpenSearchConnector("http://localhost:9200")

    docs = [{"id": "1", "title": "hello"}, {"id": "2", "title": "world"}]
    await oc.bulk_index("my-index", docs, chunk_size=100)

    # Scroll over results
    async for hit in oc.scroll("my-index", {"query": {"match_all": {}}}):
        print(hit)

    await oc.close()


asyncio.run(main())
```

## 5. Query MySQL

```python
import asyncio
from multids.connectors.sql import MySQLConnector


async def main():
    mysql = MySQLConnector("mysql+asyncmy://user:pass@localhost:3306/db")

    rows = [{"id": i, "name": f"name-{i}"} for i in range(1000)]
    result = await mysql.bulk_insert("users", rows, chunk_size=200)
    print(f"Inserted {result.items_written} rows")

    await mysql.close()


asyncio.run(main())
```

## 6. Prefer synchronous code?

Every core connector has a sync twin under `multids.connectors.sync` with the same method names:

```python
from multids.connectors.sync.local import SyncLocalConnector
from multids.connectors.sync.s3 import SyncS3Connector

# Local files
local = SyncLocalConnector()
local.write_bytes(b"hello", "out.txt")
print(local.read_bytes("out.txt"))

# S3
s3 = SyncS3Connector(region_name="eu-central-1")
s3.write_bytes(b"hello from sync", "my-bucket", "sync.txt")
print(s3.read_bytes("my-bucket", "sync.txt"))
s3.close()
```

## 7. Handle errors

All connectors raise typed errors from `multids.errors`. You never import a backend SDK's exception classes:

```python
import asyncio
from multids.errors import ConnectorConnectionError, ConnectorOperationError
from multids.connectors.s3 import S3Connector


async def main():
    s3 = S3Connector()
    try:
        await s3.read_bytes("my-bucket", "missing-key")
    except ConnectorOperationError:
        print("Backend rejected the request (e.g. 404)")
    except ConnectorConnectionError:
        print("Backend unreachable after retries")
    finally:
        await s3.close()


asyncio.run(main())
```

See [Reliability & Observability](observability.md) for the full error hierarchy, retry policy, and telemetry.

## Next steps

- [Usage](usage.md) — deeper patterns for every backend, including multipart resume, pagination, and AI enrichment
- [Data workflows](workflows.md) — copy and transform data between connectors
- [API reference](api.md) — full method signatures and contracts
- [Support matrix](support.md) — Python versions, extras, and CI coverage
