# multids

[![Docs](https://github.com/aljasem-tech/multids/actions/workflows/docs.yml/badge.svg)](https://aljasem-tech.github.io/multids/)
[![CI](https://github.com/aljasem-tech/multids/actions/workflows/ci.yml/badge.svg)](https://github.com/aljasem-tech/multids/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/multids)](https://pypi.org/project/multids/)
[![Python](https://img.shields.io/pypi/pyversions/multids)](https://pypi.org/project/multids/)
[![License](https://img.shields.io/github/license/aljasem-tech/multids)](https://github.com/aljasem-tech/multids/blob/main/LICENSE)

Async-first connectors for moving data across S3, OpenSearch, Athena, MySQL, SQL Server, and local files — with a
single, consistent API for streaming, bulk I/O, resumable uploads, and AI enrichment.

`multids` gives you one interface for many backends. Write a pipeline once, then point it at local files for testing
and at S3 or a database for production. Every connector supports both async and sync usage, typed errors, automatic
retries, and structured telemetry.

Full documentation: **https://aljasem-tech.github.io/multids/**

---

## Connectors at a glance

| Backend      | Async | Sync | Install         | Good for                                  |
|--------------|:-----:|:----:|-----------------|-------------------------------------------|
| Local files  |  ✅   |  ✅  | `multids[local]`     | Local disk, temp workspaces, testing      |
| Amazon S3    |  ✅   |  ✅  | `multids[s3]`        | Object storage, data lakes, multipart     |
| OpenSearch   |  ✅   |  —   | `multids[opensearch]`| Search, indexing, analytics               |
| Athena       |  ✅   |  ✅  | `multids[s3]`        | Querying S3 data with SQL (read-only)     |
| MySQL        |  ✅   |  ✅  | `multids[mysql]`     | Relational data via SQLAlchemy            |
| PostgreSQL   |  ✅   |  ✅  | `multids[postgres]`  | Relational data via asyncpg (COPY inserts) |
| SQL Server   |  ✅   |  ✅  | `multids[sqlserver]` | Relational data via ODBC                  |

Every connector implements the same core operations — `read_stream`, `read_bytes`, `write_stream`, `write_bytes`,
`read_json`, `write_json` — plus backend-specific methods like `bulk_insert` (SQL) and `bulk_index` (OpenSearch).

---

## Installation

```bash
pip install multids
```

Install only the backends you need:

```bash
pip install 'multids[s3]'               # S3 + local files
pip install 'multids[opensearch]'       # OpenSearch / Elasticsearch
pip install 'multids[mysql]'            # MySQL (asyncmy + SQLAlchemy)
pip install 'multids[postgres]'         # PostgreSQL (asyncpg + psycopg2)
pip install 'multids[sqlserver]'        # SQL Server (aioodbc + pyodbc)
pip install 'multids[local]'            # Local files (aiofiles)
pip install 'multids[ai]'               # OpenAI-compatible AI hooks

# Or install everything:
pip install 'multids[all]'
```

> **SQL Server note:** the `sqlserver` extra requires an ODBC driver on your system (e.g. *ODBC Driver 18 for SQL
> Server*). It is not available on Python 3.12+ because `pyodbc` does not publish wheels for those versions.

---

## Quick start

### Async: write and read JSON locally

```python
import asyncio
from multids.connectors.local import LocalConnector


async def main():
    local = LocalConnector()

    data = {"message": "Hello", "lang": "你好"}

    # Write JSON with unescaped Unicode
    await local.write_json(data, "path/to/data.json")

    # Read it back
    result = await local.read_json("path/to/data.json")
    print(result)


asyncio.run(main())
```

### Async: upload to S3

```python
import asyncio
from multids.connectors.s3 import S3Connector


async def main():
    s3 = S3Connector(region_name="eu-central-1")

    # Small payloads use a single PUT automatically
    await s3.write_bytes(b"hello world", "my-bucket", "greeting.txt")

    # Large streams use multipart upload automatically
    async def gen():
        for _ in range(100):
            yield b"x" * (1024 * 1024)  # 1 MiB chunks

    await s3.write_stream(gen(), "my-bucket", "large-file.bin")

    # Read back
    data = await s3.read_bytes("my-bucket", "greeting.txt")
    print(data)

    await s3.close()


asyncio.run(main())
```

### Sync: same API, blocking calls

Prefer synchronous code? Every core connector has a sync twin under `multids.connectors.sync`:

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

### Error handling

All connectors raise stable, typed errors from `multids.errors` — you never need to import a backend SDK's exception:

```python
import asyncio
from multids.errors import ConnectorConnectionError, ConnectorOperationError
from multids.connectors.s3 import S3Connector


async def main():
    s3 = S3Connector()
    try:
        await s3.read_bytes("my-bucket", "missing-key")
    except ConnectorOperationError:
        print("Backend rejected the request (e.g. 404, invalid input)")
    except ConnectorConnectionError:
        print("Backend unreachable after retries — check network/credentials")
    finally:
        await s3.close()


asyncio.run(main())
```

| Error                        | Meaning                                                         |
|------------------------------|-----------------------------------------------------------------|
| `ConnectorError`             | Base class for all connector errors                            |
| `ConnectorDependencyError`   | An optional dependency (driver) is not installed               |
| `ConnectorConnectionError`   | Backend unreachable or timed out after retries (safe to retry) |
| `ConnectorOperationError`    | Backend rejected a valid request                               |
| `ConnectorSchemaError`       | Request/result shape is wrong (subclass of `ConnectorOperationError`) |
| `ConnectorDataError`         | A row/document was rejected (subclass of `ConnectorOperationError`) |

---

## S3: multipart and resumable uploads

`S3Connector` automatically switches from a single `put_object` to a multipart upload when the payload exceeds
`min_multipart_upload_size` (default 5 MiB). You can also force multipart for checkpoint-based resume.

```python
import asyncio
from multids.connectors.s3 import S3Connector


async def main():
    s3 = S3Connector(part_size=5 * 1024 * 1024)

    async def stream():
        for _ in range(100):
            yield b"{" + b" " * 1024 + b"}\n"

    checkpoint = "/tmp/uploads/my-object.chk"

    # First attempt: start upload and write a checkpoint
    await s3.write_stream(
        stream(),
        "my-bucket",
        "my-object.json",
        checkpoint_path=checkpoint,
        force_multipart=True,
    )

    # If interrupted, rerun with resume=True — committed parts are not re-uploaded
    await s3.write_stream(
        stream(),
        "my-bucket",
        "my-object.json",
        checkpoint_path=checkpoint,
        resume=True,
    )

    await s3.close()


asyncio.run(main())
```

The checkpoint file stores the `bucket`, `key`, `upload_id`, and completed parts so a retry continues from the last
committed part instead of restarting from byte 0.

---

## SQL connectors (MySQL, SQL Server)

SQL connectors provide `execute`, `fetch_rows`, `bulk_insert`, and record-streaming helpers. They use SQLAlchemy
under the hood and create their engine lazily on first use.

```python
import asyncio
from multids.connectors.sql import MySQLConnector


async def main():
    mysql = MySQLConnector("mysql+asyncmy://user:pass@localhost:3306/db")

    rows = [{"id": i, "name": f"name-{i}"} for i in range(1000)]
    result = await mysql.bulk_insert("users", rows, chunk_size=200)
    print(f"Inserted {result.items_written} rows")

    fetched = await mysql.fetch_rows("SELECT id, name FROM users LIMIT 10")
    for row in fetched:
        print(row)

    await mysql.close()


asyncio.run(main())
```

Sync equivalent:

```python
from multids.connectors.sync.sql import SyncMySQLConnector

mysql = SyncMySQLConnector("mysql+pymysql://user:pass@localhost:3306/db")
mysql.bulk_insert("users", [{"id": 1, "name": "alice"}], chunk_size=100)
rows = mysql.fetch_rows("SELECT id, name FROM users")
mysql.close()
```

For SQL Server, use `MSSQLConnector` (async) or `SyncMSSQLConnector` (sync) with an ODBC connection string:

```python
import asyncio
from multids.connectors.mssql import MSSQLConnector


async def main():
    conn = MSSQLConnector(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=master;UID=sa;PWD=YourPassword;TrustServerCertificate=yes"
    )
    await conn.connect_pool()
    rows = await conn.fetch_rows("SELECT id, name FROM users")
    await conn.close()


asyncio.run(main())
```

### PostgreSQL

`PostgreSQLConnector` uses `asyncpg` directly and supports native COPY-protocol bulk inserts for high-throughput
ingestion:

```python
import asyncio
from multids.connectors.postgres import PostgreSQLConnector


async def main():
    pg = PostgreSQLConnector("postgresql://user:pass@localhost:5432/db")

    # Bulk insert via COPY (fastest path)
    rows = [{"id": i, "name": f"name-{i}"} for i in range(10000)]
    result = await pg.bulk_insert("users", rows)
    print(f"Inserted {result.items_written} rows via COPY")

    # Streaming reads with $1-style parameters
    async for row in pg.fetch_rows("SELECT * FROM users WHERE id > $1", 100):
        print(row)

    await pg.close()


asyncio.run(main())
```

Sync equivalent:

```python
from multids.connectors.sync.postgres import SyncPostgreSQLConnector

pg = SyncPostgreSQLConnector("postgresql://user:pass@localhost:5432/db")
pg.bulk_insert("users", [{"id": 1, "name": "alice"}])
rows = list(pg.fetch_rows("SELECT * FROM users WHERE id > %s", (0,)))
pg.close()
```

---

## OpenSearch

`OpenSearchConnector` is an async, `httpx`-based client for indexing, searching, and scrolling OpenSearch /
Elasticsearch.

```python
import asyncio
from multids.connectors.opensearch import OpenSearchConnector


async def main():
    oc = OpenSearchConnector("http://localhost:9200")

    # Index a single document
    await oc.index_doc("my-index", {"name": "alice"})

    # Bulk index
    docs = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    await oc.bulk_index("my-index", docs, chunk_size=100)

    # Search
    result = await oc.search("my-index", {"query": {"match_all": {}}}, size=10)

    # Scroll over large result sets
    async for hit in oc.scroll("my-index", {"query": {"match_all": {}}}):
        print(hit)

    await oc.close()


asyncio.run(main())
```

Authentication:

```python
from multids.connectors.opensearch import OpenSearchConnector

# API key
oc = OpenSearchConnector("https://es.example.com", api_key="BASE64_API_KEY")

# Basic auth
oc = OpenSearchConnector("https://es.example.com", basic_auth=("user", "pass"))
```

---

## AI enrichment

`multids.ai` provides a pluggable text-generation interface that works with any OpenAI-compatible client. Use it to
enrich records before indexing them:

```python
import asyncio
from openai import AsyncOpenAI
from multids.ai import OpenAIClient
from multids.connectors.opensearch import OpenSearchConnector


async def main():
    ai = OpenAIClient(AsyncOpenAI())
    oc = OpenSearchConnector("http://localhost:9200")

    docs = [{"id": "1", "text": "OpenSearch + AI"}]
    for doc in docs:
        response = await ai.generate(doc["text"], model="gpt-4o-mini")
        doc["summary"] = response["content"]

    await oc.bulk_index("my-index", docs)
    await oc.close()


asyncio.run(main())
```

---

## Data workflows

`multids.workflows` connects two connectors without buffering the whole payload in memory:

```python
import asyncio
from multids.connectors.local import LocalConnector
from multids.connectors.s3 import S3Connector
from multids.workflows import copy_stream


async def main():
    local = LocalConnector(base_path="./exports")
    s3 = S3Connector(region_name="eu-central-1")

    result = await copy_stream(
        local,
        s3,
        source_args=("daily/events.jsonl",),
        destination_args=("my-bucket", "exports/events.jsonl"),
    )
    print(f"Copied {result.bytes_copied} bytes in {result.chunks_copied} chunks")

    await local.close()
    await s3.close()


asyncio.run(main())
```

See [docs/workflows.md](docs/workflows.md) for record transforms, batching, and checkpoints.

---

## CLI

A built-in CLI handles safe local-file operations:

```bash
multids ping --path ./data
multids copy ./data/input.jsonl ./out/input.jsonl
multids validate workflow.json
```

---

## Documentation

| Guide              | Description                                              |
|--------------------|----------------------------------------------------------|
| [Quickstart](docs/quickstart.md)           | Install and run your first connector             |
| [Usage](docs/usage.md)                     | Patterns for every backend, sync and async        |
| [API reference](docs/api.md)               | Method signatures, errors, contracts              |
| [Data workflows](docs/workflows.md)        | Copy, transform, and batch between connectors     |
| [Reliability](docs/observability.md)       | Retries, timeouts, telemetry, error handling      |
| [Support matrix](docs/support.md)          | Python versions, extras, CI coverage              |

---

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing, and the pull
request process.

## License

MIT — see [LICENSE](LICENSE).
