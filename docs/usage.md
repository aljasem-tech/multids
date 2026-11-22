# Usage

This page covers patterns for each connector in `multids`, in both async and sync form. Every example is
self-contained.

## S3: multipart upload with resume and progress

```python
import asyncio
from multids.connectors.s3 import S3Connector


async def run():
    s3 = S3Connector(region_name="eu-central-1")

    async def gen_bytes():
        for _ in range(10):
            yield b"x" * (1024 * 1024)  # 1 MiB chunks

    def progress(part_number, bytes_uploaded):
        print(f"part={part_number} bytes={bytes_uploaded}")

    await s3.write_stream(
        gen_bytes(),
        "my-bucket",
        "big-file.bin",
        checkpoint_path="./.s3-checkpoint.json",
        progress_callback=progress,
        force_multipart=True,
    )

    await s3.close()


asyncio.run(run())
```

If the upload is interrupted, rerun with `resume=True` and the same `checkpoint_path`. The connector resumes from the
last committed part instead of restarting from byte 0:

```python
import asyncio
from multids.connectors.s3 import S3Connector


async def resume():
    s3 = S3Connector(part_size=5 * 1024 * 1024)

    async def gen_bytes():
        for _ in range(10):
            yield b"x" * (1024 * 1024)

    await s3.write_stream(
        gen_bytes(),
        "my-bucket",
        "big-file.bin",
        checkpoint_path="./.s3-checkpoint.json",
        resume=True,
    )

    await s3.close()


asyncio.run(resume())
```

## S3: pagination and cleanup

```python
import asyncio
from multids.connectors.s3 import S3Connector


async def run():
    s3 = S3Connector()

    # Iterate over every key under a prefix (pagination is handled for you)
    async for key in s3.list_keys("my-bucket", prefix="exports/"):
        print(key)

    await s3.delete_object("my-bucket", "exports/old-file.json")
    await s3.close()


asyncio.run(run())
```

## S3: sync usage

```python
from multids.connectors.sync.s3 import SyncS3Connector

s3 = SyncS3Connector(region_name="eu-central-1")

s3.write_bytes(b"hello", "my-bucket", "greeting.txt")
print(s3.read_bytes("my-bucket", "greeting.txt"))

for key in s3.list_keys("my-bucket", prefix="exports/"):
    print(key)

s3.close()
```

## OpenSearch: streaming search results

```python
import asyncio
from multids.connectors.opensearch import OpenSearchConnector


async def stream_search():
    oc = OpenSearchConnector("http://localhost:9200")

    # search() returns the raw OpenSearch response
    result = await oc.search("my-index", {"query": {"match_all": {}}}, size=100)

    # scroll() iterates over large result sets without loading everything at once
    async for hit in oc.scroll("my-index", {"query": {"match_all": {}}}):
        print(hit)

    await oc.close()


asyncio.run(stream_search())
```

OpenSearch uses exponential backoff by default. Tune it with `max_retries`, `backoff_factor`, and `max_backoff`:

```python
from multids.connectors.opensearch import OpenSearchConnector

oc = OpenSearchConnector(
    "http://localhost:9200",
    max_retries=5,
    backoff_factor=0.5,
    max_backoff=30.0,
)
```

## MySQL: bulk insert and fetch

```python
import asyncio
from multids.connectors.sql import MySQLConnector


async def bulk_insert():
    mysql = MySQLConnector("mysql+asyncmy://user:pass@localhost:3306/db")

    rows = [{"id": i, "name": f"name-{i}"} for i in range(1000)]
    result = await mysql.bulk_insert("users", rows, chunk_size=200)
    print(f"Inserted {result.items_written} rows")

    fetched = await mysql.fetch_rows("SELECT id, name FROM users LIMIT 10")
    for row in fetched:
        print(row)

    await mysql.close()


asyncio.run(bulk_insert())
```

Sync equivalent:

```python
from multids.connectors.sync.sql import SyncMySQLConnector

mysql = SyncMySQLConnector("mysql+pymysql://user:pass@localhost:3306/db")

mysql.bulk_insert("users", [{"id": 1, "name": "alice"}], chunk_size=100)
rows = mysql.fetch_rows("SELECT id, name FROM users")
mysql.close()
```

## SQL Server

SQL Server uses `MSSQLConnector` (async) or `SyncMSSQLConnector` (sync) with an ODBC connection string. An ODBC
driver must be installed on the host.

```python
import asyncio
from multids.connectors.mssql import MSSQLConnector


async def run():
    conn = MSSQLConnector(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=master;"
        "UID=sa;"
        "PWD=YourPassword;"
        "TrustServerCertificate=yes"
    )
    await conn.connect_pool()
    rows = await conn.fetch_rows("SELECT id, name FROM users")
    await conn.close()


asyncio.run(run())
```

## Athena: query streaming

Athena is read-only. Queries stream rows as they become available:

```python
import asyncio
from multids.connectors.athena import AthenaConnector


async def run_query():
    ac = AthenaConnector(region_name="eu-central-1")
    async for row in ac.query_stream("SELECT * FROM sample_table", database="default"):
        print(row)

    await ac.close()


asyncio.run(run_query())
```

## AI enrichment

`multids.ai` wraps any OpenAI-compatible async client. Use it to enrich records before indexing:

```python
import asyncio
from openai import AsyncOpenAI
from multids.ai import OpenAIClient
from multids.connectors.opensearch import OpenSearchConnector


async def enrich_and_index():
    ai = OpenAIClient(AsyncOpenAI())
    oc = OpenSearchConnector("http://localhost:9200")

    docs = [{"id": "1", "text": "OpenSearch + AI"}]
    for doc in docs:
        response = await ai.generate(doc["text"], model="gpt-4o-mini")
        doc["summary"] = response["content"]

    await oc.bulk_index("my-index", docs)
    await oc.close()


asyncio.run(enrich_and_index())
```

## Next steps

- [Data workflows](workflows.md) — copy and transform data between connectors without buffering
- [Reliability & Observability](observability.md) — retries, timeouts, telemetry, error handling
- [API reference](api.md) — full method signatures and contracts
