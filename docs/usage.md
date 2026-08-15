# Usage Examples

This document contains more detailed usage examples and patterns for the common connectors in `multids`.

1) S3 multipart upload with resume and progress

```py
import asyncio

from multids.connectors.s3 import S3Connector


async def run():
    s3 = S3Connector(region_name="eu-central-1")

    # Example stream (could be an aiofiles stream, generator, or bytes)
    async def gen_bytes():
        for i in range(10):
            yield (b"x" * 1024 * 1024)  # 1MB chunks

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

The connector also handles S3 pagination for you when listing objects:

```py
async for key in s3.list_keys("my-bucket", prefix="exports/"):
    print(key)

await s3.delete_object("my-bucket", "exports/old-file.json")
```

OpenSearch calls raise stable exceptions from `multids.errors`. Handle
`ConnectorConnectionError` for exhausted transient retries,
`ConnectorDependencyError` for a missing optional package, and
`ConnectorDataError` when a bulk backend reports rejected data. OpenSearch uses exponential backoff by default; tune it
with `max_retries`,
`backoff_factor`, and `max_backoff` on `OpenSearchConnector`.

2) Streaming search results from OpenSearch

```py
import asyncio
from multids.connectors.opensearch import OpenSearchConnector


async def stream_search():
    oc = OpenSearchConnector("http://localhost:9200")
    # simple scroll helper; `search` returns raw ES response
    res = await oc.search("my-index", {"query": {"match_all": {}}}, size=100)
    # use `scroll` to iterate if you need many hits
    async for hit in oc.scroll("my-index", {"query": {"match_all": {}}}):
        print(hit)
    await oc.close()


asyncio.run(stream_search())
```

3) MySQL bulk insert

```py
import asyncio
from multids.connectors.sql import MySQLConnector


async def bulk_insert():
    mysql = MySQLConnector("mysql+asyncmy://user:pass@localhost:3306/db")
    rows = [{"id": i, "name": f"name-{i}"} for i in range(1000)]
    await mysql.bulk_insert("users", rows, chunk_size=200)
    await mysql.close()


asyncio.run(bulk_insert())
```

4) Using an OpenAI-compatible client for enrichment

```py
from openai import AsyncOpenAI
from multids.ai import OpenAIClient
from multids.connectors.opensearch import OpenSearchConnector

ai = OpenAIClient(AsyncOpenAI())
oc = OpenSearchConnector("http://localhost:9200")


# Fetch docs, enrich text, and re-index.
async def enrich_and_index():
    docs = [{"id": "1", "text": "OpenSearch + AI"}]
    for d in docs:
        d["summary"] = (await ai.generate(d["text"], model="gpt-4o-mini"))["content"]
    await oc.bulk_index("my-index", docs)
    await oc.close()

```

5) Athena query streaming

```py
import asyncio
from multids.connectors.athena import AthenaConnector


async def run_query():
    ac = AthenaConnector(region_name="eu-central-1", workgroup=None)
    async for row in ac.query_stream("SELECT * FROM sample_table", database="default"):
        print(row)


asyncio.run(run_query())
```
