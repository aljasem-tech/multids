# multids Quickstart

This quickstart shows how to install the package with common extras and run simple operations with the connectors.

Install with extras you need (example: OpenSearch + SQL Server + S3):

```bash
pip install "multids[opensearch,sqlserver,s3]"
```

Basic usage examples:

- Read a local file:

```py
import asyncio
from multids.connectors.local import LocalConnector


async def main():
    lc = LocalConnector()
    async for chunk in lc.read_stream("./data/example.txt"):
        print(chunk)


asyncio.run(main())
```

- Upload bytes to S3 (single PUT or multipart depending on size):

```py
import asyncio
from multids.connectors.s3 import S3Connector


async def upload():
    s3 = S3Connector(region_name="eu-central-1")
    await s3.write_bytes(b"small payload", "my-bucket", "path/to/key")
    await s3.close()


asyncio.run(upload())
```

- Bulk index documents into OpenSearch:

```py
import asyncio
from multids.connectors.opensearch import OpenSearchConnector


async def index():
    oc = OpenSearchConnector("http://localhost:9200")
    docs = [{"id": "1", "title": "hello"}, {"id": "2", "title": "world"}]
    await oc.bulk_index("my-index", docs)
    await oc.close()


asyncio.run(index())
```

See the `docs/usage.md` for more advanced examples (S3 multipart resume, streaming reads, SQL bulk inserts).
