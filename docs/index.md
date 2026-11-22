# multids

Async-first connectors for moving data across S3, OpenSearch, Athena, MySQL, SQL Server, and local files — with a
single, consistent API for streaming, bulk I/O, resumable uploads, and AI enrichment.

`multids` gives you one interface for many backends. Write a pipeline once, then point it at local files for testing
and at S3 or a database for production. Every connector supports both async and sync usage, typed errors, automatic
retries, and structured telemetry.

## Connectors at a glance

| Backend      | Async | Sync | Install                | Good for                                  |
|--------------|:-----:|:----:|------------------------|-------------------------------------------|
| Local files  |  ✅   |  ✅  | `multids[local]`       | Local disk, temp workspaces, testing      |
| Amazon S3    |  ✅   |  ✅  | `multids[s3]`          | Object storage, data lakes, multipart     |
| OpenSearch   |  ✅   |  —   | `multids[opensearch]`  | Search, indexing, analytics               |
| Athena       |  ✅   |  ✅  | `multids[athena]`      | Querying S3 data with SQL (read-only)     |
| MySQL        |  ✅   |  ✅  | `multids[mysql]`       | Relational data via SQLAlchemy            |
| SQL Server   |  ✅   |  ✅  | `multids[sqlserver]`   | Relational data via ODBC                  |

## Install

```bash
pip install multids

# Pick the backends you need:
pip install 'multids[s3]'
pip install 'multids[opensearch]'
pip install 'multids[mysql]'
pip install 'multids[all]'      # everything
```

## First steps

```python
import asyncio
from multids.connectors.local import LocalConnector


async def main():
    local = LocalConnector()
    await local.write_json({"message": "Hello", "lang": "你好"}, "data.json")
    print(await local.read_json("data.json"))


asyncio.run(main())
```

See the [Quickstart](quickstart.md) for S3, OpenSearch, SQL, sync usage, and error handling.

## Where to go next

- [Quickstart](quickstart.md) — install and run your first connector
- [Usage](usage.md) — patterns for every backend, sync and async
- [API reference](api.md) — method signatures, errors, contracts
- [Data workflows](workflows.md) — copy, transform, and batch between connectors
- [Reliability](observability.md) — retries, timeouts, telemetry, error handling
- [Support matrix](support.md) — Python versions, extras, CI coverage
