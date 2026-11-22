# multids

Async-first connectors for moving data across S3, OpenSearch, Athena, MySQL, SQL Server, and local files — with a
single, consistent API for streaming, bulk I/O, resumable uploads, and AI enrichment.

Install and run your first connector:

```bash
pip install multids
```

```python
import asyncio
from multids.connectors.local import LocalConnector


async def main():
    local = LocalConnector()
    await local.write_json({"message": "Hello", "lang": "你好"}, "data.json")
    print(await local.read_json("data.json"))


asyncio.run(main())
```

## Start here

- [Quickstart](quickstart.md) — install, then S3, OpenSearch, SQL, sync usage, and error handling
- [Usage](usage.md) — patterns for every backend, including multipart resume, pagination, and AI enrichment

## Guides

- [Data workflows](workflows.md) — copy and transform data between connectors without buffering
- [Reliability & Observability](observability.md) — retries, timeouts, telemetry, and the error hierarchy
- [API reference](api.md) — method signatures, errors, contracts, and the sync connector table
- [Support & Compatibility](support.md) — Python versions, extras, CI coverage, and benchmark baselines

## Design notes

- [Retry Safety & Idempotency](phase2-contract.md) — which failures are retryable and which writes are idempotent
- [Hardening](phase5-hardening.md) — type checking, packaging, and release controls
