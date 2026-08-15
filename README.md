# multids

[![Docs Status](https://github.com/aljasem-tech/multids/actions/workflows/docs.yml/badge.svg)](https://aljasem-tech.github.io/multids/)
[![CI](https://github.com/aljasem-tech/multids/actions/workflows/ci.yml/badge.svg)](https://github.com/aljasem-tech/multids/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/multids)](https://pypi.org/project/multids/)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/multids?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/multids)
[![GitHub](https://img.shields.io/github/stars/aljasem-tech/multids?style=social)](https://github.com/aljasem-tech/multids)

Async multi–data-source connectors for Python.
`multids` is an async-first library for moving data across S3, OpenSearch, Athena, MySQL, SQL Server, and local files.
It combines protocol-based abstractions, sync adapters, streaming and bulk read/write helpers, resumable multipart uploads,
and optional AI hooks into a composable toolkit for practical data workflows.

---

## Why multids

- Build async pipelines around a consistent connector interface.
- Use sync adapters when a blocking workflow is easier to integrate.
- Move data with streaming and bulk-style operations.
- Add AI-driven processing with lightweight hooks for enrichment or transformation.
- Start from examples and tests that cover common integration patterns.

## Features

- Async connectors for local files, AWS S3, OpenSearch / Elasticsearch, AWS Athena, MySQL, and SQL Server.
- Protocol-based abstractions for readable, writable, and connector behaviors.
- Sync adapters for local files and S3 workflows under `multids.connectors.sync`.
- Unified read/write APIs for streaming and bulk operations.
- JSON helpers with proper Unicode handling (no escaped `你好` / `مرحبا`).
- Resumable multipart uploads for S3 with checkpoint support.
- Optional AI hooks built around an OpenAI-compatible interface.
- Integration tests and example scripts for common workflows.

See the full documentation at: https://aljasem-tech.github.io/multids/

---

## Installation

Install the core library:

```powershell
pip install multids
```

Optional extras:

```powershell
# AI features (OpenAI client, etc.)

pip install 'multids[ai]'

# S3 and local file helpers

pip install 'multids[s3]'

# OpenSearch connector

pip install 'multids[opensearch]'

# SQL Server connector (requires system ODBC driver)

pip install 'multids[sqlserver]'

# Everything commonly used together

pip install 'multids[ai,s3,opensearch,sqlserver]'

```

> Note: For SQL Server, you must have a SQL Server ODBC driver installed on your system (for example, “Microsoft ODBC
> Driver for SQL Server” on Linux/Windows).

If you prefer `poetry`:

```powershell
poetry add multids
poetry add multids -E ai -E s3 -E opensearch -E sqlserver
```

---

## Quick start

### Basic usage

```python

import asyncio
from multids.connectors.local import LocalConnector


async def main():
    local = LocalConnector()


data = {"message": "Hello", "lang": "你好"}

# Write JSON with unescaped Unicode
await local.write_json(data, "path/to/data.json")

# Read JSON back
result = await local.read_json("path/to/data.json")
print(result)

asyncio.run(main())

```

More examples are available under `examples/` in the repository.

---

## JSON helpers

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

---

## S3 uploads: multipart & resumable

`S3Connector` supports both single PUT and multipart uploads, plus optional resumable uploads via checkpoints.

Key options (constructor / write-time):

- `min_multipart_upload_size` (default: 5 MiB)
  Threshold for switching from a single `put_object` to multipart upload.
- `part_size` (default: 8 MiB)
  Size of each multipart part.
- `enforce_min_part_size` (default: `False`)
  When `True`, enforces AWS’s minimum 5 MiB part size.
- `force_multipart` (write-time flag, default `False`)
  Force multipart even for small objects (for resumable semantics).

Example: force multipart for a small stream to enable checkpointing:

```python

async def small_stream():
    for _ in range(100):
        yield b"{" + b" " * 1024 + b"}\n"


await s3.write_stream(
    small_stream(),
    bucket="my-bucket",
    key="my-object.json",
    force_multipart=True,
)

```

### Resumable uploads with checkpoints

If you pass a `checkpoint_path`, the connector will store a small JSON file describing the multipart upload. On restart
you can resume:

```python

from multids.connectors.s3 import S3Connector


async def upload_with_resume():
    conn = S3Connector(part_size=5 * 1024 * 1024)


chk = "/tmp/uploads/my-object.chk"


async def small_stream():
    for _ in range(100):
        yield b"{" + b" " * 1024 + b"}\n"


# First attempt: start upload and write checkpoint
await conn.write_stream(
    small_stream(),
    bucket="my-bucket",
    key="my-object.json",
    checkpoint_path=chk,
    force_multipart=True,
)

# If interrupted, rerun with resume=True
await conn.write_stream(
    small_stream(),
    bucket="my-bucket",
    key="my-object.json",
    checkpoint_path=chk,
    resume=True,
)

await conn.close()

```

The checkpoint file stores `bucket`, `key`, `upload_id`, and completed parts.

---

## SQL Server connector

`MSSQLConnector` provides an async SQL Server connector built on `aioodbc` (ODBC).

System requirements:

- A SQL Server ODBC driver installed on your host (for example, “ODBC Driver 18 for SQL Server”).

Install the extras:

```powershell
pip install 'multids[sqlserver]'
```

Example:

```python

from multids.connectors.mssql import MSSQLConnector


async def example():
    conn = MSSQLConnector()


# Configure connection string/DSN via env or arguments, then:
await conn.connect_pool()
rows = await conn.fetch_rows("SELECT id, name FROM users")
await conn.close()

```

---

## OpenSearch connector

`OpenSearchConnector` is an async, `httpx`-based helper for indexing and querying OpenSearch/Elasticsearch.

Install:

```powershell
pip install 'multids[opensearch]'
```

Example:

```python

from multids.connectors.opensearch import OpenSearchConnector

oc = OpenSearchConnector("http://localhost:9200")

# Index a document

await oc.index_doc("my-index", {"name": "alice"})

# Bulk index

docs = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
await oc.bulk_index("my-index", docs, chunk_size=100)

# Search

res = await oc.search("my-index", {"query": {"match_all": {}}}, size=10)

# Scroll over results

async for hit in oc.scroll("my-index", {"query": {"match_all": {}}}):
    print(hit)

await oc.close()

```

Authentication examples:

```python

# API key

oc = OpenSearchConnector("https://es.example.com", api_key="BASE64_API_KEY")

# Basic auth

oc = OpenSearchConnector("https://es.example.com", basic_auth=("user", "pass"))

```

---

## Development and contributing

Development setup, virtualenv/Poetry instructions, and contribution guidelines are documented in:

- `CONTRIBUTING.md`
- `docs/` (developer guide)

Contributions, bug reports, and feature requests are welcome via GitHub issues and pull requests.

```
