# Copilot Instructions for multids

## Project Overview

**multids** is an async multi-data-source connector library (Python 3.11+) providing pluggable I/O primitives for S3,
OpenSearch, Athena, MySQL, SQL Server, and local files. It emphasizes:

- **Async-first design** with streaming reads/writes
- **Protocol-based architecture** for extensibility
- **Lazy imports** for optional dependencies (AI, SQL Server)
- **S3 multipart uploads with resumability** via checkpoint files

## Architecture & Key Components

### Core Interfaces (`src/multids/interfaces.py`)

The library uses Protocol-based interfaces (not inheritance):

- **`Readable`**: Methods `read_stream()` → `AsyncByteIterator`, `read_bytes()` → bytes
- **`Writable`**: Methods `write_stream(stream, ...)`, `write_bytes(data, ...)`
- **`Connector`**: Base protocol with required `close()` async method and optional `ping()`

Implementations combine these (e.g., `S3Connector(Connector, Readable, Writable)`).

### Connector Implementations (`src/multids/connectors/`)

Each connector is in its own module and must implement `close()`:

- **`S3Connector`**: Multipart uploads with configurable `part_size` (8 MiB default), `min_multipart_upload_size` (5 MiB
  default), checkpoint-based resume via `checkpoint_path` parameter
- **`LocalConnector`**: File I/O using `aiofiles` with 64 KiB chunk size
- **`SQLConnectorBase`** / **`MySQLConnector`** / **`MSSQLConnector`**: Bulk operations via SQLAlchemy 2.0
- **`OpenSearchConnector`**: Bulk indexing, scroll-based search, retry logic
- **`AthenaConnector`**: Query execution via AWS Athena

**Import pattern**: Avoid importing at package level. Import specific modules directly (e.g.,
`from multids.connectors.s3 import S3Connector`) to prevent loading heavy optional deps (httpx for OpenSearch, aioodbc
for SQL Server).

### AI Hooks (`src/multids/ai.py`)

Defines `AIClientInterface` protocol with async `generate(prompt, **kwargs)` method. Implementations like `OpenAIClient`
wrap provider-specific clients (requires `openai` extra: `pip install -e .[ai]`).

## Developer Workflows

### Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .[ai]
```

### Testing

```powershell
pytest                          # Run all tests
pytest -m integration           # Run only integration tests (require external services)
pytest tests/test_s3_*.py       # Run specific test file
```

**Test structure**: Unit tests mock clients (see `DummyClient` in `test_s3_multipart.py`). Integration tests in
`tests/integration/` require live services. Markers: `@pytest.mark.integration`.

### Code Quality

- **Formatter**: Black (88 char line length)
- **Linter**: Ruff (E, F, W, C90, N rules)
- **Import sorter**: isort
- Run via pre-commit or manually: `black .`, `ruff check .`, `isort .`

### Examples

Located in `examples/`: Run directly with `python examples/s3_multipart_example.py`. Use these to validate connector
behavior and as integration test references.

## Project-Specific Patterns

### Async Generators for Streaming

All read methods yield `AsyncIterator[bytes]`. Prefer this over reading entire payloads into memory:

```python
async def read_stream(self, ...):
    async with resource:
        while True:
            chunk = await get_chunk()
            if not chunk:
                break
            yield chunk
```

### Lazy Import of Heavy Dependencies

Imports like `aioboto3`, `httpx`, `aioodbc` are only loaded when connectors are instantiated, not at module import:

```python
def __init__(self):
    try:
        import aioboto3 as _aioboto3
        self._session = _aioboto3.Session()
    except Exception:
        self._session = None
```

Raise `RuntimeError` at method call time if dependency is missing.

### S3 Checkpoint Format (JSON)

For resumable multipart uploads, checkpoints record:

```json
{
  "bucket": "...",
  "key": "...",
  "upload_id": "...",
  "parts": [
    {
      "PartNumber": 1,
      "ETag": "...",
      "size": 5242880
    }
  ]
}
```

File is removed on successful completion.

### Configuration Parameters

Connectors use sensible defaults. Common patterns:

- **`part_size`**: Defaults to 8 MiB; used for chunked reads and multipart uploads
- **`max_concurrency`**: Limits concurrent operations (e.g., S3 part uploads)
- **`min_multipart_upload_size`**: Threshold before switching from PUT to multipart (5 MiB)

## Documentation & Examples

- **`README.md`**: Overview, S3 resume semantics, checkpoint format
- **`docs/quickstart.md`**: Installation, basic examples
- **`docs/usage.md`**: Advanced patterns (S3 resume, OpenSearch scroll, MySQL bulk insert)
- **`examples/`**: Runnable demos for each connector

Docs are markdown-based; view with any renderer or `python -m http.server`.

## Common Tasks

### Adding a New Connector

1. Create `src/multids/connectors/newconnector.py`
2. Implement `Connector` protocol (must have `async close()`)
3. Optionally implement `Readable` and/or `Writable` protocols
4. Add to `__all__` in `src/multids/connectors/__init__.py`
5. Create unit tests in `tests/test_newconnector.py` (mock external client)
6. Optional: Add integration test in `tests/integration/`

### Extending S3 Connector Features

S3 code is in `src/multids/connectors/s3.py` (~296 lines). Key features to preserve:

- Multipart upload concurrency via `asyncio.gather()`
- Checkpoint-based resume logic
- Spill-to-disk for large parts (see `spill_to_disk_threshold`)
- Progress callbacks at part and overall level

### Debugging Tests

- Use `pytest -vv` for verbose output
- Mock clients (like `DummyClient`) allow offline testing
- Integration tests require live services; skip with `-m "not integration"`

## Key Files Reference

| File                                   | Purpose                                              |
|----------------------------------------|------------------------------------------------------|
| `src/multids/interfaces.py`            | Protocol definitions (Readable, Writable, Connector) |
| `src/multids/connectors/s3.py`         | S3 with multipart and resume (most complex)          |
| `src/multids/connectors/sql.py`        | SQLAlchemy-based SQL connectors                      |
| `src/multids/connectors/opensearch.py` | OpenSearch with scroll and bulk                      |
| `src/multids/ai.py`                    | AI client interface skeleton                         |
| `examples/basic_usage.py`              | Minimal async connector example                      |
| `tests/test_interfaces.py`             | Basic interface imports test                         |
| `pytest.ini`                           | Test markers (integration) and config                |
| `pyproject.toml`                       | Poetry config, extras (ai, sqlserver, opensearch)    |
