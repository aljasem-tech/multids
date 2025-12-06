# API Reference

This reference highlights the primary high-level classes and functions you will use from `multids`.

Core interfaces

- `multids.interfaces.Readable` — Protocol describing `read_stream(path_or_key, range_spec=None)`.
- `multids.interfaces.Writable` — Protocol describing `write_stream(path_or_key, source, **kwargs)` and helpers such as
  `write_bytes`.
- `multids.interfaces.Connector` — Convenience protocol that exposes `read_stream`, `write_stream`, and optional
  management methods like `close()`.

Notable connectors

- `multids.connectors.local.LocalConnector`
    - `read_stream(path)` — async iterator yielding bytes
    - `write_stream(path, source)` — accept bytes/async iterator

- `multids.connectors.s3.S3Connector`
    - `read_stream(bucket, key, range_spec=None)` — streaming read from S3
    -
  `write_stream(bucket, key, source, *, checkpoint_path=None, progress_callback=None, force_multipart=False, min_multipart_upload_size=None)` —
  multipart-capable writer with checkpointing
    - Helpers: `write_bytes(bucket, key, data)`

- `multids.connectors.opensearch.OpenSearchConnector`
    - `bulk_index(index, docs, id_field=None, routing_field=None, chunk_size=100)` — accepts sync iterable or async
      generator of documents
    - `search(index, body, size=10, **kwargs)` — raw search call
    - `scroll(index, body, page_size=100)` — async iterator yielding search hits

- `multids.connectors.sql.MySQLConnector`
    - `bulk_insert(table, columns, rows, chunk_size=1000)` — bulk insert helper that shards rows into transactions

AI integration

- `multids.ai.AIClient` — a pluggable wrapper interface. Provide an implementation that supports the methods you need (
  e.g., `generate_embeddings`, `summarize_text`, `complete`). The package provides a small example adapter in
  `src/multids/ai.py`.

Examples for each method appear in `docs/usage.md` and `docs/quickstart.md`.
