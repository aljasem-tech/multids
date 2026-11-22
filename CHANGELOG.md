# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **PostgreSQL connector** (async + sync): `PostgreSQLConnector` (asyncpg) and `SyncPostgreSQLConnector` (psycopg2)
  with standard record operations (`fetch_rows`, `execute`, `bulk_insert`, `write_records`) plus native COPY-protocol
  bulk inserts via `copy_records_to_table` for high-throughput ingestion. Supports transactions, rollback, and
  schema-qualified tables. Install with `pip install multids[postgres]`.
- `map_postgres_error` in `multids.error_mapping` for normalizing asyncpg/psycopg2 exceptions.
- **Sync OpenSearch connector:** `SyncOpenSearchConnector` (`multids.connectors.sync.opensearch`) completes the sync
  matrix. Mirrors the async `OpenSearchConnector` API — `index_doc`, `bulk_index`, `search`, `scroll`, `write_records`,
  `ping` — using a synchronous `httpx.Client`, with retry/timeout parity and API-key / basic-auth support.
- CI: PostgreSQL integration tests against a `postgres:16` container on Python 3.11–3.13.
- CI: `postgres` extra added to the packaging job matrix.

## [1.0.0rc1] - 2026-11-22

First release candidate for 1.0.0. All Phase 1–5 exit criteria pass in CI:
contract conformance, retry safety, observability parity, real-service
validation, and packaging/hardening. This RC is published for feedback from
real workloads; only correctness, security, packaging, documentation, and
performance-regression fixes will be accepted during the stabilization window.

### Added

- **Retry safety and idempotency (Phase 2):** `RetryConfig` with per-operation timeouts, `retry_async`/`retry_sync`,
  and `run_operation`/`run_operation_sync` helpers. Non-idempotent writes (`bulk_insert`, `write_records`) are bounded
  by a timeout but never retried, preventing duplicate rows. S3 multipart abort/resume and workflow `checkpoint`
  callbacks let callers persist resume offsets.
- **Observability (Phase 3):** shared telemetry with a closed event schema (`start`/`completion`/`retry`/`failure`),
  backend-exception mapping to `multids.errors` (`map_sqlalchemy_error`, `map_botocore_error`, `map_pyodbc_error`),
  and optional `tracer`/`metrics` callbacks. Telemetry carries no payloads, SQL, or credentials.
- **Real-service validation (Phase 4):** opt-in CI suites for S3 (MinIO), MySQL, SQL Server, OpenSearch, and Athena,
  covering auth, pagination, large streams, Unicode, cleanup, and failure recovery. Reproducible benchmarks with
  recorded baselines for SQL batch, S3 transfer, and OpenSearch bulk.
- **Hardening (Phase 5):** mypy configuration and CI type-check, `py.typed` marker, build + twine verification,
  `greenlet` declared as an optional dependency, and `docs/phase5-hardening.md`.
- Contract-conformance tests covering every async/sync connector against the object-storage and record reader/writer
  protocols, including context-manager and idempotent-close behaviour.
- `write_records` and `bulk_insert` on `SQLConnectorBase` and `SyncSQLConnectorBase`, returning real affected-row and
  bulk counts; `execute` / `execute_statement` now return a populated `WriteResult`.

### Changed

- SQL connectors create their SQLAlchemy engine lazily on first use instead of at construction, so optional DBAPI
  drivers are only imported when an operation actually runs.
- Documentation rewritten for public users: accurate runnable examples, sync coverage, error handling, and a complete
  API reference.

### Fixed

- SQL connectors no longer fail to import when only the synchronous SQLAlchemy layer is available: `text` is imported
  independently of the asyncio layer, and a clear `ConnectorDependencyError` is raised on first use if greenlet /
  asyncio support is missing. Added `greenlet` to the `mysql` and `all` extras.
- S3 `complete_multipart_upload` no longer sends the internal `size` field, which botocore rejects.
- S3 stream cleanup handles both sync and async `StreamingBody.close()` across aiobotocore versions.
- Replaced the withdrawn `minio/minio` Docker image with the `pgsty/silo` drop-in fork in CI workflows.

## [0.2.1] - 2026-08-15

### Added

- Typed connector capability contracts and explicit operation result types.
- Data-movement workflows for byte streams and record pipelines.
- A local-file CLI with `ping`, `copy`, and configuration validation commands.
- Optional dependency extras, a support matrix, compatibility policy, opt-in real-service integration checks, and
  benchmark helpers.

### Changed

- Aligned local and S3 object operations, added cursor-pagination helpers, and introduced consistent connector
  lifecycles.
- Added OpenSearch retry/backoff, structured retry logs, and typed errors.
- Made backend dependencies optional and updated documentation examples and CI validation.

## [0.1.0] - 2025-12-06

### Added

- Initial release of `multids`.
- Async connectors for:
    - AWS S3 (multipart upload support, resumable uploads).
    - OpenSearch (using `httpx`).
    - AWS Athena (using `aiobotocore`/`aioboto3`).
    - SQL: MySQL (`asyncmy`), SQL Server (`aioodbc`), Generic SQLAlchemy support.
    - Local filesystem.
- Abstractions for `Connector`, `FileStorage`, `VectorDatabase`.
- Integrations with OpenAI for AI hooks.
- JSON support: `read_json` and `write_json` methods in `LocalConnector` and `S3Connector` with proper Unicode handling.
- Unicode support: `OpenSearchConnector` explicitly preserves non-ASCII characters (e.g., German, Chinese) during
  indexing.
