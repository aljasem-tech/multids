# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
