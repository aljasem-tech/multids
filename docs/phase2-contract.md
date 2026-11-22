# Phase 2 design: retry safety and idempotency contract

**Status: implemented.** The contract below is realised by `multids.reliability` (timeout + retry), per-connector
retry/timeout wiring, S3 multipart abort/resume, and workflow `checkpoint` callbacks. Deterministic tests live in
`tests/test_phase2_reliability.py`.

This document is the design contract for Phase 2 (failure-safe data movement). It defines, per operation class,
which failures are retryable, which operations are idempotent, and what state a caller can rely on after a failure.
Implementing Phase 2 means making the connectors and workflows honour this contract and adding deterministic tests
for each row.

## 1. Error classification (retry safety)

Retry decisions use the stable error hierarchy in `multids.errors` plus the raw backend exception type.

| Condition | Retry? | Rationale |
|-----------|--------|-----------|
| `asyncio.CancelledError` | **Never** | Caller-initiated cancellation must propagate immediately. |
| `ConnectorDependencyError` | **Never** | Missing driver/credentials will not fix itself; fail fast. |
| `ConnectorConnectionError` | **Yes** | Transient network/endpoint failures (already retried by `retry_async`). |
| `ConnectorOperationError` (base) | **No** | The backend rejected a valid request; retrying the same input fails again. |
| `ConnectorSchemaError` | **No** | The request/result shape is wrong; retrying cannot help. |
| `ConnectorDataError` | **No** | A specific row/document was rejected; retry needs a corrected payload, not the same one. |
| Backend-specific transient signals (e.g. throttling, 5xx, timeouts) | **Yes** | Only when the connector maps them to `ConnectorConnectionError` (see Phase 3). |
| Unknown `Exception` | **No** | Default to fail-safe: do not retry what we cannot classify. |

**Rule:** a connector must only raise `ConnectorConnectionError` for failures that are safe to retry with the same
input. Everything else must surface as a non-retryable `ConnectorError` (or a documented backend exception).

## 2. Idempotency classification (per operation)

Idempotency determines whether a workflow may safely replay an operation after a partial failure.

| Operation | Idempotent? | Notes |
|-----------|-------------|-------|
| `read_stream` / `read_bytes` / `fetch_rows` | **Yes** | Reads have no side effects; safe to retry/replay. |
| `write_bytes` / `write_stream` (no resume) | **Yes (last-write-wins)** | Re-writing the same bytes to the same key/row yields the same end state. A partial write is overwritten on retry. |
| `write_stream` with `resume=True` (S3) | **Yes, via checkpoint** | Resumes from the last committed part; duplicate parts are de-duplicated by `PartNumber` + ETag. |
| `delete_object` / `delete_object_ref` | **Yes (with `missing_ok`)** | Deleting an absent object is a no-op when `missing_ok=True`. |
| `write_object` / `write_json` | **Yes (last-write-wins)** | Same key/bytes ⇒ same result. |
| `execute` (INSERT) | **No** | Re-running inserts duplicate rows unless the table has a unique constraint / upsert. |
| `execute` (UPDATE/DELETE) | **Conditionally** | Idempotent if the predicate is stable and the new value is absolute (not relative). |
| `execute_statement` (DDL) | **Conditionally** | `CREATE TABLE IF NOT EXISTS` is idempotent; bare `CREATE TABLE` is not. |
| `bulk_insert` / `write_records` | **No** | Replaying the same batch duplicates rows. Requires an upsert or dedup key. |
| `bulk_index` (OpenSearch) | **Yes, if `id` is set** | Same `_id` overwrites; without `_id` it duplicates. |
| `Athena` query execution | **Yes** | Queries are read-only; re-running is safe (cost aside). |

**Rule:** workflows must document which idempotency class their destination falls into, and `copy_records` /
`copy_stream` must state that callers are responsible for idempotent destinations or explicit checkpoints when retries
are required.

## 3. Failure-state guarantees (what a caller can rely on)

After any operation raises, the connector must leave the system in one of these documented states:

### 3.1 Byte streams (`copy_stream`)
- **Partial destination object**: the destination may contain a prefix of the source bytes.
- **Resume**: for S3 with `resume=True`, a `checkpoint_path` file records committed parts; a retry continues from
  the last committed part. Without `resume`, a retry restarts from byte 0 (last-write-wins).
- **No silent loss**: bytes are only counted as copied after the destination writer accepts them.

### 3.2 Record batches (`copy_records`)
- **At-least-once or at-most-once**: batches submitted before the failure remain written (at-least-once for the
  successful prefix). There is **no rollback** of already-submitted batches.
- **No partial batch**: a batch is either fully submitted or not at all (the writer is called once per batch).
- **Checkpoint**: callers needing resume must pass a `write_batch` that is idempotent or tracks its own offset.

### 3.3 S3 multipart
- **Cleanup**: on success, the multipart upload is completed and the checkpoint removed. On failure without resume,
  the incomplete upload is **not** auto-aborted (the caller/S3 lifecycle policy handles it); with `resume=True`, the
  upload id is preserved in the checkpoint for continuation.

### 3.4 SQL transactions
- **Atomicity**: `execute`, `execute_many`, `bulk_insert`, and `write_records` each run inside a single transaction
  (`engine.begin()`); a failure rolls back that transaction's writes.
- **No cross-batch atomicity**: a multi-batch `bulk_insert` commits per batch; a mid-way failure leaves earlier
  batches committed.

### 3.5 OpenSearch bulk
- **Partial success**: `bulk_index` reports per-item failures in `BulkWriteResult.failed_items`; successful items
  remain indexed. A retry re-sends only the failed items (caller's responsibility) or the whole batch (duplicates
  unless `id` is set).

## 4. Cancellation

- All workflow helpers (`copy_stream`, `copy_records`, `map_records`, `filter_records`, `batch_records`) propagate
  `asyncio.CancelledError` without swallowing it.
- `_apply_concurrently` cancels in-flight tasks on any exception (including cancellation) and awaits their
  completion before re-raising.
- Connectors must cancel/close in-flight network operations on cancellation (no orphaned tasks).

## 5. Test obligations (Phase 2 exit criteria)

For each row above, Phase 2 must add deterministic tests (mocked backends, no live services) covering:

1. **Timeout** — an operation that exceeds a deadline raises `ConnectorConnectionError` (or documented timeout).
2. **Cancellation** — cancelling a `copy_stream` / `copy_records` mid-flight stops promptly and does not leak tasks.
3. **Retry** — a transient failure is retried up to `max_retries`, then raises; a non-retryable error is raised
   immediately without retry.
4. **Partial write** — a failure mid-stream leaves a documented partial destination state.
5. **Replay / resume** — S3 multipart resume continues from the checkpoint and does not re-upload committed parts.
6. **Rollback** — a failed SQL transaction leaves no partial rows; a failed multi-batch insert leaves earlier batches.
7. **No silent loss/duplication** — within the documented guarantees, records/bytes are neither lost nor duplicated.

## 6. Out of scope for Phase 2 (deferred to Phase 3)

- Applying `retry_async` uniformly to all connectors (Phase 3).
- Mapping every backend exception to the error hierarchy (Phase 3).
- Structured telemetry/metrics callbacks (Phase 3).
