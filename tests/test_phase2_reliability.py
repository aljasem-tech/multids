"""Phase 2: deterministic tests for failure-safe data movement.

Covers the exit criteria: timeouts, cancellation, retries, partial writes,
replay/resume, rollback, and no-silent-loss/duplication within documented
guarantees. All tests use mocked backends (no live services).
"""

import asyncio
import json

import pytest

from multids import workflows
from multids.connectors.sql import SQLConnectorBase
from multids.errors import ConnectorConnectionError
from multids.reliability import RetryConfig, retry_async, with_timeout

# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_with_timeout_raises_connection_error():
    async def slow():
        await asyncio.sleep(10)

    with pytest.raises(ConnectorConnectionError, match="timed out"):
        await with_timeout(slow, timeout=0.01, operation_name="test.slow")


@pytest.mark.asyncio
async def test_with_timeout_passthrough_when_none():
    async def quick():
        return 42

    assert await with_timeout(quick, timeout=None, operation_name="test.quick") == 42


@pytest.mark.asyncio
async def test_retry_times_out_then_raises():
    calls = {"n": 0}

    async def always_slow():
        calls["n"] += 1
        await asyncio.sleep(10)

    config = RetryConfig(max_retries=2, backoff_factor=0.001, timeout=0.01)
    with pytest.raises(ConnectorConnectionError, match="timed out"):
        await retry_async(always_slow, config=config, operation_name="test.timeout")
    # initial attempt + 2 retries
    assert calls["n"] == 3


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_recovers_after_transient_failures():
    state = {"n": 0}

    async def flaky():
        state["n"] += 1
        if state["n"] < 3:
            raise ConnectorConnectionError("transient")
        return "ok"

    config = RetryConfig(max_retries=3, backoff_factor=0.001)
    assert await retry_async(flaky, config=config, operation_name="test.retry") == "ok"
    assert state["n"] == 3


@pytest.mark.asyncio
async def test_retry_gives_up_after_max_retries():
    state = {"n": 0}

    async def always_fail():
        state["n"] += 1
        raise ConnectorConnectionError("transient")

    config = RetryConfig(max_retries=2, backoff_factor=0.001)
    with pytest.raises(ConnectorConnectionError):
        await retry_async(always_fail, config=config, operation_name="test.retry")
    assert state["n"] == 3  # 1 initial + 2 retries


@pytest.mark.asyncio
async def test_retry_does_not_retry_non_transient():
    state = {"n": 0}

    async def bad_request():
        state["n"] += 1
        raise ValueError("permanent")

    config = RetryConfig(max_retries=3, backoff_factor=0.001)
    with pytest.raises(ValueError):
        await retry_async(
            bad_request,
            config=config,
            operation_name="test.retry",
            retry_if=lambda exc: isinstance(exc, ConnectorConnectionError),
        )
    assert state["n"] == 1  # not retried


@pytest.mark.asyncio
async def test_retry_propagates_cancellation():
    async def cancel_me():
        raise asyncio.CancelledError()

    config = RetryConfig(max_retries=3, backoff_factor=0.001)
    with pytest.raises(asyncio.CancelledError):
        await retry_async(cancel_me, config=config, operation_name="test.cancel")


# ---------------------------------------------------------------------------
# Cancellation of workflows
# ---------------------------------------------------------------------------


class _Source:
    def __init__(self, chunks):
        self._chunks = chunks

    async def read_stream(self, *args, **kwargs):
        for c in self._chunks:
            yield c


class _Destination:
    def __init__(self, on_chunk=None):
        self.received = []
        self._on_chunk = on_chunk

    async def write_stream(self, stream, *args, **kwargs):
        async for chunk in stream:
            self.received.append(chunk)
            if self._on_chunk is not None:
                await self._on_chunk(len(self.received))


@pytest.mark.asyncio
async def test_copy_stream_propagates_cancellation():
    async def cancel_after_two(count):
        if count >= 2:
            raise asyncio.CancelledError()

    src = _Source([b"a", b"b", b"c", b"d"])
    dst = _Destination(on_chunk=cancel_after_two)

    with pytest.raises(asyncio.CancelledError):
        await workflows.copy_stream(src, dst)
    # only the chunks before cancellation were accepted
    assert dst.received == [b"a", b"b"]


@pytest.mark.asyncio
async def test_copy_records_propagates_cancellation():
    written = []

    async def write_batch(batch):
        written.extend(batch)
        if len(written) >= 2:
            raise asyncio.CancelledError()

    records = [1, 2, 3, 4]
    with pytest.raises(asyncio.CancelledError):
        await workflows.copy_records(records, write_batch, batch_size=1)
    assert written == [1, 2]


# ---------------------------------------------------------------------------
# Partial writes (byte stream)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_copy_stream_partial_destination_on_failure():
    class FailingDestination:
        def __init__(self):
            self.received = []

        async def write_stream(self, stream, *args, **kwargs):
            async for chunk in stream:
                self.received.append(chunk)
                raise ConnectorConnectionError("boom")

    src = _Source([b"hello", b"world"])
    dst = FailingDestination()
    with pytest.raises(ConnectorConnectionError):
        await workflows.copy_stream(src, dst)
    # first chunk was accepted before the failure -> partial destination state
    assert dst.received == [b"hello"]


# ---------------------------------------------------------------------------
# Checkpoint / resume (workflow-level)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_copy_stream_checkpoint_callback_tracks_progress():
    checkpoints = []

    async def record(result):
        checkpoints.append(result.bytes_copied)

    src = _Source([b"x" * 10, b"y" * 10, b"z" * 10])
    dst = _Destination()
    result = await workflows.copy_stream(src, dst, checkpoint=record)
    assert result.bytes_copied == 30
    assert checkpoints == [10, 20, 30]


@pytest.mark.asyncio
async def test_copy_records_checkpoint_callback_tracks_progress():
    checkpoints = []

    async def record(result):
        checkpoints.append(result.records_copied)

    async def write_batch(batch):
        return None

    result = await workflows.copy_records([1, 2, 3, 4, 5], write_batch, batch_size=2, checkpoint=record)
    assert result.records_copied == 5
    assert checkpoints == [2, 4, 5]


# ---------------------------------------------------------------------------
# S3 multipart resume / replay / abort-on-failure
# ---------------------------------------------------------------------------


class _S3Client:
    """Minimal S3 client that can fail part uploads on demand."""

    def __init__(self, fail_after=0):
        self.uploads = {}
        self._part_calls = 0
        self._fail_after = fail_after
        self.aborted = []
        self.completed = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def create_multipart_upload(self, Bucket, Key):  # noqa: N803
        uid = f"upload-{len(self.uploads) + 1}"
        self.uploads[uid] = []
        return {"UploadId": uid}

    async def upload_part(self, Bucket, Key, PartNumber, UploadId, Body):  # noqa: N803
        self._part_calls += 1
        if self._fail_after and self._part_calls > self._fail_after:
            raise ConnectorConnectionError("simulated network error")
        data = Body.read() if hasattr(Body, "read") else Body
        etag = f"etag-{PartNumber}"
        self.uploads[UploadId].append({"PartNumber": PartNumber, "ETag": etag, "Size": len(data)})
        return {"ETag": etag}

    async def list_parts(self, Bucket, Key, UploadId):  # noqa: N803
        parts = self.uploads.get(UploadId, [])
        return {"Parts": [{"PartNumber": p["PartNumber"], "ETag": p["ETag"], "Size": p["Size"]} for p in parts]}

    async def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):  # noqa: N803
        self.completed.append(UploadId)
        return {"Completed": MultipartUpload}

    async def abort_multipart_upload(self, Bucket, Key, UploadId):  # noqa: N803
        self.aborted.append(UploadId)
        self.uploads.pop(UploadId, None)


def _s3_with_client(client):
    from multids.connectors.s3 import S3Connector

    conn = S3Connector(part_size=512, max_concurrency=1)

    class _Ctx:
        async def __aenter__(self):
            return client

        async def __aexit__(self, exc_type, exc, tb):
            return False

    conn._session = type("S", (), {"client": lambda self, *a, **k: _Ctx()})()
    return conn


@pytest.mark.asyncio
async def test_s3_multipart_aborts_on_failure_without_resume():
    # fail on the 2nd part upload; no checkpoint -> should abort
    client = _S3Client(fail_after=1)
    conn = _s3_with_client(client)

    async def gen():
        for _ in range(4):
            yield b"x" * 512

    with pytest.raises(ConnectorConnectionError):
        await conn.write_stream(gen(), bucket="b", key="k", force_multipart=True)

    # incomplete upload should have been aborted, not left orphaned
    assert len(client.aborted) == 1
    assert client.completed == []


@pytest.mark.asyncio
async def test_s3_multipart_resumes_from_checkpoint(tmp_path):
    chk = tmp_path / "upload.chk"

    # First attempt: fail after 1 part, with a checkpoint -> saves state
    client = _S3Client(fail_after=1)
    conn = _s3_with_client(client)

    async def gen():
        for _ in range(4):
            yield b"x" * 512

    with pytest.raises(ConnectorConnectionError):
        await conn.write_stream(gen(), bucket="b", key="k", checkpoint_path=str(chk), force_multipart=True)

    assert chk.exists(), "checkpoint should be saved for resume"
    saved = json.loads(chk.read_text())
    assert saved["upload_id"]
    first_parts = len(saved["parts"])
    assert first_parts >= 1

    # Second attempt: resume, no failures -> completes, reuses prior parts
    client2 = _S3Client(fail_after=0)
    conn2 = _s3_with_client(client2)
    # seed the new client with the parts already uploaded in attempt 1
    client2.uploads[saved["upload_id"]] = [
        {"PartNumber": p["PartNumber"], "ETag": p["ETag"], "Size": p["size"]} for p in saved["parts"]
    ]

    await conn2.write_stream(gen(), bucket="b", key="k", resume=True, checkpoint_path=str(chk), force_multipart=True)

    # completed exactly once, and the checkpoint is cleaned up
    assert client2.completed == [saved["upload_id"]]
    assert not chk.exists()


@pytest.mark.asyncio
async def test_s3_resume_does_not_reupload_committed_parts(tmp_path):
    chk = tmp_path / "upload.chk"

    # attempt 1: 2 parts succeed, 3rd fails
    client = _S3Client(fail_after=2)
    conn = _s3_with_client(client)

    async def gen():
        for _ in range(5):
            yield b"x" * 512

    with pytest.raises(ConnectorConnectionError):
        await conn.write_stream(gen(), bucket="b", key="k", checkpoint_path=str(chk), force_multipart=True)

    saved = json.loads(chk.read_text())
    committed = {p["PartNumber"] for p in saved["parts"]}
    assert committed == {1, 2}

    # attempt 2: resume; track which part numbers get uploaded
    uploaded_parts = []
    client2 = _S3Client(fail_after=0)

    orig_upload_part = client2.upload_part

    async def tracking_upload_part(Bucket, Key, PartNumber, UploadId, Body):  # noqa: N803
        uploaded_parts.append(PartNumber)
        return await orig_upload_part(Bucket, Key, PartNumber, UploadId, Body)

    client2.upload_part = tracking_upload_part
    client2.uploads[saved["upload_id"]] = [
        {"PartNumber": p["PartNumber"], "ETag": p["ETag"], "Size": p["size"]} for p in saved["parts"]
    ]
    conn2 = _s3_with_client(client2)

    await conn2.write_stream(gen(), bucket="b", key="k", resume=True, checkpoint_path=str(chk), force_multipart=True)

    # committed parts 1 and 2 must NOT be re-uploaded
    assert 1 not in uploaded_parts
    assert 2 not in uploaded_parts


# ---------------------------------------------------------------------------
# SQL rollback (transaction atomicity)
# ---------------------------------------------------------------------------


class _RecordingConn:
    def __init__(self, fail_on_call=None):
        self.executed = []
        self.committed = False
        self.rolled_back = False
        self._fail_on_call = fail_on_call
        self._call = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None:
            self.rolled_back = True
        return False

    async def execute(self, stmt, params=None):
        self._call += 1
        self.executed.append((str(stmt), params))
        if self._fail_on_call and self._call == self._fail_on_call:
            raise ConnectorConnectionError("simulated mid-batch failure")

    async def stream(self, stmt, params=None):
        return self


class _RecordingEngine:
    def __init__(self, conn):
        self.conn = conn

    def begin(self):
        return self.conn


@pytest.mark.asyncio
async def test_sql_bulk_insert_rolls_back_on_failure():
    conn = _RecordingConn(fail_on_call=2)
    engine = _RecordingEngine(conn)

    connector = object.__new__(SQLConnectorBase)
    connector._engine = engine
    connector.retry_config = RetryConfig(max_retries=0, backoff_factor=0.0, max_backoff=0.0, timeout=None)

    rows = [{"id": i} for i in range(5)]
    # first chunk succeeds, second fails -> transaction rolls back
    with pytest.raises(ConnectorConnectionError):
        await connector.bulk_insert("t", rows, chunk_size=2)

    assert conn.rolled_back, "transaction should roll back on failure"


@pytest.mark.asyncio
async def test_sql_execute_many_rolls_back_on_failure():
    conn = _RecordingConn(fail_on_call=2)
    engine = _RecordingEngine(conn)

    connector = object.__new__(SQLConnectorBase)
    connector._engine = engine
    connector.retry_config = RetryConfig(max_retries=0, backoff_factor=0.0, max_backoff=0.0, timeout=None)

    with pytest.raises(ConnectorConnectionError):
        await connector.execute_many("INSERT INTO t VALUES (:id)", [{"id": 1}, {"id": 2}, {"id": 3}])

    assert conn.rolled_back


@pytest.mark.asyncio
async def test_sql_execute_returns_affected_rows():
    class _Result:
        rowcount = 3

    class _Conn(_RecordingConn):
        async def execute(self, stmt, params=None):
            self.executed.append((str(stmt), params))
            return _Result()

    conn = _Conn()
    connector = object.__new__(SQLConnectorBase)
    connector._engine = _RecordingEngine(conn)
    connector.retry_config = RetryConfig(max_retries=0, backoff_factor=0.0, max_backoff=0.0, timeout=None)

    result = await connector.execute("UPDATE t SET x=1")
    assert result.items_written == 3


# ---------------------------------------------------------------------------
# No silent loss / duplication within guarantees
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_copy_records_no_loss_on_success():
    written = []

    async def write_batch(batch):
        written.extend(batch)

    records = list(range(10))
    result = await workflows.copy_records(records, write_batch, batch_size=3)
    assert result.records_copied == 10
    assert written == records  # every record written exactly once


@pytest.mark.asyncio
async def test_copy_stream_no_loss_on_success():
    src = _Source([b"a", b"b", b"c"])
    dst = _Destination()
    result = await workflows.copy_stream(src, dst)
    assert result.bytes_copied == 3
    assert b"".join(dst.received) == b"abc"
