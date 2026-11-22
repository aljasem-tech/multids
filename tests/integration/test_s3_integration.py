"""Opt-in integration coverage for a real S3-compatible service.

Runs against AWS S3 or any S3-compatible service (MinIO, LocalStack) when the
following environment variables are set:

* ``MULTIDS_S3_TEST_BUCKET`` - bucket to use (must already exist)
* ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` - credentials
* ``AWS_REGION`` - optional region (default: ``us-east-1``)
* ``AWS_ENDPOINT_URL`` - optional endpoint for S3-compatible services

Scenarios covered: authentication, Unicode round-trips, large multipart streams,
pagination, cleanup, failure recovery and checkpoint/resume.
"""

from __future__ import annotations

import hashlib
import os
import uuid

import pytest

from multids.connectors.s3 import S3Connector
from multids.contracts import ObjectRef
from multids.errors import ConnectorOperationError

pytestmark = [pytest.mark.real_integration, pytest.mark.asyncio]

MIB = 1024 * 1024


def _config() -> dict | None:
    bucket = os.environ.get("MULTIDS_S3_TEST_BUCKET")
    if not bucket or not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return None
    return {
        "bucket": bucket,
        "region": os.environ.get("AWS_REGION", "us-east-1"),
        "endpoint_url": os.environ.get("AWS_ENDPOINT_URL") or None,
    }


def _connector(cfg: dict, **kwargs) -> S3Connector:
    return S3Connector(region_name=cfg["region"], endpoint_url=cfg["endpoint_url"], **kwargs)


async def _make_objects(connector: S3Connector, bucket: str, prefix: str, count: int) -> list[str]:
    keys = []
    for i in range(count):
        key = f"{prefix}/obj-{i:03d}.txt"
        await connector.write_bytes(f"value-{i}".encode(), bucket, key)
        keys.append(key)
    return keys


async def test_s3_ping_and_authentication():
    cfg = _config()
    if cfg is None:
        pytest.skip("Set MULTIDS_S3_TEST_BUCKET and AWS credentials to run real S3 integration tests")

    connector = _connector(cfg)
    try:
        assert await connector.ping() is True
    finally:
        await connector.close()


async def test_s3_unicode_round_trip():
    cfg = _config()
    if cfg is None:
        pytest.skip("S3 integration environment not configured")

    key = f"multids-it/{uuid.uuid4()}-unicode.json"
    payload = '{"message": "héllo 世界 — ünïcode"}'.encode("utf-8")
    connector = _connector(cfg)
    try:
        await connector.write_bytes(payload, cfg["bucket"], key)
        assert await connector.read_bytes(cfg["bucket"], key) == payload
    finally:
        try:
            await connector.delete_object(cfg["bucket"], key)
        except Exception:
            pass
        await connector.close()


async def test_s3_large_multipart_stream_round_trip():
    cfg = _config()
    if cfg is None:
        pytest.skip("S3 integration environment not configured")

    key = f"multids-it/{uuid.uuid4()}-large.bin"
    # 12 MiB forces multiple 5 MiB parts.
    data = os.urandom(12 * MIB)
    connector = _connector(
        cfg,
        part_size=5 * MIB,
        min_multipart_upload_size=5 * MIB,
        max_concurrency=2,
        timeout=600.0,
    )

    async def gen():
        step = MIB
        for offset in range(0, len(data), step):
            yield data[offset : offset + step]

    try:
        await connector.write_stream(gen(), bucket=cfg["bucket"], key=key, force_multipart=True)
        read_back = await connector.read_bytes(cfg["bucket"], key)
        assert len(read_back) == len(data)
        assert hashlib.sha256(read_back).digest() == hashlib.sha256(data).digest()
    finally:
        try:
            await connector.delete_object(cfg["bucket"], key)
        except Exception:
            pass
        await connector.close()


async def test_s3_pagination_and_cleanup():
    cfg = _config()
    if cfg is None:
        pytest.skip("S3 integration environment not configured")

    prefix = f"multids-it/{uuid.uuid4()}-page"
    connector = _connector(cfg)
    created: list[str] = []
    try:
        created = await _make_objects(connector, cfg["bucket"], prefix, count=5)

        # list_keys yields every object exactly once.
        listed = [k async for k in connector.list_keys(cfg["bucket"], prefix=prefix)]
        assert sorted(listed) == sorted(created)

        # list_object_page walks pages via cursors.
        seen: list[str] = []
        cursor = None
        pages = 0
        while True:
            page = await connector.list_object_page(ObjectRef(prefix, cfg["bucket"]), cursor=cursor, page_size=2)
            seen.extend(item.key for item in page.items)
            pages += 1
            cursor = page.next_cursor
            if not cursor:
                break
            assert pages < 20, "pagination did not terminate"
        assert sorted(seen) == sorted(created)
    finally:
        for key in created:
            try:
                await connector.delete_object(cfg["bucket"], key)
            except Exception:
                pass
        await connector.close()

    remaining = [k async for k in connector.list_keys(cfg["bucket"], prefix=prefix)]
    assert remaining == []


async def test_s3_missing_key_raises_typed_error():
    cfg = _config()
    if cfg is None:
        pytest.skip("S3 integration environment not configured")

    connector = _connector(cfg)
    try:
        with pytest.raises(ConnectorOperationError):
            await connector.read_bytes(cfg["bucket"], f"multids-it/{uuid.uuid4()}-does-not-exist")
    finally:
        await connector.close()


async def test_s3_resumable_multipart_with_checkpoint(tmp_path):
    cfg = _config()
    if cfg is None:
        pytest.skip("S3 integration environment not configured")

    key = f"multids-it/{uuid.uuid4()}-resume.bin"
    checkpoint = tmp_path / "upload.chk"
    data = os.urandom(11 * MIB)
    connector = _connector(cfg, part_size=5 * MIB, min_multipart_upload_size=5 * MIB, max_concurrency=1)

    async def gen():
        for offset in range(0, len(data), MIB):
            yield data[offset : offset + MIB]

    try:
        # First attempt with a checkpoint path (fresh upload).
        await connector.write_stream(
            gen(),
            bucket=cfg["bucket"],
            key=key,
            checkpoint_path=str(checkpoint),
        )
        # Checkpoint is removed after a successful upload.
        assert not checkpoint.exists()

        read_back = await connector.read_bytes(cfg["bucket"], key)
        assert hashlib.sha256(read_back).digest() == hashlib.sha256(data).digest()

        # A subsequent resumable write with resume=True starts cleanly.
        await connector.write_stream(
            gen(),
            bucket=cfg["bucket"],
            key=key,
            resume=True,
            checkpoint_path=str(checkpoint),
        )
        assert await connector.read_bytes(cfg["bucket"], key) == data
    finally:
        try:
            await connector.delete_object(cfg["bucket"], key)
        except Exception:
            pass
        await connector.close()
