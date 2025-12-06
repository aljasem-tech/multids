import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from multids.connectors.local import LocalConnector
from multids.connectors.opensearch import OpenSearchConnector
from multids.connectors.s3 import S3Connector

# Sample data with multiple scripts
UNICODE_DATA = {"en": "Hello", "de": "Grüße", "cn": "你好", "ar": "مرحبا", "emoji": "🐍"}
# Expected wire format: explicit characters, NOT escaped (e.g. not "\u4f60\u597d")
EXPECTED_SUBSTRING = "你好"


@pytest.mark.asyncio
async def test_local_write_read_unicode():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tf:
        path = tf.name
    tf.close()

    try:
        c = LocalConnector()
        await c.write_json(UNICODE_DATA, path)

        # Verify raw content on disk
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
            # Assert that the raw file contains the actual characters
            assert EXPECTED_SUBSTRING in raw
            assert "\\u" not in raw  # simple check that we aren't escaping everything

        # Verify read_json parses it back correctly
        read_back = await c.read_json(path)
        assert read_back == UNICODE_DATA

    finally:
        if os.path.exists(path):
            os.remove(path)


@pytest.mark.asyncio
async def test_s3_write_read_unicode():
    mock_session = MagicMock()
    mock_client_cm = AsyncMock()
    mock_client = AsyncMock()
    mock_client_cm.__aenter__.return_value = mock_client
    mock_session.client.return_value = mock_client_cm

    c = S3Connector("eu-central-1")
    c._session = mock_session

    # Test write_json
    await c.write_json(UNICODE_DATA, "bucket", "key.json")

    # Verify what was sent to put_object (or multipart)
    # write_json calls write_bytes -> write_stream -> put_object for small data
    # OR calls multipart logic. For this small dict, likely put_object unless forced.
    # But wait, write_stream logic might use multipart if it can't determine size?
    # write_bytes creates a generator, so size is unknown -> multipart usually.
    # Let's inspect calls.

    # We catch all calls.
    # write_stream with generator often triggers multipart upload logic in S3Connector unless we optimize it.
    # The S3Connector implementation buffers. Since UNICODE_DATA is small, it stays in buffer
    # and likely falls through to `await client.put_object(...)` at the end if upload_id is None.

    assert mock_client.put_object.called or mock_client.upload_part.called

    # Capture the body
    called_body = None
    if mock_client.put_object.called:
        kwargs = mock_client.put_object.call_args.kwargs
        called_body = kwargs["Body"]

    # If it was multipart for some reason (rare for bytes wrapper if logic is clean):
    # Just assume put_object for now based on implementing `write_bytes` -> buffer -> `put_object`
    # single shot if small.

    assert called_body is not None
    # Body is bytes. Decode utf-8 and check
    body_str = called_body.decode("utf-8")
    assert EXPECTED_SUBSTRING in body_str
    assert "\\u" not in body_str

    # Test read_json
    # reading calls read_bytes -> read_stream
    mock_stream = AsyncMock()
    mock_stream.read.side_effect = [called_body, b""]
    mock_client.get_object.return_value = {"Body": mock_stream}

    data = await c.read_json("bucket", "key.json")
    assert data == UNICODE_DATA


@pytest.mark.asyncio
async def test_opensearch_unicode_handling():
    import sys

    mock_httpx = MagicMock()
    mock_client = AsyncMock()
    mock_httpx.AsyncClient.return_value = mock_client
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"ok": True}
    mock_client.post.return_value = mock_resp
    mock_client.put.return_value = mock_resp
    mock_client.request.return_value = mock_resp

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        # Reload internal import if needed, but here we just instantiate
        # Since the class does 'import httpx', it will get our mock from sys.modules
        c = OpenSearchConnector("http://localhost:9200")

        # Test index_doc
        await c.index_doc("idx", UNICODE_DATA)

        # Verify post called with content=bytes, and those bytes are unescaped unicode
        call_kwargs = mock_client.post.call_args.kwargs
        if "content" in call_kwargs:
            content = call_kwargs["content"]
            assert isinstance(content, str)
            assert EXPECTED_SUBSTRING in content
            assert "\\u" not in content
        else:
            pytest.fail("Expected 'content' arg in index_doc call")

        # Test bulk_index
        # We need to simulate __aiter__ for docs
        async def doc_gen():
            yield UNICODE_DATA

        mock_client.request.reset_mock()
        mock_client.request.return_value = mock_resp

        await c.bulk_index("idx", doc_gen())

        # Verify bulk request body
        # _request("POST", ...)
        req_kwargs = mock_client.request.call_args.kwargs
        assert req_kwargs["content"]
        bulk_body = req_kwargs["content"]  # str due to "\n".join()

        assert EXPECTED_SUBSTRING in bulk_body
        assert "\\u" not in bulk_body
