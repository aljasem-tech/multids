from unittest.mock import AsyncMock, MagicMock

import pytest

from multids.ai import OpenAIClient
from multids.connectors.s3 import S3Connector
from multids.interfaces import ContentHook


class ReversingHook(ContentHook):
    """Simple hook that reverses string/bytes."""

    async def pre_write(self, data, **kwargs):
        if isinstance(data, str):
            return data[::-1]
        if isinstance(data, bytes):
            return data[::-1]
        if isinstance(data, dict):
            # reverse keys
            return {k[::-1]: v for k, v in data.items()}
        return data

    async def post_read(self, data, **kwargs):
        # same logic
        return await self.pre_write(data)


@pytest.mark.asyncio
async def test_openai_client():
    mock_client = AsyncMock()
    mock_client.chat.completions.create.return_value.choices[0].message.content = "Filtered Content"

    client = OpenAIClient(mock_client)
    res = await client.generate("Hello")

    assert res["content"] == "Filtered Content"
    mock_client.chat.completions.create.assert_called_once()
    assert mock_client.chat.completions.create.call_args[1]["messages"][0]["content"] == "Hello"


@pytest.mark.asyncio
async def test_s3_hook_read_bytes():
    mock_session = MagicMock()
    mock_client_cm = AsyncMock()
    mock_client = AsyncMock()
    mock_client_cm.__aenter__.return_value = mock_client
    mock_session.client.return_value = mock_client_cm

    c = S3Connector("eu-central-1")
    c._session = mock_session

    # Mock read_stream output (bytes)
    # The connector.read_stream uses obj["Body"] as stream and calls await stream.read()
    mock_body = MagicMock()

    # helper to yield one chunk then empty
    chunks = [b"dlroW olleH"]

    async def mock_read(n):
        if chunks:
            return chunks.pop(0)
        return b""

    mock_body.read = AsyncMock(side_effect=mock_read)
    mock_body.close = AsyncMock()
    mock_client.get_object.return_value = {"Body": mock_body}

    hook = ReversingHook()
    # Reading bytes should reverse the "dlroW olleH" back to "Hello World"
    data = await c.read_bytes("bucket", "key", hook=hook)

    assert data == b"Hello World"


@pytest.mark.asyncio
async def test_s3_hook_write_json():
    mock_session = MagicMock()
    mock_client_cm = AsyncMock()
    mock_client = AsyncMock()
    mock_client_cm.__aenter__.return_value = mock_client
    mock_session.client.return_value = mock_client_cm

    c = S3Connector("eu-central-1")
    c._session = mock_session

    hook = ReversingHook()
    data = {"hello": "world"}
    # Hook pre_write on dict: {"olleh": "world"}
    # Then json.dumps -> '{"olleh": "world"}'
    # Then write_bytes (no hook passed)

    await c.write_json(data, "bucket", "key", indent=None, hook=hook)

    # Verify put_object (or upload_part) called with serialized reversed dict

    # We need to capture the uploaded bytes
    # write_json -> write_bytes -> write_stream -> (eventually) put_object or multipart

    # Since we didn't mock write_stream logic to force multipart, and data is small,
    # it might trigger put_object if write_bytes uses simple path?
    # Actually write_bytes calls write_stream. write_stream buffers.
    # It calls put_object if it fits in buffer.

    assert mock_client.put_object.called or mock_client.upload_part.called

    call_args = None
    if mock_client.put_object.called:
        call_args = mock_client.put_object.call_args.kwargs

    body = call_args["Body"]  # bytes
    body_str = body.decode("utf-8")

    assert '"olleh":' in body_str
