import pytest

from multids.connectors.s3 import S3Connector


class DummyClient:
    def __init__(self):
        self.uploads = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def create_multipart_upload(self, Bucket, Key):  # noqa: N803
        uid = "upload-1"
        self.uploads[uid] = []
        return {"UploadId": uid}

    async def upload_part(self, Bucket, Key, PartNumber, UploadId, Body):  # noqa: N803
        if hasattr(Body, "read"):
            data = Body.read()
        else:
            data = Body
        self.uploads[UploadId].append((PartNumber, len(data)))
        return {"ETag": f"etag-{PartNumber}"}

    async def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):  # noqa: N803
        return {"Completed": MultipartUpload}

    async def put_object(self, Bucket, Key, Body):  # noqa: N803
        self._put = Body
        return {"Result": "ok"}


@pytest.mark.asyncio
async def test_force_multipart_upload():
    conn = S3Connector(part_size=1024, max_concurrency=2, enforce_min_part_size=False)

    dummy = DummyClient()

    class Ctx:
        def __init__(self, client):
            self._client = client

        async def __aenter__(self):
            return self._client

        async def __aexit__(self, exc_type, exc, tb):
            return False

    # patch session.client to return our context
    conn._session = type("S", (), {"client": lambda self, *a, **k: Ctx(dummy)})()

    async def gen():
        for _ in range(4):
            yield b"x" * 600

    # force multipart even though total size < default min multipart size
    await conn.write_stream(gen(), bucket="b", key="k", force_multipart=True)

    assert "upload-1" in dummy.uploads
    parts = dummy.uploads["upload-1"]
    assert len(parts) >= 1
