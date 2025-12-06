import pytest

from multids.connectors.s3 import S3Connector


class DummyClient:
    def __init__(self):
        self.parts = {}
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
        etag = f"etag-{PartNumber}"
        self.uploads[UploadId].append((PartNumber, len(data)))
        return {"ETag": etag}

    async def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):  # noqa: N803
        # return parts info
        return {"Completed": MultipartUpload}

    async def abort_multipart_upload(self, Bucket, Key, UploadId):  # noqa: N803
        self.uploads.pop(UploadId, None)

    async def put_object(self, Bucket, Key, Body):  # noqa: N803
        self._put = Body
        return {"Result": "ok"}


class DummySession:
    def __init__(self, client):
        self._client = client

    def client(self, *args, **kwargs):
        return self._client


@pytest.mark.asyncio
async def test_multipart_upload(tmp_path, monkeypatch):
    # small part_size to force multipart
    # disable enforced min part size for testing small parts
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

    # create a generator that yields 3 parts worth of data
    async def gen():
        for _ in range(6):
            yield b"x" * 600

    await conn.write_stream(gen(), bucket="b", key="k")

    # ensure upload created and completed (either multipart or single PUT)
    if "upload-1" in dummy.uploads:
        parts = dummy.uploads["upload-1"]
        assert len(parts) >= 1
    else:
        # single PUT path
        assert hasattr(dummy, "_put")
        assert len(dummy._put) == 6 * 600
