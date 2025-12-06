import json

import pytest

from multids.connectors.s3 import S3Connector


class FailingClient:
    def __init__(self):
        self.uploads = {}
        self._upload_calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def create_multipart_upload(self, Bucket, Key):  # noqa: N803
        uid = "upload-1"
        self.uploads[uid] = []
        return {"UploadId": uid}

    async def upload_part(self, Bucket, Key, PartNumber, UploadId, Body):  # noqa: N803
        self._upload_calls += 1
        if hasattr(Body, "read"):
            data = Body.read()
        else:
            data = Body
        # succeed only for the first call, then raise to simulate interruption
        if self._upload_calls > 1:
            raise Exception("simulated network error")
        etag = f"etag-{PartNumber}"
        self.uploads[UploadId].append({"PartNumber": PartNumber, "ETag": etag, "Size": len(data)})
        return {"ETag": etag}

    async def list_parts(self, Bucket, Key, UploadId):  # noqa: N803
        parts = self.uploads.get(UploadId, [])
        # adapt shape to what S3 would return
        return {"Parts": [{"PartNumber": p["PartNumber"], "ETag": p["ETag"], "Size": p["Size"]} for p in parts]}

    async def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):  # noqa: N803
        return {"Completed": MultipartUpload}

    async def abort_multipart_upload(self, Bucket, Key, UploadId):  # noqa: N803
        self.uploads.pop(UploadId, None)


class CompletingClient:
    def __init__(self, existing_parts):
        self.uploads = {}
        self.existing_parts = existing_parts

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def list_parts(self, Bucket, Key, UploadId):  # noqa: N803
        return {"Parts": self.existing_parts}

    async def upload_part(self, Bucket, Key, PartNumber, UploadId, Body):  # noqa: N803
        if hasattr(Body, "read"):
            data = Body.read()
        else:
            data = Body
        etag = f"etag-{PartNumber}"
        self.uploads.setdefault(UploadId, []).append((PartNumber, len(data)))
        return {"ETag": etag}

    async def create_multipart_upload(self, Bucket, Key):  # noqa: N803
        uid = "upload-1"
        self.uploads[uid] = []
        return {"UploadId": uid}

    async def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):  # noqa: N803
        return {"Completed": MultipartUpload}


@pytest.mark.asyncio
async def test_resume_from_checkpoint(tmp_path):
    chk = tmp_path / "upload.chk"

    conn = S3Connector(part_size=1024, max_concurrency=2, enforce_min_part_size=False)

    failing = FailingClient()

    class Ctx:
        def __init__(self, client):
            self._client = client

        async def __aenter__(self):
            return self._client

        async def __aexit__(self, exc_type, exc, tb):
            return False

    # Patch session to return failing client context
    conn._session = type("S", (), {"client": lambda self, *a, **k: Ctx(failing)})()

    async def gen():
        for _ in range(4):
            yield b"x" * 800

    # First attempt should raise due to simulated failure, but leave a checkpoint
    with pytest.raises(Exception):
        await conn.write_stream(gen(), bucket="b", key="k", checkpoint_path=str(chk), force_multipart=True)

    assert chk.exists(), "checkpoint file should exist after partial upload"
    data = json.loads(chk.read_text())
    assert data.get("upload_id")
    # Now resume with a client that knows about the existing part
    # normalize size key (checkpoint uses 'size') to 'Size' expected by list_parts
    existing_parts = [
        {"PartNumber": p["PartNumber"], "ETag": p["ETag"], "Size": int(p.get("size", p.get("Size", 0)) or 0)}
        for p in data.get("parts", [])
    ]
    completing = CompletingClient(existing_parts)
    conn._session = type("S", (), {"client": lambda self, *a, **k: Ctx(completing)})()

    # Resume should complete without raising
    await conn.write_stream(gen(), bucket="b", key="k", resume=True, checkpoint_path=str(chk))

    # checkpoint should be removed on success
    assert not chk.exists()
