from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple

from ..interfaces import Connector, ContentHook, Readable, Writable

ProgressCallback = Callable[[int, int], Any]
OverallProgressCallback = Callable[[int, Optional[int]], Any]


class S3Connector(Connector, Readable, Writable):
    """
    Async S3 connector with streaming reads and multipart uploads.

    Features:
    - Streaming reads via `read_stream`/`read_bytes`.
    - Multipart uploads with concurrent part uploads.
    - Disk-backed staging for very large parts.
    - Checkpointing to resume multipart uploads.
    - Part-level and overall progress callbacks.
    """

    def __init__(
        self,
        aws_region: Optional[str] = None,
        part_size: int = 8 * 1024 * 1024,
        max_concurrency: int = 4,
        spill_to_disk_threshold: int = 32 * 1024 * 1024,
        enforce_min_part_size: bool = False,
        min_multipart_upload_size: int = 5 * 1024 * 1024,
    ):
        self._region = aws_region
        # Lazy-import aioboto3 to avoid import-time dependency issues during tests.
        try:
            import aioboto3 as _aioboto3

            self._aioboto3 = _aioboto3
            self._session = _aioboto3.Session()
        except Exception:
            self._aioboto3 = None
            self._session = None
        if enforce_min_part_size:
            self.part_size = max(part_size, 5 * 1024 * 1024)
        else:
            self.part_size = part_size
        self.max_concurrency = max(1, max_concurrency)
        self.spill_to_disk_threshold = max(spill_to_disk_threshold, self.part_size)
        # threshold before switching from single PUT to multipart upload
        self.min_multipart_upload_size = max(0, int(min_multipart_upload_size))

    async def read_stream(
        self,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
        hook: Optional[ContentHook] = None,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncIterator[bytes]:
        if not bucket or not key:
            raise ValueError("bucket and key are required for S3Connector")
        if self._session is None:
            raise RuntimeError("aioboto3 is not available; install aioboto3 to use S3Connector")
        async with self._session.client("s3", region_name=self._region) as client:
            obj = await client.get_object(Bucket=bucket, Key=key)
            stream = obj["Body"]
            try:
                if hook:
                    # Hook is responsible for yielding chunks.
                    # Use 'await' to allow hook to initialize/wrap the stream
                    # Then iterate over the result.
                    async for chunk in await hook.post_read(stream):
                        yield chunk
                else:
                    while True:
                        chunk = await stream.read(self.part_size)
                        if not chunk:
                            break
                        yield chunk
            finally:
                await stream.close()

    async def read_bytes(
        self,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
        hook: Optional[ContentHook] = None,
        *args: Any,
        **kwargs: Any,
    ) -> bytes:
        if not bucket or not key:
            raise ValueError("bucket and key are required for S3Connector")
        # Do NOT pass hook to read_stream, we apply it here on the full bytes
        buf = bytearray()
        async for chunk in self.read_stream(bucket, key):
            buf.extend(chunk)
        data = bytes(buf)
        if hook:
            data = await hook.post_read(data)
        return data

    async def read_json(
        self,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
        hook: Optional[ContentHook] = None,
    ) -> Any:
        """Read a JSON object from S3."""
        if not bucket or not key:
            raise ValueError("bucket and key are required for S3Connector")
        # Do NOT pass hook to read_bytes, we apply it here on the object
        data = await self.read_bytes(bucket, key)
        obj = json.loads(data.decode("utf-8"))
        if hook:
            obj = await hook.post_read(obj)
        return obj

    async def write_json(
        self,
        data: Any,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
        indent: int = 2,
        hook: Optional[ContentHook] = None,
    ) -> None:
        """Write a JSON object to S3."""
        if not bucket or not key:
            raise ValueError("bucket and key are required for S3Connector")

        if hook:
            data = await hook.pre_write(data)

        content = json.dumps(data, ensure_ascii=False, indent=indent)
        # Do NOT pass hook to write_bytes
        await self.write_bytes(content.encode("utf-8"), bucket, key)

    def _save_checkpoint(self, checkpoint_file: Path, data: dict) -> None:
        if checkpoint_file is None:
            return
        checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(data, f)

    def _load_checkpoint(self, checkpoint_file: Path) -> Optional[dict]:
        if checkpoint_file is None or not checkpoint_file.exists():
            return None
        try:
            return json.loads(checkpoint_file.read_text(encoding="utf-8"))
        except Exception:
            return None

    async def _resume_upload(
        self, client: Any, bucket: str, key: str, checkpoint_file: Optional[Path]
    ) -> Tuple[Optional[str], List[Dict[str, Any]], int, int]:
        upload_id = None
        parts: List[Dict[str, Any]] = []
        part_no = 1
        uploaded_bytes = 0

        if not checkpoint_file or not checkpoint_file.exists():
            return upload_id, parts, part_no, uploaded_bytes

        chk = self._load_checkpoint(checkpoint_file)
        if chk and chk.get("bucket") == bucket and chk.get("key") == key and chk.get("upload_id"):
            upload_id = chk["upload_id"]
            try:
                listed = await client.list_parts(Bucket=bucket, Key=key, UploadId=upload_id)
                existing = {int(p["PartNumber"]): p for p in listed.get("Parts", [])}
                for partnum, info in existing.items():
                    parts.append({"ETag": info["ETag"], "PartNumber": partnum, "size": int(info.get("Size", 0) or 0)})
                    uploaded_bytes += int(info.get("Size", 0) or 0)
            except Exception:
                for p in chk.get("parts", []):
                    parts.append(p)
                    uploaded_bytes += int(p.get("size", 0) or 0)
            if parts:
                part_no = max(int(p["PartNumber"]) for p in parts) + 1

        return upload_id, parts, part_no, uploaded_bytes

    async def _upload_part_from_bytes(
        self,
        client: Any,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        data: bytes,
        semaphore: asyncio.Semaphore,
        progress_callback: Optional[ProgressCallback],
    ) -> Dict[str, Any]:
        async with semaphore:
            resp = await client.upload_part(
                Bucket=bucket, Key=key, PartNumber=part_number, UploadId=upload_id, Body=data
            )
            if progress_callback:
                progress_callback(part_number, len(data))
            return {"ETag": resp["ETag"], "PartNumber": part_number, "size": len(data)}

    async def _upload_part_from_file(
        self,
        client: Any,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        path: str,
        semaphore: asyncio.Semaphore,
        progress_callback: Optional[ProgressCallback],
    ) -> Dict[str, Any]:
        async with semaphore:
            with open(path, "rb") as fh:
                resp = await client.upload_part(
                    Bucket=bucket, Key=key, PartNumber=part_number, UploadId=upload_id, Body=fh
                )
                size = os.path.getsize(path)
            if progress_callback:
                progress_callback(part_number, size)
            return {"ETag": resp["ETag"], "PartNumber": part_number, "size": size}

    async def write_stream(  # noqa: C901
        self,
        stream: AsyncIterator[bytes],
        bucket: Optional[str] = None,
        key: Optional[str] = None,
        hook: Optional[ContentHook] = None,
        *args: Any,
        progress_callback: Optional[ProgressCallback] = None,
        overall_progress: Optional[OverallProgressCallback] = None,
        resume: bool = False,
        checkpoint_path: Optional[str] = None,
        force_multipart: bool = False,
        **kwargs: Any,
    ) -> None:
        """Write an async byte stream to S3 with resumable multipart support.

        If `resume` is True and `checkpoint_path` points to a valid checkpoint file,
        the upload will attempt to resume from the recorded `upload_id` and parts.
        """
        if not bucket or not key:
            raise ValueError("bucket and key are required for S3Connector")
        if self._session is None:
            raise RuntimeError("aioboto3 is not available; install aioboto3 to use S3Connector")
        async with self._session.client("s3", region_name=self._region) as client:
            part_no = 1
            upload_id: Optional[str] = None
            parts: List[Dict[str, Any]] = []
            tasks: List[asyncio.Task] = []
            tmp_files: List[str] = []

            uploaded_bytes = 0
            checkpoint_file: Optional[Path] = Path(checkpoint_path) if checkpoint_path else None

            if resume:
                upload_id, parts, part_no, uploaded_bytes = await self._resume_upload(
                    client, bucket, key, checkpoint_file
                )

            semaphore = asyncio.Semaphore(self.max_concurrency)
            buffer = bytearray()
            try:
                async for chunk in stream:
                    buffer.extend(chunk)
                    if upload_id is None:
                        if force_multipart:
                            mpu = await client.create_multipart_upload(Bucket=bucket, Key=key)
                            upload_id = mpu["UploadId"]
                        else:
                            if uploaded_bytes + len(buffer) < self.min_multipart_upload_size:
                                continue
                            mpu = await client.create_multipart_upload(Bucket=bucket, Key=key)
                            upload_id = mpu["UploadId"]

                    if len(buffer) >= self.part_size:
                        if len(buffer) >= self.spill_to_disk_threshold:
                            tf = tempfile.NamedTemporaryFile(delete=False)
                            tf.write(bytes(buffer))
                            tf.close()
                            tmp_files.append(tf.name)
                            task = asyncio.create_task(
                                self._upload_part_from_file(
                                    client, bucket, key, upload_id, part_no, tf.name, semaphore, progress_callback
                                )
                            )
                        else:
                            task = asyncio.create_task(
                                self._upload_part_from_bytes(
                                    client,
                                    bucket,
                                    key,
                                    upload_id,
                                    part_no,
                                    bytes(buffer),
                                    semaphore,
                                    progress_callback,
                                )
                            )

                        tasks.append(task)
                        part_no += 1
                        buffer.clear()

                        if len(tasks) >= self.max_concurrency:
                            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                            for d in done:
                                res = d.result()
                                parts.append(res)
                                uploaded_bytes += int(res.get("size", 0) or 0)
                                if checkpoint_file is not None and upload_id is not None:
                                    self._save_checkpoint(
                                        checkpoint_file,
                                        {"bucket": bucket, "key": key, "upload_id": upload_id, "parts": parts},
                                    )
                                if overall_progress:
                                    overall_progress(uploaded_bytes, None)
                            tasks = list(pending)

                if upload_id is None:
                    await client.put_object(Bucket=bucket, Key=key, Body=bytes(buffer))
                    if overall_progress:
                        overall_progress(len(buffer), len(buffer))
                    return

                if buffer:
                    if len(buffer) >= self.spill_to_disk_threshold:
                        tf = tempfile.NamedTemporaryFile(delete=False)
                        tf.write(bytes(buffer))
                        tf.close()
                        tmp_files.append(tf.name)
                        task = asyncio.create_task(
                            self._upload_part_from_file(
                                client, bucket, key, upload_id, part_no, tf.name, semaphore, progress_callback
                            )
                        )
                    else:
                        task = asyncio.create_task(
                            self._upload_part_from_bytes(
                                client, bucket, key, upload_id, part_no, bytes(buffer), semaphore, progress_callback
                            )
                        )
                    tasks.append(task)

                if tasks:
                    for t in asyncio.as_completed(tasks):
                        res = await t
                        parts.append(res)
                        uploaded_bytes += int(res.get("size", 0) or 0)
                        if checkpoint_file is not None and upload_id is not None:
                            self._save_checkpoint(
                                checkpoint_file,
                                {"bucket": bucket, "key": key, "upload_id": upload_id, "parts": parts},
                            )
                        if overall_progress:
                            overall_progress(uploaded_bytes, None)

                parts_sorted = sorted(parts, key=lambda p: int(p["PartNumber"]))
                await client.complete_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts_sorted},
                )

                if checkpoint_file is not None and checkpoint_file.exists():
                    try:
                        checkpoint_file.unlink()
                    except Exception:
                        pass
            except Exception:
                if upload_id is not None and checkpoint_file is not None:
                    try:
                        self._save_checkpoint(
                            checkpoint_file,
                            {"bucket": bucket, "key": key, "upload_id": upload_id, "parts": parts},
                        )
                    except Exception:
                        pass
                else:
                    if upload_id is not None:
                        try:
                            await client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
                        except Exception:
                            pass
                raise
            finally:
                for p in tmp_files:
                    try:
                        os.unlink(p)
                    except Exception:
                        pass

    async def write_bytes(
        self,
        data: bytes,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
        hook: Optional[ContentHook] = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if hook:
            data = await hook.pre_write(data)

        async def gen() -> AsyncIterator[bytes]:
            yield data

        await self.write_stream(gen(), bucket, key, *args, **kwargs)

    async def close(self) -> None:
        return

    async def ping(self) -> bool:
        """Check AWS connectivity by listing buckets."""
        try:
            if self._session is None:
                return False
            async with self._session.client("s3", region_name=self._region) as client:
                await client.list_buckets()
            return True
        except Exception:
            return False
