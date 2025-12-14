import json
from typing import Any, Iterator, Optional

try:
    import boto3
except ImportError:
    boto3 = None

from ...interfaces import SyncConnector, SyncContentHook, SyncReadable, SyncWritable


class IteratorFile:
    """Helper to convert an iterator of bytes into a file-like object for boto3."""

    def __init__(self, iterator):
        self._iterator = iterator
        self._buffer = b""

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""

        # If we have enough in buffer, return it
        if 0 < size <= len(self._buffer):
            ret = self._buffer[:size]
            self._buffer = self._buffer[size:]
            return ret

        # Consume iterator
        try:
            while size < 0 or len(self._buffer) < size:
                chunk = next(self._iterator)
                self._buffer += chunk
        except StopIteration:
            pass

        if size < 0:
            ret = self._buffer
            self._buffer = b""
            return ret
        else:
            ret = self._buffer[:size]
            self._buffer = self._buffer[size:]
            return ret


class SyncS3Connector(SyncConnector, SyncReadable, SyncWritable):
    """
    Synchronous S3 Connector using boto3.
    """

    def __init__(
        self,
        region_name: str = "eu-central-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        session_token: Optional[str] = None,
        profile_name: Optional[str] = None,
    ):
        if boto3 is None:
            raise ImportError("boto3 is required for SyncS3Connector. Install with `pip install multids[sync]`")

        self._session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=session_token,
            profile_name=profile_name,
            region_name=region_name,
        )
        self._client = self._session.client("s3")

    def close(self) -> None:
        if self._client:
            self._client.close()

    def ping(self) -> bool:
        try:
            # Simple list buckets as a connectivity check
            self._client.list_buckets()
            return True
        except Exception:
            return False

    def read_stream(
        self,
        bucket: str = "",
        key: str = "",
        chunk_size: int = 65536,
        hook: Optional[SyncContentHook] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Iterator[bytes]:
        if not bucket or not key:
            raise ValueError("bucket and key are required")

        response = self._client.get_object(Bucket=bucket, Key=key)
        stream = response["Body"]

        # If hook exists, it wraps the stream logic
        if hook:
            # hook.post_read should return an iterator if it wraps the stream
            # or we might need to apply it differently.
            # Protocol says: post_read(data). If data is stream, it transforms stream.
            yield from hook.post_read(stream)
        else:
            for chunk in stream.iter_chunks(chunk_size):
                yield chunk

    def read_bytes(
        self,
        bucket: str = "",
        key: str = "",
        hook: Optional[SyncContentHook] = None,
        *args: Any,
        **kwargs: Any,
    ) -> bytes:
        if not bucket or not key:
            raise ValueError("bucket and key are required")

        # Optimization: use read_stream to aggregate or direct get if small?
        # Let's use read_stream to be consistent
        buf = bytearray()
        for chunk in self.read_stream(bucket, key):
            buf.extend(chunk)
        data = bytes(buf)

        if hook:
            data = hook.post_read(data)
        return data

    def read_json(
        self,
        bucket: str,
        key: str,
        hook: Optional[SyncContentHook] = None,
    ) -> Any:
        # Do not use read_bytes hook logic, we apply hook to the object
        buf = bytearray()
        # Call internal read_stream without hook to get raw bytes
        response = self._client.get_object(Bucket=bucket, Key=key)
        for chunk in response["Body"].iter_chunks():
            buf.extend(chunk)

        data = bytes(buf)
        obj = json.loads(data.decode("utf-8"))

        if hook:
            obj = hook.post_read(obj)
        return obj

    def write_stream(
        self,
        stream: Iterator[bytes],
        bucket: str = "",
        key: str = "",
        hook: Optional[SyncContentHook] = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if not bucket or not key:
            raise ValueError("bucket and key are required")

        if hook:
            stream = hook.pre_write(stream)

        fileobj = IteratorFile(stream)
        self._client.upload_fileobj(fileobj, bucket, key)

    def write_bytes(
        self,
        data: bytes,
        bucket: str = "",
        key: str = "",
        hook: Optional[SyncContentHook] = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if not bucket or not key:
            raise ValueError("bucket and key are required")

        if hook:
            data = hook.pre_write(data)

        # Direct put for bytes
        self._client.put_object(Bucket=bucket, Key=key, Body=data)

    def write_json(
        self,
        data: Any,
        bucket: str,
        key: str,
        indent: int = 2,
        hook: Optional[SyncContentHook] = None,
    ) -> None:
        if hook:
            data = hook.pre_write(data)

        content = json.dumps(data, ensure_ascii=False, indent=indent)
        self.write_bytes(content.encode("utf-8"), bucket, key)
