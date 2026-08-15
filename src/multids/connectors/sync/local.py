import json
from pathlib import Path
from typing import Any, Iterator, Optional

from ...contracts import ObjectInfo, ObjectPage, ObjectRef, WriteResult
from ...interfaces import SyncConnector, SyncConnectorContext, SyncReadable, SyncWritable


class SyncLocalConnector(SyncConnectorContext, SyncConnector, SyncReadable, SyncWritable):
    """
    Synchronous connector to read/write files from the local filesystem.
    """

    def __init__(self, base_path: Optional[str] = None):
        self._base_path = Path(base_path) if base_path else Path.cwd()

    def close(self) -> None:
        pass

    def ping(self) -> bool:
        return True

    def _resolve_path(self, path: str) -> Path:
        p = Path(path)
        if not p.is_absolute():
            p = self._base_path / p
        return p

    def read_stream(self, path: str = "", chunk_size: int = 65536, *args: Any, **kwargs: Any) -> Iterator[bytes]:
        """Yield chunks of bytes from a local file."""
        if not path:
            raise ValueError("path is required")
        full_path = self._resolve_path(path)
        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")

        with open(full_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    def read_bytes(self, path: str = "", *args: Any, **kwargs: Any) -> bytes:
        """Read whole file as bytes."""
        if not path:
            raise ValueError("path is required")
        full_path = self._resolve_path(path)
        return full_path.read_bytes()

    def read_json(self, path: str) -> Any:
        content = self.read_bytes(path)
        return json.loads(content.decode("utf-8"))

    def write_stream(self, stream: Iterator[bytes], path: str = "", *args: Any, **kwargs: Any) -> None:
        """Write stream of bytes to a local file."""
        if not path:
            raise ValueError("path is required")
        full_path = self._resolve_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with open(full_path, "wb") as f:
            for chunk in stream:
                f.write(chunk)

    def write_bytes(self, data: bytes, path: str = "", *args: Any, **kwargs: Any) -> None:
        """Write bytes to a local file."""
        if not path:
            raise ValueError("path is required")
        full_path = self._resolve_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(data)

    def write_json(self, data: Any, path: str, indent: int = 2) -> None:
        content = json.dumps(data, ensure_ascii=False, indent=indent)
        self.write_bytes(content.encode("utf-8"), path)

    def list_objects(self, prefix: str = "", recursive: bool = True) -> Iterator[dict[str, Any]]:
        """Yield S3-shaped metadata for files below ``prefix``."""
        root = self._resolve_path(prefix) if prefix else self._base_path
        if not root.exists():
            return
        iterator = root.rglob("*") if recursive else root.glob("*")
        for entry in iterator:
            if entry.is_file():
                stat = entry.stat()
                yield {
                    "Key": str(entry.relative_to(self._base_path)),
                    "Size": stat.st_size,
                    "LastModified": stat.st_mtime,
                }

    def list_keys(self, prefix: str = "", recursive: bool = True) -> Iterator[str]:
        """Yield file paths below ``prefix`` without their metadata."""
        yield from (obj["Key"] for obj in self.list_objects(prefix, recursive))

    def delete_object(self, path: str, missing_ok: bool = False) -> None:
        """Delete a local file using the same operation name as S3."""
        try:
            self._resolve_path(path).unlink()
        except FileNotFoundError:
            if not missing_ok:
                raise

    def read_object(self, ref: ObjectRef) -> bytes:
        return self.read_bytes(ref.key)

    def write_object(self, ref: ObjectRef, data: bytes) -> WriteResult:
        self.write_bytes(data, ref.key)
        return WriteResult(bytes_written=len(data))

    def list_object_page(
        self, prefix: ObjectRef = ObjectRef(""), *, cursor: Optional[str] = None, page_size: int = 1_000
    ) -> ObjectPage:
        if page_size <= 0:
            raise ValueError("page_size must be greater than zero")
        objects = sorted(self.list_objects(prefix.key), key=lambda item: item["Key"])
        start = int(cursor) if cursor else 0
        page = objects[start : start + page_size]
        next_cursor = str(start + page_size) if start + page_size < len(objects) else None
        return ObjectPage(
            items=tuple(ObjectInfo(item["Key"], item["Size"], item["LastModified"]) for item in page),
            next_cursor=next_cursor,
        )

    def delete_object_ref(self, ref: ObjectRef) -> WriteResult:
        self.delete_object(ref.key)
        return WriteResult(items_written=1)
