from __future__ import annotations

import json
from pathlib import Path
from typing import Any, AsyncIterator, Optional

try:
    import aiofiles
except ImportError:  # pragma: no cover - exercised in minimal installations
    aiofiles = None

from ..contracts import ObjectInfo, ObjectPage, ObjectRef, WriteResult
from ..errors import ConnectorDependencyError
from ..interfaces import AsyncConnectorContext, Connector, Readable, Writable


class LocalConnector(AsyncConnectorContext, Connector, Readable, Writable):
    """Async filesystem connector with the same byte-oriented API as S3.

    Relative paths are resolved from ``base_path`` when supplied, which makes a
    connector convenient to reuse for a directory or a temporary workspace.
    """

    def __init__(self, base_path: Optional[str] = None):
        if aiofiles is None:
            raise ConnectorDependencyError("aiofiles is required; install it with `pip install multids[local]`")
        self._base_path = Path(base_path) if base_path else None

    def _resolve_path(self, path: str) -> Path:
        if not path:
            raise ValueError("path is required")
        candidate = Path(path)
        return self._base_path / candidate if self._base_path and not candidate.is_absolute() else candidate

    async def read_stream(self, path: str, chunk_size: int = 64 * 1024) -> AsyncIterator[bytes]:
        """Yield a file's bytes in chunks."""
        full_path = self._resolve_path(path)
        async with aiofiles.open(full_path, "rb") as f:
            while True:
                chunk = await f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    async def read_bytes(self, path: str) -> bytes:
        """Read a complete file into memory."""
        async with aiofiles.open(self._resolve_path(path), "rb") as f:
            return await f.read()

    async def write_stream(self, stream: AsyncIterator[bytes], path: str) -> None:
        """Write a stream of bytes, creating parent directories as needed."""
        full_path = self._resolve_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(full_path, "wb") as f:
            async for chunk in stream:
                await f.write(chunk)

    async def write_bytes(self, data: bytes, path: str) -> None:
        """Write bytes to a file, creating parent directories as needed."""
        full_path = self._resolve_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(full_path, "wb") as f:
            await f.write(data)

    # Backwards-compatible aliases for the original local connector API.
    read = read_stream
    write = write_stream

    async def read_json(self, path: str) -> Any:
        """Read a JSON file and return the parsed object."""
        async with aiofiles.open(self._resolve_path(path), "r", encoding="utf-8") as f:
            content = await f.read()
            return json.loads(content)

    async def write_json(self, data: Any, path: str, indent: int = 2) -> None:
        """Write an object as JSON to a file."""
        full_path = self._resolve_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(full_path, "w", encoding="utf-8") as f:
            content = json.dumps(data, indent=indent, ensure_ascii=False)
            await f.write(content)

    async def list_objects(self, prefix: str = "", recursive: bool = True) -> AsyncIterator[dict[str, Any]]:
        """Yield S3-shaped metadata for files below ``prefix``.

        The name mirrors :meth:`S3Connector.list_objects`, so code can use the
        same operation for local staging and object storage.
        """
        root = self._resolve_path(prefix) if prefix else (self._base_path or Path.cwd())
        if not root.exists():
            return
        iterator = root.rglob("*") if recursive else root.glob("*")
        for entry in iterator:
            if entry.is_file():
                key = str(entry.relative_to(self._base_path)) if self._base_path else str(entry)
                stat = entry.stat()
                yield {"Key": key, "Size": stat.st_size, "LastModified": stat.st_mtime}

    async def list_keys(self, prefix: str = "", recursive: bool = True) -> AsyncIterator[str]:
        """Yield file paths below ``prefix`` without their metadata."""
        async for obj in self.list_objects(prefix, recursive):
            yield obj["Key"]

    async def delete_object(self, path: str, missing_ok: bool = False) -> None:
        """Delete a local file using the same operation name as S3."""
        full_path = self._resolve_path(path)
        try:
            full_path.unlink()
        except FileNotFoundError:
            if not missing_ok:
                raise

    async def read_object(self, ref: ObjectRef) -> bytes:
        return await self.read_bytes(ref.key)

    async def write_object(self, ref: ObjectRef, data: bytes) -> WriteResult:
        await self.write_bytes(data, ref.key)
        return WriteResult(bytes_written=len(data))

    async def list_object_page(
        self, prefix: ObjectRef = ObjectRef(""), *, cursor: Optional[str] = None, page_size: int = 1_000
    ) -> ObjectPage:
        if page_size <= 0:
            raise ValueError("page_size must be greater than zero")
        objects = []
        async for item in self.list_objects(prefix.key):
            objects.append(item)
        objects.sort(key=lambda item: item["Key"])
        start = int(cursor) if cursor else 0
        page = objects[start : start + page_size]
        next_cursor = str(start + page_size) if start + page_size < len(objects) else None
        return ObjectPage(
            items=tuple(ObjectInfo(item["Key"], item["Size"], item["LastModified"]) for item in page),
            next_cursor=next_cursor,
        )

    async def delete_object_ref(self, ref: ObjectRef) -> WriteResult:
        await self.delete_object(ref.key)
        return WriteResult(items_written=1)

    async def close(self) -> None:
        return

    async def ping(self) -> bool:
        """Check if the filesystem is accessible (always True)."""
        return True
