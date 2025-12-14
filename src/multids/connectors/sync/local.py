import json
from pathlib import Path
from typing import Any, Iterator, Optional

from ...interfaces import SyncConnector, SyncReadable, SyncWritable


class SyncLocalConnector(SyncConnector, SyncReadable, SyncWritable):
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
