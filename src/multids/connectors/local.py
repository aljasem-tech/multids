from __future__ import annotations

import json
from typing import Any, AsyncIterator

import aiofiles

from ..interfaces import Connector, Readable, Writable


class LocalConnector(Connector, Readable, Writable):
    async def read(self, path: str) -> AsyncIterator[bytes]:
        async with aiofiles.open(path, "rb") as f:
            while True:
                chunk = await f.read(64 * 1024)
                if not chunk:
                    break
                yield chunk

    async def write(self, stream: AsyncIterator[bytes], path: str) -> None:
        async with aiofiles.open(path, "wb") as f:
            async for chunk in stream:
                await f.write(chunk)

    async def read_json(self, path: str) -> Any:
        """Read a JSON file and return the parsed object."""
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            content = await f.read()
            return json.loads(content)

    async def write_json(self, data: Any, path: str, indent: int = 2) -> None:
        """Write an object as JSON to a file."""
        async with aiofiles.open(path, "w", encoding="utf-8") as f:
            content = json.dumps(data, indent=indent, ensure_ascii=False)
            await f.write(content)

    async def close(self) -> None:
        return

    async def ping(self) -> bool:
        """Check if the filesystem is accessible (always True)."""
        return True
