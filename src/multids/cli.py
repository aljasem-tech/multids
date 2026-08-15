"""Small command-line entry point for local connector operations."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from .connectors.local import LocalConnector
from .workflows import copy_stream


def _load_config(path: str) -> dict[str, Any]:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read configuration: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("Configuration must be a JSON object")
    return data


def _validate_config(data: dict[str, Any]) -> None:
    command = data.get("command")
    if command not in {"ping", "copy"}:
        raise ValueError("Configuration command must be 'ping' or 'copy'")
    if command == "copy":
        for field in ("source", "destination"):
            value = data.get(field)
            if not isinstance(value, dict) or value.get("connector") != "local" or not value.get("path"):
                raise ValueError(f"copy.{field} must contain connector='local' and a path")


async def _ping(path: str) -> int:
    connector = LocalConnector(path)
    print("local: ok" if await connector.ping() else "local: unavailable")
    return 0


async def _copy(source: str, destination: str) -> int:
    source_path = Path(source)
    destination_path = Path(destination)
    source_connector = LocalConnector(str(source_path.parent))
    destination_connector = LocalConnector(str(destination_path.parent))
    result = await copy_stream(
        source_connector,
        destination_connector,
        source_args=(source_path.name,),
        destination_args=(destination_path.name,),
    )
    print(f"copied {result.bytes_copied} bytes in {result.chunks_copied} chunks")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="multids", description="Utility commands for multids connectors")
    commands = parser.add_subparsers(dest="command", required=True)
    ping = commands.add_parser("ping", help="check a local connector path")
    ping.add_argument("--path", default=".")
    copy = commands.add_parser("copy", help="copy a local file using the streaming workflow")
    copy.add_argument("source")
    copy.add_argument("destination")
    validate = commands.add_parser("validate", help="validate a local-workflow JSON configuration")
    validate.add_argument("config")
    args = parser.parse_args(argv)

    try:
        if args.command == "ping":
            return asyncio.run(_ping(args.path))
        if args.command == "copy":
            return asyncio.run(_copy(args.source, args.destination))
        data = _load_config(args.config)
        _validate_config(data)
        print("configuration: valid")
        return 0
    except Exception as exc:
        parser.error(str(exc))
    return 2  # pragma: no cover


if __name__ == "__main__":  # pragma: no cover
    main()
