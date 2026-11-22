"""Execute the self-contained examples from README.md and docs/ in CI.

Only examples that do not require external services (local files, CLI) are
run here. Service-backed examples (S3, OpenSearch, SQL) are covered by the
Real Service Integration workflow.
"""

import asyncio
import json
import subprocess
import sys
from pathlib import Path

from multids.connectors.local import LocalConnector
from multids.connectors.sync.local import SyncLocalConnector

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_readme_local_json_example(tmp_path, monkeypatch):
    """The README 'Async: write and read JSON locally' example."""
    monkeypatch.chdir(tmp_path)

    async def main():
        local = LocalConnector()
        data = {"message": "Hello", "lang": "你好"}
        await local.write_json(data, "data.json")
        return await local.read_json("data.json")

    result = asyncio.run(main())
    assert result == {"message": "Hello", "lang": "你好"}
    # Unicode must be preserved, not escaped.
    raw = (tmp_path / "data.json").read_text(encoding="utf-8")
    assert "你好" in raw


def test_quickstart_local_example(tmp_path, monkeypatch):
    """The quickstart local-file example."""
    monkeypatch.chdir(tmp_path)

    async def main():
        local = LocalConnector()
        await local.write_json({"message": "Hello", "lang": "你好"}, "data.json")
        return await local.read_json("data.json")

    assert asyncio.run(main()) == {"message": "Hello", "lang": "你好"}


def test_sync_local_example(tmp_path, monkeypatch):
    """The sync local-file example from the quickstart."""
    monkeypatch.chdir(tmp_path)

    local = SyncLocalConnector()
    local.write_bytes(b"hello", "out.txt")
    assert local.read_bytes("out.txt") == b"hello"


def test_basic_usage_example_script_runs():
    """The examples/basic_usage.py script must run and produce output."""
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "examples" / "basic_usage.py")],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        timeout=30,
    )
    assert result.returncode == 0, f"basic_usage.py failed: {result.stderr}"
    assert "hello world" in result.stdout


def test_cli_ping_and_copy_end_to_end(tmp_path):
    """The CLI ping and copy commands work as documented."""
    from multids.cli import main

    # ping
    assert main(["ping", "--path", str(tmp_path)]) == 0

    # copy
    source = tmp_path / "input.txt"
    source.write_bytes(b"cli copy payload")
    dest = tmp_path / "out" / "output.txt"
    assert main(["copy", str(source), str(dest)]) == 0
    assert dest.read_bytes() == b"cli copy payload"

    # validate
    config = tmp_path / "workflow.json"
    config.write_text(
        json.dumps(
            {
                "command": "copy",
                "source": {"connector": "local", "path": "input.txt"},
                "destination": {"connector": "local", "path": "output.txt"},
            }
        ),
        encoding="utf-8",
    )
    assert main(["validate", str(config)]) == 0


def test_workflow_copy_stream_example(tmp_path):
    """The workflows copy_stream local-to-local example."""
    from multids.workflows import copy_stream

    local_a = LocalConnector(base_path=str(tmp_path / "a"))
    local_b = LocalConnector(base_path=str(tmp_path / "b"))

    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "source.txt").write_bytes(b"workflow payload")

    async def run():
        return await copy_stream(
            local_a,
            local_b,
            source_args=("source.txt",),
            destination_args=("dest.txt",),
        )

    result = asyncio.run(run())
    assert result.bytes_copied == len(b"workflow payload")
    assert (tmp_path / "b" / "dest.txt").read_bytes() == b"workflow payload"
