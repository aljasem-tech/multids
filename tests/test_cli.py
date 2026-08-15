import json

from multids.cli import main


def test_cli_copies_local_file_and_validates_config(tmp_path):
    source = tmp_path / "source.txt"
    destination = tmp_path / "nested" / "destination.txt"
    source.write_bytes(b"hello")

    assert main(["copy", str(source), str(destination)]) == 0
    assert destination.read_bytes() == b"hello"

    config = tmp_path / "workflow.json"
    config.write_text(
        json.dumps(
            {
                "command": "copy",
                "source": {"connector": "local", "path": "source.txt"},
                "destination": {"connector": "local", "path": "destination.txt"},
            }
        ),
        encoding="utf-8",
    )
    assert main(["validate", str(config)]) == 0
