"""Syntax-check every Python fence in the published Markdown documentation."""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FENCE = re.compile(r"```(?:python|py)\s*\n(.*?)```", re.DOTALL | re.IGNORECASE)


def main() -> None:
    failures = []
    for path in (ROOT / "README.md", *(ROOT / "docs").glob("*.md")):
        for number, code in enumerate(FENCE.findall(path.read_text(encoding="utf-8")), start=1):
            try:
                compile(code, f"{path}:fence-{number}", "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
            except SyntaxError as exc:
                failures.append(f"{path}: fence {number}: {exc.msg} (line {exc.lineno})")
    if failures:
        raise SystemExit("\n".join(failures))
    print("Documentation Python fences are syntactically valid.")


if __name__ == "__main__":
    main()
