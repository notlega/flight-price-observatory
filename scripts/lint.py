"""Run project linting (ruff) and type checking (basedpyright)."""

import shutil
import subprocess
import sys
from pathlib import Path

_TOOLS = ("ruff", "basedpyright")


def _resolve(tool: str) -> str:
    venv_bin = Path(sys.prefix) / "bin" / tool
    if venv_bin.is_file():
        return str(venv_bin)
    resolved = shutil.which(tool)
    if resolved is None:
        raise SystemExit(
            f"missing tool '{tool}' (install dev group: uv sync --group dev)"
        )
    return resolved


def main() -> None:
    raise SystemExit(
        subprocess.call([_resolve("ruff"), "format", "--check", "."])
        or subprocess.call([_resolve("ruff"), "check", "."])
        or subprocess.call([_resolve("basedpyright")])
    )
