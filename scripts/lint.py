"""Run project linting (ruff) and type checking (basedpyright)."""

import subprocess


def main() -> None:
    raise SystemExit(
        subprocess.call(["ruff", "format", "--check", "."])
        or subprocess.call(["ruff", "check", "."])
        or subprocess.call(["basedpyright"])
    )
