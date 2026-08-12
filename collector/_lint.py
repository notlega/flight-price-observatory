import subprocess


def main() -> None:
    raise SystemExit(
        subprocess.call(["ruff", "check", "."]) or subprocess.call(["basedpyright"])
    )
