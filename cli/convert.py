"""Convert subcommand: export search DB to JSONL."""

import argparse
import asyncio

from collector.config import DEFAULT_DB_PATH
from collector.convert import convert


def configure_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],  # type: ignore[reportPrivateUsage]
) -> None:
    p = subparsers.add_parser("convert", help="Export SQLite state to JSONL")
    p.add_argument(
        "db",
        nargs="?",
        type=str,
        default=DEFAULT_DB_PATH,
        help=f"Path to SQLite state file (default: {DEFAULT_DB_PATH})",
    )
    p.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSONL path (default: storage/raw/search_YYYYMMDD_HHMMSS.jsonl)",
    )
    p.set_defaults(func=run)


def run(args: argparse.Namespace) -> None:
    output = asyncio.run(convert(args.db, args.output))
    print(f"Output: {output}")
