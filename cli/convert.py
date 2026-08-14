"""Convert subcommand: export search DB to JSONL."""

import asyncio
from typing import Any

from collector.config import DEFAULT_DB_PATH
from collector.convert import convert


def configure_parser(subparsers: Any):
    p = subparsers.add_parser("convert", help="Convert SQLite state to JSONL")
    p.add_argument(
        "--db",
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
    p.add_argument(
        "--keep-db",
        action="store_true",
        help="Keep SQLite file after conversion (default: delete)",
    )
    p.set_defaults(func=run)


def run(args):
    output = asyncio.run(convert(args.db, args.output, delete=not args.keep_db))
    print(f"Output: {output}")
