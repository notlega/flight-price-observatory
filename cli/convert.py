import asyncio
import logging
from typing import Any

from collector.convert import convert


def configure_parser(subparsers: Any):
    p = subparsers.add_parser("convert", help="Convert SQLite state to JSONL")
    p.add_argument(
        "--db",
        type=str,
        default="storage/db/search_state.db",
        help="Path to SQLite state file (default: storage/db/search_state.db)",
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    output = asyncio.run(convert(args.db, args.output, delete=not args.keep_db))
    print(f"Output: {output}")
