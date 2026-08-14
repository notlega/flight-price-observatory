"""Search subcommand: run bulk flight price collection."""

import asyncio
from datetime import date, timedelta
from typing import Any

from collector import CollectorManager, ProviderRegistry
from collector.config import (
    DEFAULT_CURRENCY,
    DEFAULT_MAX_DAYS_AHEAD,
    DEFAULT_RATE,
    DEFAULT_WORKERS,
)


def configure_parser(subparsers: Any):
    p = subparsers.add_parser("search", help="Bulk flight search")
    p.add_argument(
        "--start",
        type=date.fromisoformat,
        default=date.today(),
        help="Start date (YYYY-MM-DD, default: today)",
    )
    p.add_argument(
        "--max-days",
        type=int,
        default=DEFAULT_MAX_DAYS_AHEAD,
        help=f"Max days ahead from start (default: {DEFAULT_MAX_DAYS_AHEAD})",
    )
    p.add_argument(
        "--currency",
        type=str,
        default=DEFAULT_CURRENCY,
        help=f"Currency code for pricing (default: {DEFAULT_CURRENCY})",
    )
    p.add_argument(
        "--rate",
        type=float,
        default=DEFAULT_RATE,
        help=f"Requests per second (default: {DEFAULT_RATE})",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Max concurrent searches (default: {DEFAULT_WORKERS})",
    )
    p.add_argument(
        "--keep-db",
        action="store_true",
        help="Keep SQLite state file after JSONL export (debug)",
    )
    p.set_defaults(func=run)


def _ahead_days(end: date) -> int:
    return max((end - date.today()).days, 0)


def run(args):
    asyncio.run(_async_run(args))


async def _async_run(args):
    start = args.start
    end = start + timedelta(days=args.max_days)

    registry = ProviderRegistry()
    manager = CollectorManager(registry)
    await manager.run(
        start_date=start,
        end_date=end,
        max_days_ahead=_ahead_days(end),
        currency=args.currency,
        rate=args.rate,
        workers=args.workers,
        keep_db=args.keep_db,
    )
