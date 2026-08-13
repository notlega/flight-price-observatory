"""Search subcommand: run bulk flight price collection."""

import asyncio
from datetime import date, timedelta
from typing import Any

from collector import CollectorManager, ProviderRegistry


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
        default=330,
        help="Max days ahead from start (default: 330)",
    )
    p.add_argument(
        "--currency",
        type=str,
        default="SGD",
        help="Currency code for pricing (default: SGD)",
    )
    p.add_argument(
        "--rate",
        type=float,
        default=200,
        help="Requests per second (default: 200)",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=50,
        help="Max concurrent searches (default: 50)",
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
