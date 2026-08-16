"""Search subcommand: run bulk flight price collection."""

import argparse
import asyncio
import sys
from datetime import date, timedelta

from collector import CollectorManager, ProviderRegistry
from collector.config import (
    DEFAULT_CURRENCY,
    DEFAULT_MAX_DAYS_AHEAD,
    DEFAULT_RATE,
    DEFAULT_WORKERS,
)
from collector.errors import RepositoryStateError


def configure_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],  # type: ignore[reportPrivateUsage]
) -> None:
    """Register the ``search`` subcommand on ``subparsers``."""
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
        "--continue",
        dest="continue_run",
        action="store_true",
        help="Retry only failed tasks from an existing database "
        "(--start/--max-days ignored)",
    )
    p.set_defaults(func=run)


def _ahead_days(end: date) -> int:
    """Return days from today to ``end``, floored at zero."""
    return max((end - date.today()).days, 0)


def run(args: argparse.Namespace) -> None:
    """Run the search subcommand, exiting non-zero on repository state errors."""
    try:
        asyncio.run(_async_run(args))
    except RepositoryStateError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)


async def _async_run(args: argparse.Namespace) -> None:
    """Build the search window and delegate to the collector manager."""
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
        continue_run=args.continue_run,
    )
