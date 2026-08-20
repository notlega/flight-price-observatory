"""Transform subcommand: export raw JSONL to Parquet (silver)."""

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import cast

logger = logging.getLogger(__name__)

DEFAULT_RAW_DIR = "storage/raw"
DEFAULT_SILVER_DIR = "storage/silver"


def configure_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],  # type: ignore[reportPrivateUsage]
) -> None:
    """Register the ``transform`` subcommand on ``subparsers``."""
    p = subparsers.add_parser("transform", help="Convert raw JSONL to Parquet")
    p.add_argument(
        "--input",
        type=str,
        default=None,
        help=f"Input JSONL path (default: latest search_*.jsonl in {DEFAULT_RAW_DIR})",
    )
    p.add_argument(
        "--output",
        type=str,
        default=None,
        help=f"Output directory (default: {DEFAULT_SILVER_DIR}/<timestamp>)",
    )
    p.set_defaults(func=run)


def _latest_jsonl() -> str:
    """Return the most recently modified search JSONL file path."""
    raw = Path(DEFAULT_RAW_DIR)
    candidates = sorted(
        raw.glob("search_*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True
    )
    if not candidates:
        raise SystemExit(f"error: no search_*.jsonl found in {raw}")
    return str(candidates[0])


def _sql_literal(value: str) -> str:
    """Escape a value for safe interpolation into a SQL string literal."""
    return value.replace("'", "''")


def run(args: argparse.Namespace) -> None:
    """Convert the input JSONL to partitioned Parquet and print the output."""
    import duckdb

    input_path = args.input or _latest_jsonl()
    output = args.output or f"{DEFAULT_SILVER_DIR}/{datetime.now():%Y%m%d_%H%M%S}"
    Path(output).mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        query = (
            f"SELECT * FROM read_json_auto('{_sql_literal(input_path)}', "
            "format='newline_delimited', union_by_name=true)"
        )
        count_row = con.execute(f"SELECT count(*) FROM ({query})").fetchone()
        if count_row is None:
            raise SystemExit("error: failed to read row count")
        total = cast(int, count_row[0])
        con.execute(
            f"COPY ({query}) TO '{_sql_literal(output)}' "
            "(FORMAT PARQUET, PARTITION_BY (route), OVERWRITE_OR_IGNORE)"
        )
    finally:
        con.close()

    logger.info("Wrote %d rows to %s", total, output)
    print(f"Output: {output}")
