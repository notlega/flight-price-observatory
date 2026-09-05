"""Transform subcommand: export raw JSONL to conformed Parquet (silver).

Unnests the ``flights`` array so each row represents one flight option.
Casts column types, maps full airport names → IATA codes, partitions by
``origin``/``destination`` with zstd compression, and re-materializes the
partition columns (DuckDB's ``PARTITION_BY`` strips them from the files).

The SQL-building and partition-materialization helpers live in
``collector.silver``; this module orchestrates them and exposes
:func:`transform_jsonl`, also used by operational scripts.

Schema (silver v2):
  origin, destination     VARCHAR  IATA (materialized into files)
  dep_date, return_date   DATE
  flight_type             VARCHAR  ONE_WAY | ROUND_TRIP
  searched_at             TIMESTAMP
  price                   DECIMAL(12,2)
  price_present           BOOLEAN  false when fare unavailable
  currency                VARCHAR
  duration_minutes        INTEGER
  stops                   INTEGER
  airline                 VARCHAR  display name (code lives in booking_token)
  co2_emissions_g         INTEGER
  emissions_tag           VARCHAR
  booking_token           VARCHAR  opaque fare token
  run_ts                  VARCHAR  YYYYMMDD_HHMMSS collection run id
  lead_days               INTEGER  dep_date - searched_at (advance-purchase window)
  direction               VARCHAR  ROUND_TRIP | OUTBOUND | RETURN | OTHER
  itinerary_id            VARCHAR  MD5(searched_at, booking_token, flight_type)
"""

import argparse
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from collector.silver import (
    build_query,
    materialize_partition_cols,
    raw_columns,
)

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


def run(args: argparse.Namespace) -> None:
    """Convert the input JSONL to partitioned Parquet and print the output."""
    input_path = args.input or _latest_jsonl()
    output = args.output or f"{DEFAULT_SILVER_DIR}/{datetime.now():%Y%m%d_%H%M%S}"
    run_ts = re.fullmatch(r"(\d{8})_(\d{6})", output.rsplit("/", 1)[-1])
    run_ts = run_ts.group(0) if run_ts else None

    total = transform_jsonl(input_path, output, run_ts=run_ts)

    logger.info("Wrote %d rows to %s", total, output)
    print(f"Output: {output}")


def transform_jsonl(input_path: str, output_dir: str, run_ts: str | None = None) -> int:
    """Transform a single JSONL file to partitioned Parquet. Returns row count.

    Used by ``scripts/backfill.py`` and ``scripts/optimize_r2.py``.
    """
    import duckdb

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    con: Any = duckdb.connect()
    try:
        columns = raw_columns(con, input_path)
        query = build_query(input_path, columns, run_ts=run_ts)
        count_row = con.execute(f"SELECT count(*) FROM ({query})").fetchone()
        total = count_row[0] if count_row else 0

        con.execute(
            f"COPY ({query}) TO '{_sql_literal_runtime(str(output))}' "
            "(FORMAT PARQUET, PARTITION_BY (origin, destination), "
            "COMPRESSION 'zstd', OVERWRITE_OR_IGNORE)"
        )
    finally:
        con.close()

    materialize_partition_cols(output)

    return total


def _sql_literal_runtime(value: str) -> str:
    """Escape ``value`` for SQL string literal interpolation."""
    return value.replace("'", "''")
