"""Transform subcommand: export raw JSONL to optimised Parquet (silver).

Unnests the ``flights`` array so each row represents one flight option.
Casts column types, maps full airport names → IATA codes, and partitions
by ``origin``/``destination``/``year``/``month`` with zstd compression.
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path

from collector.airports import AIRPORT_NAME_TO_IATA

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


def _iata_case_expr(col: str) -> str:
    """Build DuckDB CASE expression mapping airport names → IATA codes."""
    cases = [
        f"WHEN {col} = '{_sql_literal(name)}' THEN '{code}'"
        for name, code in AIRPORT_NAME_TO_IATA.items()
    ]
    return f"CASE {' '.join(cases)} ELSE {col} END"


def run(args: argparse.Namespace) -> None:
    """Convert the input JSONL to partitioned Parquet and print the output."""
    import duckdb

    input_path = args.input or _latest_jsonl()
    output = args.output or f"{DEFAULT_SILVER_DIR}/{datetime.now():%Y%m%d_%H%M%S}"
    Path(output).mkdir(parents=True, exist_ok=True)

    iata_origin = _iata_case_expr("origin")
    iata_dest = _iata_case_expr("destination")

    con = duckdb.connect()
    try:
        query = f"""
            WITH raw AS (
                SELECT * FROM read_json_auto(
                    '{_sql_literal(input_path)}',
                    format='newline_delimited',
                    union_by_name=true
                )
            ),
            unnested AS (
                SELECT
                    origin,
                    destination,
                    dep_date::DATE        AS dep_date,
                    CASE WHEN return_date::VARCHAR = '' THEN NULL
                         ELSE TRY_CAST(return_date::VARCHAR AS DATE)
                    END AS return_date,
                    flight_type::VARCHAR  AS flight_type,
                    searched_at::TIMESTAMP AS searched_at,
                    f.price::FLOAT        AS price,
                    f.currency::VARCHAR   AS currency,
                    f.duration::INT       AS duration_minutes,
                    f.stops::INT          AS stops,
                    f.primary_airline::VARCHAR     AS airline,
                    f.primary_airline_name::VARCHAR AS airline_name,
                    f.co2_emissions_g::INT AS co2_emissions_g,
                    f.emissions_tag::VARCHAR AS emissions_tag,
                    f.booking_token::VARCHAR AS booking_token
                FROM raw, UNNEST(flights) AS t(f)
            )
            SELECT
                ({iata_origin}) AS origin,
                ({iata_dest}) AS destination,
                dep_date,
                return_date,
                flight_type,
                searched_at,
                price,
                currency,
                duration_minutes,
                stops,
                airline,
                airline_name,
                co2_emissions_g,
                emissions_tag,
                booking_token
            FROM unnested
        """
        count_row = con.execute(
            f"SELECT count(*) FROM ({query})"
        ).fetchone()
        if count_row is None:
            raise SystemExit("error: failed to read row count")
        total = count_row[0]

        con.execute(
            f"COPY ({query}) TO '{_sql_literal(output)}' "
            "(FORMAT PARQUET, PARTITION_BY (origin, destination), "
            "COMPRESSION 'zstd', OVERWRITE_OR_IGNORE)"
        )
    finally:
        con.close()

    logger.info("Wrote %d rows to %s", total, output)
    print(f"Output: {output}")


def transform_jsonl(input_path: str, output_dir: str) -> int:
    """Transform a single JSONL file to partitioned Parquet. Returns row count.

    Used by ``scripts/backfill.py`` and ``scripts/optimize_r2.py``.
    """
    import duckdb

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    iata_origin = _iata_case_expr("origin")
    iata_dest = _iata_case_expr("destination")

    con = duckdb.connect()
    try:
        query = f"""
            WITH raw AS (
                SELECT * FROM read_json_auto(
                    '{_sql_literal(input_path)}',
                    format='newline_delimited',
                    union_by_name=true
                )
            ),
            unnested AS (
                SELECT
                    origin,
                    destination,
                    dep_date::DATE        AS dep_date,
                    CASE WHEN return_date::VARCHAR = '' THEN NULL
                         ELSE TRY_CAST(return_date::VARCHAR AS DATE)
                    END AS return_date,
                    flight_type::VARCHAR  AS flight_type,
                    searched_at::TIMESTAMP AS searched_at,
                    f.price::FLOAT        AS price,
                    f.currency::VARCHAR   AS currency,
                    f.duration::INT       AS duration_minutes,
                    f.stops::INT          AS stops,
                    f.primary_airline::VARCHAR     AS airline,
                    f.primary_airline_name::VARCHAR AS airline_name,
                    f.co2_emissions_g::INT AS co2_emissions_g,
                    f.emissions_tag::VARCHAR AS emissions_tag,
                    f.booking_token::VARCHAR AS booking_token
                FROM raw, UNNEST(flights) AS t(f)
            )
            SELECT
                ({iata_origin}) AS origin,
                ({iata_dest}) AS destination,
                dep_date,
                return_date,
                flight_type,
                searched_at,
                price,
                currency,
                duration_minutes,
                stops,
                airline,
                airline_name,
                co2_emissions_g,
                emissions_tag,
                booking_token
            FROM unnested
        """
        count_row = con.execute(
            f"SELECT count(*) FROM ({query})"
        ).fetchone()
        total = count_row[0] if count_row else 0

        con.execute(
            f"COPY ({query}) TO '{_sql_literal(output_dir)}' "
            "(FORMAT PARQUET, PARTITION_BY (origin, destination), "
            "COMPRESSION 'zstd', OVERWRITE_OR_IGNORE)"
        )
    finally:
        con.close()

    return total
