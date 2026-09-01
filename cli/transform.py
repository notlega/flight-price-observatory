"""Transform subcommand: export raw JSONL to conformed Parquet (silver).

Unnests the ``flights`` array so each row represents one flight option.
Casts column types, maps full airport names → IATA codes, partitions by
``origin``/``destination`` with zstd compression, and re-materializes the
partition columns (DuckDB's ``PARTITION_BY`` strips them from the files).

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


def _raw_columns(con, input_path: str) -> set[str]:
    """Return the set of top-level columns present in a JSONL file."""
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_json_auto("
        f"'{_sql_literal(input_path)}', "
        f"format='newline_delimited', union_by_name=true)"
    ).fetchall()
    return {r[0] for r in rows}


def _raw_col(columns: set[str], name: str, cast: str) -> str:
    """SQL expr that casts a raw column, or NULL literal if column is absent.

    Historical raw JSONL is schema-lean (e.g. missing ``return_date`` /
    ``flight_type``). Referencing an absent column raises a binder error, so
    absent columns become typed NULL instead.
    """
    if name not in columns:
        return f"NULL::{cast}"
    return f"{name}::{cast}"


def _return_date_expr(columns: set[str]) -> str:
    """SQL expression yielding return_date as DATE, NULL if column absent."""
    if "return_date" not in columns:
        return "NULL::DATE"
    return (
        "CASE WHEN return_date::VARCHAR = '' THEN NULL "
        "ELSE TRY_CAST(return_date::VARCHAR AS DATE) END"
    )


def _flight_field(field: str, cast: str) -> str:
    """Null-safe access to a flight struct field via JSON.

    Some historical flight items are schema-lean (e.g. a probe/bot-check
    response carrying only ``price``/``origin``). Field access on a missing
    struct key raises a binder error, so we route access through
    ``json_extract_string`` (returns NULL for absent keys, and clean
    unquoted strings) then cast.
    """
    return f"TRY_CAST(json_extract_string(to_json(f), '$.{field}') AS {cast})"


def _run_ts_literal(run_ts: str | None) -> str:
    """SQL literal for the run_ts column, or typed NULL when unknown."""
    if run_ts is None:
        return "NULL::VARCHAR"
    return f"'{_sql_literal(run_ts)}'"


def _build_query(input_path: str, columns: set[str], run_ts: str | None = None) -> str:
    """Build the full transform SQL (unnest + casts + IATA mapping + v2 columns)."""
    iata_origin = _iata_case_expr("raw_origin")
    iata_dest = _iata_case_expr("raw_destination")

    def raw_col(name: str, cast: str) -> str:
        return _raw_col(columns, name, cast)

    return f"""
        WITH raw AS (
            SELECT * FROM read_json_auto(
                '{_sql_literal(input_path)}',
                format='newline_delimited',
                union_by_name=true
            )
        ),
        unnested AS (
            SELECT
                origin            AS raw_origin,
                destination       AS raw_destination,
                {raw_col('dep_date', 'DATE')}        AS dep_date,
                {_return_date_expr(columns)}     AS return_date,
                {raw_col('flight_type', 'VARCHAR')} AS flight_type,
                {raw_col('searched_at', 'TIMESTAMP')} AS searched_at,
                {_flight_field('price', 'DOUBLE')}     AS price,
                {_flight_field('currency', 'VARCHAR')} AS currency,
                {_flight_field('duration', 'INT')}     AS duration_minutes,
                {_flight_field('stops', 'INT')}        AS stops,
                {_flight_field('primary_airline', 'VARCHAR')} AS airline,
                {_flight_field('co2_emissions_g', 'INT')} AS co2_emissions_g,
                {_flight_field('emissions_tag', 'VARCHAR')} AS emissions_tag,
                {_flight_field('booking_token', 'VARCHAR')} AS booking_token,
                {_run_ts_literal(run_ts)}          AS run_ts
            FROM raw, UNNEST(flights) AS t(f)
        )
        SELECT
            ({iata_origin}) AS origin,
            ({iata_dest}) AS destination,
            dep_date,
            return_date,
            flight_type,
            searched_at,
            ROUND(price::DOUBLE, 2)::DECIMAL(12, 2) AS price,
            price IS NOT NULL AS price_present,
            currency,
            duration_minutes,
            stops,
            airline,
            co2_emissions_g,
            emissions_tag,
            booking_token,
            run_ts,
            CAST((dep_date - CAST(searched_at AS DATE)) AS INT) AS lead_days,
            CASE
                WHEN return_date IS NOT NULL THEN 'ROUND_TRIP'
                WHEN ({iata_dest}) = 'SIN' THEN 'RETURN'
                WHEN ({iata_origin}) = 'SIN' THEN 'OUTBOUND'
                ELSE 'OTHER'
            END AS direction,
            MD5(
                CAST(searched_at AS VARCHAR) || '|'
                || COALESCE(booking_token, '') || '|'
                || COALESCE(flight_type, '')
            ) AS itinerary_id
        FROM unnested
    """


def _materialize_partition_cols(output_dir: Path) -> None:
    """Rewrite each Parquet file adding literal origin/destination columns.

    DuckDB ``COPY ... PARTITION_BY`` strips the partition columns from the
    written files (values only live in the path), so consumers would have to
    re-parse paths. This re-injects them per partition directory.
    """
    import duckdb

    con = duckdb.connect()
    try:
        for pf in sorted(output_dir.rglob("*.parquet")):
            parts = pf.relative_to(output_dir).parts
            origin = dest = None
            for part in parts:
                if part.startswith("origin="):
                    origin = part.split("=", 1)[1]
                elif part.startswith("destination="):
                    dest = part.split("=", 1)[1]
            if origin is None or dest is None:
                continue
            con.execute(
                f"COPY (SELECT *, '{_sql_literal(origin)}' AS origin, "
                f"'{_sql_literal(dest)}' AS destination FROM read_parquet("
                f"'{_sql_literal(str(pf))}', hive_partitioning=false)) TO "
                f"'{_sql_literal(str(pf))}' "
                "(FORMAT PARQUET, COMPRESSION 'zstd', OVERWRITE_OR_IGNORE)"
            )
    finally:
        con.close()


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

    con = duckdb.connect()
    try:
        raw_columns = _raw_columns(con, input_path)
        query = _build_query(input_path, raw_columns, run_ts=run_ts)
        count_row = con.execute(
            f"SELECT count(*) FROM ({query})"
        ).fetchone()
        total = count_row[0] if count_row else 0

        con.execute(
            f"COPY ({query}) TO '{_sql_literal(str(output))}' "
            "(FORMAT PARQUET, PARTITION_BY (origin, destination), "
            "COMPRESSION 'zstd', OVERWRITE_OR_IGNORE)"
        )
    finally:
        con.close()

    _materialize_partition_cols(output)

    return total
