"""Silver-tier transform SQL: raw JSONL → conformed Parquet query building.

Builds the DuckDB transform query (unnest + casts + IATA mapping + v2
columns) and re-materializes partition columns into the partition files,
since DuckDB's ``PARTITION_BY`` strips them from the written files.

Lives outside the CLI so the SQL can be shared with operational scripts.
"""

import logging
from pathlib import Path
from typing import Any

from collector.airports import AIRPORT_NAME_TO_IATA

logger = logging.getLogger(__name__)


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


def raw_columns(con: Any, input_path: str) -> set[str]:
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


def build_query(input_path: str, columns: set[str], run_ts: str | None = None) -> str:
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
                {raw_col("dep_date", "DATE")}        AS dep_date,
                {_return_date_expr(columns)}     AS return_date,
                {raw_col("flight_type", "VARCHAR")} AS flight_type,
                {raw_col("searched_at", "TIMESTAMP")} AS searched_at,
                {_flight_field("price", "DOUBLE")}     AS price,
                {_flight_field("currency", "VARCHAR")} AS currency,
                {_flight_field("duration", "INT")}     AS duration_minutes,
                {_flight_field("stops", "INT")}        AS stops,
                {_flight_field("primary_airline", "VARCHAR")} AS airline,
                {_flight_field("co2_emissions_g", "INT")} AS co2_emissions_g,
                {_flight_field("emissions_tag", "VARCHAR")} AS emissions_tag,
                {_flight_field("booking_token", "VARCHAR")} AS booking_token,
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


def materialize_partition_cols(output_dir: Path) -> None:
    """Rewrite each Parquet file adding literal origin/destination columns.

    DuckDB ``COPY ... PARTITION_BY`` strips the partition columns from the
    written files (values only live in the path), so consumers would have to
    re-parse paths. This re-injects them per partition directory.
    """
    import duckdb

    con: Any = duckdb.connect()
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
