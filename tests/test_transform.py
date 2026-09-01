"""Tests for cli/transform.py — Parquet transformation pipeline."""

import json
import os
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


@pytest.fixture
def sample_jsonl(tmp_path: Path) -> Path:
    """Create a minimal JSONL file with one route, two flights."""
    rows = [
        {
            "route": "Singapore Changi International Airport|Kuala Lumpur International Airport",
            "dep_date": "2026-08-15",
            "return_date": "",
            "flight_type": "ONE_WAY",
            "origin": "Singapore Changi International Airport",
            "destination": "Kuala Lumpur International Airport",
            "flights": [
                {
                    "price": 120.0,
                    "currency": "SGD",
                    "duration": 300,
                    "stops": 0,
                    "primary_airline": "SQ",
                    "primary_airline_name": "Singapore Airlines",
                    "co2_emissions_g": 120000,
                    "emissions_tag": "lower",
                    "booking_token": "tok1",
                },
                {
                    "price": 95.0,
                    "currency": "SGD",
                    "duration": 360,
                    "stops": 1,
                    "primary_airline": "AK",
                    "primary_airline_name": "AirAsia",
                    "co2_emissions_g": 150000,
                    "emissions_tag": "typical",
                    "booking_token": "tok2",
                },
            ],
            "searched_at": "2026-08-14T15:00:00+00:00",
        },
        {
            "route": "Singapore Changi International Airport|Kuala Lumpur International Airport",
            "dep_date": "2026-08-16",
            "return_date": "",
            "flight_type": "ONE_WAY",
            "origin": "Singapore Changi International Airport",
            "destination": "Kuala Lumpur International Airport",
            "flights": [
                {
                    "price": 110.0,
                    "currency": "SGD",
                    "duration": 310,
                    "stops": 0,
                    "primary_airline": "SQ",
                    "primary_airline_name": "Singapore Airlines",
                    "co2_emissions_g": 125000,
                    "emissions_tag": "lower",
                    "booking_token": "tok3",
                },
            ],
            "searched_at": "2026-08-14T16:00:00+00:00",
        },
    ]
    jsonl_path = tmp_path / "search_test.jsonl"
    _write_jsonl(jsonl_path, rows)
    return jsonl_path


@pytest.fixture
def roundtrip_jsonl(tmp_path: Path) -> Path:
    """Create a JSONL file with a round-trip flight."""
    rows = [
        {
            "route": "Singapore Changi International Airport|Narita International Airport",
            "dep_date": "2026-09-01",
            "return_date": "2026-09-14",
            "flight_type": "ROUND_TRIP",
            "origin": "Singapore Changi International Airport",
            "destination": "Narita International Airport",
            "flights": [
                {
                    "price": 850.0,
                    "currency": "SGD",
                    "duration": 420,
                    "stops": 0,
                    "primary_airline": "SQ",
                    "primary_airline_name": "Singapore Airlines",
                    "co2_emissions_g": 500000,
                    "emissions_tag": "typical",
                    "booking_token": "tok4",
                },
            ],
            "searched_at": "2026-08-14T10:00:00+00:00",
        },
    ]
    jsonl_path = tmp_path / "search_rt.jsonl"
    _write_jsonl(jsonl_path, rows)
    return jsonl_path


def test_transform_unnests_flights(sample_jsonl: Path, tmp_path: Path) -> None:
    """Two flights in one JSONL row → two Parquet rows."""
    from cli.transform import transform_jsonl

    output = tmp_path / "silver"
    rows = transform_jsonl(str(sample_jsonl), str(output))

    assert rows == 3  # 2 flights + 1 flight

    # Read back Parquet
    con = duckdb.connect()
    result = con.execute(
        f"SELECT * FROM read_parquet('{output}/**/*.parquet', hive_partitioning=true)"
    ).fetchall()
    con.close()

    assert len(result) == 3


def test_transform_casts_dep_date_to_date(sample_jsonl: Path, tmp_path: Path) -> None:
    """dep_date becomes DATE type."""
    from cli.transform import transform_jsonl

    output = tmp_path / "silver"
    transform_jsonl(str(sample_jsonl), str(output))

    con = duckdb.connect()
    dtype = con.execute(
        f"SELECT typeof(dep_date) FROM read_parquet('{output}/**/*.parquet', hive_partitioning=true) LIMIT 1"
    ).fetchone()[0]
    con.close()

    assert dtype == "DATE"


def test_transform_maps_iata_codes(sample_jsonl: Path, tmp_path: Path) -> None:
    """Full airport names → IATA codes."""
    from cli.transform import transform_jsonl

    output = tmp_path / "silver"
    transform_jsonl(str(sample_jsonl), str(output))

    con = duckdb.connect()
    origins = con.execute(
        f"SELECT DISTINCT origin FROM read_parquet('{output}/**/*.parquet', hive_partitioning=true)"
    ).fetchall()
    destinations = con.execute(
        f"SELECT DISTINCT destination FROM read_parquet('{output}/**/*.parquet', hive_partitioning=true)"
    ).fetchall()
    con.close()

    assert {r[0] for r in origins} == {"SIN"}
    assert {r[0] for r in destinations} == {"KUL"}


def test_transform_preserves_price_and_currency(sample_jsonl: Path, tmp_path: Path) -> None:
    """Price and currency survive transformation (silver v2: price is DECIMAL)."""
    from cli.transform import transform_jsonl

    output = tmp_path / "silver"
    transform_jsonl(str(sample_jsonl), str(output))

    con = duckdb.connect()
    prices = con.execute(
        f"SELECT price, currency FROM read_parquet('{output}/**/*.parquet', hive_partitioning=true) ORDER BY price"
    ).fetchall()
    con.close()

    assert [(Decimal("95.00"), "SGD"), (Decimal("110.00"), "SGD"), (Decimal("120.00"), "SGD")] == prices


def test_transform_handles_return_date(sample_jsonl: Path, tmp_path: Path) -> None:
    """Empty return_date → NULL."""
    from cli.transform import transform_jsonl

    output = tmp_path / "silver"
    transform_jsonl(str(sample_jsonl), str(output))

    con = duckdb.connect()
    result = con.execute(
        f"SELECT return_date IS NULL FROM read_parquet('{output}/**/*.parquet', hive_partitioning=true) LIMIT 1"
    ).fetchone()
    con.close()

    assert result[0] is True


def test_transform_roundtrip_return_date(roundtrip_jsonl: Path, tmp_path: Path) -> None:
    """Round-trip: return_date populated."""
    from cli.transform import transform_jsonl

    output = tmp_path / "silver"
    transform_jsonl(str(roundtrip_jsonl), str(output))

    con = duckdb.connect()
    result = con.execute(
        f"SELECT return_date FROM read_parquet('{output}/**/*.parquet', hive_partitioning=true) LIMIT 1"
    ).fetchone()
    con.close()

    assert result[0].year == 2026
    assert result[0].month == 9
    assert result[0].day == 14


def test_transform_empty_flights_skipped(tmp_path: Path) -> None:
    """Row with no flights produces zero Parquet rows."""
    from cli.transform import transform_jsonl

    rows = [
        {
            "route": "Singapore Changi International Airport|Kuala Lumpur International Airport",
            "dep_date": "2026-08-15",
            "return_date": "",
            "flight_type": "ONE_WAY",
            "origin": "Singapore Changi International Airport",
            "destination": "Kuala Lumpur International Airport",
            "flights": [],
            "searched_at": "2026-08-14T15:00:00+00:00",
        },
    ]
    jsonl_path = tmp_path / "search_empty.jsonl"
    _write_jsonl(jsonl_path, rows)

    output = tmp_path / "silver"
    total = transform_jsonl(str(jsonl_path), str(output))

    assert total == 0


def test_transform_creates_partition_dirs(sample_jsonl: Path, tmp_path: Path) -> None:
    """Output has origin= and destination= partition directories."""
    from cli.transform import transform_jsonl

    output = tmp_path / "silver"
    transform_jsonl(str(sample_jsonl), str(output))

    # Find partition dirs
    origin_dirs = list(output.rglob("origin=*"))
    dest_dirs = list(output.rglob("destination=*"))

    assert len(origin_dirs) > 0
    assert len(dest_dirs) > 0
    assert any("SIN" in d.name for d in origin_dirs)
    assert any("KUL" in d.name for d in dest_dirs)


def test_transform_parquet_compressed_zstd(sample_jsonl: Path, tmp_path: Path) -> None:
    """Output Parquet files use zstd compression."""
    from cli.transform import transform_jsonl

    output = tmp_path / "silver"
    transform_jsonl(str(sample_jsonl), str(output))

    parquet_files = list(output.rglob("*.parquet"))
    assert len(parquet_files) > 0

    # DuckDB can read zstd Parquet (would fail if wrong codec)
    con = duckdb.connect()
    con.execute(f"SELECT * FROM read_parquet('{parquet_files[0]}')")
    con.close()


def test_transform_iata_case_expr_covers_all_airports() -> None:
    """_iata_case_expr covers all airports in the mapping."""
    from cli.transform import _iata_case_expr
    from collector.airports import AIRPORT_NAME_TO_IATA

    expr_origin = _iata_case_expr("origin")
    expr_dest = _iata_case_expr("destination")
    for name in AIRPORT_NAME_TO_IATA:
        assert name in expr_origin
        assert name in expr_dest


def test_transform_maps_bali_airport_name() -> None:
    """Ngurah Rai (Bali) International Airport → DPS."""
    from collector.airports import resolve_iata

    assert resolve_iata("Ngurah Rai (Bali) International Airport") == "DPS"
    assert resolve_iata("Ngurah Rai International Airport") == "DPS"


def test_transform_maps_tokyo_airport_name() -> None:
    """Tokyo International Airport → HND."""
    from collector.airports import resolve_iata

    assert resolve_iata("Tokyo International Airport") == "HND"
    assert resolve_iata("Tokyo Haneda Airport") == "HND"


def test_transform_schema_lean_raw(tmp_path: Path) -> None:
    """Old raw JSONL lacking return_date/flight_type transforms with NULLs."""
    from cli.transform import transform_jsonl

    path = tmp_path / "lean.jsonl"
    _write_jsonl(
        path,
        [
            {
                "route": "Singapore Changi International Airport|Ngurah Rai (Bali) International Airport",
                "dep_date": "2026-08-10",
                "origin": "Singapore Changi International Airport",
                "destination": "Ngurah Rai (Bali) International Airport",
                "flights": [
                    {
                        "price": 200.0,
                        "currency": "SGD",
                        "duration": 180,
                        "stops": 0,
                        "primary_airline": "SQ",
                        "primary_airline_name": "Singapore Airlines",
                        "co2_emissions_g": 80000,
                        "emissions_tag": "lower",
                        "booking_token": "tok",
                    }
                ],
                "searched_at": "2026-08-09T10:00:00+00:00",
            }
        ],
    )
    output = tmp_path / "silver"
    rows = transform_jsonl(str(path), str(output))
    assert rows == 1

    con = duckdb.connect()
    cols = con.execute(
        "DESCRIBE SELECT * FROM read_parquet("
        f"'{output}/**/*.parquet', hive_partitioning=true)"
    ).fetchall()
    names = {c[0]: c[1] for c in cols}
    assert names["origin"] == "VARCHAR"
    assert names["destination"] == "VARCHAR"
    assert names["return_date"] == "DATE"
    assert names["flight_type"] == "VARCHAR"

    row = con.execute(
        f"SELECT origin, destination, dep_date, return_date IS NULL, "
        f"flight_type IS NULL FROM read_parquet("
        f"'{output}/**/*.parquet', hive_partitioning=true)"
    ).fetchone()
    assert row[0] == "SIN"
    assert row[1] == "DPS"
    assert row[3] is True
    assert row[4] is True
    con.close()


def test_transform_v2_materializes_partition_and_new_columns(
    sample_jsonl: Path, tmp_path: Path
) -> None:
    """Silver v2: origin/destination are file columns too; v2 cols present."""
    from cli.transform import transform_jsonl

    output = tmp_path / "silver"
    transform_jsonl(str(sample_jsonl), str(output))

    con = duckdb.connect()
    cols = {
        c[0]: c[1]
        for c in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{output}/**/*.parquet', "
            "hive_partitioning=false)"
        ).fetchall()
    }
    assert len(cols) == len(
        con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{output}/**/*.parquet', "
            "hive_partitioning=false)"
        ).fetchall()
    )  # no duplicate (renamed _1) columns

    assert cols["origin"] == "VARCHAR"
    assert cols["destination"] == "VARCHAR"
    assert cols["price"] == "DECIMAL(12,2)"
    assert cols["price_present"] == "BOOLEAN"
    assert cols["run_ts"] == "VARCHAR"
    assert cols["lead_days"] == "INTEGER"
    assert cols["direction"] == "VARCHAR"
    assert cols["itinerary_id"] == "VARCHAR"
    assert "airline_name" not in cols  # duplicate of airline, dropped in v2
    con.close()


def test_transform_v2_direction_and_derived_fields(
    sample_jsonl: Path, roundtrip_jsonl: Path, tmp_path: Path
) -> None:
    """direction/lead_days/itinerary_id populated per flight semantics."""
    from cli.transform import transform_jsonl

    output = tmp_path / "silver"
    transform_jsonl(str(sample_jsonl), str(output))

    con = duckdb.connect()
    rows = con.execute(
        f"SELECT direction, lead_days, itinerary_id IS NOT NULL FROM "
        f"read_parquet('{output}/**/*.parquet', hive_partitioning=true)"
    ).fetchall()
    assert rows == [
        ("OUTBOUND", 1, True),
        ("OUTBOUND", 1, True),
        ("OUTBOUND", 2, True),
    ]  # searched 08-14, deps 08-15/08-15/08-16

    rt = tmp_path / "rt"
    transform_jsonl(str(roundtrip_jsonl), str(rt), run_ts="20260819_053000")
    row = con.execute(
        f"SELECT direction, run_ts, itinerary_id IS NOT NULL FROM "
        f"read_parquet('{rt}/**/*.parquet', hive_partitioning=true)"
    ).fetchone()
    assert row[0] == "ROUND_TRIP"
    assert row[1] == "20260819_053000"
    assert row[2] is True
    con.close()


def test_transform_v2_marks_return_and_null_price(tmp_path: Path) -> None:
    """Reverse-leg flights → RETURN, null price → price_present false."""
    from cli.transform import transform_jsonl

    path = tmp_path / "reverse.jsonl"
    _write_jsonl(
        path,
        [
            {
                "route": "Kul|Singapore",
                "dep_date": "2026-08-15",
                "return_date": "",
                "flight_type": "ONE_WAY",
                "origin": "Kuala Lumpur International Airport",
                "destination": "Singapore Changi International Airport",
                "flights": [
                    {
                        "price": None,
                        "currency": "SGD",
                        "duration": 300,
                        "stops": 1,
                        "primary_airline": "AK",
                        "primary_airline_name": "AirAsia",
                        "co2_emissions_g": 150000,
                        "emissions_tag": "typical",
                        "booking_token": "tok2",
                    }
                ],
                "searched_at": "2026-08-14T16:00:00+00:00",
            }
        ],
    )
    output = tmp_path / "silver"
    transform_jsonl(str(path), str(output))

    con = duckdb.connect()
    row = con.execute(
        f"SELECT origin, destination, direction, price_present, price FROM "
        f"read_parquet('{output}/**/*.parquet', hive_partitioning=true)"
    ).fetchone()
    assert row[:4] == ("KUL", "SIN", "RETURN", False)
    assert row[4] is None
    con.close()
