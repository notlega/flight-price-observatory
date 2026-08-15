import json
import os

import pytest

from collector.convert import _jsonl_row, convert
from collector.repository import SearchRepository


@pytest.fixture
async def seeded_db(tmp_path):
    db = str(tmp_path / "state.db")
    repo = SearchRepository(db)
    await repo.open()
    await repo.upsert(
        route="SIN|KUL",
        dep_date="2026-08-01",
        return_date="",
        flight_type="ONE_WAY",
        origin="SIN",
        destination="KUL",
        flights=[{"price": 100}],
        error_type=None,
        retries=0,
        success=True,
        searched_at="t1",
    )
    await repo.upsert(
        route="SIN|BKK",
        dep_date="2026-08-01",
        return_date="",
        flight_type="ONE_WAY",
        origin="SIN",
        destination="BKK",
        flights=[{"price": 90}],
        error_type=None,
        retries=0,
        success=True,
        searched_at="t2",
    )
    await repo.flush()
    await repo.close()
    return db


async def test_convert_writes_jsonl_and_deletes(seeded_db, tmp_path):
    out = str(tmp_path / "out.jsonl")
    path = await convert(seeded_db, out, delete=True)
    assert path == out
    assert not os.path.exists(seeded_db)

    with open(out) as f:
        lines = f.read().strip().splitlines()
    assert len(lines) == 2
    rows = [json.loads(line) for line in lines]
    assert {r["route"] for r in rows} == {"SIN|KUL", "SIN|BKK"}
    assert {r["flights"][0]["price"] for r in rows} == {100, 90}


async def test_convert_keep_db(seeded_db, tmp_path):
    out = str(tmp_path / "out.jsonl")
    await convert(seeded_db, out, delete=False)
    assert os.path.exists(seeded_db)


async def test_convert_empty_db_writes_nothing(tmp_path):
    db = str(tmp_path / "state.db")
    repo = SearchRepository(db)
    await repo.open()
    await repo.close()
    out = str(tmp_path / "out.jsonl")
    path = await convert(db, out, delete=False)
    assert path == out
    assert not os.path.exists(out)


async def test_convert_empty_db_delete_true_removes_state(tmp_path):
    db = str(tmp_path / "state.db")
    repo = SearchRepository(db)
    await repo.open()
    await repo.close()
    assert os.path.exists(db)
    out = str(tmp_path / "out.jsonl")
    await convert(db, out, delete=True)
    assert not os.path.exists(db)


@pytest.mark.parametrize("n", [999, 1000, 1001])
async def test_convert_flushes_buffer_at_boundary(tmp_path, n):
    db = str(tmp_path / "state.db")
    repo = SearchRepository(db)
    await repo.open()
    for i in range(n):
        await repo.upsert(
            route=f"SIN|KUL{i:04d}",
            dep_date="2026-08-01",
            return_date="",
            flight_type="ONE_WAY",
            origin="SIN",
            destination=f"KUL{i:04d}",
            flights=[{"price": i}],
            error_type=None,
            retries=0,
            success=True,
            searched_at=f"t{i}",
        )
    await repo.flush()
    await repo.close()

    out = str(tmp_path / "out.jsonl")
    await convert(db, out, delete=True)
    with open(out) as f:
        lines = [line for line in f if line.strip()]
    assert len(lines) == n


async def test_convert_null_flights_exports_empty_array(tmp_path):
    db = str(tmp_path / "state.db")
    repo = SearchRepository(db)
    await repo.open()
    await repo.upsert(
        route="SIN|KUL",
        dep_date="2026-08-01",
        return_date="",
        flight_type="ONE_WAY",
        origin="SIN",
        destination="KUL",
        flights=None,
        error_type=None,
        retries=0,
        success=True,
        searched_at="t1",
    )
    await repo.flush()
    await repo.close()
    out = str(tmp_path / "out.jsonl")
    await convert(db, out, delete=False)
    with open(out) as f:
        row = json.loads(f.readline())
    assert row["flights"] == []


def test_jsonl_row_passthrough_flights_string():
    row = {
        "route": "SIN|KUL",
        "dep_date": "2026-08-01",
        "return_date": "",
        "flight_type": "ONE_WAY",
        "origin": "SIN",
        "destination": "KUL",
        "flights": '[{"price": 100}]',
        "searched_at": "2026-01-01T00:00:00Z",
    }
    line = _jsonl_row(row)
    parsed = json.loads(line)
    assert parsed == {
        "route": "SIN|KUL",
        "dep_date": "2026-08-01",
        "return_date": "",
        "flight_type": "ONE_WAY",
        "origin": "SIN",
        "destination": "KUL",
        "flights": [{"price": 100}],
        "searched_at": "2026-01-01T00:00:00Z",
    }


def test_jsonl_row_splices_flights_unescaped():
    flights_raw = '[{"price": 100.5, "name": "大阪"}]'
    row = {
        "route": "SIN|KUL",
        "dep_date": "2026-08-01",
        "return_date": "",
        "flight_type": "ONE_WAY",
        "origin": "SIN",
        "destination": "KUL",
        "flights": flights_raw,
        "searched_at": "2026-01-01T00:00:00Z",
    }
    line = _jsonl_row(row)
    assert f'"flights":{flights_raw}' in line
    assert json.loads(line)["flights"] == [{"price": 100.5, "name": "大阪"}]
