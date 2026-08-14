import asyncio
import json
import os
import sqlite3

import aiosqlite
import pytest

from collector.errors import ErrorType
from collector.repository import SearchRepository, _RETRY_ERROR_TYPES
from collector.services.search_pipeline import _MAX_ATTEMPTS


async def _upsert(
    repo,
    route,
    *,
    flights=None,
    error_type=None,
    retries=0,
    success=True,
    origin="O",
    destination="D",
    searched_at="t",
):
    await repo.upsert(
        route=route,
        dep_date="2026-08-01",
        return_date="",
        flight_type="ONE_WAY",
        origin=origin,
        destination=destination,
        flights=flights,
        error_type=error_type,
        retries=retries,
        success=success,
        searched_at=searched_at,
    )


async def test_empty_db_queries_return_defaults(repo):
    assert await repo.get_failed(max_retries=3) == []
    assert await repo.count_status() == (0, 0)
    assert await repo.count_by_error() == []


@pytest.mark.parametrize("n", [499, 500, 501])
async def test_writer_auto_flushes_at_batch_boundary(repo, n):
    for i in range(n):
        await _upsert(repo, f"r{i}", error_type="data", success=False)
    await repo.flush()
    assert await repo.count_failed() == n


async def test_iter_successful_raises_on_corrupt_flights_json(repo):
    await _upsert(repo, "r", flights=[{"p": 1}])
    await repo.flush()
    await repo._c.execute(
        "UPDATE search_results SET flights = '{broken' WHERE route = 'r'"
    )
    await repo._c.commit()
    with pytest.raises(json.JSONDecodeError):
        rows = [row async for row in repo.iter_successful()]
        assert rows


async def test_legacy_schema_without_return_date_is_dropped(tmp_path):
    db = str(tmp_path / "legacy.db")
    conn = await aiosqlite.connect(db, isolation_level=None)
    await conn.execute(
        "CREATE TABLE search_results ("
        "route TEXT, dep_date TEXT, flight_type TEXT, origin TEXT, destination TEXT)"
    )
    await conn.commit()
    await conn.close()

    repo = SearchRepository(db)
    await repo.open()
    cursor = await repo._c.execute("PRAGMA table_info(search_results)")
    cols = {row[1] for row in await cursor.fetchall()}
    assert "return_date" in cols
    await repo.close()


async def test_open_corrupt_db_file_raises(tmp_path):
    db = str(tmp_path / "not_a_file")
    (tmp_path / "not_a_file").mkdir()
    repo = SearchRepository(db)
    with pytest.raises(sqlite3.DatabaseError):
        await repo.open()


async def test_close_is_idempotent(repo):
    await repo.close()
    await repo.close()


async def test_concurrent_flushes_serialize(repo):
    for i in range(20):
        await _upsert(repo, f"r{i}", flights=[{"p": i}])
    await asyncio.gather(repo.flush(), repo.flush(), repo.flush())
    assert await repo.count_successful() == 20


async def test_delete_db_removes_wal_and_shm(tmp_path):
    db = str(tmp_path / "state.db")
    repo = SearchRepository(db)
    await repo.open()
    await _upsert(repo, "r", flights=[{"p": 1}])
    await repo.flush()
    wal, shm = f"{db}-wal", f"{db}-shm"
    existed = os.path.exists(wal) or os.path.exists(shm)
    await repo.delete_db()
    assert not os.path.exists(db)
    if existed:
        assert not os.path.exists(wal)
        assert not os.path.exists(shm)


async def test_upsert_flush_counts(repo):
    await _upsert(
        repo,
        "SIN|KUL",
        flights=[{"price": 100}],
        origin="SIN",
        destination="KUL",
        searched_at="2026-01-01T00:00:00Z",
    )
    await repo.flush()
    assert await repo.count_successful() == 1
    assert await repo.count_failed() == 0


async def test_upsert_replace_same_primary_key(repo):
    await _upsert(
        repo,
        "SIN|KUL",
        flights=[{"price": 100}],
        origin="SIN",
        destination="KUL",
        searched_at="t1",
    )
    await _upsert(
        repo,
        "SIN|KUL",
        flights=[{"price": 200}],
        origin="SIN",
        destination="KUL",
        searched_at="t2",
    )
    await repo.flush()
    rows = [r async for r in repo.iter_successful()]
    assert len(rows) == 1
    assert rows[0]["flights"][0]["price"] == 200


async def test_get_failed_filters_error_types(repo):
    seeds = [
        ("r1", "2026-08-01", "O", "D", "429"),
        ("r2", "2026-08-01", "O", "D", "timeout"),
        ("r3", "2026-08-01", "O", "D", "other"),
        ("r4", "2026-08-01", "O", "D", "no_proxy"),
        ("r5", "2026-08-01", "O", "D", "data"),
    ]
    for route, dep, o, d, err in seeds:
        await repo.upsert(
            route=route,
            dep_date=dep,
            return_date="",
            flight_type="ONE_WAY",
            origin=o,
            destination=d,
            flights=None,
            error_type=err,
            retries=2,
            success=False,
            searched_at="t",
        )
    await repo.flush()

    failed = await repo.get_failed(max_retries=3)
    assert {r[0] for r in failed} == {"r1", "r2", "r4", "r5"}


def test_retry_error_types_cover_transient_and_proxy_errors():
    assert set(_RETRY_ERROR_TYPES) == {
        ErrorType.RATE_LIMITED,
        ErrorType.TIMEOUT,
        ErrorType.CONNECTION,
        ErrorType.NO_PROXY,
        ErrorType.DATA,
    }
    assert ErrorType.OTHER not in _RETRY_ERROR_TYPES


async def test_get_failed_respects_max_retries(repo):
    await _upsert(repo, "r1", error_type="429", retries=3, success=False)
    await _upsert(repo, "r2", error_type="429", retries=4, success=False)
    await repo.flush()

    failed = await repo.get_failed(max_retries=3)
    assert {r[0] for r in failed} == {"r1"}


async def test_get_failed_retry_round_boundaries(repo):
    for route, retries in [
        ("r0", _MAX_ATTEMPTS),
        ("r1", 2 * _MAX_ATTEMPTS),
        ("r2", 3 * _MAX_ATTEMPTS),
        ("r3", 4 * _MAX_ATTEMPTS),
    ]:
        await _upsert(repo, route, error_type="data", retries=retries, success=False)
    await repo.flush()

    assert {r[0] for r in await repo.get_failed(max_retries=1 * _MAX_ATTEMPTS)} == {
        "r0"
    }
    assert {r[0] for r in await repo.get_failed(max_retries=2 * _MAX_ATTEMPTS)} == {
        "r0",
        "r1",
    }
    assert {r[0] for r in await repo.get_failed(max_retries=3 * _MAX_ATTEMPTS)} == {
        "r0",
        "r1",
        "r2",
    }


async def test_count_by_error(repo):
    await _upsert(repo, "r1", error_type="429", retries=2, success=False)
    await _upsert(repo, "r2", error_type="timeout", retries=2, success=False)
    await _upsert(repo, "r3", error_type="429", retries=2, success=False)
    await repo.flush()

    counts = dict(await repo.count_by_error())
    assert counts == {"429": 2, "timeout": 1}


async def test_insert_ignore_all_does_not_overwrite(repo):
    await _upsert(
        repo, "SIN|KUL", flights=[{"price": 100}], origin="SIN", destination="KUL"
    )
    await repo.flush()
    await repo.insert_ignore_all(
        [("SIN|KUL", "2026-08-01", "", "ONE_WAY", "SIN", "KUL")]
    )
    rows = [r async for r in repo.iter_successful()]
    assert len(rows) == 1


async def test_purge_abandoned_seeds_removes_null_error_rows(repo):
    await repo.insert_ignore_all(
        [("SIN|KUL", "2026-08-01", "", "ONE_WAY", "SIN", "KUL")]
    )
    await _upsert(repo, "BKK|KUL", error_type="timeout", retries=2, success=False)
    await _upsert(
        repo, "SIN|BKK", flights=[{"price": 100}], origin="SIN", destination="BKK"
    )
    await repo.flush()

    purged = await repo.purge_abandoned_seeds()

    assert purged == 1
    failed = [r[0] for r in await repo.get_failed(max_retries=3)]
    assert failed == ["BKK|KUL"]
    successful = [r async for r in repo.iter_successful()]
    assert [r["route"] for r in successful] == ["SIN|BKK"]


async def test_purge_abandoned_seeds_noop_when_clean(repo):
    await repo.insert_ignore_all(
        [("SIN|KUL", "2026-08-01", "", "ONE_WAY", "SIN", "KUL")]
    )
    await _upsert(repo, "SIN|KUL", error_type="timeout", retries=2, success=False)
    await repo.flush()

    assert await repo.purge_abandoned_seeds() == 0


async def test_wal_journal_mode(repo):
    cursor = await repo._conn.execute("PRAGMA journal_mode")
    row = await cursor.fetchone()
    assert row[0].lower() == "wal"


async def test_flush_when_closed_is_noop(repo):
    await repo.close()
    await repo.flush()


async def test_close_flushes_pending_batch(tmp_path):
    db = str(tmp_path / "state.db")
    repo = SearchRepository(db)
    await repo.open()
    await _upsert(repo, "r1", error_type="timeout", success=False)
    await repo.close()

    reopened = SearchRepository(db)
    await reopened.open()
    failed = await reopened.get_failed(max_retries=3)
    assert [r[0] for r in failed] == ["r1"]
    await reopened.close()


async def test_iter_successful_raw_preserves_json_string(repo):
    await _upsert(
        repo, "SIN|KUL", flights=[{"price": 100}], origin="SIN", destination="KUL"
    )
    await repo.flush()
    rows = [r async for r in repo.iter_successful_raw()]
    assert rows[0]["flights"] == '[{"price": 100}]'


async def test_delete_db_removes_file(tmp_path):
    db = str(tmp_path / "state.db")
    repo = SearchRepository(db)
    await repo.open()
    await repo.close()
    assert os.path.exists(db)
    await repo.delete_db()
    assert not os.path.exists(db)


async def test_delete_db_when_missing_is_noop(tmp_path):
    db = str(tmp_path / "absent.db")
    repo = SearchRepository(db)
    await repo.open()
    await repo.close()
    await repo.delete_db()
