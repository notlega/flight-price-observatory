import os

from collector.errors import ErrorType
from collector.repository import SearchRepository, _RETRY_ERROR_TYPES


async def test_upsert_flush_counts(repo):
    await repo.upsert(
        "SIN|KUL",
        "2026-08-01",
        "",
        "ONE_WAY",
        "SIN",
        "KUL",
        [{"price": 100}],
        None,
        0,
        True,
        "2026-01-01T00:00:00Z",
    )
    await repo.flush()
    assert await repo.count_successful() == 1
    assert await repo.count_failed() == 0


async def test_upsert_replace_same_primary_key(repo):
    await repo.upsert(
        "SIN|KUL",
        "2026-08-01",
        "",
        "ONE_WAY",
        "SIN",
        "KUL",
        [{"price": 100}],
        None,
        0,
        True,
        "t1",
    )
    await repo.upsert(
        "SIN|KUL",
        "2026-08-01",
        "",
        "ONE_WAY",
        "SIN",
        "KUL",
        [{"price": 200}],
        None,
        0,
        True,
        "t2",
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
        await repo.upsert(route, dep, "", "ONE_WAY", o, d, None, err, 2, False, "t")
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
    await repo.upsert(
        "r1", "2026-08-01", "", "ONE_WAY", "O", "D", None, "429", 2, False, "t"
    )
    await repo.upsert(
        "r2", "2026-08-01", "", "ONE_WAY", "O", "D", None, "429", 3, False, "t"
    )
    await repo.flush()

    failed = await repo.get_failed(max_retries=3)
    assert {r[0] for r in failed} == {"r1"}


async def test_count_by_error(repo):
    await repo.upsert(
        "r1", "2026-08-01", "", "ONE_WAY", "O", "D", None, "429", 2, False, "t"
    )
    await repo.upsert(
        "r2", "2026-08-01", "", "ONE_WAY", "O", "D", None, "timeout", 2, False, "t"
    )
    await repo.upsert(
        "r3", "2026-08-01", "", "ONE_WAY", "O", "D", None, "429", 2, False, "t"
    )
    await repo.flush()

    counts = dict(await repo.count_by_error())
    assert counts == {"429": 2, "timeout": 1}


async def test_insert_ignore_all_does_not_overwrite(repo):
    await repo.upsert(
        "SIN|KUL",
        "2026-08-01",
        "",
        "ONE_WAY",
        "SIN",
        "KUL",
        [{"price": 100}],
        None,
        0,
        True,
        "t1",
    )
    await repo.flush()
    await repo.insert_ignore_all(
        [("SIN|KUL", "2026-08-01", "", "ONE_WAY", "SIN", "KUL")]
    )
    rows = [r async for r in repo.iter_successful()]
    assert len(rows) == 1


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
    await repo.upsert(
        "r1", "2026-08-01", "", "ONE_WAY", "O", "D", None, "timeout", 0, False, "t"
    )
    await repo.close()

    reopened = SearchRepository(db)
    await reopened.open()
    failed = await reopened.get_failed(max_retries=3)
    assert [r[0] for r in failed] == ["r1"]
    await reopened.close()


async def test_iter_successful_raw_preserves_json_string(repo):
    await repo.upsert(
        "SIN|KUL",
        "2026-08-01",
        "",
        "ONE_WAY",
        "SIN",
        "KUL",
        [{"price": 100}],
        None,
        0,
        True,
        "t1",
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
