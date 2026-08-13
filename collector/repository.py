"""SQLite-backed search result store with upsert and retry tracking."""

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from pathlib import Path

import aiosqlite

from collector.errors import ErrorType

logger = logging.getLogger(__name__)

_BUSY_TIMEOUT_MS = 10000

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS search_results (
    route       TEXT NOT NULL,
    dep_date    TEXT NOT NULL,
    return_date TEXT NOT NULL DEFAULT '',
    flight_type TEXT NOT NULL,
    origin      TEXT NOT NULL,
    destination TEXT NOT NULL,
    flights     TEXT,
    error_type  TEXT,
    retries     INTEGER DEFAULT 0,
    success     BOOL DEFAULT 0,
    searched_at TEXT,
    PRIMARY KEY (route, dep_date, return_date, flight_type)
)
"""

_INSERT_SQL = """
INSERT OR REPLACE INTO search_results
    (route, dep_date, return_date, flight_type, origin, destination, flights, error_type, retries, success, searched_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_RETRY_ERROR_TYPES = (
    ErrorType.RATE_LIMITED,
    ErrorType.TIMEOUT,
    ErrorType.CONNECTION,
    ErrorType.NO_PROXY,
    ErrorType.DATA,
)

_WRITE_BATCH_SIZE = 500

_STOP = object()
_FLUSH = object()


class SearchRepository:
    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn: aiosqlite.Connection | None = None
        self._write_queue: asyncio.Queue | None = None
        self._writer_task: asyncio.Task | None = None
        self._write_lock = asyncio.Lock()

    @property
    def _c(self) -> aiosqlite.Connection:
        assert self._conn is not None, "repository connection not open"
        return self._conn

    @property
    def _q(self) -> asyncio.Queue:
        assert self._write_queue is not None, "writer queue not started"
        return self._write_queue

    async def open(self):
        """Open the database connection and start the writer loop."""
        db_dir = Path(self._db_path).parent
        if db_dir:
            db_dir.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(self._db_path, isolation_level=None)
        self._conn = conn
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
        await self._migrate()
        await conn.execute(_CREATE_SQL)
        await conn.commit()
        self._write_queue = asyncio.Queue()
        self._writer_task = asyncio.create_task(self._writer_loop())

    async def _migrate(self):
        cursor = await self._c.execute("PRAGMA table_info(search_results)")
        columns = {row[1] for row in await cursor.fetchall()}
        if columns and "return_date" not in columns:
            await self._c.execute("DROP TABLE search_results")

    async def _writer_loop(self):
        batch: list[tuple] = []
        while True:
            item = await self._q.get()
            self._q.task_done()
            if item is _STOP:
                if batch:
                    await self._c.executemany(_INSERT_SQL, batch)
                    await self._c.commit()
                return
            if item is _FLUSH:
                if batch:
                    await self._c.executemany(_INSERT_SQL, batch)
                    await self._c.commit()
                    batch.clear()
                continue
            batch.append(item)
            if len(batch) >= _WRITE_BATCH_SIZE:
                await self._c.executemany(_INSERT_SQL, batch)
                await self._c.commit()
                batch.clear()

    async def flush(self):
        """Commit all queued writes (await the writer draining the queue)."""
        if self._conn is None or self._write_queue is None:
            return
        async with self._write_lock:
            self._write_queue.put_nowait(_FLUSH)
            await self._q.join()

    async def close(self):
        if self._conn is None:
            return
        if self._write_queue is not None:
            self._write_queue.put_nowait(_STOP)
            if self._writer_task is not None:
                await self._writer_task
            self._write_queue = None
            self._writer_task = None
        await self._c.close()
        self._conn = None

    async def upsert(
        self,
        route: str,
        dep_date: str,
        return_date: str,
        flight_type: str,
        origin: str,
        destination: str,
        flights: list[dict] | None,
        error_type: str | None,
        retries: int,
        success: bool,
        searched_at: str,
    ):
        """Queue a search result write, keyed by route and dates."""
        flights_json = json.dumps(flights) if flights else None
        self._q.put_nowait(
            (
                route,
                dep_date,
                return_date,
                flight_type,
                origin,
                destination,
                flights_json,
                error_type,
                retries,
                int(success),
                searched_at,
            )
        )

    async def insert_ignore_all(self, tasks: list[tuple[str, str, str, str, str, str]]):
        sql = (
            "INSERT OR IGNORE INTO search_results "
            "(route, dep_date, return_date, flight_type, origin, destination, retries, success) "
            "VALUES (?, ?, ?, ?, ?, ?, 0, 0)"
        )
        async with self._write_lock:
            await self._c.executemany(sql, tasks)
            await self._c.commit()

    async def get_failed(self, max_retries: int = 3) -> list[tuple[str, str, str, str]]:
        placeholders = ",".join("?" for _ in _RETRY_ERROR_TYPES)
        cursor = await self._c.execute(
            "SELECT route, dep_date, return_date, flight_type "
            f"FROM search_results WHERE success = 0 "
            f"AND error_type IN ({placeholders}) AND retries < ?",
            (*_RETRY_ERROR_TYPES, max_retries),
        )
        rows = await cursor.fetchall()
        return [(r[0], r[1], r[2], r[3]) for r in rows]

    async def count_status(self) -> tuple[int, int]:
        cursor = await self._c.execute(
            "SELECT "
            "COALESCE(SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END), 0), "
            "COALESCE(SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END), 0) "
            "FROM search_results"
        )
        row = await cursor.fetchone()
        return (row[0], row[1]) if row else (0, 0)

    async def count_successful(self) -> int:
        return (await self.count_status())[0]

    async def count_failed(self) -> int:
        return (await self.count_status())[1]

    async def count_by_error(self) -> list[tuple[str, int]]:
        cursor = await self._c.execute(
            "SELECT error_type, COUNT(*) FROM search_results "
            "WHERE success = 0 GROUP BY error_type"
        )
        rows = await cursor.fetchall()
        return [(r[0], r[1]) for r in rows]

    async def _iter_successful(self, raw: bool) -> AsyncIterator[dict]:
        cursor = await self._c.execute(
            "SELECT route, dep_date, return_date, flight_type, origin, "
            "destination, flights, searched_at "
            "FROM search_results WHERE success = 1 ORDER BY route, dep_date"
        )
        async for row in cursor:
            flights = row[6]
            if raw:
                flights = flights or "[]"
            else:
                flights = json.loads(flights) if flights else []
            yield {
                "route": row[0],
                "dep_date": row[1],
                "return_date": row[2],
                "flight_type": row[3],
                "origin": row[4],
                "destination": row[5],
                "flights": flights,
                "searched_at": row[7],
            }

    async def iter_successful(self) -> AsyncIterator[dict]:
        async for row in self._iter_successful(raw=False):
            yield row

    async def iter_successful_raw(self) -> AsyncIterator[dict]:
        async for row in self._iter_successful(raw=True):
            yield row

    async def delete_db(self):
        await self.close()
        for path in (
            Path(self._db_path),
            Path(f"{self._db_path}-shm"),
            Path(f"{self._db_path}-wal"),
        ):
            if path.exists():
                path.unlink()
        logger.info("Deleted SQLite state: %s", self._db_path)
