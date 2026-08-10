import asyncio
import json
import logging
import os
from collections.abc import AsyncIterator

import aiosqlite

from collector.errors import ErrorType

logger = logging.getLogger(__name__)

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS search_results (
    route       TEXT NOT NULL,
    dep_date    TEXT NOT NULL,
    origin      TEXT NOT NULL,
    destination TEXT NOT NULL,
    flights     TEXT,
    error_type  TEXT,
    retries     INTEGER DEFAULT 0,
    success     BOOL DEFAULT 0,
    searched_at TEXT,
    PRIMARY KEY (route, dep_date)
)
"""

_INSERT_SQL = """
INSERT OR REPLACE INTO search_results
    (route, dep_date, origin, destination, flights, error_type, retries, success, searched_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_RETRY_ERROR_TYPES = (
    ErrorType.RATE_LIMITED,
    ErrorType.TIMEOUT,
    ErrorType.CONNECTION,
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

    async def open(self):
        db_dir = os.path.dirname(self._db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self._conn = await aiosqlite.connect(self._db_path, isolation_level=None)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA busy_timeout=10000")
        await self._conn.execute(_CREATE_SQL)
        await self._conn.commit()
        self._write_queue = asyncio.Queue()
        self._writer_task = asyncio.create_task(self._writer_loop())

    async def _writer_loop(self):
        batch: list[tuple] = []
        while True:
            item = await self._write_queue.get()
            self._write_queue.task_done()
            if item is _STOP:
                if batch:
                    await self._conn.executemany(_INSERT_SQL, batch)
                    await self._conn.commit()
                return
            if item is _FLUSH:
                if batch:
                    await self._conn.executemany(_INSERT_SQL, batch)
                    await self._conn.commit()
                    batch.clear()
                continue
            batch.append(item)
            if len(batch) >= _WRITE_BATCH_SIZE:
                await self._conn.executemany(_INSERT_SQL, batch)
                await self._conn.commit()
                batch.clear()

    async def flush(self):
        """Commit all queued writes (await the writer draining the queue)."""
        if self._conn is None or self._write_queue is None:
            return
        async with self._write_lock:
            self._write_queue.put_nowait(_FLUSH)
            await self._write_queue.join()

    async def close(self):
        if self._conn is None:
            return
        if self._write_queue is not None:
            self._write_queue.put_nowait(_STOP)
            await self._writer_task
            self._write_queue = None
            self._writer_task = None
        await self._conn.close()
        self._conn = None

    async def upsert(
        self,
        route: str,
        dep_date: str,
        origin: str,
        destination: str,
        flights: list[dict] | None,
        error_type: str | None,
        retries: int,
        success: bool,
        searched_at: str,
    ):
        flights_json = json.dumps(flights) if flights else None
        self._write_queue.put_nowait(
            (
                route,
                dep_date,
                origin,
                destination,
                flights_json,
                error_type,
                retries,
                int(success),
                searched_at,
            )
        )

    async def insert_ignore_all(self, tasks: list[tuple[str, str, str, str]]):
        sql = (
            "INSERT OR IGNORE INTO search_results "
            "(route, dep_date, origin, destination, retries, success) "
            "VALUES (?, ?, ?, ?, 0, 0)"
        )
        async with self._write_lock:
            await self._conn.executemany(sql, tasks)
            await self._conn.commit()

    async def get_failed(self, max_retries: int = 3) -> list[tuple[str, str]]:
        placeholders = ",".join("?" for _ in _RETRY_ERROR_TYPES)
        cursor = await self._conn.execute(
            "SELECT route, dep_date "
            f"FROM search_results WHERE success = 0 "
            f"AND error_type IN ({placeholders}) AND retries < ?",
            (*_RETRY_ERROR_TYPES, max_retries),
        )
        rows = await cursor.fetchall()
        return [(r[0], r[1]) for r in rows]

    async def count_status(self) -> tuple[int, int]:
        cursor = await self._conn.execute(
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
        cursor = await self._conn.execute(
            "SELECT error_type, COUNT(*) FROM search_results "
            "WHERE success = 0 GROUP BY error_type"
        )
        rows = await cursor.fetchall()
        return [(r[0], r[1]) for r in rows]

    async def _iter_successful(self, raw: bool) -> AsyncIterator[dict]:
        cursor = await self._conn.execute(
            "SELECT route, dep_date, origin, destination, flights, searched_at "
            "FROM search_results WHERE success = 1 ORDER BY route, dep_date"
        )
        async for row in cursor:
            flights = row[4]
            if raw:
                flights = flights or "[]"
            else:
                flights = json.loads(flights) if flights else []
            yield {
                "route": row[0],
                "dep_date": row[1],
                "origin": row[2],
                "destination": row[3],
                "flights": flights,
                "searched_at": row[5],
            }

    async def iter_successful(self) -> AsyncIterator[dict]:
        async for row in self._iter_successful(raw=False):
            yield row

    async def iter_successful_raw(self) -> AsyncIterator[dict]:
        async for row in self._iter_successful(raw=True):
            yield row

    async def delete_db(self):
        await self.close()
        if os.path.exists(self._db_path):
            os.remove(self._db_path)
            logger.info("Deleted SQLite state: %s", self._db_path)
