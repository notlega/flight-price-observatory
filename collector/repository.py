"""SQLite-backed search result store with upsert and retry tracking."""

import asyncio
import json
import logging
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path
from typing import NamedTuple, cast

import aiosqlite

from collector.errors import ErrorType, RepositoryStateError
from collector.models.flight_result import FlightResultDict, SearchResultRow

logger = logging.getLogger(__name__)

_BUSY_TIMEOUT_MS = 10000

_WriteItem = tuple[object, ...] | object


class SeedRow(NamedTuple):
    """A route to seed in the DB before any search attempt runs."""

    route: str
    dep_date: str
    return_date: str
    flight_type: str
    origin: str
    destination: str


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
INSERT OR REPLACE INTO search_results (
    route, dep_date, return_date, flight_type, origin, destination,
    flights, error_type, retries, success, searched_at
)
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

_REQUIRED_COLUMNS = frozenset(
    {
        "route",
        "dep_date",
        "return_date",
        "flight_type",
        "origin",
        "destination",
        "flights",
        "error_type",
        "retries",
        "success",
    }
)


class SearchRepository:
    """SQLite search result store with async batching and retry tracking."""

    def __init__(self, db_path: str) -> None:
        """Create repository targeting ``db_path``; connection opens lazily."""
        self._db_path = db_path
        self._conn: aiosqlite.Connection | None = None
        self._queue: asyncio.Queue[_WriteItem] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._write_lock = asyncio.Lock()
        self._writer_error: Exception | None = None

    @property
    def _connection(self) -> aiosqlite.Connection:
        """Return the open connection, raising when not opened."""
        if self._conn is None:
            raise RuntimeError("repository connection not open")
        return self._conn

    @property
    def _write_queue(self) -> asyncio.Queue[_WriteItem]:
        """Return the writer queue, raising when the writer is not started."""
        if self._queue is None:
            raise RuntimeError("writer queue not started")
        return self._queue

    async def open(self) -> None:
        """Open the database connection and start the writer loop."""
        db_dir = Path(self._db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        try:
            probe = sqlite3.connect(self._db_path)
            try:
                probe.execute("SELECT 1")
            finally:
                probe.close()
        except sqlite3.Error as e:
            raise RepositoryStateError(
                f"invalid database at {self._db_path}: {e}"
            ) from e
        conn = await aiosqlite.connect(self._db_path, isolation_level=None)
        self._conn = conn
        try:
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
            await self._migrate()
            await conn.execute(_CREATE_SQL)
            await conn.commit()
        except Exception:
            await conn.close()
            self._conn = None
            raise
        self._queue = asyncio.Queue()
        self._writer_task = asyncio.create_task(self._writer_loop())

    async def require_existing(self) -> None:
        """Validate that a usable search state exists for a continue run.

        Read-only: never creates or modifies the database. Raises
        :class:`RepositoryStateError` when the file is missing, not a valid
        SQLite database, lacks the expected schema, or holds no rows.
        """
        db_path = Path(self._db_path)
        if not db_path.is_file():
            raise RepositoryStateError(
                f"no existing database at {self._db_path} "
                "(--continue requires a prior run)"
            )
        try:
            probe = sqlite3.connect(self._db_path)
            try:
                probe.execute("SELECT 1")
                cursor = probe.execute("PRAGMA table_info(search_results)")
                columns = {row[1] for row in cursor.fetchall()}
            finally:
                probe.close()
        except sqlite3.Error as e:
            raise RepositoryStateError(
                f"invalid database at {self._db_path}: {e}"
            ) from e
        if not _REQUIRED_COLUMNS.issubset(columns):
            missing = sorted(_REQUIRED_COLUMNS - columns)
            raise RepositoryStateError(
                f"invalid database schema at {self._db_path}: missing columns {missing}"
            )
        async with aiosqlite.connect(self._db_path, isolation_level=None) as conn:
            cursor = await conn.execute("SELECT COUNT(*) FROM search_results")
            row = await cursor.fetchone()
        if row is None or row[0] == 0:
            raise RepositoryStateError(
                f"empty database at {self._db_path}: no search state to continue"
            )

    async def _migrate(self) -> None:
        """Drop legacy schemas missing the ``return_date`` column."""
        cursor = await self._connection.execute("PRAGMA table_info(search_results)")
        columns = {row[1] for row in await cursor.fetchall()}
        if columns and "return_date" not in columns:
            await self._connection.execute("DROP TABLE search_results")

    async def _writer_loop(self) -> None:
        """Drain the write queue, committing in batches; stay alive on errors."""
        batch: list[tuple[object, ...]] = []
        while True:
            item = await self._write_queue.get()
            try:
                if item is _STOP:
                    await self._commit(batch)
                    batch.clear()
                    return
                if item is _FLUSH:
                    await self._commit(batch)
                    batch.clear()
                    continue
                batch.append(cast(tuple[object, ...], item))
                if len(batch) >= _WRITE_BATCH_SIZE:
                    await self._commit(batch)
                    batch.clear()
            except Exception as e:
                # Keep the writer alive so the queue drains and callers never
                # hang in flush()/close(); surface the failure on the next
                # sync point instead.
                self._writer_error = e
                logger.exception("Writer commit failed; dropping pending batch")
                batch.clear()
            finally:
                self._write_queue.task_done()

    async def _commit(self, batch: list[tuple[object, ...]]) -> None:
        """Executemany-insert ``batch`` in one transaction."""
        if not batch:
            return
        async with self._write_lock:
            await self._connection.executemany(_INSERT_SQL, batch)
            await self._connection.commit()

    async def flush(self) -> None:
        """Commit all queued writes (await the writer draining the queue)."""
        if self._conn is None or self._queue is None:
            return
        # Drop the lock here: _commit takes it, so holding it while waiting on
        # the queue would deadlock the writer.
        self._queue.put_nowait(_FLUSH)
        await self._write_queue.join()
        if self._writer_error is not None:
            raise self._writer_error

    async def close(self) -> None:
        """Stop the writer and close the connection; safe when never opened."""
        if self._conn is None:
            return
        if self._queue is not None:
            self._queue.put_nowait(_STOP)
            if self._writer_task is not None:
                await self._writer_task
            self._queue = None
            self._writer_task = None
        await self._connection.close()
        self._conn = None

    async def upsert(
        self,
        *,
        route: str,
        dep_date: str,
        return_date: str,
        flight_type: str,
        origin: str,
        destination: str,
        flights: list[FlightResultDict] | None,
        error_type: str | None,
        retries: int,
        success: bool,
        searched_at: str,
    ) -> None:
        """Queue a search result write, keyed by route and dates."""
        if self._writer_error is not None:
            raise self._writer_error
        flights_json = json.dumps(flights) if flights else None
        self._write_queue.put_nowait(
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

    async def insert_ignore_all(self, tasks: list[SeedRow]) -> None:
        """Bulk-insert ``tasks`` as placeholder rows, ignoring existing keys."""
        sql = (
            "INSERT OR IGNORE INTO search_results "
            "(route, dep_date, return_date, flight_type, origin, destination, "
            "retries, success) VALUES (?, ?, ?, ?, ?, ?, 0, 0)"
        )
        async with self._write_lock:
            await self._connection.executemany(sql, tasks)

    async def purge_abandoned_seeds(self) -> int:
        """Delete seeded placeholder rows from interrupted runs.

        Rows with ``success = 0`` and NULL ``error_type`` are only ever
        created by ``insert_ignore_all``; a surviving row means its task was
        never searched (e.g. the process was killed mid-run). Purging them
        keeps ``count_by_error()`` from reporting abandoned seeds as failures.
        """
        async with self._write_lock:
            cursor = await self._connection.execute(
                "DELETE FROM search_results WHERE success = 0 AND error_type IS NULL"
            )
            await self._connection.commit()
        return cursor.rowcount

    async def get_failed(
        self, max_retries: int | None = 3, since: str | None = None
    ) -> list[tuple[str, str, str, str]]:
        """Return failed tasks retried at most ``max_retries`` attempts.

        ``retries`` stores cumulative attempts consumed (1-based), so a task
        that failed once per round carries ``retries = round * _MAX_ATTEMPTS``.
        A ``None`` ``max_retries`` fetches every retryable failure regardless
        of how many attempts were already consumed. Pass ``since`` (ISO date)
        to skip departures before that date.
        """
        placeholders = ",".join("?" for _ in _RETRY_ERROR_TYPES)
        since_clause = " AND dep_date >= ?" if since is not None else ""
        since_params = () if since is None else (since,)
        if max_retries is None:
            cursor = await self._connection.execute(
                "SELECT route, dep_date, return_date, flight_type "
                f"FROM search_results WHERE success = 0 "
                f"AND error_type IN ({placeholders}){since_clause}",
                (*_RETRY_ERROR_TYPES, *since_params),
            )
        else:
            cursor = await self._connection.execute(
                "SELECT route, dep_date, return_date, flight_type "
                f"FROM search_results WHERE success = 0 "
                f"AND error_type IN ({placeholders}){since_clause} AND retries <= ?",
                (*_RETRY_ERROR_TYPES, *since_params, max_retries),
            )
        rows = await cursor.fetchall()
        return [(r[0], r[1], r[2], r[3]) for r in rows]

    async def count_status(self) -> tuple[int, int]:
        """Return (success, failed) row counts."""
        cursor = await self._connection.execute(
            "SELECT "
            "COALESCE(SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END), 0), "
            "COALESCE(SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END), 0) "
            "FROM search_results"
        )
        row = await cursor.fetchone()
        return (row[0], row[1]) if row else (0, 0)

    async def count_successful(self) -> int:
        """Return number of successful rows."""
        return (await self.count_status())[0]

    async def count_failed(self) -> int:
        """Return number of failed rows."""
        return (await self.count_status())[1]

    async def count_by_error(self) -> list[tuple[str, int]]:
        """Return (error_type, count) pairs for failed rows, grouped by error."""
        cursor = await self._connection.execute(
            "SELECT error_type, COUNT(*) FROM search_results "
            "WHERE success = 0 GROUP BY error_type"
        )
        rows = await cursor.fetchall()
        return [(r[0], r[1]) for r in rows]

    async def _iter_successful(self, raw: bool) -> AsyncIterator[SearchResultRow]:
        """Yield every successful row, parsing ``flights`` unless ``raw``."""
        cursor = await self._connection.execute(
            "SELECT route, dep_date, return_date, flight_type, origin, "
            "destination, flights, searched_at "
            "FROM search_results WHERE success = 1 ORDER BY route, dep_date"
        )
        async for row in cursor:
            raw_flights = row[6]
            if raw:
                flights: list[FlightResultDict] | str = raw_flights or "[]"
            else:
                flights = json.loads(raw_flights) if raw_flights else []
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

    async def iter_successful(self) -> AsyncIterator[SearchResultRow]:
        """Yield successful rows with ``flights`` parsed to Python objects."""
        async for row in self._iter_successful(raw=False):
            yield row

    async def iter_successful_raw(self) -> AsyncIterator[SearchResultRow]:
        """Yield successful rows with ``flights`` as the stored JSON string."""
        async for row in self._iter_successful(raw=True):
            yield row

    async def delete_db(self) -> None:
        """Close the repository and remove the database file plus WAL/SHM."""
        await self.close()
        for path in (
            Path(self._db_path),
            Path(f"{self._db_path}-shm"),
            Path(f"{self._db_path}-wal"),
        ):
            if path.exists():
                path.unlink()
        logger.info("Deleted SQLite state: %s", self._db_path)
