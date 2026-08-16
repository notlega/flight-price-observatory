"""Bulk search orchestration: task building, batched execution, retries."""

import asyncio
import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import NamedTuple

from curl_cffi.requests import AsyncSession
from fli.models import Airport

from collector.config import (
    DEFAULT_CURRENCY,
    DEFAULT_DB_PATH,
    DEFAULT_MAX_DAYS_AHEAD,
    DEFAULT_RATE,
    DEFAULT_WORKERS,
)
from collector.errors import (
    ErrorType,
    ProviderBlockedError,
    ProviderConnectionError,
    ProviderDataError,
    ProviderRateLimitedError,
    ProviderTimeoutError,
)
from collector.models.flight_result import FlightResultDict
from collector.models.flight_type import FlightType
from collector.models.proxy import ProxyInfo
from collector.providers.base import BaseProvider
from collector.proxy import ProxyRotator
from collector.repository import SearchRepository, SeedRow
from collector.routes import RouteCatalog
from collector.services.progress import ProgressLogger
from collector.services.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 3
_PROGRESS_LOG_STEP_PCT = 5
_MIN_POOL_BEFORE_RETRY = 20
_NO_PROXY_DELAY = 0.5
_STUB_BACKOFF_S = 30
_MID_REFRESH_GAP = 300


@dataclass(slots=True, kw_only=True)
class SearchTask:
    """One provider + route + date combination to search."""

    provider: BaseProvider
    origin: Airport
    dest: Airport
    departure: str
    return_date: str | None
    flight_type: str


def _route_key(origin: Airport, dest: Airport) -> str:
    """Return the stable DB key for an origin-destination pair."""
    return f"{origin.value}|{dest.value}"


def _supported_routes(
    provider: BaseProvider,
) -> Iterator[tuple[Airport, Airport]]:
    """Yield (origin, dest) airports for every one-way route the provider supports."""
    for r in RouteCatalog.one_way_routes():
        if (
            provider.supports is not None
            and (r.origin, r.dest) not in provider.supports
        ):
            continue
        yield RouteCatalog.resolve(r.origin), RouteCatalog.resolve(r.dest)


def _dates_between(start_date: date, end_date: date, today: date) -> list[date]:
    """Return dates in [start_date, end_date] on or after ``today``."""
    return [
        current
        for current in (
            start_date + timedelta(days=n)
            for n in range((end_date - start_date).days + 1)
        )
        if current >= today
    ]


def _format_duration(seconds: float) -> str:
    """Format seconds as a compact ``HhMm``/``MmSs``/``Ss`` string."""
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


class AttemptResult(NamedTuple):
    """Outcome of one provider search attempt."""

    flights: list[FlightResultDict] | None
    error_type: str | None
    proxy_info: ProxyInfo | None
    stubbed: bool = False


class BulkSearchPipeline:
    """Orchestrate bulk searches: task building, batched execution, retries."""

    def __init__(
        self,
        providers: list[BaseProvider],
        rate: float = DEFAULT_RATE,
        max_concurrent: int = DEFAULT_WORKERS,
        db_path: str = DEFAULT_DB_PATH,
        currency: str = DEFAULT_CURRENCY,
    ):
        """Create pipeline with given providers, rate, concurrency, and store."""
        self.providers = providers
        self.rotator = ProxyRotator()
        self.rate_limiter = RateLimiter(max_rate=rate)
        self.max_concurrent = max_concurrent
        self.repo = SearchRepository(db_path)
        self.db_path = db_path
        self.currency = currency
        self._mid_refresh = False
        self._last_mid_refresh = float("-inf")

    async def _attempt_once(
        self,
        provider: BaseProvider,
        origin: Airport,
        dest: Airport,
        departure: str,
        return_date: str | None,
        session: AsyncSession,
    ) -> AttemptResult:
        """Run one search attempt, mapping provider errors to result types."""
        proxy_info = await self.rotator.get_proxy()
        if proxy_info is None:
            await asyncio.sleep(_NO_PROXY_DELAY)
            return AttemptResult(None, ErrorType.NO_PROXY, None)
        proxy_url = proxy_info.url

        await self.rate_limiter.acquire()

        try:
            flights = await provider.search(
                origin,
                dest,
                departure,
                currency=self.currency,
                proxy_url=proxy_url,
                session=session,
                return_date=return_date,
            )
            await self.rate_limiter.report_success()
            if not flights:
                return AttemptResult(None, ErrorType.DATA, proxy_info)
            return AttemptResult(flights, None, proxy_info)
        except ProviderRateLimitedError:
            await self.rate_limiter.report_429()
            await self.rotator.report_rate_limited(proxy_info)
            return AttemptResult(None, ErrorType.RATE_LIMITED, proxy_info)
        except ProviderBlockedError:
            await self.rotator.report_stub(proxy_info)
            return AttemptResult(None, ErrorType.DATA, proxy_info, stubbed=True)
        except ProviderTimeoutError:
            return AttemptResult(None, ErrorType.TIMEOUT, proxy_info)
        except ProviderConnectionError:
            return AttemptResult(None, ErrorType.CONNECTION, proxy_info)
        except ProviderDataError:
            return AttemptResult(None, ErrorType.DATA, proxy_info)
        except Exception as e:
            logger.warning(
                "Unexpected failure for %s->%s on %s via %s: %s",
                origin.value,
                dest.value,
                departure,
                proxy_url,
                e,
            )
            return AttemptResult(None, ErrorType.OTHER, proxy_info)

    async def _search_and_store(
        self,
        task: SearchTask,
        session: AsyncSession,
        retry_round: int = 0,
    ) -> None:
        """Retry ``task`` up to ``_MAX_ATTEMPTS`` times, then store the outcome."""
        searched_at = datetime.now(UTC).isoformat()
        error_type: str | None = None

        for attempt in range(_MAX_ATTEMPTS):
            result = await self._attempt_once(
                task.provider,
                task.origin,
                task.dest,
                task.departure,
                task.return_date,
                session,
            )
            error_type = result.error_type
            if (
                error_type in (ErrorType.TIMEOUT, ErrorType.CONNECTION)
                and result.proxy_info is not None
            ):
                await self.rotator.report_failure(result.proxy_info)
            if result.error_type is None:
                await self._store_result(
                    task,
                    flights=result.flights,
                    error_type=None,
                    retries=attempt + 1,
                    success=True,
                    searched_at=searched_at,
                )
                return
            logger.debug(
                "Attempt %d/%d failed for %s->%s on %s via %s: %s",
                attempt + 1,
                _MAX_ATTEMPTS,
                task.origin.value,
                task.dest.value,
                task.departure,
                result.proxy_info.url if result.proxy_info else "direct",
                result.error_type,
            )
            if result.stubbed:
                await asyncio.sleep(_STUB_BACKOFF_S)

        await self._store_result(
            task,
            flights=[],
            error_type=error_type,
            retries=(retry_round + 1) * _MAX_ATTEMPTS,
            success=False,
            searched_at=searched_at,
        )

    async def _store_result(
        self,
        task: SearchTask,
        *,
        flights: list[FlightResultDict] | None,
        error_type: str | None,
        retries: int,
        success: bool,
        searched_at: str,
    ) -> None:
        """Persist one search outcome, enforcing failed rows carry an error."""
        if not success and error_type is None:
            raise ValueError("failed result must carry an error_type")
        await self.repo.upsert(
            route=_route_key(task.origin, task.dest),
            dep_date=task.departure,
            return_date=task.return_date or "",
            flight_type=task.flight_type,
            origin=task.origin.value,
            destination=task.dest.value,
            flights=flights,
            error_type=error_type,
            retries=retries,
            success=success,
            searched_at=searched_at,
        )

    async def _process_task(
        self,
        task: SearchTask,
        retry_round: int,
        session: AsyncSession,
    ) -> None:
        """Run ``task`` and trigger a mid-round proxy refresh when the pool shrinks."""
        try:
            await self._search_and_store(task, session, retry_round=retry_round)
        except Exception as e:
            logger.warning(
                "Task failed unexpectedly for %s->%s on %s: %s",
                task.origin.value,
                task.dest.value,
                task.departure,
                e,
            )
            await self._record_failure(task, retry_round)
        if (
            retry_round > 0
            and self.rotator.working_count() < _MIN_POOL_BEFORE_RETRY
            and not self._mid_refresh
            and time.monotonic() - self._last_mid_refresh > _MID_REFRESH_GAP
        ):
            self._mid_refresh = True
            self._last_mid_refresh = time.monotonic()
            try:
                await self.rotator.refresh(force=True)
            except Exception:
                logger.exception("Mid-round proxy refresh failed")
            finally:
                self._mid_refresh = False

    async def _run_batch(
        self,
        tasks: list[SearchTask],
        desc: str,
        retry_round: int = 0,
    ) -> None:
        """Process ``tasks`` with concurrent workers, logging step progress."""
        total = len(tasks)
        queue: asyncio.Queue[SearchTask] = asyncio.Queue()
        for task in tasks:
            queue.put_nowait(task)

        done = 0
        progress = ProgressLogger(logger, step_pct=_PROGRESS_LOG_STEP_PCT)

        def log_progress(final: bool = False) -> None:
            def render(pct: int, done: int, total: int, elapsed: float) -> str:
                if done:
                    rate = done / elapsed
                    eta = (total - done) / rate if rate > 0 else 0.0
                    stats = (
                        f"{rate:.1f}/s | {_format_duration(elapsed)} elapsed "
                        f"| {_format_duration(eta)} ETA"
                    )
                else:
                    stats = f"n/a | {_format_duration(elapsed)} elapsed | n/a ETA"
                return f"{desc}: {pct}% ({done}/{total}) [{stats}]"

            progress.maybe_log(done, total, render, force=final)

        async def worker() -> None:
            nonlocal done
            async with AsyncSession() as session:
                while True:
                    try:
                        task = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    try:
                        await self._process_task(task, retry_round, session)
                    finally:
                        done += 1
                        log_progress()

        n_workers = min(max(self.max_concurrent, 1), max(total, 1))
        workers = [asyncio.create_task(worker()) for _ in range(n_workers)]
        results = await asyncio.gather(*workers, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                logger.error("Search batch worker crashed: %s", result)
        log_progress(final=True)

    async def _record_failure(
        self,
        task: SearchTask,
        retry_round: int,
    ) -> None:
        """Persist an unexpected task failure as an OTHER error."""
        try:
            await self._store_result(
                task,
                flights=[],
                error_type=ErrorType.OTHER,
                retries=(retry_round + 1) * _MAX_ATTEMPTS,
                success=False,
                searched_at=datetime.now(UTC).isoformat(),
            )
        except Exception:
            logger.exception(
                "Failed to record failed task %s->%s on %s",
                task.origin.value,
                task.dest.value,
                task.departure,
            )

    async def _get_provider_map(
        self,
    ) -> dict[str, tuple[BaseProvider, Airport, Airport]]:
        """Map route key to the provider that covers it."""
        m: dict[str, tuple[BaseProvider, Airport, Airport]] = {}
        for p in self.providers:
            for origin, dest in _supported_routes(p):
                m[_route_key(origin, dest)] = (p, origin, dest)
        return m

    async def _retry_loop(
        self, rounds: int = 3, retry_all_failures: bool = False
    ) -> None:
        """Retry failed tasks over ``rounds`` passes, refreshing proxies between."""
        provider_map = await self._get_provider_map()
        since = date.today().isoformat()
        for rnd in range(1, rounds + 1):
            failed = await self.repo.get_failed(
                max_retries=None if retry_all_failures else rnd * _MAX_ATTEMPTS,
                since=since,
            )
            if not failed:
                logger.info("No failed tasks to retry")
                return
            if rnd == 1:
                # Retry queries failed on the first-pass pool, which Google has
                # throttled; a fresh pool is the main recovery lever (measured:
                # pool 3 -> 62 recovered 226/240 in round 3 vs 0/240 in rounds
                # 1-2 on the stale pool).
                logger.info(
                    "Refreshing proxy pool before retry round 1 (%d usable)",
                    self.rotator.usable_count(),
                )
                await self.rotator.refresh(force=True)
            elif self.rotator.working_count() < _MIN_POOL_BEFORE_RETRY:
                logger.info(
                    "Proxy pool low (%d); refreshing before retry round %d",
                    self.rotator.working_count(),
                    rnd,
                )
                await self.rotator.refresh(force=True)

            retry_tasks: list[SearchTask] = []
            for route, dep_date, return_date, flight_type in failed:
                if dep_date < date.today().isoformat():
                    continue
                if route in provider_map:
                    provider, origin, dest = provider_map[route]
                    retry_tasks.append(
                        SearchTask(
                            provider=provider,
                            origin=origin,
                            dest=dest,
                            departure=dep_date,
                            return_date=return_date,
                            flight_type=flight_type,
                        )
                    )
                else:
                    logger.warning(
                        "Skipping failed route %s: no provider covers it", route
                    )

            if retry_tasks:
                logger.info(
                    "Retry round %d/%d: %d tasks", rnd, rounds, len(retry_tasks)
                )
                await self._run_batch(
                    retry_tasks,
                    f"Retry round {rnd}/{rounds}",
                    retry_round=rnd,
                )

    async def _log_counts(self, label: str) -> None:
        """Log success/failure totals under ``label``."""
        success, failed = await self.repo.count_status()
        logger.info("%s: %d success, %d failed", label, success, failed)

    async def _log_failure_breakdown(self) -> None:
        """Log failed-task counts grouped by error type."""
        by_error = await self.repo.count_by_error()
        if by_error:
            logger.warning("Failed breakdown: %s", dict(by_error))

    async def _build_tasks(
        self,
        start_date: date,
        effective_end: date,
    ) -> tuple[list[SearchTask], list[SeedRow]]:
        """Build search tasks and seed rows for the provider date window."""
        tasks: list[SearchTask] = []
        seed_rows: list[SeedRow] = []
        today = date.today()

        def emit(
            provider: BaseProvider,
            origin: Airport,
            dest: Airport,
            dep_date: date,
            return_date: str | None,
        ) -> None:
            ds = dep_date.isoformat()
            flight_type = (
                FlightType.ROUND_TRIP.value if return_date else FlightType.ONE_WAY.value
            )
            tasks.append(
                SearchTask(
                    provider=provider,
                    origin=origin,
                    dest=dest,
                    departure=ds,
                    return_date=return_date,
                    flight_type=flight_type,
                )
            )
            seed_rows.append(
                SeedRow(
                    route=_route_key(origin, dest),
                    dep_date=ds,
                    return_date=return_date or "",
                    flight_type=flight_type,
                    origin=origin.value,
                    destination=dest.value,
                )
            )

        for provider in self.providers:
            for origin, dest in _supported_routes(provider):
                for current in _dates_between(start_date, effective_end, today):
                    emit(provider, origin, dest, current, None)

            for r in RouteCatalog.round_trip_routes():
                if (
                    provider.supports is not None
                    and (r.origin, r.dest) not in provider.supports
                ):
                    continue
                origin = RouteCatalog.resolve(r.origin)
                dest = RouteCatalog.resolve(r.dest)
                for offset in RouteCatalog.ROUND_TRIP_OFFSETS:
                    for current in _dates_between(start_date, effective_end, today):
                        emit(
                            provider,
                            origin,
                            dest,
                            current,
                            (current + timedelta(days=offset)).isoformat(),
                        )

        return tasks, seed_rows

    async def run(
        self,
        start_date: date,
        end_date: date,
        max_days_ahead: int = DEFAULT_MAX_DAYS_AHEAD,
        continue_run: bool = False,
    ) -> None:
        """Run the bulk search lifecycle.

        Builds tasks, seeds the DB, refreshes the proxy pool, executes
        batches and retries transient failures. The SQLite state file is
        always retained; export to JSONL is a separate step
        (``cli convert storage/db/search_state.db``). With ``continue_run``
        the existing DB is reused and only previously failed tasks are
        retried; seeding and the full search pass are skipped.

        Args:
            start_date: First departure date.
            end_date: Last departure date.
            max_days_ahead: Hard cap on departure horizon from today.
            continue_run: Resume by retrying failed tasks from an existing
                database. ``start_date``/``end_date``/``max_days_ahead`` are
                ignored and the DB must already hold search state.
        """
        tasks: list[SearchTask] = []
        seed_rows: list[SeedRow] = []
        if continue_run:
            await self.repo.require_existing()
            logger.info("Continue run: retrying failed tasks from existing state")
        else:
            cutoff = date.today() + timedelta(days=max_days_ahead)
            effective_end = min(end_date, cutoff)

            tasks, seed_rows = await self._build_tasks(start_date, effective_end)

            logger.info(
                "Total tasks: %d across %d provider(s)",
                len(tasks),
                len(self.providers),
            )

            if not tasks:
                logger.warning(
                    "No tasks in window %s -> %s; nothing to search "
                    "(is --start in the future?)",
                    start_date,
                    effective_end,
                )
                return

        proxy_task = asyncio.create_task(self.rotator.refresh())
        try:
            await self.repo.open()
            if not continue_run:
                purged = await self.repo.purge_abandoned_seeds()
                if purged:
                    logger.info("Purged %d abandoned seed rows", purged)
                await self.repo.insert_ignore_all(seed_rows)
            await proxy_task

            if self.rotator.working_count() == 0:
                logger.warning("Zero working proxies; force-refreshing before abort")
                await self.rotator.refresh(force=True)
            if self.rotator.working_count() == 0:
                raise RuntimeError(
                    "Zero working proxies — refusing to run without proxy cover"
                )

            if not continue_run:
                await self._run_batch(tasks, "Searching flights")

            label = "Search complete" if not continue_run else "Continue pass"
            await self.repo.flush()
            await self._log_counts(label)

            await self._retry_loop(retry_all_failures=continue_run)

            await self.repo.flush()
            await self._log_counts("After retries")
            await self._log_failure_breakdown()

            await self.repo.close()
            logger.info("State retained in %s (export via: cli convert)", self.db_path)
        finally:
            if not proxy_task.done():
                proxy_task.cancel()
            await self.repo.close()
