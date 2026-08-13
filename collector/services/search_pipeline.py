"""Bulk search orchestration: task building, batched execution, retries."""

import asyncio
import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import NamedTuple

from curl_cffi.requests import AsyncSession
from fli.models import Airport

from collector.convert import convert, default_output_path
from collector.errors import (
    ErrorType,
    ProviderConnectionError,
    ProviderDataError,
    ProviderRateLimitedError,
    ProviderTimeoutError,
)
from collector.models.flight_type import FlightType
from collector.models.proxy import ProxyInfo
from collector.providers.base import BaseProvider
from collector.proxy import ProxyRotator
from collector.repository import SearchRepository
from collector.routes import RouteCatalog
from collector.services.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 3
_MIN_POOL_BEFORE_RETRY = 20
_NO_PROXY_DELAY = 0.5

type ProviderTask = tuple[BaseProvider, Airport, Airport, str, str | None, str]
type SeedRow = tuple[str, str, str, str, str, str]


def _route_key(origin: Airport, dest: Airport) -> str:
    return f"{origin.value}|{dest.value}"


def _dates_between(start_date: date, end_date: date, today: date) -> list[date]:
    return [
        current
        for current in (
            start_date + timedelta(days=n)
            for n in range((end_date - start_date).days + 1)
        )
        if current >= today
    ]


def _format_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


class AttemptResult(NamedTuple):
    flights: list[dict] | None
    error_type: str | None
    proxy_info: ProxyInfo | None


class BulkSearchPipeline:
    def __init__(
        self,
        providers: list[BaseProvider],
        rate: float = 200,
        max_concurrent: int = 50,
        db_path: str = "storage/db/search_state.db",
        currency: str = "SGD",
        keep_db: bool = False,
    ):
        self.providers = providers
        self.rotator = ProxyRotator()
        self.rate_limiter = RateLimiter(max_rate=rate)
        self.max_concurrent = max_concurrent
        self.repo = SearchRepository(db_path)
        self.db_path = db_path
        self.currency = currency
        self.keep_db = keep_db

    async def _attempt_once(
        self,
        provider: BaseProvider,
        origin: Airport,
        dest: Airport,
        departure: str,
        return_date: str | None,
        session: AsyncSession,
    ) -> AttemptResult:
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
        provider: BaseProvider,
        origin: Airport,
        dest: Airport,
        departure: str,
        return_date: str | None,
        flight_type: str,
        session: AsyncSession,
        retry_round: int = 0,
    ):
        searched_at = datetime.now(timezone.utc).isoformat()
        error_type: str | None = None

        for attempt in range(_MAX_ATTEMPTS):
            result = await self._attempt_once(
                provider, origin, dest, departure, return_date, session
            )
            error_type = result.error_type
            if (
                error_type in (ErrorType.TIMEOUT, ErrorType.CONNECTION)
                and result.proxy_info is not None
            ):
                await self.rotator.report_failure(result.proxy_info)
            if result.error_type is None:
                await self._store_result(
                    origin,
                    dest,
                    departure,
                    return_date,
                    flight_type,
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
                origin.value,
                dest.value,
                departure,
                result.proxy_info.url if result.proxy_info else "direct",
                result.error_type,
            )

        await self._store_result(
            origin,
            dest,
            departure,
            return_date,
            flight_type,
            flights=[],
            error_type=error_type,
            retries=(retry_round + 1) * _MAX_ATTEMPTS,
            success=False,
            searched_at=searched_at,
        )

    async def _store_result(
        self,
        origin: Airport,
        dest: Airport,
        departure: str,
        return_date: str | None,
        flight_type: str,
        flights: list[dict] | None,
        error_type: str | None,
        retries: int,
        success: bool,
        searched_at: str,
    ):
        await self.repo.upsert(
            route=_route_key(origin, dest),
            dep_date=departure,
            return_date=return_date or "",
            flight_type=flight_type,
            origin=origin.value,
            destination=dest.value,
            flights=flights,
            error_type=error_type,
            retries=retries,
            success=success,
            searched_at=searched_at,
        )

    async def _run_batch(
        self,
        tasks: list[ProviderTask],
        desc: str,
        retry_round: int = 0,
    ):
        total = len(tasks)
        queue: asyncio.Queue = asyncio.Queue()
        for task in tasks:
            queue.put_nowait(task)

        started = time.monotonic()
        done = 0
        next_log_pct = 5

        def log_progress(final: bool = False) -> None:
            nonlocal next_log_pct
            pct = done * 100 // total if total else 100
            if not final and pct < next_log_pct:
                return
            elapsed = time.monotonic() - started
            if done:
                rate = done / elapsed
                eta = (total - done) / rate if rate > 0 else 0.0
                stats = f"{rate:.1f}/s | {_format_duration(elapsed)} elapsed | {_format_duration(eta)} ETA"
            else:
                stats = f"n/a | {_format_duration(elapsed)} elapsed | n/a ETA"
            logger.info(
                "%s: %d%% (%d/%d) [%s]",
                desc,
                pct,
                done,
                total,
                stats,
            )
            next_log_pct = pct + 5

        async def worker():
            nonlocal done, next_log_pct
            async with AsyncSession() as session:
                while True:
                    try:
                        (
                            provider,
                            origin,
                            dest,
                            departure,
                            return_date,
                            flight_type,
                        ) = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    try:
                        await self._search_and_store(
                            provider,
                            origin,
                            dest,
                            departure,
                            return_date,
                            flight_type,
                            session,
                            retry_round=retry_round,
                        )
                    except Exception as e:
                        logger.warning(
                            "Task failed unexpectedly for %s->%s on %s: %s",
                            origin.value,
                            dest.value,
                            departure,
                            e,
                        )
                        await self._record_failure(
                            origin,
                            dest,
                            departure,
                            return_date,
                            flight_type,
                            retry_round,
                        )
                    finally:
                        done += 1
                        log_progress()

        n_workers = min(max(self.max_concurrent, 1), max(total, 1))
        workers = [asyncio.create_task(worker()) for _ in range(n_workers)]
        await asyncio.gather(*workers)
        log_progress(final=True)

    async def _record_failure(
        self,
        origin: Airport,
        dest: Airport,
        departure: str,
        return_date: str | None,
        flight_type: str,
        retry_round: int,
    ):
        try:
            await self._store_result(
                origin,
                dest,
                departure,
                return_date,
                flight_type,
                flights=[],
                error_type=ErrorType.OTHER,
                retries=(retry_round + 1) * _MAX_ATTEMPTS,
                success=False,
                searched_at=datetime.now(timezone.utc).isoformat(),
            )
        except Exception:
            logger.exception(
                "Failed to record failed task %s->%s on %s",
                origin.value,
                dest.value,
                departure,
            )

    async def _get_provider_map(
        self,
    ) -> dict[str, tuple[BaseProvider, Airport, Airport]]:
        m: dict[str, tuple[BaseProvider, Airport, Airport]] = {}
        for p in self.providers:
            for r in RouteCatalog.one_way_routes():
                if p.supports is not None and (r.origin, r.dest) not in p.supports:
                    continue
                origin = RouteCatalog.resolve(r.origin)
                dest = RouteCatalog.resolve(r.dest)
                m[_route_key(origin, dest)] = (p, origin, dest)
        return m

    async def _retry_loop(self, rounds: int = 3):
        provider_map = await self._get_provider_map()
        for rnd in range(1, rounds + 1):
            failed = await self.repo.get_failed(max_retries=rnd * _MAX_ATTEMPTS)
            if not failed:
                logger.info("No failed tasks to retry")
                return
            if self.rotator.working_count() < _MIN_POOL_BEFORE_RETRY:
                logger.info(
                    "Proxy pool low (%d); refreshing before retry round %d",
                    self.rotator.working_count(),
                    rnd,
                )
                await self.rotator.refresh(force=True)

            retry_tasks: list[
                tuple[BaseProvider, Airport, Airport, str, str | None, str]
            ] = []
            for route, dep_date, return_date, flight_type in failed:
                if dep_date < date.today().isoformat():
                    continue
                if route in provider_map:
                    provider, origin, dest = provider_map[route]
                    retry_tasks.append(
                        (provider, origin, dest, dep_date, return_date, flight_type)
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

    async def _log_counts(self, label: str):
        success, failed = await self.repo.count_status()
        logger.info("%s: %d success, %d failed", label, success, failed)

    async def _log_failure_breakdown(self):
        by_error = await self.repo.count_by_error()
        if by_error:
            logger.warning("Failed breakdown: %s", dict(by_error))

    def _tasks_for_provider(
        self,
        provider: BaseProvider,
        start_date: date,
        effective_end: date,
        today: date,
        tasks: list[ProviderTask],
        seed_rows: list[SeedRow],
    ) -> None:
        def supports(origin: str, dest: str) -> bool:
            return provider.supports is None or (origin, dest) in provider.supports

        def emit(
            origin: Airport,
            dest: Airport,
            dep_date: date,
            return_date: str | None,
        ) -> None:
            ds = dep_date.isoformat()
            flight_type = (
                FlightType.ROUND_TRIP.value if return_date else FlightType.ONE_WAY.value
            )
            tasks.append((provider, origin, dest, ds, return_date, flight_type))
            seed_rows.append(
                (
                    _route_key(origin, dest),
                    ds,
                    return_date or "",
                    flight_type,
                    origin.value,
                    dest.value,
                )
            )

        for r in RouteCatalog.one_way_routes():
            if not supports(r.origin, r.dest):
                continue
            origin = RouteCatalog.resolve(r.origin)
            dest = RouteCatalog.resolve(r.dest)
            for current in _dates_between(start_date, effective_end, today):
                emit(origin, dest, current, None)

        for r in RouteCatalog.round_trip_routes():
            if not supports(r.origin, r.dest):
                continue
            origin = RouteCatalog.resolve(r.origin)
            dest = RouteCatalog.resolve(r.dest)
            for offset in RouteCatalog.ROUND_TRIP_OFFSETS:
                for current in _dates_between(start_date, effective_end, today):
                    emit(
                        origin,
                        dest,
                        current,
                        (current + timedelta(days=offset)).isoformat(),
                    )

    async def _build_tasks(
        self,
        start_date: date,
        effective_end: date,
    ) -> tuple[list[ProviderTask], list[SeedRow]]:
        tasks: list[ProviderTask] = []
        seed_rows: list[SeedRow] = []
        today = date.today()

        for provider in self.providers:
            self._tasks_for_provider(
                provider, start_date, effective_end, today, tasks, seed_rows
            )

        return tasks, seed_rows

    async def run(
        self,
        start_date: date,
        end_date: date,
        max_days_ahead: int = 330,
    ):
        """Run the full bulk search lifecycle.

        Builds tasks, seeds the DB, refreshes the proxy pool, executes
        batches, retries transient failures, then exports to JSONL.

        Args:
            start_date: First departure date.
            end_date: Last departure date.
            max_days_ahead: Hard cap on departure horizon from today.
        """
        cutoff = date.today() + timedelta(days=max_days_ahead)
        effective_end = min(end_date, cutoff)

        tasks, seed_rows = await self._build_tasks(start_date, effective_end)

        logger.info(
            "Total tasks: %d across %d provider(s)", len(tasks), len(self.providers)
        )

        proxy_task = asyncio.create_task(self.rotator.refresh())
        try:
            await self.repo.open()
            await self.repo.insert_ignore_all(seed_rows)
            await proxy_task

            if self.rotator.working_count() == 0:
                logger.warning("Zero working proxies; force-refreshing before abort")
                await self.rotator.refresh(force=True)
            if self.rotator.working_count() == 0:
                raise RuntimeError(
                    "Zero working proxies — refusing to run without proxy cover"
                )

            await self._run_batch(tasks, "Searching flights")

            await self.repo.flush()
            await self._log_counts("Search complete")

            await self._retry_loop()

            await self.repo.flush()
            await self._log_counts("After retries")
            await self._log_failure_breakdown()

            output_path = default_output_path()
            await self.repo.close()
            await convert(self.db_path, output_path, delete=not self.keep_db)

            logger.info("Output: %s", output_path)
        finally:
            if not proxy_task.done():
                proxy_task.cancel()
            await self.repo.close()
