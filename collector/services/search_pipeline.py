import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import NamedTuple

from curl_cffi.requests import AsyncSession
from fli.models import Airport
from tqdm import tqdm

from collector.convert import convert, default_output_path
from collector.errors import (
    ErrorType,
    ProviderConnectionError,
    ProviderDataError,
    ProviderRateLimitedError,
    ProviderTimeoutError,
)
from collector.models.proxy import ProxyInfo
from collector.providers.base import BaseProvider
from collector.proxy import ProxyRotator
from collector.repository import SearchRepository
from collector.services.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 3
_MIN_POOL_BEFORE_RETRY = 20
_NO_PROXY_DELAY = 0.5


def _route_key(origin: Airport, dest: Airport) -> str:
    return f"{origin.value}|{dest.value}"


class AttemptResult(NamedTuple):
    flights: list[dict] | None
    error_type: str | None
    proxy_info: ProxyInfo | None


class BulkSearchPipeline:
    def __init__(
        self,
        providers: list[BaseProvider],
        rate: float = 200,
        max_concurrent: int = 200,
        db_path: str = "storage/db/search_state.db",
        currency: str = "SGD",
    ):
        self.providers = providers
        self.rotator = ProxyRotator()
        self.rate_limiter = RateLimiter(max_rate=rate)
        self.max_concurrent = max_concurrent
        self.repo = SearchRepository(db_path)
        self.db_path = db_path
        self.currency = currency

    async def _attempt_once(
        self,
        provider: BaseProvider,
        origin: Airport,
        dest: Airport,
        departure: str,
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
            )
            await self.rate_limiter.report_success()
            return AttemptResult(flights, None, proxy_info)
        except ProviderRateLimitedError:
            await self.rate_limiter.report_429()
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
        session: AsyncSession,
        retry_round: int = 0,
    ):
        searched_at = datetime.now(timezone.utc).isoformat()
        error_type: str | None = None

        for attempt in range(_MAX_ATTEMPTS):
            result = await self._attempt_once(provider, origin, dest, departure, session)
            error_type = result.error_type
            if error_type in (ErrorType.TIMEOUT, ErrorType.CONNECTION, ErrorType.OTHER):
                await self.rotator.report_failure(result.proxy_info)
            if result.error_type is None:
                await self._store_result(
                    origin,
                    dest,
                    departure,
                    flights=result.flights,
                    error_type=None,
                    retries=attempt,
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
            flights=[],
            error_type=error_type,
            retries=retry_round,
            success=False,
            searched_at=searched_at,
        )

    async def _store_result(
        self,
        origin: Airport,
        dest: Airport,
        departure: str,
        flights: list[dict] | None,
        error_type: str | None,
        retries: int,
        success: bool,
        searched_at: str,
    ):
        await self.repo.upsert(
            route=_route_key(origin, dest),
            dep_date=departure,
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
        tasks: list[tuple[BaseProvider, Airport, Airport, str]],
        desc: str,
        retry_round: int = 0,
    ):
        queue: asyncio.Queue = asyncio.Queue()
        for task in tasks:
            queue.put_nowait(task)

        async def worker():
            async with AsyncSession() as session:
                while True:
                    try:
                        provider, origin, dest, departure = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    try:
                        await self._search_and_store(
                            provider,
                            origin,
                            dest,
                            departure,
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
                            origin, dest, departure, retry_round
                        )
                    finally:
                        pbar.update(1)

        with tqdm(total=len(tasks), desc=desc, unit="task") as pbar:
            n_workers = min(self.max_concurrent, max(len(tasks), 1))
            workers = [asyncio.create_task(worker()) for _ in range(n_workers)]
            await asyncio.gather(*workers)

    async def _record_failure(
        self,
        origin: Airport,
        dest: Airport,
        departure: str,
        retry_round: int,
    ):
        try:
            await self._store_result(
                origin,
                dest,
                departure,
                flights=[],
                error_type=ErrorType.OTHER,
                retries=retry_round,
                success=False,
                searched_at=datetime.now(timezone.utc).isoformat(),
            )
        except Exception:
            logger.exception("Failed to record failed task %s->%s on %s",
                             origin.value, dest.value, departure)

    async def _get_provider_map(
        self,
    ) -> dict[str, tuple[BaseProvider, Airport, Airport]]:
        m: dict[str, tuple[BaseProvider, Airport, Airport]] = {}
        for p in self.providers:
            for o, d in p.routes:
                m[_route_key(o, d)] = (p, o, d)
        return m

    async def _retry_loop(self, rounds: int = 3):
        provider_map = await self._get_provider_map()
        for rnd in range(1, rounds + 1):
            failed = await self.repo.get_failed(max_retries=rnd)
            if not failed:
                logger.info("No failed tasks to retry")
                return
            logger.info("Retry round %d/%d: %d tasks", rnd, rounds, len(failed))

            if self.rotator.working_count() < _MIN_POOL_BEFORE_RETRY:
                logger.info(
                    "Proxy pool low (%d); refreshing before retry round %d",
                    self.rotator.working_count(),
                    rnd,
                )
                await self.rotator.refresh(force=True)

            retry_tasks: list[tuple[BaseProvider, Airport, Airport, str]] = []
            for route, dep_date in failed:
                if route in provider_map:
                    provider, origin, dest = provider_map[route]
                    retry_tasks.append((provider, origin, dest, dep_date))

            if retry_tasks:
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

    async def run(
        self,
        start_date: date,
        end_date: date,
        max_days_ahead: int = 330,
    ):
        cutoff = date.today() + timedelta(days=max_days_ahead)
        effective_end = min(end_date, cutoff)

        tasks: list[tuple[BaseProvider, Airport, Airport, str]] = []
        seed_rows: list[tuple[str, str, str, str]] = []
        for provider in self.providers:
            for origin, dest in provider.routes:
                current = start_date
                while current <= effective_end:
                    ds = current.isoformat()
                    route = _route_key(origin, dest)
                    tasks.append((provider, origin, dest, ds))
                    seed_rows.append((route, ds, origin.value, dest.value))
                    current += timedelta(days=1)

        logger.info(
            "Total tasks: %d across %d provider(s)", len(tasks), len(self.providers)
        )

        proxy_task = asyncio.create_task(self.rotator.refresh())
        try:
            await self.repo.open()
            await self.repo.insert_ignore_all(seed_rows)
            await proxy_task

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
            await convert(self.db_path, output_path, delete=True)

            logger.info("Output: %s", output_path)
        finally:
            if not proxy_task.done():
                proxy_task.cancel()
            await self.repo.close()
