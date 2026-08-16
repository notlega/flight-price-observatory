"""Proxy rotation: weighted pool selection, refresh, eviction."""

import asyncio
import logging
import random
import time
from bisect import bisect_left
from contextlib import suppress

from curl_cffi.requests import AsyncSession

from collector.models.proxy import ProxyInfo
from collector.proxy import cache, sources, validation
from collector.services.progress import ProgressLogger

logger = logging.getLogger(__name__)

_RATE_LIMIT_COOLDOWN = 120
_MAX_429_EVICTIONS = 3
_MAX_STUB_EVICTIONS = 3
_REFILL_THRESHOLD = 20

_EMPTY_REFETCH_BACKOFF = (60, 120, 300, 600)
_AUTO_REFRESH_GAP = 5
_EVICT_BLACKLIST_TTL = 30 * 60
_DEAD_BLACKLIST_TTL = 10 * 60
_REFRESH_AWAIT_TIMEOUT = 60.0


class ProxyRotator:
    def __init__(
        self,
        max_concurrent: int = 200,
        max_per_source: int = 2500,
    ):
        self._proxies: list[ProxyInfo] = []
        self._index = 0
        self._lock = asyncio.Lock()
        self._last_refresh = 0.0
        self._max_concurrent = max_concurrent
        self._max_per_source = max_per_source
        self._cum_weights: list[float] = []
        self._total_weight = 0.0
        self._weight_pool_len = 0
        self._refresh_task: asyncio.Task[None] | None = None
        self._schedule_lock = asyncio.Lock()
        self._last_force_refresh = float("-inf")
        self._last_auto_refresh = float("-inf")
        self._consecutive_force_refetches = 0
        self._consecutive_auto_refills = 0
        self._blacklist: dict[str, float] = {}
        self._real_ip: str = ""

    async def _validate(
        self, proxies: list[ProxyInfo], target: int | None = None
    ) -> list[ProxyInfo]:
        if not proxies:
            return []

        logger.debug("TCP prefiltering %d proxies", len(proxies))
        input_by_source = validation.count_by_source(proxies)
        alive_target = (
            target if target is not None else validation.VALIDATE_TARGET
        ) * validation.ALIVE_TO_VALID_MULTIPLIER
        alive = await validation.prefilter_tcp_until(proxies, alive_target)
        logger.debug("TCP-alive: %d/%d", len(alive), len(proxies))
        if not alive:
            return []
        proxies = alive

        self._real_ip = await validation.fetch_real_ip()

        queue: asyncio.Queue[ProxyInfo | None] = asyncio.Queue()
        for p in proxies:
            queue.put_nowait(p)

        valid: list[ProxyInfo] = []
        checked = 0
        progress = ProgressLogger(
            logger, step_pct=validation.VALIDATE_LOG_STEP_PCT, level=logging.DEBUG
        )

        async def worker() -> None:
            nonlocal checked
            async with AsyncSession() as session:
                while True:
                    if target is not None and len(valid) >= target:
                        return
                    proxy = await queue.get()
                    if proxy is None:
                        return
                    try:
                        result = await validation.validate_proxy(
                            proxy, session, self._real_ip
                        )
                        if result is not None:
                            valid.append(result)
                    finally:
                        checked += 1
                        progress.maybe_log(
                            checked,
                            len(proxies),
                            lambda pct, done, total, _: (
                                f"Validated {done}/{total} proxies ({pct}%)"
                            ),
                        )

        n_workers = min(
            max(validation.VALIDATE_MAX_CONCURRENT, 1), max(len(proxies), 1)
        )
        for _ in range(n_workers):
            queue.put_nowait(None)
        workers = [asyncio.create_task(worker()) for _ in range(n_workers)]
        await asyncio.gather(*workers)

        valid.sort(key=lambda p: p.quality_score, reverse=True)
        alive_by_source = validation.count_by_source(alive)
        valid_by_source = validation.count_by_source(valid)
        sources_set = sorted(
            set(input_by_source) | set(alive_by_source) | set(valid_by_source)
        )
        if len(sources_set) > 1:
            logger.info(
                "Proxy source yield (candidates/alive/valid): %s",
                {
                    s: (
                        input_by_source.get(s, 0),
                        alive_by_source.get(s, 0),
                        valid_by_source.get(s, 0),
                    )
                    for s in sources_set
                },
            )
        return valid

    async def _set_pool(self, proxies: list[ProxyInfo]) -> None:
        async with self._lock:
            self._proxies = proxies
            self._index = 0
            self._last_refresh = time.monotonic()
            self._weight_pool_len = len(proxies)
            self._recompute_weights()

    async def _apply_valid(self, valid: list[ProxyInfo], total: int) -> None:
        if not valid:
            logger.warning(
                "Validation produced no working proxies; keeping existing pool "
                "(cache not overwritten)"
            )
            return
        await self._set_pool(valid)
        cache.save_cache(valid)
        logger.info(
            "Working proxies: %d/%d (%.1f%%)",
            len(valid),
            total,
            len(valid) / max(total, 1) * 100,
        )

    def _recompute_weights(self, pool: list[ProxyInfo] | None = None) -> None:
        pool = pool if pool is not None else self._proxies
        self._cum_weights = []
        total = 0.0
        for p in pool:
            total += p.quality_score
            self._cum_weights.append(total)
        self._total_weight = total

    async def refresh(
        self, force: bool = False, max_per_source: int | None = None
    ) -> None:
        """Repopulate the proxy pool from cache or all sources.

        Args:
            force: Bypass cache freshness and refetch every source.
            max_per_source: Cap on candidates validated per source.
        """
        cap = self._max_per_source if max_per_source is None else max_per_source
        logger.info(
            "Fetching proxy lists from %d sources...", len(sources.PROXY_SOURCES)
        )
        if not force and await self._refresh_from_cache():
            return

        all_proxies = await sources.parse_all_sources(max_per_source=cap)
        if not all_proxies:
            logger.warning("No proxies fetched; keeping existing pool")
            return
        excluded = self._active_blacklist()
        if excluded:
            all_proxies = [p for p in all_proxies if p.url not in excluded]
        logger.info("Total unique proxies: %d", len(all_proxies))

        valid = await self._validate(all_proxies, target=validation.VALIDATE_TARGET)
        await self._apply_valid(valid, len(all_proxies))

    async def _refresh_from_cache(self) -> bool:
        """Apply cached proxies when fresh, else revalidate when stale.

        Returns:
            True when the cache settled the pool, False when a full fetch is needed.
        """
        cached = cache.load_cache()
        if cached is None:
            return False
        cached_at, cached_proxies = cached
        age = time.time() - cached_at
        if age < cache.CACHE_FRESH_TTL:
            excluded = self._active_blacklist()
            filtered = [p for p in cached_proxies if p.url not in excluded]
            if len(filtered) < cache.MIN_CACHE_POOL:
                logger.info(
                    "Proxy cache fresh (%.0fs old): %d proxies below min pool "
                    "%d; fetching fresh",
                    age,
                    len(filtered),
                    cache.MIN_CACHE_POOL,
                )
                return False
            if len(filtered) > self.usable_count():
                await self._set_pool(filtered)
            logger.info(
                "Proxy cache fresh (%.0fs old): %d proxies (%d blacklisted)",
                age,
                len(filtered),
                len(cached_proxies) - len(filtered),
            )
            return True
        if age < cache.CACHE_MAX_AGE:
            logger.info(
                "Proxy cache stale (%.0fs old); revalidating %d cached proxies",
                age,
                len(cached_proxies),
            )
            valid = await self._validate(cached_proxies)
            if valid:
                if len(valid) >= cache.MIN_CACHE_POOL:
                    await self._apply_valid(valid, len(cached_proxies))
                    return True
                logger.warning(
                    "Cached proxies revalidated to %d below min pool %d; "
                    "fetching fresh",
                    len(valid),
                    cache.MIN_CACHE_POOL,
                )
            else:
                logger.warning("All cached proxies dead; fetching fresh")
        return False

    def _pick(self) -> ProxyInfo | None:
        now = time.monotonic()
        if any(p.rate_limit_until > now for p in self._proxies):
            pool = [p for p in self._proxies if p.rate_limit_until <= now]
        else:
            pool = self._proxies
        if not pool:
            return None
        if len(pool) != self._weight_pool_len:
            self._recompute_weights(pool)
            self._weight_pool_len = len(pool)
        total = self._total_weight
        if total <= 0:
            proxy = pool[self._index % len(pool)]
            self._index += 1
        else:
            r = random.uniform(0, total)
            i = bisect_left(self._cum_weights, r)
            proxy = pool[min(i, len(pool) - 1)]
        return proxy

    async def get_proxy(
        self, await_timeout: float = _REFRESH_AWAIT_TIMEOUT
    ) -> ProxyInfo | None:
        """Return the next usable proxy, triggering a refresh when empty.

        Args:
            await_timeout: Cap on how long an in-flight refresh is awaited;
                on timeout the refresh keeps running and None is returned.
        """
        async with self._lock:
            proxy = self._pick()
        if proxy is None:
            await self._ensure_refresh_task()
            task = self._refresh_task
            if task is not None and not task.done():
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(asyncio.shield(task), timeout=await_timeout)
            async with self._lock:
                proxy = self._pick()
        return proxy

    async def _ensure_refresh_task(self) -> None:
        async with self._schedule_lock:
            if self._refresh_task is not None and not self._refresh_task.done():
                return
            self._refresh_task = asyncio.create_task(self._auto_refresh())

    async def _auto_refresh(self) -> None:
        try:
            now = time.monotonic()
            # Bypass the gap when the pool is still starved: a 5-minute cooldown
            # after each refill left a dead pool (3 proxies) sitting idle for
            # minutes during retry rounds. Repeated refills are bounded by the
            # refresh itself + the empty-pool force-refetch backoff below.
            if now - self._last_auto_refresh < _AUTO_REFRESH_GAP:
                if self.usable_count() >= _REFILL_THRESHOLD:
                    return
                if self._consecutive_auto_refills > 0:
                    return
            self._last_auto_refresh = now
            logger.info("Proxy pool exhausted; auto-refreshing")
            await self.refresh(max_per_source=sources.REFILL_MAX_PER_SOURCE)
            usable = self.usable_count()
            if usable > 0:
                self._consecutive_force_refetches = 0
                self._consecutive_auto_refills = 0
                return
            self._consecutive_auto_refills += 1
            index = min(
                self._consecutive_auto_refills - 1,
                len(_EMPTY_REFETCH_BACKOFF) - 1,
            )
            cooldown = _EMPTY_REFETCH_BACKOFF[index]
            if now - self._last_force_refresh > cooldown:
                self._last_force_refresh = now
                self._consecutive_force_refetches += 1
                logger.warning(
                    "Proxy pool low (%d usable < %d); fetching fresh lists",
                    usable,
                    _REFILL_THRESHOLD,
                )
                await self.refresh(
                    force=True, max_per_source=sources.REFILL_MAX_PER_SOURCE
                )
        except Exception:
            logger.exception("Proxy auto-refresh failed")
        finally:
            self._refresh_task = None

    async def report_failure(self, proxy: ProxyInfo) -> None:
        async with self._lock:
            try:
                self._proxies.remove(proxy)
                logger.debug(
                    "Removed dead proxy %s (%d remaining)",
                    proxy.url,
                    len(self._proxies),
                )
            except ValueError:
                pass
            self._blacklist[proxy.url] = time.monotonic() + _DEAD_BLACKLIST_TTL

    async def report_stub(self, proxy: ProxyInfo) -> None:
        async with self._lock:
            proxy.stub_count += 1
            if proxy.stub_count >= _MAX_STUB_EVICTIONS:
                try:
                    self._proxies.remove(proxy)
                    logger.info(
                        "Evicted proxy %s after %d stubs (%d remaining)",
                        proxy.url,
                        proxy.stub_count,
                        len(self._proxies),
                    )
                except ValueError:
                    pass
                self._blacklist[proxy.url] = time.monotonic() + _EVICT_BLACKLIST_TTL

    async def report_rate_limited(
        self, proxy: ProxyInfo, seconds: float = _RATE_LIMIT_COOLDOWN
    ):
        async with self._lock:
            proxy.rate_limited_count += 1
            if proxy.rate_limited_count >= _MAX_429_EVICTIONS:
                try:
                    self._proxies.remove(proxy)
                    logger.info(
                        "Evicted proxy %s after %d 429s (%d remaining)",
                        proxy.url,
                        proxy.rate_limited_count,
                        len(self._proxies),
                    )
                except ValueError:
                    pass
                self._blacklist[proxy.url] = time.monotonic() + _EVICT_BLACKLIST_TTL
                return
            proxy.rate_limit_until = time.monotonic() + seconds
            self._weight_pool_len = 0
            logger.debug(
                "Proxy %s rate-limited; parked for %.0fs (%d/%d)",
                proxy.url,
                seconds,
                proxy.rate_limited_count,
                _MAX_429_EVICTIONS,
            )

    def working_count(self) -> int:
        return len(self._proxies)

    def usable_count(self) -> int:
        now = time.monotonic()
        return sum(1 for p in self._proxies if p.rate_limit_until <= now)

    def _active_blacklist(self) -> set[str]:
        now = time.monotonic()
        expired = [u for u, exp in self._blacklist.items() if exp <= now]
        for u in expired:
            del self._blacklist[u]
        return set(self._blacklist)
