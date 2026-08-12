import asyncio
import json
import logging
import os
import random
import time
from bisect import bisect_left

import httpx
from curl_cffi.requests import AsyncSession
from tqdm import tqdm

from collector.models.proxy import ProxyInfo

logger = logging.getLogger(__name__)

_PROXY_CACHE_PATH = "storage/proxy_cache.json"
_CACHE_FRESH_TTL = 15 * 60
_CACHE_MAX_AGE = 24 * 3600

_FAST_LATENCY_MS = 500
_SLOW_LATENCY_MS = 1000
_QUALITY_LATENCY_FAST = 0.3
_QUALITY_LATENCY_SLOW = 0.1

_TEST_ECHO_URLS = [
    "https://httpbin.org/ip",
    "https://api.ipify.org",
    "https://icanhazip.com",
]

_TCP_TIMEOUT = 3.0
_HTTP_ECHO_TIMEOUT = 5.0
_TCP_FILTER_LIMIT = 500
_VALIDATE_TARGET = 100

_RATE_LIMIT_COOLDOWN = 60


def _build_sources() -> list[tuple[str, str]]:
    protocols = ["http", "https", "socks4", "socks5"]
    gh = "https://raw.githubusercontent.com/"
    js = "https://cdn.jsdelivr.net/gh/"
    sources: list[tuple[str, str]] = []
    for p in protocols:
        sources.append((p, f"{gh}iplocate/free-proxy-list/main/protocols/{p}.txt"))
        sources.append((p, f"https://vakhov.github.io/fresh-proxy-list/{p}.txt"))
        sources.append(
            (p, f"{js}proxyscrape/free-proxy-list@main/proxies/protocols/{p}/data.txt")
        )
        sources.append(
            (p, f"{js}proxifly/free-proxy-list@main/proxies/protocols/{p}/data.txt")
        )
        sources.append(
            (p, f"{gh}jetkai/proxy-list/main/online-proxies/txt/proxies-{p}.txt")
        )
        sources.append(
            (p, f"{gh}Thordata/awesome-free-proxy-list/main/proxies/{p}.txt")
        )
    for p in ("http", "socks4", "socks5"):
        sources.append((p, f"{gh}TheSpeedX/PROXY-List/master/{p}.txt"))
    return sources


_PROXY_SOURCES: list[tuple[str, str]] = _build_sources()

_PROTOCOL_PREFIX: dict[str, str] = {
    "http": "http://",
    "https": "https://",
    "socks4": "socks4://",
    "socks5": "socks5://",
}


def _normalise_url(protocol: str, raw: str) -> str | None:
    if "://" in raw:
        return raw
    if ":" not in raw:
        return None
    ip, port = raw.split(":", 1)
    try:
        int(port)
    except ValueError:
        return None
    prefix = _PROTOCOL_PREFIX[protocol]
    return f"{prefix}{ip}:{port}"


async def _parse_source(
    protocol: str, url: str, client: httpx.AsyncClient
) -> list[ProxyInfo]:
    try:
        resp = await client.get(url)
        resp.raise_for_status()
    except Exception as e:
        logger.debug("Failed to fetch %s: %s", url, e)
        return []

    proxies: list[ProxyInfo] = []
    for line in resp.text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        if "://" in line:
            url_str = line
            proto = line.split("://", 1)[0]
        else:
            url_str = _normalise_url(protocol, line)
            if url_str is None:
                continue
            proto = protocol
        proxies.append(ProxyInfo(url=url_str, protocol=proto))
    return proxies


async def _parse_all_sources(max_per_source: int = 0) -> list[ProxyInfo]:
    async with httpx.AsyncClient(timeout=10) as client:
        results = await asyncio.gather(
            *[_parse_source(proto, url, client) for proto, url in _PROXY_SOURCES],
            return_exceptions=True,
        )
    all_proxies: list[ProxyInfo] = []
    for r in results:
        if isinstance(r, list):
            if max_per_source > 0 and len(r) > max_per_source:
                random.shuffle(r)
                r = r[:max_per_source]
            all_proxies.extend(r)

    seen: set[str] = set()
    unique: list[ProxyInfo] = []
    for p in all_proxies:
        if p.url not in seen:
            seen.add(p.url)
            unique.append(p)
    return unique


async def _test_http_echo(
    proxy_url: str,
    session: AsyncSession,
    timeout: float = _HTTP_ECHO_TIMEOUT,
) -> float:
    async def probe(url: str) -> float:
        t0 = time.monotonic()
        try:
            r = await session.get(
                url,
                proxies={"all": proxy_url},
                timeout=timeout,
            )
            latency = (time.monotonic() - t0) * 1000
            if r.status_code == 200:
                return latency
        except Exception:
            pass
        return 0.0

    results = await asyncio.gather(
        *[probe(u) for u in _TEST_ECHO_URLS],
        return_exceptions=True,
    )
    latencies = [r for r in results if isinstance(r, float) and r > 0]
    return min(latencies) if latencies else 0.0


async def _tcp_alive(url: str, timeout: float = _TCP_TIMEOUT) -> bool:
    try:
        hostport = url.split("://", 1)[1].rstrip("/")
        host, sep, port = hostport.rpartition(":")
        if not sep or not port:
            return False
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host.strip("[]"), int(port)),
            timeout=timeout,
        )
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True
    except Exception:
        return False


async def _prefilter_tcp(proxies: list[ProxyInfo]) -> list[ProxyInfo]:
    sem = asyncio.Semaphore(_TCP_FILTER_LIMIT)

    async def check(proxy: ProxyInfo) -> bool:
        async with sem:
            return await _tcp_alive(proxy.url)

    results = await asyncio.gather(*[check(p) for p in proxies])
    return [p for p, ok in zip(proxies, results) if ok]


async def _validate_proxy(
    proxy: ProxyInfo, session: AsyncSession
) -> ProxyInfo | None:
    latency = await _test_http_echo(proxy.url, session)

    if latency == 0.0:
        return None

    quality = 1.0
    if latency < _FAST_LATENCY_MS:
        quality += _QUALITY_LATENCY_FAST
    elif latency < _SLOW_LATENCY_MS:
        quality += _QUALITY_LATENCY_SLOW

    quality = max(0.1, quality)
    proxy.quality_score = quality
    proxy.latency_ms = latency
    proxy.last_validated = time.time()
    return proxy


def _load_cache() -> tuple[float, list[ProxyInfo]] | None:
    try:
        with open(_PROXY_CACHE_PATH) as f:
            data = json.load(f)
        cached_at = float(data.get("cached_at", 0))
        proxies = [ProxyInfo.from_dict(d) for d in data.get("proxies", [])]
        if not proxies:
            return None
        return cached_at, proxies
    except (OSError, ValueError, TypeError, KeyError):
        return None


def _save_cache(proxies: list[ProxyInfo]) -> None:
    os.makedirs(os.path.dirname(_PROXY_CACHE_PATH), exist_ok=True)
    tmp = f"{_PROXY_CACHE_PATH}.tmp"
    data = {
        "cached_at": time.time(),
        "proxies": [p.to_dict() for p in proxies],
    }
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, _PROXY_CACHE_PATH)


class ProxyRotator:
    def __init__(
        self,
        max_concurrent: int = 200,
        max_per_source: int = 500,
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
        self._refresh_task: asyncio.Task | None = None
        self._schedule_lock = asyncio.Lock()

    async def _validate(
        self, proxies: list[ProxyInfo], target: int | None = None
    ) -> list[ProxyInfo]:
        if not proxies:
            return []

        logger.info("TCP prefiltering %d proxies", len(proxies))
        alive = await _prefilter_tcp(proxies)
        logger.info("TCP-alive: %d/%d", len(alive), len(proxies))
        if not alive:
            return []
        proxies = alive

        queue: asyncio.Queue[ProxyInfo | None] = asyncio.Queue()
        for p in proxies:
            queue.put_nowait(p)

        valid: list[ProxyInfo] = []

        async def worker():
            async with AsyncSession() as session:
                while True:
                    if target is not None and len(valid) >= target:
                        return
                    proxy = await queue.get()
                    if proxy is None:
                        return
                    try:
                        result = await _validate_proxy(proxy, session)
                        if result is not None:
                            valid.append(result)
                    finally:
                        pbar.update(1)

        n_workers = min(self._max_concurrent, max(len(proxies), 1))
        for _ in range(n_workers):
            queue.put_nowait(None)
        with tqdm(
            total=len(proxies), desc="Testing proxies", unit="px"
        ) as pbar:
            workers = [asyncio.create_task(worker()) for _ in range(n_workers)]
            await asyncio.gather(*workers)

        valid.sort(key=lambda p: p.quality_score, reverse=True)
        return valid

    async def _set_pool(self, proxies: list[ProxyInfo]):
        async with self._lock:
            self._proxies = proxies
            self._index = 0
            self._last_refresh = time.monotonic()
            self._weight_pool_len = len(proxies)
            self._recompute_weights()

    async def _apply_valid(self, valid: list[ProxyInfo], total: int):
        await self._set_pool(valid)
        _save_cache(valid)
        logger.info(
            "Working proxies: %d/%d (%.1f%%)",
            len(valid),
            total,
            len(valid) / max(total, 1) * 100,
        )

    def _recompute_weights(self, pool: list[ProxyInfo] | None = None):
        pool = pool if pool is not None else self._proxies
        self._cum_weights = []
        total = 0.0
        for p in pool:
            total += p.quality_score
            self._cum_weights.append(total)
        self._total_weight = total

    async def refresh(
        self, force: bool = False, max_per_source: int | None = None
    ):
        cap = self._max_per_source if max_per_source is None else max_per_source
        logger.info("Fetching proxy lists from %d sources...", len(_PROXY_SOURCES))
        if not force:
            cached = _load_cache()
            if cached is not None:
                cached_at, cached_proxies = cached
                age = time.time() - cached_at
                if age < _CACHE_FRESH_TTL:
                    await self._set_pool(cached_proxies)
                    logger.info(
                        "Proxy cache fresh (%.0fs old): %d proxies",
                        age,
                        len(cached_proxies),
                    )
                    return
                if age < _CACHE_MAX_AGE:
                    logger.info(
                        "Proxy cache stale (%.0fs old); revalidating %d cached proxies",
                        age,
                        len(cached_proxies),
                    )
                    valid = await self._validate(cached_proxies)
                    if valid:
                        await self._apply_valid(valid, len(cached_proxies))
                        return
                    logger.warning("All cached proxies dead; fetching fresh")

        all_proxies = await _parse_all_sources(max_per_source=cap)
        if not all_proxies:
            logger.warning("No proxies fetched; keeping existing pool")
            return
        logger.info("Total unique proxies: %d", len(all_proxies))

        valid = await self._validate(all_proxies, target=_VALIDATE_TARGET)
        await self._apply_valid(valid, len(all_proxies))

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

    async def get_proxy(self) -> ProxyInfo | None:
        async with self._lock:
            proxy = self._pick()
        if proxy is None:
            await self._ensure_refresh_task()
            task = self._refresh_task
            if task is not None and not task.done():
                await task
            async with self._lock:
                proxy = self._pick()
        return proxy

    async def _ensure_refresh_task(self):
        async with self._schedule_lock:
            if self._refresh_task is not None and not self._refresh_task.done():
                return
            self._refresh_task = asyncio.create_task(self._auto_refresh())

    async def _auto_refresh(self):
        try:
            logger.info("Proxy pool exhausted; auto-refreshing")
            await self.refresh(force=True, max_per_source=150)
        except Exception:
            logger.exception("Proxy auto-refresh failed")
        finally:
            self._refresh_task = None

    async def report_failure(self, proxy: ProxyInfo):
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

    async def report_rate_limited(self, proxy: ProxyInfo, seconds: float = _RATE_LIMIT_COOLDOWN):
        async with self._lock:
            proxy.rate_limit_until = time.monotonic() + seconds
            self._weight_pool_len = 0
            logger.debug(
                "Proxy %s rate-limited; parked for %.0fs",
                proxy.url,
                seconds,
            )

    def working_count(self) -> int:
        return len(self._proxies)
