"""Free-proxy pool management: source fetch, validation, caching, rotation."""

import asyncio
import ipaddress
import json
import logging
import os
import random
import re
import time
from bisect import bisect_left
from pathlib import Path

import httpx
from curl_cffi.requests import AsyncSession

from collector.models.proxy import ProxyInfo
from collector.services.progress import ProgressLogger

logger = logging.getLogger(__name__)

_PROXY_CACHE_PATH = "storage/proxy_cache.json"
_CACHE_FRESH_TTL = 30 * 60
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
_IPIFY_TIMEOUT = 5.0
_HTTP_ECHO_TIMEOUT = 5.0
_TCP_FILTER_LIMIT = 500
_VALIDATE_TARGET = 100
_VALIDATE_LOG_STEP_PCT = 10

_RATE_LIMIT_COOLDOWN = 120
_MAX_429_EVICTIONS = 3
_REFILL_THRESHOLD = 20

_EMPTY_REFETCH_BACKOFF = (60, 120, 300, 600)
_AUTO_REFRESH_GAP = 5
_EVICT_BLACKLIST_TTL = 30 * 60
_DEAD_BLACKLIST_TTL = 10 * 60
_FETCH_TIMEOUT = 10
_REFILL_MAX_PER_SOURCE = 150


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
        sources.append((p, f"{gh}ErcinDedeoglu/proxies/main/proxies/{p}.txt"))
    for p in ("http", "socks4", "socks5"):
        sources.append((p, f"{gh}monosans/proxy-list/main/proxies/{p}.txt"))
    for p in ("http", "socks4", "socks5"):
        sources.append((p, f"{gh}TheSpeedX/PROXY-List/master/{p}.txt"))
    sources.append(("http", f"{gh}r00tee/Proxy-List/main/Https.txt"))
    sources.append(("socks4", f"{gh}r00tee/Proxy-List/main/Socks4.txt"))
    sources.append(("socks5", f"{gh}r00tee/Proxy-List/main/Socks5.txt"))
    for p in protocols:
        sources.append(
            (
                p,
                f"https://api.proxyscrape.com/v2/?request=getproxies&protocol={p}&timeout=10000",
            )
        )
    sources.append(("http", "https://proxyspace.pro/http.txt"))
    sources.append(("https", "https://proxyspace.pro/https.txt"))
    sources.append(("http", "https://openproxylist.xyz/http.txt"))
    for p in protocols:
        sources.append((p, f"{js}hproxy-com/free-proxy-list@main/{p}.txt"))
        sources.append((p, f"{js}VMHeaven/VMHeaven-Free-Proxy-Updated@main/{p}.txt"))
    for p in ("http", "socks4", "socks5"):
        sources.append((p, f"{js}databay-labs/free-proxy-list@master/{p}.txt"))
        sources.append(
            (p, f"{js}proxygenerator1/ProxyGenerator@main/MostStable/{p}.txt")
        )
        sources.append((p, f"{js}ClearProxy/checked-proxy-list@main/{p}/raw/all.txt"))
    sources.append(
        ("http", f"{js}ClearProxy/checked-proxy-list@main/custom/google/http.txt")
    )
    sources.append(("http", f"{js}theriturajps/proxy-list@main/proxies.txt"))
    sources.append(
        (
            "http",
            "https://databay.com/api/v1/proxy-list?google=true&ssl=strict&format=txt&protocol=http",
        )
    )
    return sources


_PROXY_SOURCES: list[tuple[str, str]] = _build_sources()

_PROTOCOL_PREFIX: dict[str, str] = {
    "http": "http://",
    "https": "https://",
    "socks4": "socks4://",
    "socks5": "socks5://",
}


def _normalise_url(protocol: str, raw: str) -> str | None:
    raw = raw.strip()
    if "://" in raw:
        scheme, rest = raw.split("://", 1)
        scheme = scheme.lower()
        if scheme not in _PROTOCOL_PREFIX:
            return None
        prefix = f"{scheme}://"
    else:
        scheme = protocol
        if scheme not in _PROTOCOL_PREFIX:
            return None
        prefix = _PROTOCOL_PREFIX[scheme]
        rest = raw

    rest = rest.rstrip("/")
    if not rest:
        return None
    if rest.startswith("["):
        end = rest.find("]")
        if end == -1:
            return None
        host = rest[: end + 1]
        tail = rest[end + 1 :]
    else:
        if ":" not in rest:
            return None
        host, _, port_str = rest.partition(":")
        if not host:
            return None
        tail = f":{port_str}"

    port = tail.lstrip(":")
    if not port or ":" in port:
        return None
    try:
        port = int(port)
    except ValueError:
        return None
    if not 1 <= port <= 65535:
        return None
    return f"{prefix}{host}:{port}"


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
        if not line or line.startswith("#"):
            continue
        url_str = _normalise_url(protocol, line)
        if url_str is None:
            continue
        proto = url_str.split("://", 1)[0]
        proxies.append(ProxyInfo(url=url_str, protocol=proto))
    return proxies


async def _parse_all_sources(max_per_source: int = 0) -> list[ProxyInfo]:
    async with httpx.AsyncClient(timeout=_FETCH_TIMEOUT) as client:
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


_REAL_IP: str = ""

_BLOCK_MARKERS = ("captcha", "unusual traffic", "attention required", "access denied")

_IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


def _valid_ip(candidate: str) -> bool:
    try:
        ipaddress.ip_address(candidate)
        return True
    except ValueError:
        return False


def _extract_ip(text: str) -> str | None:
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            for key in ("origin", "ip", "query"):
                cand = data.get(key)
                if isinstance(cand, str):
                    cand = cand.split(",", 1)[0].strip()
                    if _valid_ip(cand):
                        return cand
    except ValueError, TypeError:
        pass
    for m in _IP_RE.finditer(text):
        if _valid_ip(m.group(0)):
            return m.group(0)
    return None


async def _fetch_real_ip() -> str:
    try:
        async with AsyncSession() as session:
            r = await session.get("https://api.ipify.org", timeout=_IPIFY_TIMEOUT)
            if r.status_code == 200:
                return r.text.strip()
    except Exception as e:
        logger.debug("Failed to fetch real IP: %s", e)
    return ""


def _log_probe_status(proxy_url: str, url: str, status: int) -> None:
    kind = {
        407: "auth",
        403: "blocked",
        429: "rate_limited",
    }.get(status, f"http_{status}")
    logger.debug("Proxy %s rejected on %s: %s", proxy_url, url, kind)


async def _probe_url(
    proxy_url: str, url: str, session: AsyncSession, timeout: float
) -> float | None:
    t0 = time.monotonic()
    try:
        r = await session.get(
            url,
            proxies={"all": proxy_url},
            timeout=timeout,
        )
    except Exception as e:
        logger.debug("Proxy %s failure on %s: %s", proxy_url, url, e)
        return None

    latency = (time.monotonic() - t0) * 1000
    status = r.status_code
    if status != 200:
        _log_probe_status(proxy_url, url, status)
        return None

    body = r.text
    exit_ip = _extract_ip(body)
    if exit_ip is None:
        logger.debug("Proxy %s bad echo body on %s", proxy_url, url)
        return None
    if _REAL_IP and exit_ip == _REAL_IP:
        logger.debug("Proxy %s transparent (leaked real IP)", proxy_url)
        return None
    lower = body.lower()
    if any(marker in lower for marker in _BLOCK_MARKERS):
        logger.debug("Proxy %s block marker on %s", proxy_url, url)
        return None
    return latency


async def _test_http_echo(
    proxy_url: str,
    session: AsyncSession,
    timeout: float = _HTTP_ECHO_TIMEOUT,
) -> float:
    results = await asyncio.gather(
        *[_probe_url(proxy_url, u, session, timeout) for u in _TEST_ECHO_URLS],
        return_exceptions=True,
    )
    latencies = [r for r in results if isinstance(r, float)]
    if len(latencies) < 2:
        return 0.0
    latencies.sort()
    return latencies[len(latencies) // 2]


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


async def _validate_proxy(proxy: ProxyInfo, session: AsyncSession) -> ProxyInfo | None:
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
    except OSError, ValueError, TypeError, KeyError:
        return None


def _save_cache(proxies: list[ProxyInfo]) -> None:
    Path(_PROXY_CACHE_PATH).parent.mkdir(parents=True, exist_ok=True)
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
        self._last_force_refresh = float("-inf")
        self._last_auto_refresh = float("-inf")
        self._consecutive_force_refetches = 0
        self._blacklist: dict[str, float] = {}

    async def _validate(
        self, proxies: list[ProxyInfo], target: int | None = None
    ) -> list[ProxyInfo]:
        if not proxies:
            return []

        logger.debug("TCP prefiltering %d proxies", len(proxies))
        alive = await _prefilter_tcp(proxies)
        logger.debug("TCP-alive: %d/%d", len(alive), len(proxies))
        if not alive:
            return []
        proxies = alive

        global _REAL_IP
        _REAL_IP = await _fetch_real_ip()

        queue: asyncio.Queue[ProxyInfo | None] = asyncio.Queue()
        for p in proxies:
            queue.put_nowait(p)

        valid: list[ProxyInfo] = []
        checked = 0
        progress = ProgressLogger(
            logger, step_pct=_VALIDATE_LOG_STEP_PCT, level=logging.DEBUG
        )

        async def worker():
            nonlocal checked
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
                        checked += 1
                        progress.maybe_log(
                            checked,
                            len(proxies),
                            lambda pct, done, total, _: (
                                f"Validated {done}/{total} proxies ({pct}%)"
                            ),
                        )

        n_workers = min(max(self._max_concurrent, 1), max(len(proxies), 1))
        for _ in range(n_workers):
            queue.put_nowait(None)
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

    async def refresh(self, force: bool = False, max_per_source: int | None = None):
        """Repopulate the proxy pool from cache or all sources.

        Args:
            force: Bypass cache freshness and refetch every source.
            max_per_source: Cap on candidates validated per source.
        """
        cap = self._max_per_source if max_per_source is None else max_per_source
        logger.info("Fetching proxy lists from %d sources...", len(_PROXY_SOURCES))
        if not force and await self._refresh_from_cache():
            return

        all_proxies = await _parse_all_sources(max_per_source=cap)
        if not all_proxies:
            logger.warning("No proxies fetched; keeping existing pool")
            return
        excluded = self._active_blacklist()
        if excluded:
            all_proxies = [p for p in all_proxies if p.url not in excluded]
        logger.info("Total unique proxies: %d", len(all_proxies))

        valid = await self._validate(all_proxies, target=_VALIDATE_TARGET)
        await self._apply_valid(valid, len(all_proxies))

    async def _refresh_from_cache(self) -> bool:
        """Apply cached proxies when fresh, else revalidate when stale.

        Returns:
            True when the cache settled the pool, False when a full fetch is needed.
        """
        cached = _load_cache()
        if cached is None:
            return False
        cached_at, cached_proxies = cached
        age = time.time() - cached_at
        if age < _CACHE_FRESH_TTL:
            excluded = self._active_blacklist()
            filtered = [p for p in cached_proxies if p.url not in excluded]
            if len(filtered) > self.usable_count():
                await self._set_pool(filtered)
            logger.info(
                "Proxy cache fresh (%.0fs old): %d proxies (%d blacklisted)",
                age,
                len(filtered),
                len(cached_proxies) - len(filtered),
            )
            return True
        if age < _CACHE_MAX_AGE:
            logger.info(
                "Proxy cache stale (%.0fs old); revalidating %d cached proxies",
                age,
                len(cached_proxies),
            )
            valid = await self._validate(cached_proxies)
            if valid:
                await self._apply_valid(valid, len(cached_proxies))
                return True
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

    async def get_proxy(self) -> ProxyInfo | None:
        """Return the next usable proxy, triggering a refresh when empty."""
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
            now = time.monotonic()
            if now - self._last_auto_refresh < _AUTO_REFRESH_GAP:
                return
            self._last_auto_refresh = now
            logger.info("Proxy pool exhausted; auto-refreshing")
            await self.refresh(max_per_source=_REFILL_MAX_PER_SOURCE)
            usable = self.usable_count()
            if usable > 0:
                self._consecutive_force_refetches = 0
                return
            index = min(
                self._consecutive_force_refetches,
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
                await self.refresh(force=True, max_per_source=_REFILL_MAX_PER_SOURCE)
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
            self._blacklist[proxy.url] = time.monotonic() + _DEAD_BLACKLIST_TTL

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
