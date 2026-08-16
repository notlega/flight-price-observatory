"""Proxy validation: TCP prefiltering and HTTP echo probing."""

import asyncio
import ipaddress
import json
import logging
import re
import time
from contextlib import suppress
from typing import Any, cast

from curl_cffi.requests import AsyncSession

from collector.models.proxy import ProxyInfo

logger = logging.getLogger(__name__)

_FAST_LATENCY_MS = 500
_SLOW_LATENCY_MS = 1000
_QUALITY_LATENCY_FAST = 0.3
_QUALITY_LATENCY_SLOW = 0.1

_TEST_ECHO_URLS = [
    "https://httpbin.org/ip",
    "https://api.ipify.org",
    "https://icanhazip.com",
]

_TCP_TIMEOUT = 1.5
_IPIFY_TIMEOUT = 5.0
_HTTP_ECHO_TIMEOUT = 5.0
_TCP_FILTER_LIMIT = 500
VALIDATE_TARGET = 100
VALIDATE_MAX_CONCURRENT = 50
VALIDATE_LOG_STEP_PCT = 10

ALIVE_TO_VALID_MULTIPLIER = 30

_BLOCK_MARKERS = ("captcha", "unusual traffic", "attention required", "access denied")

_IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


def _valid_ip(candidate: str) -> bool:
    """Return True when ``candidate`` parses as a valid IP address."""
    try:
        ipaddress.ip_address(candidate)
        return True
    except ValueError:
        return False


def extract_ip(text: str) -> str | None:
    """Extract the first IP literal from a JSON or plain-text echo response."""
    try:
        data: Any = json.loads(text)
        if isinstance(data, dict):
            payload = cast(dict[str, object], data)
            for key in ("origin", "ip", "query"):
                cand = payload.get(key)
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


async def fetch_real_ip() -> str:
    """Return the caller's public IP (for transparency checks), else empty."""
    try:
        async with AsyncSession() as session:
            r = await session.get("https://api.ipify.org", timeout=_IPIFY_TIMEOUT)
            if r.status_code == 200:
                return r.text.strip()
    except Exception as e:
        logger.debug("Failed to fetch real IP: %s", e)
    return ""


def count_by_source(proxies: list[ProxyInfo]) -> dict[str, int]:
    """Count ``proxies`` grouped by source name."""
    counts: dict[str, int] = {}
    for p in proxies:
        counts[p.source] = counts.get(p.source, 0) + 1
    return counts


def _log_probe_status(proxy_url: str, url: str, status: int) -> None:
    """Log a rejected probe with a human-readable status kind."""
    kind = {
        407: "auth",
        403: "blocked",
        429: "rate_limited",
    }.get(status, f"http_{status}")
    logger.debug("Proxy %s rejected on %s: %s", proxy_url, url, kind)


async def probe_url(
    proxy_url: str,
    url: str,
    session: AsyncSession,
    timeout: float,
    real_ip: str = "",
) -> float | None:
    """Probe one echo URL via ``proxy_url``; return latency ms or None if bad."""
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
    exit_ip = extract_ip(body)
    if exit_ip is None:
        logger.debug("Proxy %s bad echo body on %s", proxy_url, url)
        return None
    if real_ip and exit_ip == real_ip:
        logger.debug("Proxy %s transparent (leaked real IP)", proxy_url)
        return None
    lower = body.lower()
    if any(marker in lower for marker in _BLOCK_MARKERS):
        logger.debug("Proxy %s block marker on %s", proxy_url, url)
        return None
    return latency


async def test_http_echo(
    proxy_url: str,
    session: AsyncSession,
    timeout: float = _HTTP_ECHO_TIMEOUT,
    real_ip: str = "",
) -> float:
    """Echo-probe all test URLs; return median latency, 0.0 when <2 succeed."""
    results = await asyncio.gather(
        *[probe_url(proxy_url, u, session, timeout, real_ip) for u in _TEST_ECHO_URLS],
        return_exceptions=True,
    )
    latencies = [r for r in results if isinstance(r, float)]
    if len(latencies) < 2:
        return 0.0
    latencies.sort()
    return latencies[len(latencies) // 2]


async def tcp_alive(url: str, timeout: float = _TCP_TIMEOUT) -> bool:
    """Return True when a TCP connection to ``url`` succeeds within timeout."""
    try:
        hostport = url.split("://", 1)[1].rstrip("/")
        host, sep, port = hostport.rpartition(":")
        if not sep or not port:
            return False
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host.strip("[]"), int(port)),
            timeout=timeout,
        )
        writer.close()
        with suppress(Exception):
            await writer.wait_closed()
        return True
    except Exception:
        return False


async def prefilter_tcp(proxies: list[ProxyInfo]) -> list[ProxyInfo]:
    """Return ``proxies`` that survive a concurrent TCP connect check."""
    sem = asyncio.Semaphore(_TCP_FILTER_LIMIT)

    async def check(proxy: ProxyInfo) -> bool:
        async with sem:
            return await tcp_alive(proxy.url)

    results = await asyncio.gather(*[check(p) for p in proxies])
    return [p for p, ok in zip(proxies, results, strict=True) if ok]


async def prefilter_tcp_until(
    proxies: list[ProxyInfo], alive_target: int
) -> list[ProxyInfo]:
    """Probe candidates in batches until ``alive_target`` survive, or run out."""
    alive: list[ProxyInfo] = []
    for start in range(0, len(proxies), _TCP_FILTER_LIMIT):
        batch = proxies[start : start + _TCP_FILTER_LIMIT]
        alive.extend(await prefilter_tcp(batch))
        if len(alive) >= alive_target:
            break
    return alive


async def validate_proxy(
    proxy: ProxyInfo, session: AsyncSession, real_ip: str = ""
) -> ProxyInfo | None:
    """Score one proxy via echo probes; return None when it fails validation."""
    latency = await test_http_echo(proxy.url, session, real_ip=real_ip)

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
