"""Proxy list sources: URL catalog, priority ordering, parsing, dedup."""

import asyncio
import logging
import random

import httpx

from collector.models.proxy import ProxyInfo

logger = logging.getLogger(__name__)

_FETCH_TIMEOUT = 10
REFILL_MAX_PER_SOURCE = 750


def build_sources() -> list[tuple[str, str]]:
    """Build the protocol/URL source list, prioritised by expected yield."""
    gh = "https://raw.githubusercontent.com/"
    js = "https://cdn.jsdelivr.net/gh/"
    sources: list[tuple[str, str]] = [
        ("http", f"{js}databay-labs/free-proxy-list@master/http.txt"),
        ("socks5", f"{js}databay-labs/free-proxy-list@master/socks5.txt"),
        (
            "socks5",
            f"{js}proxyscrape/free-proxy-list@main/proxies/protocols/socks5/data.txt",
        ),
        (
            "http",
            "https://databay.com/api/v1/proxy-list?google=true&ssl=strict&format=txt&protocol=http",
        ),
        (
            "http",
            f"{js}proxyscrape/free-proxy-list@main/proxies/protocols/http/data.txt",
        ),
        (
            "socks4",
            f"{js}proxyscrape/free-proxy-list@main/proxies/protocols/socks4/data.txt",
        ),
        ("socks4", f"{js}VMHeaven/VMHeaven-Free-Proxy-Updated@main/socks4.txt"),
        (
            "http",
            "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000",
        ),
        ("socks4", f"{js}databay-labs/free-proxy-list@master/socks4.txt"),
        ("socks5", f"{js}VMHeaven/VMHeaven-Free-Proxy-Updated@main/socks5.txt"),
        (
            "https",
            f"{js}proxyscrape/free-proxy-list@main/proxies/protocols/https/data.txt",
        ),
        ("http", f"{gh}monosans/proxy-list/main/proxies/http.txt"),
        ("socks5", f"{js}ClearProxy/checked-proxy-list@main/socks5/raw/all.txt"),
        (
            "socks4",
            "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks4&timeout=10000",
        ),
        (
            "socks5",
            "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=10000",
        ),
    ]
    return _prioritise_sources(sources)


_PRIORITY_MARKERS = (
    "checked-proxy-list",
    "databay",
    "proxyscrape",
    "vmheaven",
)


def _prioritise_sources(sources: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Order high-yield sources first so streaming prefilter meets its
    alive target from fewer, richer batches."""

    def key(source: tuple[str, str]) -> tuple[int, int]:
        _, url = source
        lower = url.lower()
        for rank, marker in enumerate(_PRIORITY_MARKERS):
            if marker in lower:
                return (0, rank)
        return (1, 0)

    return sorted(sources, key=key)


PROXY_SOURCES: list[tuple[str, str]] = build_sources()

_PROTOCOL_PREFIX: dict[str, str] = {
    "http": "http://",
    "https": "https://",
    "socks4": "socks4://",
    "socks5": "socks5://",
}


def normalise_url(protocol: str, raw: str) -> str | None:
    """Normalise a proxy address to ``scheme://host:port``, else None."""
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


async def parse_source(
    protocol: str, url: str, client: httpx.AsyncClient
) -> list[ProxyInfo]:
    """Fetch and parse one proxy source into ProxyInfo entries."""
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
        url_str = normalise_url(protocol, line)
        if url_str is None:
            continue
        proto = url_str.split("://", 1)[0]
        proxies.append(
            ProxyInfo(url=url_str, protocol=proto, source=url.split("://", 1)[-1])
        )
    return proxies


async def parse_all_sources(max_per_source: int = 0) -> list[ProxyInfo]:
    """Fetch every source concurrently, deduplicating by URL."""
    async with httpx.AsyncClient(timeout=_FETCH_TIMEOUT) as client:
        results = await asyncio.gather(
            *[parse_source(proto, url, client) for proto, url in PROXY_SOURCES],
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
