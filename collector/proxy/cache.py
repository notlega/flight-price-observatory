"""Proxy cache persistence."""

import json
import logging
import os
import time
from pathlib import Path

from collector.models.proxy import ProxyInfo

logger = logging.getLogger(__name__)

PROXY_CACHE_PATH = "storage/proxy_cache.json"
CACHE_FRESH_TTL = 30 * 60
CACHE_MAX_AGE = 24 * 3600
MIN_CACHE_POOL = 50


def load_cache() -> tuple[float, list[ProxyInfo]] | None:
    try:
        with open(PROXY_CACHE_PATH) as f:
            data = json.load(f)
        cached_at = float(data.get("cached_at", 0))
        proxies = [ProxyInfo.from_dict(d) for d in data.get("proxies", [])]
        if not proxies:
            return None
        return cached_at, proxies
    except OSError, ValueError, TypeError, KeyError:
        return None


def save_cache(proxies: list[ProxyInfo]) -> None:
    Path(PROXY_CACHE_PATH).parent.mkdir(parents=True, exist_ok=True)
    tmp = f"{PROXY_CACHE_PATH}.tmp"
    data = {
        "cached_at": time.time(),
        "proxies": [p.to_dict() for p in proxies],
    }
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, PROXY_CACHE_PATH)
