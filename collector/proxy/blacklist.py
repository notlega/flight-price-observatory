"""Evicted proxy URL blacklist with time-based expiry (lazily pruned)."""

import time


class Blacklist:
    """Track evicted proxy URLs until their park TTL expires."""

    def __init__(self) -> None:
        """Create an empty blacklist."""
        self._entries: dict[str, float] = {}

    def park(self, url: str, ttl: float = 0.0) -> None:
        """Blacklist ``url`` for ``ttl`` seconds (negative TTL = already expired)."""
        self._entries[url] = time.monotonic() + ttl

    def active(self) -> set[str]:
        """Return non-expired URLs, pruning expired entries."""
        now = time.monotonic()
        expired = [url for url, exp in self._entries.items() if exp <= now]
        for url in expired:
            del self._entries[url]
        return set(self._entries)
