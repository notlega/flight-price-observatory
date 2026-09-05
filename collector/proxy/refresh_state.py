"""Proxy refresh bookkeeping: refill gaps and empty-pool backoff."""

import time

_EMPTY_REFETCH_BACKOFF = (60, 120, 300, 600)
_AUTO_REFRESH_GAP = 5


class RefreshState:
    """Track auto-refresh timing and escalate empty-pool force refetches.

    State fields are mutable so callers and tests can drive cooldown and
    backoff scenarios directly.
    """

    def __init__(
        self,
        auto_refresh_gap: float = _AUTO_REFRESH_GAP,
        empty_backoff: tuple[int, ...] = _EMPTY_REFETCH_BACKOFF,
    ) -> None:
        """Create state with the default gap and backoff ladder."""
        self.auto_refresh_gap = auto_refresh_gap
        self.empty_backoff = empty_backoff
        self.last_auto_refresh = float("-inf")
        self.last_force_refresh = float("-inf")
        self.consecutive_auto_refills = 0
        self.consecutive_force_refetches = 0

    def should_refill(self, usable: int, refill_threshold: int) -> bool:
        """Whether a refill should be attempted given the gap and backoff rules.

        Args:
            usable: Proxies currently available.
            refill_threshold: Minimum pool considered healthy.
        """
        now = time.monotonic()
        if now - self.last_auto_refresh < self.auto_refresh_gap:
            if usable >= refill_threshold:
                return False
            if self.consecutive_auto_refills > 0:
                return False
        self.last_auto_refresh = now
        return True

    def refill_result(self, usable: int) -> bool:
        """Record a refill outcome; True when a force refetch is due.

        Args:
            usable: Proxies available after the refill attempt.
        """
        if usable > 0:
            self.consecutive_auto_refills = 0
            self.consecutive_force_refetches = 0
            return False
        self.consecutive_auto_refills += 1
        index = min(self.consecutive_auto_refills - 1, len(self.empty_backoff) - 1)
        cooldown = self.empty_backoff[index]
        if time.monotonic() - self.last_force_refresh > cooldown:
            self.last_force_refresh = time.monotonic()
            self.consecutive_force_refetches += 1
            return True
        return False
