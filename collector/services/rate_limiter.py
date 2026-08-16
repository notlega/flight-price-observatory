"""Adaptive rate limiter with 429 backoff and recovery."""

import asyncio
import logging
import time
from collections import deque

logger = logging.getLogger(__name__)

_RATE_429_WINDOW = 30
_RATE_429_THRESHOLD = 0.2
_RATE_RECOVERY_WINDOW = 60
_RATE_BACKOFF = 0.5
_RATE_RECOVERY = 2.0


class RateLimiter:
    """Token-bucket limiter with adaptive rate and 429 backoff."""

    def __init__(self, max_rate: float = 8, min_rate: float = 0.5) -> None:
        """Create limiter bounded by ``max_rate`` and ``min_rate`` requests/sec."""
        if max_rate <= 0:
            raise ValueError("max_rate must be > 0")
        if min_rate <= 0:
            raise ValueError("min_rate must be > 0")
        if min_rate > max_rate:
            raise ValueError("min_rate cannot exceed max_rate")
        self.rate = max_rate
        self.max_rate = max_rate
        self.min_rate = min_rate
        self.tokens = max_rate
        self.refill_at = time.monotonic()
        self._next_token_at = 0.0
        self._lock = asyncio.Lock()
        self._429_times: deque[float] = deque()

    async def acquire(self) -> None:
        """Consume one token, sleeping until a slot is available."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.refill_at
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.refill_at = now
            if self.tokens >= 1:
                self.tokens -= 1
                return
            # Reserve a slot: each waiter gets a distinct wake-up time, so
            # concurrent acquirers drain at the configured rate instead of
            # all sleeping in lockstep (thundering herd).
            when = max(now + (1 - self.tokens) / self.rate, self._next_token_at)
            self._next_token_at = when + 1.0 / self.rate
            self.tokens = 0.0
            delay = when - now
        await asyncio.sleep(delay)

    async def report_429(self) -> None:
        """Record a 429 and back the rate off when the recent ratio is high."""
        async with self._lock:
            now = time.monotonic()
            self._429_times.append(now)
            cutoff = now - _RATE_429_WINDOW
            while self._429_times and self._429_times[0] < cutoff:
                self._429_times.popleft()
            expected = _RATE_429_WINDOW * self.rate
            if len(self._429_times) > expected * _RATE_429_THRESHOLD:
                old = self.rate
                self.rate = max(self.min_rate, self.rate * _RATE_BACKOFF)
                if self.rate != old:
                    logger.info(
                        "Rate %.1f->%.1f (%d 429s in %ds)",
                        old,
                        self.rate,
                        len(self._429_times),
                        _RATE_429_WINDOW,
                    )

    async def report_success(self) -> None:
        """Record a clean request and recover the rate after a clean window."""
        async with self._lock:
            now = time.monotonic()
            cutoff = now - _RATE_RECOVERY_WINDOW
            while self._429_times and self._429_times[0] < cutoff:
                self._429_times.popleft()
            if not self._429_times and self.rate < self.max_rate:
                old = self.rate
                self.rate = min(self.max_rate, self.rate * _RATE_RECOVERY)
                if self.rate != old:
                    logger.info(
                        "Rate %.1f->%.1f (no 429s in %ds)",
                        old,
                        self.rate,
                        _RATE_RECOVERY_WINDOW,
                    )
