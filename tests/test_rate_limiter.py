import asyncio
import time
from collections import deque

import pytest

from collector.services.rate_limiter import RateLimiter


def test_zero_max_rate_rejected():
    with pytest.raises(ValueError, match="max_rate"):
        RateLimiter(max_rate=0)


def test_negative_max_rate_rejected():
    with pytest.raises(ValueError, match="max_rate"):
        RateLimiter(max_rate=-5)


def test_zero_min_rate_rejected():
    with pytest.raises(ValueError, match="min_rate"):
        RateLimiter(max_rate=8, min_rate=0)


def test_min_rate_above_max_rate_rejected():
    with pytest.raises(ValueError, match="min_rate cannot exceed"):
        RateLimiter(max_rate=2, min_rate=8)


async def test_initial_burst_allows_rate_tokens():
    rl = RateLimiter(max_rate=10)
    for _ in range(10):
        await rl.acquire()


async def test_acquire_blocks_beyond_rate():
    rl = RateLimiter(max_rate=10)
    t0 = time.monotonic()
    for _ in range(11):
        await rl.acquire()
    assert time.monotonic() - t0 >= 0.09


async def test_429_backoff_halves_rate():
    rl = RateLimiter(max_rate=4)
    for _ in range(25):
        await rl.report_429()
    assert rl.rate == 2.0


async def test_429_backoff_never_below_min_rate():
    rl = RateLimiter(max_rate=4, min_rate=0.5)
    for _ in range(300):
        await rl.report_429()
    assert rl.rate >= 0.5


async def test_success_recovers_rate():
    rl = RateLimiter(max_rate=4)
    rl.rate = 2.0
    rl._429_times.extend([time.monotonic() - 120] * 25)
    await rl.report_success()
    assert rl.rate == 4.0


async def test_concurrent_acquire_respects_rate():
    rl = RateLimiter(max_rate=50)
    t0 = time.monotonic()
    await asyncio.gather(*[rl.acquire() for _ in range(100)])
    elapsed = time.monotonic() - t0
    assert elapsed >= 1.0


async def test_429_window_prunes_old_timestamps():
    rl = RateLimiter(max_rate=4)
    old = time.monotonic() - 120
    rl._429_times.extend([old] * 10)
    await rl.report_429()
    assert all(t >= time.monotonic() - 31 for t in rl._429_times)


async def test_429_backoff_only_above_threshold():
    rl = RateLimiter(max_rate=4)
    for _ in range(int(30 * 4 * 0.2)):
        await rl.report_429()
    assert rl.rate == 4.0
    await rl.report_429()
    assert rl.rate < 4.0


async def test_report_success_prunes_old_timestamps():
    rl = RateLimiter(max_rate=4)
    rl.rate = 2.0
    rl._429_times.extend([time.monotonic() - 120] * 25)
    await rl.report_success()
    assert rl.rate == 4.0
    assert rl._429_times == deque()


async def test_acquire_after_long_idle_caps_burst_at_rate():
    rl = RateLimiter(max_rate=3)
    rl.refill_at = time.monotonic() - 3600
    rl.tokens = 0.0
    for _ in range(3):
        await rl.acquire()
    assert rl.tokens < 3


@pytest.mark.parametrize(
    "reports,min_rate,expected",
    [
        (25, 0.5, 2.0),
        (300, 0.5, 0.5),
    ],
)
async def test_429_backoff_parametrized(reports, min_rate, expected):
    rl = RateLimiter(max_rate=4, min_rate=min_rate)
    for _ in range(reports):
        await rl.report_429()
    assert rl.rate == expected
