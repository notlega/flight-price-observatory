import asyncio
import time

import pytest

from collector.services.rate_limiter import RateLimiter


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
