import logging
from datetime import date, timedelta
from typing import Any, cast
from unittest.mock import ANY, AsyncMock, patch

import pytest
from fli.models import Airport

from collector.errors import (
    ErrorType,
    ProviderBlockedError,
    ProviderConnectionError,
    ProviderDataError,
    ProviderRateLimitedError,
    ProviderTimeoutError,
)
from collector.models import FlightType
from collector.services.search_pipeline import (
    _MAX_ATTEMPTS,
    _MIN_POOL_BEFORE_RETRY,
    BulkSearchPipeline,
    SearchTask,
)
from tests.libs.factories import make_flights, make_proxy
from tests.libs.fakes import FakeCurlSession, FakeProvider, FakeRepo, FakeRotator

SIN = Airport["SIN"]
KUL = Airport["KUL"]
ROUTE = f"{SIN.value}|{KUL.value}"
DEP = "2026-08-01"


def test_pipeline_default_workers_bounded():
    pipeline = BulkSearchPipeline(providers=[], rate=10)
    assert pipeline.max_concurrent == 50


def test_search_parser_worker_default():
    from argparse import ArgumentParser

    from cli.search import configure_parser

    parser = ArgumentParser()
    configure_parser(parser.add_subparsers())
    ns = parser.parse_args(["search", "--start", "2026-08-01"])
    assert ns.workers == 50


def _make_pipeline(provider=None, rotator=None, repo=None, **kwargs):
    pipeline = BulkSearchPipeline(
        providers=[provider or FakeProvider()],
        rate=1000,
        **kwargs,
    )
    pipeline.rotator = cast(Any, rotator or FakeRotator(proxies=[make_proxy()]))
    pipeline.repo = cast(Any, repo or FakeRepo())
    return cast(Any, pipeline)


async def test_attempt_once_success():
    proxy = make_proxy()
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[make_flights(100)]),
        rotator=FakeRotator(proxies=[proxy]),
    )
    result = await pipeline._attempt_once(
        pipeline.providers[0], SIN, KUL, DEP, None, AsyncMock()
    )
    assert result.error_type is None
    assert result.flights == make_flights(100)
    assert result.proxy_info is proxy


async def test_attempt_once_empty_result_is_data_error():
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[None]),
        rotator=FakeRotator(proxies=[make_proxy()]),
    )
    result = await pipeline._attempt_once(
        pipeline.providers[0], SIN, KUL, DEP, None, AsyncMock()
    )
    assert result.error_type == ErrorType.DATA
    assert result.flights is None
    assert result.proxy_info is not None


async def test_attempt_once_no_proxy():
    pipeline = _make_pipeline(rotator=FakeRotator(proxies=[]))
    with patch("collector.services.search_pipeline.asyncio.sleep", new=AsyncMock()):
        result = await pipeline._attempt_once(
            pipeline.providers[0], SIN, KUL, DEP, None, AsyncMock()
        )
    assert result.error_type == ErrorType.NO_PROXY
    assert result.proxy_info is None


async def test_attempt_once_empty_list_is_data_error():
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[[]]),
        rotator=FakeRotator(proxies=[make_proxy()]),
    )
    result = await pipeline._attempt_once(
        pipeline.providers[0], SIN, KUL, DEP, None, AsyncMock()
    )
    assert result.error_type == ErrorType.DATA


@pytest.mark.parametrize(
    "exc,expected",
    [
        (ProviderRateLimitedError("429"), ErrorType.RATE_LIMITED),
        (ProviderTimeoutError("t"), ErrorType.TIMEOUT),
        (ProviderConnectionError("c"), ErrorType.CONNECTION),
        (ProviderDataError("d"), ErrorType.DATA),
        (RuntimeError("boom"), ErrorType.OTHER),
    ],
)
async def test_attempt_once_error_mapping(exc, expected):
    pipeline = _make_pipeline(provider=FakeProvider(script=[exc]))
    result = await pipeline._attempt_once(
        pipeline.providers[0], SIN, KUL, DEP, None, AsyncMock()
    )
    assert result.error_type == expected
    assert result.proxy_info is not None


async def test_attempt_once_429_reports_rate_limiter():
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[ProviderRateLimitedError("429")])
    )
    with patch.object(pipeline.rate_limiter, "report_429", new=AsyncMock()) as report:
        result = await pipeline._attempt_once(
            pipeline.providers[0], SIN, KUL, DEP, None, AsyncMock()
        )
    assert result.error_type == ErrorType.RATE_LIMITED
    report.assert_awaited_once()


async def test_attempt_once_429_parks_proxy():
    proxy = make_proxy()
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[ProviderRateLimitedError("429")]),
        rotator=FakeRotator(proxies=[proxy]),
    )
    result = await pipeline._attempt_once(
        pipeline.providers[0], SIN, KUL, DEP, None, AsyncMock()
    )
    assert result.error_type == ErrorType.RATE_LIMITED
    assert pipeline.rotator.rate_limited == [(proxy, 60)]


async def test_attempt_once_blocked_reports_stub_as_data():
    proxy = make_proxy()
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[ProviderBlockedError("blocked")]),
        rotator=FakeRotator(proxies=[proxy]),
    )
    result = await pipeline._attempt_once(
        pipeline.providers[0], SIN, KUL, DEP, None, AsyncMock()
    )
    assert result.error_type == ErrorType.DATA
    assert result.stubbed is True
    assert pipeline.rotator.stubs == [proxy]


def _task(provider, return_date=None):
    return SearchTask(
        provider=provider,
        origin=SIN,
        dest=KUL,
        departure=DEP,
        return_date=return_date,
        flight_type=FlightType.ONE_WAY.value,
    )


async def test_search_and_store_stub_backoff_sleeps_between_attempts():
    pipeline = _make_pipeline(
        provider=FakeProvider(
            script=[
                ProviderBlockedError("blocked"),
                ProviderBlockedError("blocked"),
                make_flights(100),
            ]
        )
    )
    with patch(
        "collector.services.search_pipeline.asyncio.sleep", new=AsyncMock()
    ) as sleep:
        await pipeline._search_and_store(_task(pipeline.providers[0]), AsyncMock())
    assert sleep.await_count == 2
    assert pipeline.repo.upserts[0]["success"] is True
    assert pipeline.repo.upserts[0]["retries"] == 3


async def test_search_and_store_all_stubs_burns_full_round():
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[ProviderBlockedError("blocked")] * 3)
    )
    with patch(
        "collector.services.search_pipeline.asyncio.sleep", new=AsyncMock()
    ) as sleep:
        await pipeline._search_and_store(_task(pipeline.providers[0]), AsyncMock())
    assert len(pipeline.providers[0].calls) == 3
    assert sleep.await_count == 3
    assert pipeline.repo.upserts[0]["success"] is False
    assert pipeline.repo.upserts[0]["error_type"] == ErrorType.DATA
    assert pipeline.repo.upserts[0]["retries"] == 3


async def test_store_result_requires_error_type_on_failure():
    pipeline = _make_pipeline(provider=FakeProvider(script=[make_flights(100)]))
    with pytest.raises(ValueError, match="error_type"):
        await pipeline._store_result(
            _task(pipeline.providers[0]),
            flights=[],
            error_type=None,
            retries=3,
            success=False,
            searched_at="t",
        )


async def test_search_and_store_success_stores_once():
    proxy = make_proxy()
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[make_flights(100)]),
        rotator=FakeRotator(proxies=[proxy]),
    )
    await pipeline._search_and_store(
        SearchTask(
            provider=pipeline.providers[0],
            origin=SIN,
            dest=KUL,
            departure=DEP,
            return_date=None,
            flight_type=FlightType.ONE_WAY.value,
        ),
        AsyncMock(),
    )
    assert len(pipeline.repo.upserts) == 1
    row = pipeline.repo.upserts[0]
    assert row["success"] is True
    assert row["error_type"] is None
    assert row["retries"] == 1
    assert pipeline.rotator.failures == []


async def test_search_and_store_retries_then_succeeds():
    pipeline = _make_pipeline(
        provider=FakeProvider(
            script=[
                ProviderTimeoutError("t"),
                ProviderTimeoutError("t"),
                make_flights(100),
            ]
        )
    )
    await pipeline._search_and_store(
        SearchTask(
            provider=pipeline.providers[0],
            origin=SIN,
            dest=KUL,
            departure=DEP,
            return_date=None,
            flight_type=FlightType.ONE_WAY.value,
        ),
        AsyncMock(),
    )
    assert len(pipeline.repo.upserts) == 1
    row = pipeline.repo.upserts[0]
    assert row["success"] is True
    assert row["retries"] == 3
    assert len(pipeline.rotator.failures) == 2


async def test_search_and_store_all_fail_stores_failure():
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[ProviderConnectionError("c")] * 3)
    )
    await pipeline._search_and_store(
        SearchTask(
            provider=pipeline.providers[0],
            origin=SIN,
            dest=KUL,
            departure=DEP,
            return_date=None,
            flight_type=FlightType.ONE_WAY.value,
        ),
        AsyncMock(),
    )
    assert len(pipeline.repo.upserts) == 1
    row = pipeline.repo.upserts[0]
    assert row["success"] is False
    assert row["error_type"] == ErrorType.CONNECTION
    assert row["retries"] == 3
    assert len(pipeline.rotator.failures) == 3


async def test_search_and_store_failure_retries_scale_with_round():
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[ProviderConnectionError("c")] * 3)
    )
    await pipeline._search_and_store(
        SearchTask(
            provider=pipeline.providers[0],
            origin=SIN,
            dest=KUL,
            departure=DEP,
            return_date=None,
            flight_type=FlightType.ONE_WAY.value,
        ),
        AsyncMock(),
        retry_round=1,
    )
    assert pipeline.repo.upserts[0]["success"] is False
    assert pipeline.repo.upserts[0]["retries"] == 6


async def test_data_error_not_reported_to_rotator():
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[ProviderDataError("d")] * 3)
    )
    await pipeline._search_and_store(
        SearchTask(
            provider=pipeline.providers[0],
            origin=SIN,
            dest=KUL,
            departure=DEP,
            return_date=None,
            flight_type=FlightType.ONE_WAY.value,
        ),
        AsyncMock(),
    )
    assert pipeline.rotator.failures == []
    assert pipeline.repo.upserts[0]["error_type"] == ErrorType.DATA
    assert pipeline.repo.upserts[0]["retries"] == 3


async def test_other_error_not_reported_to_rotator():
    pipeline = _make_pipeline(provider=FakeProvider(script=[RuntimeError("boom")] * 3))
    await pipeline._search_and_store(
        SearchTask(
            provider=pipeline.providers[0],
            origin=SIN,
            dest=KUL,
            departure=DEP,
            return_date=None,
            flight_type=FlightType.ONE_WAY.value,
        ),
        AsyncMock(),
    )
    assert pipeline.rotator.failures == []
    assert pipeline.repo.upserts[0]["error_type"] == ErrorType.OTHER


async def test_run_batch_mid_round_refresh_single_flight():
    provider = FakeProvider(script=[make_flights(100)])
    rotator = FakeRotator(proxies=[], working=1)
    pipeline = _make_pipeline(provider=provider, rotator=rotator)
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
    ):
        await pipeline._run_batch(
            [
                SearchTask(
                    provider=provider,
                    origin=SIN,
                    dest=KUL,
                    departure=DEP,
                    return_date=None,
                    flight_type=FlightType.ONE_WAY.value,
                )
            ],
            "test",
            retry_round=1,
        )
    assert rotator.refreshes == [(True, None)]


async def test_run_batch_mid_round_refresh_not_for_first_pass():
    provider = FakeProvider(script=[make_flights(100)])
    rotator = FakeRotator(proxies=[], working=1)
    pipeline = _make_pipeline(provider=provider, rotator=rotator)
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
    ):
        await pipeline._run_batch(
            [
                SearchTask(
                    provider=provider,
                    origin=SIN,
                    dest=KUL,
                    departure=DEP,
                    return_date=None,
                    flight_type=FlightType.ONE_WAY.value,
                )
            ],
            "test",
        )
    assert rotator.refreshes == []


async def test_run_batch_mid_round_refresh_failure_logged(caplog):
    provider = FakeProvider(script=[make_flights(100)])
    rotator = FakeRotator(proxies=[], working=1)

    async def boom_refresh(*args, **kwargs):
        raise RuntimeError("list fetch failed")

    rotator.refresh = boom_refresh  # type: ignore[method-assign]
    pipeline = _make_pipeline(provider=provider, rotator=rotator)
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
    ):
        await pipeline._run_batch(
            [
                SearchTask(
                    provider=provider,
                    origin=SIN,
                    dest=KUL,
                    departure=DEP,
                    return_date=None,
                    flight_type=FlightType.ONE_WAY.value,
                )
            ],
            "test",
            retry_round=1,
        )
    assert pipeline._mid_refresh is False
    assert "Mid-round proxy refresh failed" in caplog.text


async def test_run_batch_records_unexpected_failure():
    class FlakyRepo(FakeRepo):
        def __init__(self):
            super().__init__()
            self.upsert_calls = 0

        async def upsert(self, *args, **kwargs):
            self.upsert_calls += 1
            if self.upsert_calls == 1:
                raise RuntimeError("db gone")
            await super().upsert(*args, **kwargs)

    provider = FakeProvider(script=[make_flights(100)])
    pipeline = _make_pipeline(provider=provider, repo=FlakyRepo())
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
    ):
        await pipeline._run_batch(
            [
                SearchTask(
                    provider=provider,
                    origin=SIN,
                    dest=KUL,
                    departure=DEP,
                    return_date=None,
                    flight_type=FlightType.ONE_WAY.value,
                )
            ],
            "test",
        )
    assert pipeline.repo.upserts[-1]["error_type"] == ErrorType.OTHER
    assert pipeline.repo.upserts[-1]["success"] is False


async def test_run_batch_worker_crash_does_not_abandon_other_tasks():
    provider = FakeProvider(script=[RuntimeError("boom")] * 3 + [make_flights(100)])
    pipeline = _make_pipeline(provider=provider, max_concurrent=2)

    async def flaky_record_failure(task, retry_round):
        raise RuntimeError("recorder down")

    pipeline._record_failure = flaky_record_failure  # type: ignore[method-assign]
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
    ):
        await pipeline._run_batch(
            [
                SearchTask(
                    provider=provider,
                    origin=SIN,
                    dest=KUL,
                    departure=DEP,
                    return_date=None,
                    flight_type=FlightType.ONE_WAY.value,
                ),
                SearchTask(
                    provider=provider,
                    origin=SIN,
                    dest=KUL,
                    departure=DEP,
                    return_date=None,
                    flight_type=FlightType.ONE_WAY.value,
                ),
            ],
            "test",
        )
    assert any(r["success"] is True for r in pipeline.repo.upserts)


async def test_run_batch_zero_max_concurrent_processes_all():
    provider = FakeProvider(script=[make_flights(100)] * 3)
    pipeline = _make_pipeline(provider=provider, max_concurrent=0)
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
    ):
        await pipeline._run_batch(
            [
                SearchTask(
                    provider=provider,
                    origin=SIN,
                    dest=KUL,
                    departure=DEP,
                    return_date=None,
                    flight_type=FlightType.ONE_WAY.value,
                ),
                SearchTask(
                    provider=provider,
                    origin=SIN,
                    dest=KUL,
                    departure=DEP,
                    return_date="2026-08-08",
                    flight_type=FlightType.ROUND_TRIP.value,
                ),
                SearchTask(
                    provider=provider,
                    origin=SIN,
                    dest=KUL,
                    departure=DEP,
                    return_date="2026-08-15",
                    flight_type=FlightType.ROUND_TRIP.value,
                ),
            ],
            "test",
        )
    assert len(pipeline.repo.upserts) == 3
    assert all(r["success"] is True for r in pipeline.repo.upserts)


async def test_retry_loop_escalates_max_retries_with_round():
    provider = FakeProvider(script=[make_flights(100)])
    repo = FakeRepo()
    repo.failed = [("X|Y", "2000-01-01", "", "ONE_WAY")]
    calls: list[int] = []

    async def recording_get_failed(max_retries=3):
        calls.append(max_retries)
        return repo.failed

    repo.get_failed = recording_get_failed  # type: ignore[method-assign]
    pipeline = _make_pipeline(provider=provider, repo=repo)
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
    ):
        await pipeline._retry_loop(rounds=3)
    assert calls == [
        1 * _MAX_ATTEMPTS,
        2 * _MAX_ATTEMPTS,
        3 * _MAX_ATTEMPTS,
    ]


async def test_retry_loop_refreshes_when_pool_low():
    provider = FakeProvider(script=[make_flights(100)])
    rotator = FakeRotator(proxies=[make_proxy()], working=0)
    repo = FakeRepo()
    repo.failed = [(ROUTE, DEP, "", "ONE_WAY")]
    pipeline = _make_pipeline(provider=provider, rotator=rotator, repo=repo)
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
        patch("collector.services.search_pipeline.date") as fake_date,
    ):
        fake_date.today.return_value = date(2026, 8, 1)
        fake_date.side_effect = lambda *a, **k: date(*a, **k)
        await pipeline._retry_loop(rounds=1)
    assert rotator.refreshes
    assert repo.upserts[-1]["success"] is True


@pytest.mark.parametrize("working", [19, 20])
async def test_retry_loop_round1_always_refreshes_threshold_for_round2(working):
    provider = FakeProvider(script=[make_flights(100)])
    rotator = FakeRotator(proxies=[make_proxy()], working=working)
    repo = FakeRepo()
    repo.failed = [("X|Y", "2000-01-01", "", "ONE_WAY")]
    pipeline = _make_pipeline(provider=provider, rotator=rotator, repo=repo)
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
    ):
        await pipeline._retry_loop(rounds=2)
    # Round 1 always force-refreshes (stale pool throttled regardless of size);
    # the low-pool threshold applies from round 2 onward.
    if working < _MIN_POOL_BEFORE_RETRY:
        assert rotator.refreshes == [(True, None), (True, None)]
    else:
        assert rotator.refreshes == [(True, None)]


def test_dates_between_filters_past_and_future():
    from collector.services.search_pipeline import _dates_between

    today = date(2026, 8, 14)
    assert _dates_between(date(2026, 8, 10), date(2026, 8, 16), today) == [
        date(2026, 8, 14),
        date(2026, 8, 15),
        date(2026, 8, 16),
    ]
    assert _dates_between(date(2026, 8, 15), date(2026, 8, 16), today) == [
        date(2026, 8, 15),
        date(2026, 8, 16),
    ]
    assert _dates_between(date(2026, 8, 1), date(2026, 8, 13), today) == []
    assert _dates_between(date(2026, 8, 15), date(2026, 8, 15), today) == [
        date(2026, 8, 15)
    ]


async def test_retry_loop_retries_dep_today():
    provider = FakeProvider(script=[make_flights(100)])
    rotator = FakeRotator(proxies=[make_proxy()], working=20)
    repo = FakeRepo()
    repo.failed = [(ROUTE, date.today().isoformat(), "", "ONE_WAY")]
    pipeline = _make_pipeline(provider=provider, rotator=rotator, repo=repo)
    with patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession):
        await pipeline._retry_loop(rounds=1)
    assert repo.upserts[-1]["success"] is True


async def test_retry_loop_warns_route_not_covered(caplog):
    provider = FakeProvider(script=[make_flights(100)])
    rotator = FakeRotator(proxies=[make_proxy()], working=20)
    repo = FakeRepo()
    repo.failed = [
        (f"{KUL.value}|{SIN.value}", date.today().isoformat(), "", "ONE_WAY")
    ]
    pipeline = _make_pipeline(provider=provider, rotator=rotator, repo=repo)
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
        caplog.at_level(logging.WARNING),
    ):
        await pipeline._retry_loop(rounds=1)
    assert repo.upserts == []
    assert "no provider covers it" in caplog.text


async def test_retry_loop_supports_none_covers_all_routes():
    provider = FakeProvider(supports=None, script=[make_flights(100)])
    rotator = FakeRotator(proxies=[make_proxy()], working=20)
    repo = FakeRepo()
    repo.failed = [
        (f"{KUL.value}|{SIN.value}", date.today().isoformat(), "", "ONE_WAY")
    ]
    pipeline = _make_pipeline(provider=provider, rotator=rotator, repo=repo)
    with patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession):
        await pipeline._retry_loop(rounds=1)
    assert repo.upserts[-1]["success"] is True


async def test_run_max_days_ahead_zero_limits_to_today(tmp_path):
    provider = FakeProvider(script=[make_flights(100)] * 60)
    rotator = FakeRotator(proxies=[make_proxy()], working=1)
    repo = FakeRepo()
    pipeline = _make_pipeline(provider=provider, rotator=rotator, repo=repo)
    pipeline.db_path = str(tmp_path / "state.db")
    today = date.today()
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
        patch("collector.services.search_pipeline.convert", new=AsyncMock()),
    ):
        await pipeline.run(
            today - timedelta(days=5), today + timedelta(days=10), max_days_ahead=0
        )
    assert provider.calls
    assert {call[2] for call in provider.calls} == {today.isoformat()}


async def test_run_max_days_ahead_negative_builds_no_tasks(tmp_path):
    provider = FakeProvider(script=[make_flights(100)])
    rotator = FakeRotator(proxies=[make_proxy()], working=1)
    repo = FakeRepo()
    pipeline = _make_pipeline(provider=provider, rotator=rotator, repo=repo)
    pipeline.db_path = str(tmp_path / "state.db")
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
        patch("collector.services.search_pipeline.convert", new=AsyncMock()) as convert,
    ):
        await pipeline.run(date.today(), date.today(), max_days_ahead=-1)
    assert repo.inserted == []
    assert repo.upserts == []
    convert.assert_awaited_once()


async def test_retry_loop_stops_when_nothing_failed():
    pipeline = _make_pipeline(repo=FakeRepo())
    await pipeline._retry_loop(rounds=3)
    assert pipeline.rotator.refreshes == []


@pytest.mark.parametrize("error_type", [ErrorType.NO_PROXY, ErrorType.DATA])
async def test_retry_loop_recovers_no_proxy_and_data(error_type):
    provider = FakeProvider(script=[make_flights(100)])
    repo = FakeRepo()
    repo.failed = [(ROUTE, DEP, "", "ONE_WAY")]
    repo.upserts = [{"error_type": error_type, "success": False}]
    pipeline = _make_pipeline(provider=provider, repo=repo)
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
        patch("collector.services.search_pipeline.date") as fake_date,
    ):
        fake_date.today.return_value = date(2026, 8, 1)
        fake_date.side_effect = lambda *a, **k: date(*a, **k)
        await pipeline._retry_loop(rounds=1)
    assert repo.upserts[-1]["success"] is True


async def test_retry_loop_skips_past_departures():
    provider = FakeProvider(script=[make_flights(100)])
    rotator = FakeRotator(working=20)
    repo = FakeRepo()
    repo.failed = [(ROUTE, "2000-01-01", "", "ONE_WAY")]
    repo.upserts = [{"error_type": ErrorType.CONNECTION, "success": False}]
    pipeline = _make_pipeline(provider=provider, rotator=rotator, repo=repo)
    await pipeline._retry_loop(rounds=3)
    assert len(rotator.refreshes) == 1
    assert len(repo.upserts) == 1


@pytest.mark.parametrize(
    "start,end",
    [
        (date(2026, 8, 1), date(2026, 8, 5)),
        (date(2026, 8, 10), date(2026, 8, 5)),
    ],
)
async def test_run_empty_task_window_noop(tmp_path, start, end):
    provider = FakeProvider(script=[make_flights(100)])
    rotator = FakeRotator(proxies=[make_proxy()], working=1)
    repo = FakeRepo()
    pipeline = _make_pipeline(provider=provider, rotator=rotator, repo=repo)
    pipeline.db_path = str(tmp_path / "state.db")
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
        patch("collector.services.search_pipeline.convert", new=AsyncMock()) as convert,
    ):
        await pipeline.run(start, end, max_days_ahead=330)
    assert repo.inserted == []
    assert repo.upserts == []
    convert.assert_awaited_once()


async def test_run_preflight_refreshes_when_no_proxies(tmp_path):
    provider = FakeProvider(script=[make_flights(100)])
    rotator = FakeRotator(working=0)
    repo = FakeRepo()
    pipeline = _make_pipeline(provider=provider, rotator=rotator, repo=repo)
    pipeline.db_path = str(tmp_path / "state.db")
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
        patch("collector.services.search_pipeline.convert", new=AsyncMock()),
        pytest.raises(RuntimeError, match="refusing to run"),
    ):
        await pipeline.run(date(2026, 12, 1), date(2026, 12, 1), max_days_ahead=330)
    assert rotator.refreshes == [(False, None), (True, None)]


async def test_run_orchestrates_end_to_end(tmp_path):
    provider = FakeProvider(script=[make_flights(100)] * 4)
    rotator = FakeRotator(proxies=[make_proxy()], working=1)
    repo = FakeRepo()
    repo.success_count = 1
    pipeline = _make_pipeline(provider=provider, rotator=rotator, repo=repo)
    pipeline.db_path = str(tmp_path / "state.db")

    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
        patch("collector.services.search_pipeline.convert", new=AsyncMock()) as convert,
        patch("collector.services.search_pipeline.date") as fake_date,
    ):
        fake_date.today.return_value = date(2026, 8, 1)
        fake_date.side_effect = lambda *a, **k: date(*a, **k)
        await pipeline.run(date(2026, 8, 1), date(2026, 8, 1), max_days_ahead=330)

    assert repo.inserted == [
        (ROUTE, DEP, "", "ONE_WAY", SIN.value, KUL.value),
        (ROUTE, DEP, "2026-08-08", "ROUND_TRIP", SIN.value, KUL.value),
        (ROUTE, DEP, "2026-08-15", "ROUND_TRIP", SIN.value, KUL.value),
        (ROUTE, DEP, "2026-08-22", "ROUND_TRIP", SIN.value, KUL.value),
    ]
    assert rotator.refreshes == [(False, None)]
    assert all(r["success"] is True for r in repo.upserts)
    convert.assert_awaited_once_with(
        str(tmp_path / "state.db"),
        ANY,
        delete=True,
    )


async def test_run_keep_db_preserves_state(tmp_path):
    provider = FakeProvider(script=[make_flights(100)] * 4)
    rotator = FakeRotator(proxies=[make_proxy()], working=1)
    repo = FakeRepo()
    repo.success_count = 1
    pipeline = _make_pipeline(
        provider=provider,
        rotator=rotator,
        repo=repo,
        keep_db=True,
    )
    pipeline.db_path = str(tmp_path / "state.db")

    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
        patch("collector.services.search_pipeline.convert", new=AsyncMock()) as convert,
        patch("collector.services.search_pipeline.date") as fake_date,
    ):
        fake_date.today.return_value = date(2026, 8, 1)
        fake_date.side_effect = lambda *a, **k: date(*a, **k)
        await pipeline.run(date(2026, 8, 1), date(2026, 8, 1), max_days_ahead=330)

    convert.assert_awaited_once_with(
        str(tmp_path / "state.db"),
        ANY,
        delete=False,
    )


async def test_run_skips_past_dates(tmp_path):
    provider = FakeProvider(script=[make_flights(100)] * 4)
    rotator = FakeRotator(proxies=[make_proxy()], working=1)
    repo = FakeRepo()
    repo.success_count = 1
    pipeline = _make_pipeline(provider=provider, rotator=rotator, repo=repo)
    pipeline.db_path = str(tmp_path / "state.db")
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
        patch("collector.services.search_pipeline.convert", new=AsyncMock()),
        patch("collector.services.search_pipeline.date") as fake_date,
    ):
        fake_date.today.return_value = date(2026, 8, 3)
        fake_date.side_effect = lambda *a, **k: date(*a, **k)
        await pipeline.run(date(2026, 8, 1), date(2026, 8, 3), max_days_ahead=330)
    dep = "2026-08-03"
    assert repo.inserted == [
        (ROUTE, dep, "", "ONE_WAY", SIN.value, KUL.value),
        (ROUTE, dep, "2026-08-10", "ROUND_TRIP", SIN.value, KUL.value),
        (ROUTE, dep, "2026-08-17", "ROUND_TRIP", SIN.value, KUL.value),
        (ROUTE, dep, "2026-08-24", "ROUND_TRIP", SIN.value, KUL.value),
    ]


def test_ahead_days_from_today_matches_window():
    from datetime import timedelta

    from cli.search import _ahead_days

    today = date.today()
    assert _ahead_days(today + timedelta(days=2)) == 2
    assert _ahead_days(today + timedelta(days=12)) == 12
    assert _ahead_days(today - timedelta(days=3)) == 0


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (0, "0s"),
        (45, "45s"),
        (70, "1m10s"),
        (300, "5m00s"),
        (3729.5, "1h02m"),
        (7200, "2h00m"),
    ],
)
def test_format_duration(seconds, expected):
    from collector.services.search_pipeline import _format_duration

    assert _format_duration(seconds) == expected
