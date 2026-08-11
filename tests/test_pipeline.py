from datetime import date
from unittest.mock import ANY, AsyncMock, patch

import pytest
from fli.models import Airport

from collector.errors import (
    ErrorType,
    ProviderConnectionError,
    ProviderDataError,
    ProviderRateLimitedError,
    ProviderTimeoutError,
)
from collector.services.search_pipeline import BulkSearchPipeline

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


class _FakeTqdm:
    def __init__(self, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def update(self, n=1):
        pass


def _make_pipeline(provider=None, rotator=None, repo=None, **kwargs):
    pipeline = BulkSearchPipeline(
        providers=[provider or FakeProvider()],
        rate=1000,
        **kwargs,
    )
    pipeline.rotator = rotator or FakeRotator(proxies=[make_proxy()])
    pipeline.repo = repo or FakeRepo()
    return pipeline


async def test_attempt_once_success():
    proxy = make_proxy()
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[make_flights(100)]),
        rotator=FakeRotator(proxies=[proxy]),
    )
    result = await pipeline._attempt_once(pipeline.providers[0], SIN, KUL, DEP, AsyncMock())
    assert result.error_type is None
    assert result.flights == make_flights(100)
    assert result.proxy_info is proxy


async def test_attempt_once_empty_result_is_data_error():
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[None]),
        rotator=FakeRotator(proxies=[make_proxy()]),
    )
    result = await pipeline._attempt_once(
        pipeline.providers[0], SIN, KUL, DEP, AsyncMock()
    )
    assert result.error_type == ErrorType.DATA
    assert result.flights is None
    assert result.proxy_info is not None


async def test_attempt_once_no_proxy():
    pipeline = _make_pipeline(rotator=FakeRotator(proxies=[]))
    with patch("collector.services.search_pipeline.asyncio.sleep", new=AsyncMock()):
        result = await pipeline._attempt_once(
            pipeline.providers[0], SIN, KUL, DEP, AsyncMock()
        )
    assert result.error_type == ErrorType.NO_PROXY
    assert result.proxy_info is None


async def test_attempt_once_empty_list_is_data_error():
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[[]]),
        rotator=FakeRotator(proxies=[make_proxy()]),
    )
    result = await pipeline._attempt_once(
        pipeline.providers[0], SIN, KUL, DEP, AsyncMock()
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
    result = await pipeline._attempt_once(pipeline.providers[0], SIN, KUL, DEP, AsyncMock())
    assert result.error_type == expected
    assert result.proxy_info is not None


async def test_attempt_once_429_reports_rate_limiter():
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[ProviderRateLimitedError("429")])
    )
    with patch.object(
        pipeline.rate_limiter, "report_429", new=AsyncMock()
    ) as report:
        result = await pipeline._attempt_once(
            pipeline.providers[0], SIN, KUL, DEP, AsyncMock()
        )
    assert result.error_type == ErrorType.RATE_LIMITED
    report.assert_awaited_once()


async def test_search_and_store_success_stores_once():
    proxy = make_proxy()
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[make_flights(100)]),
        rotator=FakeRotator(proxies=[proxy]),
    )
    await pipeline._search_and_store(pipeline.providers[0], SIN, KUL, DEP, AsyncMock())
    assert len(pipeline.repo.upserts) == 1
    row = pipeline.repo.upserts[0]
    assert row["success"] is True
    assert row["error_type"] is None
    assert row["retries"] == 0
    assert pipeline.rotator.failures == []


async def test_search_and_store_retries_then_succeeds():
    pipeline = _make_pipeline(
        provider=FakeProvider(
            script=[ProviderTimeoutError("t"), ProviderTimeoutError("t"), make_flights(100)]
        )
    )
    await pipeline._search_and_store(pipeline.providers[0], SIN, KUL, DEP, AsyncMock())
    assert len(pipeline.repo.upserts) == 1
    row = pipeline.repo.upserts[0]
    assert row["success"] is True
    assert row["retries"] == 2
    assert len(pipeline.rotator.failures) == 2


async def test_search_and_store_all_fail_stores_failure():
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[ProviderConnectionError("c")] * 3)
    )
    await pipeline._search_and_store(pipeline.providers[0], SIN, KUL, DEP, AsyncMock())
    assert len(pipeline.repo.upserts) == 1
    row = pipeline.repo.upserts[0]
    assert row["success"] is False
    assert row["error_type"] == ErrorType.CONNECTION
    assert len(pipeline.rotator.failures) == 3


async def test_data_error_not_reported_to_rotator():
    pipeline = _make_pipeline(
        provider=FakeProvider(script=[ProviderDataError("d")] * 3)
    )
    await pipeline._search_and_store(pipeline.providers[0], SIN, KUL, DEP, AsyncMock())
    assert pipeline.rotator.failures == []
    assert pipeline.repo.upserts[0]["error_type"] == ErrorType.DATA


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
        patch("collector.services.search_pipeline.tqdm", new=_FakeTqdm),
    ):
        await pipeline._run_batch([(provider, SIN, KUL, DEP)], "test")
    assert pipeline.repo.upserts[-1]["error_type"] == ErrorType.OTHER
    assert pipeline.repo.upserts[-1]["success"] is False


async def test_retry_loop_refreshes_when_pool_low():
    provider = FakeProvider(script=[make_flights(100)])
    rotator = FakeRotator(proxies=[make_proxy()], working=0)
    repo = FakeRepo()
    repo.failed = [(ROUTE, DEP)]
    pipeline = _make_pipeline(provider=provider, rotator=rotator, repo=repo)
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
        patch("collector.services.search_pipeline.tqdm", new=_FakeTqdm),
    ):
        await pipeline._retry_loop(rounds=1)
    assert rotator.refreshes
    assert repo.upserts[-1]["success"] is True


async def test_retry_loop_stops_when_nothing_failed():
    pipeline = _make_pipeline(repo=FakeRepo())
    await pipeline._retry_loop(rounds=3)
    assert pipeline.rotator.refreshes == []


@pytest.mark.parametrize("error_type", [ErrorType.NO_PROXY, ErrorType.DATA])
async def test_retry_loop_recovers_no_proxy_and_data(error_type):
    provider = FakeProvider(script=[make_flights(100)])
    repo = FakeRepo()
    repo.failed = [(ROUTE, DEP)]
    repo.upserts = [{"error_type": error_type, "success": False}]
    pipeline = _make_pipeline(provider=provider, repo=repo)
    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
        patch("collector.services.search_pipeline.tqdm", new=_FakeTqdm),
    ):
        await pipeline._retry_loop(rounds=1)
    assert repo.upserts[-1]["success"] is True


async def test_run_orchestrates_end_to_end(tmp_path):
    provider = FakeProvider(script=[make_flights(100)])
    rotator = FakeRotator(proxies=[make_proxy()], working=1)
    repo = FakeRepo()
    repo.success_count = 1
    pipeline = _make_pipeline(provider=provider, rotator=rotator, repo=repo)
    pipeline.db_path = str(tmp_path / "state.db")

    with (
        patch("collector.services.search_pipeline.AsyncSession", new=FakeCurlSession),
        patch("collector.services.search_pipeline.tqdm", new=_FakeTqdm),
        patch("collector.services.search_pipeline.convert", new=AsyncMock()) as convert,
    ):
        await pipeline.run(date(2026, 8, 1), date(2026, 8, 1), max_days_ahead=330)

    assert repo.inserted == [(ROUTE, DEP, SIN.value, KUL.value)]
    assert rotator.refreshes == [(False, None)]
    assert repo.upserts[-1]["success"] is True
    convert.assert_awaited_once_with(
        str(tmp_path / "state.db"),
        ANY,
        delete=True,
    )
