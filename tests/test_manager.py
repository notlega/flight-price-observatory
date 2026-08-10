from datetime import date
from unittest.mock import AsyncMock, patch

from collector.manager import CollectorManager

from tests.libs.fakes import FakeProvider


class _EmptyRegistry:
    providers = {}


class _Registry:
    providers = {"fake": FakeProvider}


async def test_run_no_providers_is_noop():
    with patch("collector.manager.BulkSearchPipeline") as pipeline_cls:
        await CollectorManager(_EmptyRegistry()).run(
            date(2026, 8, 1), date(2026, 8, 2)
        )
    pipeline_cls.assert_not_called()


async def test_run_forwards_args_to_pipeline():
    with patch("collector.manager.BulkSearchPipeline") as pipeline_cls:
        pipeline_cls.return_value.run = AsyncMock()
        await CollectorManager(_Registry()).run(
            date(2026, 8, 1),
            date(2026, 8, 2),
            max_days_ahead=100,
            currency="USD",
            rate=50,
            workers=10,
            db_path="db/x.db",
        )
    pipeline_cls.assert_called_once()
    kwargs = pipeline_cls.call_args.kwargs
    assert kwargs["providers"][0].__class__ is FakeProvider
    assert kwargs["rate"] == 50
    assert kwargs["max_concurrent"] == 10
    assert kwargs["db_path"] == "db/x.db"
    assert kwargs["currency"] == "USD"
    pipeline_cls.return_value.run.assert_awaited_once_with(
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 2),
        max_days_ahead=100,
    )
