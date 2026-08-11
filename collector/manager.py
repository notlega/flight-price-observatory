import logging
from datetime import date

from collector.providers.base import BaseProvider
from collector.registry import ProviderRegistry
from collector.services.search_pipeline import BulkSearchPipeline

logger = logging.getLogger(__name__)


class CollectorManager:
    def __init__(self, registry: ProviderRegistry):
        self._registry = registry

    async def run(
        self,
        start_date: date,
        end_date: date,
        max_days_ahead: int = 330,
        currency: str = "SGD",
        rate: float = 200,
        workers: int = 50,
        db_path: str = "storage/db/search_state.db",
    ):
        providers: list[BaseProvider] = [
            provider_class() for provider_class in self._registry.providers.values()
        ]
        if not providers:
            logger.warning("No providers registered")
            return

        logger.info(
            "Starting collection: %d provider(s), %s -> %s, rate=%.1f, workers=%d",
            len(providers),
            start_date,
            end_date,
            rate,
            workers,
        )

        pipeline = BulkSearchPipeline(
            providers=providers,
            rate=rate,
            max_concurrent=workers,
            db_path=db_path,
            currency=currency,
        )
        await pipeline.run(
            start_date=start_date,
            end_date=end_date,
            max_days_ahead=max_days_ahead,
        )
