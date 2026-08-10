import logging

from collector.providers.base import BaseProvider
from collector.providers.google_flights.provider import GoogleFlightsProvider

logger = logging.getLogger(__name__)


class ProviderRegistry:
    def __init__(self):
        self.__providers: dict[str, type[BaseProvider]] = {
            "google_flights": GoogleFlightsProvider,
        }

    @property
    def providers(self) -> dict[str, type[BaseProvider]]:
        return dict(self.__providers)

    def register(self, name: str, provider: type[BaseProvider]):
        self.__providers[name] = provider

    def unregister(self, name: str):
        self.__providers.pop(name, None)
