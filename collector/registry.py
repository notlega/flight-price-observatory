"""Provider registry: discoverable provider implementations."""

import logging

from collector.providers.base import BaseProvider
from collector.providers.google_flights.provider import GoogleFlightsProvider

logger = logging.getLogger(__name__)


class ProviderRegistry:
    def __init__(self) -> None:
        self._providers: dict[str, type[BaseProvider]] = {
            "google_flights": GoogleFlightsProvider,
        }

    @property
    def providers(self) -> dict[str, type[BaseProvider]]:
        return dict(self._providers)

    def register(self, name: str, provider: type[BaseProvider]) -> None:
        self._providers[name] = provider

    def unregister(self, name: str) -> None:
        self._providers.pop(name, None)
