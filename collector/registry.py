"""Provider registry: discoverable provider implementations."""

import logging

from collector.providers.base import BaseProvider
from collector.providers.google_flights.provider import GoogleFlightsProvider

logger = logging.getLogger(__name__)


class ProviderRegistry:
    """Holds the known provider implementations, keyed by name."""

    def __init__(self) -> None:
        """Create registry pre-populated with built-in providers."""
        self._providers: dict[str, type[BaseProvider]] = {
            "google_flights": GoogleFlightsProvider,
        }

    @property
    def providers(self) -> dict[str, type[BaseProvider]]:
        """Return a copy of the name-to-provider-class mapping."""
        return dict(self._providers)

    def register(self, name: str, provider: type[BaseProvider]) -> None:
        """Register ``provider`` class under ``name``."""
        self._providers[name] = provider

    def unregister(self, name: str) -> None:
        """Remove the provider registered under ``name``, if any."""
        self._providers.pop(name, None)
