"""Abstract provider contract shared by all flight data sources."""

from abc import ABC, abstractmethod

from curl_cffi.requests import AsyncSession
from fli.models import Airport

from collector.errors import ProviderConnectionError
from collector.models.flight_result import FlightResultDict


class BaseProvider(ABC):
    """Abstract contract every flight data source implements."""

    name: str = "base"

    @property
    def supports(self) -> set[tuple[str, str]] | None:
        """Return supported route codes, or None when all routes are supported."""
        return None

    @staticmethod
    def _require_proxy(proxy_url: str | None) -> str:
        """Return ``proxy_url`` or raise; direct connections are forbidden."""
        if proxy_url is None:
            raise ProviderConnectionError(
                "proxy_url is required — direct connections are forbidden"
            )
        return proxy_url

    @abstractmethod
    async def search(
        self,
        origin: Airport,
        dest: Airport,
        date_str: str,
        currency: str = "SGD",
        proxy_url: str | None = None,
        session: AsyncSession | None = None,
        return_date: str | None = None,
    ) -> list[FlightResultDict] | None:
        """Search flights for one origin-destination-date combination."""
