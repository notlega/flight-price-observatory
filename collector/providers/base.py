"""Abstract provider contract shared by all flight data sources."""

from abc import ABC, abstractmethod

from curl_cffi.requests import AsyncSession

from collector.errors import ProviderConnectionError
from fli.models import Airport


class BaseProvider(ABC):
    name: str = "base"

    @property
    def supports(self) -> set[tuple[str, str]] | None:
        return None

    @staticmethod
    def _require_proxy(proxy_url: str | None) -> None:
        if proxy_url is None:
            raise ProviderConnectionError(
                "proxy_url is required — direct connections are forbidden"
            )

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
    ) -> list[dict] | None: ...
