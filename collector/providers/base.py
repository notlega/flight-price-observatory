"""Abstract provider contract shared by all flight data sources."""

from abc import ABC, abstractmethod
from typing import Any

from curl_cffi.requests import AsyncSession
from fli.models import Airport

from collector.errors import ProviderConnectionError


class BaseProvider(ABC):
    name: str = "base"

    @property
    def supports(self) -> set[tuple[str, str]] | None:
        return None

    @staticmethod
    def _require_proxy(proxy_url: str | None) -> str:
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
    ) -> list[dict[str, Any]] | None: ...
