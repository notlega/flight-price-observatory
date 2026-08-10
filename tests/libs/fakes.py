from unittest.mock import AsyncMock

from curl_cffi import requests

from collector.models.proxy import ProxyInfo
from collector.providers.base import BaseProvider
from fli.models import Airport


class FakeProvider(BaseProvider):
    """BaseProvider whose search behaviour follows a script.

    Each element is either an exception to raise or a result to return,
    consumed one per call.
    """

    def __init__(
        self,
        routes: list[tuple[Airport, Airport]] | None = None,
        script: list = None,
    ):
        self._routes = list(
            routes or [(Airport["SIN"], Airport["KUL"])]
        )
        self.script = list(script or [])
        self.calls: list[tuple[Airport, Airport, str, str | None]] = []

    @property
    def routes(self) -> list[tuple[Airport, Airport]]:
        return self._routes

    async def search(
        self,
        origin: Airport,
        dest: Airport,
        date_str: str,
        currency: str = "SGD",
        proxy_url: str | None = None,
        session=None,
    ) -> list[dict] | None:
        self.calls.append((origin, dest, date_str, proxy_url))
        if self.script:
            item = self.script.pop(0)
            if isinstance(item, BaseException):
                raise item
            return item
        return None


class FakeRotator:
    def __init__(self, proxies: list[ProxyInfo] | None = None, working: int = 1):
        self._proxies = list(proxies or [])
        self._working = working
        self.failures: list[ProxyInfo | None] = []
        self.refreshes: list[tuple[bool, int | None]] = []

    async def get_proxy(self) -> ProxyInfo | None:
        return self._proxies[0] if self._proxies else None

    async def report_failure(self, proxy: ProxyInfo | None):
        self.failures.append(proxy)

    def working_count(self) -> int:
        return self._working

    async def refresh(self, force: bool = False, max_per_source: int | None = None):
        self.refreshes.append((force, max_per_source))


class FakeRepo:
    def __init__(self):
        self.upserts: list[dict] = []
        self.failed: list[tuple[str, str]] = []
        self.success_count = 0
        self.failed_count = 0
        self.inserted: list[tuple] = []

    async def open(self):
        pass

    async def flush(self):
        pass

    async def close(self):
        pass

    async def upsert(
        self,
        route: str,
        dep_date: str,
        origin: str,
        destination: str,
        flights: list[dict] | None,
        error_type: str | None,
        retries: int,
        success: bool,
        searched_at: str,
    ):
        self.upserts.append(
            {
                "route": route,
                "dep_date": dep_date,
                "origin": origin,
                "destination": destination,
                "flights": flights,
                "error_type": error_type,
                "retries": retries,
                "success": success,
                "searched_at": searched_at,
            }
        )

    async def insert_ignore_all(self, tasks: list[tuple[str, str, str, str]]):
        self.inserted.extend(tasks)

    async def get_failed(self, max_retries: int = 3) -> list[tuple[str, str]]:
        return list(self.failed)

    async def count_status(self) -> tuple[int, int]:
        return (self.success_count, self.failed_count)

    async def count_by_error(self) -> list[tuple[str, int]]:
        return []

    async def iter_successful_raw(self):
        return
        yield

    async def delete_db(self):
        pass


class FakeResponse:
    def __init__(self, status_code: int = 200, text: str = ""):
        self.status_code = status_code
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(
                f"HTTP {self.status_code}"
            )


class FakeSession:
    def __init__(self, response: FakeResponse | None = None):
        self.headers: dict = {}
        self.closed = False
        self.post = AsyncMock(return_value=response or FakeResponse())

    async def close(self):
        self.closed = True


class FakeCurlSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class FakeSessionFactory:
    def __init__(self):
        self.created: list[FakeSession] = []

    def __call__(self) -> FakeSession:
        session = FakeSession()
        self.created.append(session)
        return session
