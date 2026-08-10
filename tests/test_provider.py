from unittest.mock import patch

import pytest
from curl_cffi import requests
from fli.models import Airport

from collector.errors import (
    ProviderConnectionError,
    ProviderDataError,
    ProviderRateLimitedError,
    ProviderTimeoutError,
)
from collector.providers.google_flights.provider import GoogleFlightsProvider

from tests.libs.fakes import FakeResponse, FakeSession, FakeSessionFactory

SIN = Airport["SIN"]
KUL = Airport["KUL"]
PROXY = "http://p:1"


def _provider() -> GoogleFlightsProvider:
    return GoogleFlightsProvider()


async def _search(session):
    return await _provider().search(
        SIN,
        KUL,
        "2026-12-01",
        currency="SGD",
        proxy_url=PROXY,
        session=session,
    )


@pytest.mark.parametrize(
    "status,expected",
    [
        (429, ProviderRateLimitedError),
        (500, ProviderConnectionError),
        (503, ProviderConnectionError),
        (400, ProviderDataError),
        (404, ProviderDataError),
    ],
)
async def test_search_maps_http_status(status, expected):
    session = FakeSession(response=FakeResponse(status_code=status, text=""))
    with pytest.raises(expected):
        await _search(session)


@pytest.mark.parametrize(
    "exc,expected",
    [
        (requests.exceptions.Timeout("t"), ProviderTimeoutError),
        (requests.exceptions.ProxyError("p"), ProviderConnectionError),
        (requests.exceptions.ConnectionError("c"), ProviderConnectionError),
    ],
)
async def test_search_maps_transport_errors(exc, expected):
    session = FakeSession()
    session.post.side_effect = exc
    with pytest.raises(expected):
        await _search(session)


async def test_search_posts_with_proxy_and_returns_parsed_flights():
    session = FakeSession(response=FakeResponse(status_code=200, text=""))

    class FakeFlight:
        def __init__(self, price):
            self.price = price

        def model_dump(self, mode="json"):
            return {"price": self.price}

    inner = [[], [], [[100, 200]], None]
    with (
        patch(
            "collector.providers.google_flights.provider.parse_first_wrb_payload",
            return_value=inner,
        ),
        patch(
            "collector.providers.google_flights.provider.parse_flight_row",
            side_effect=lambda row: FakeFlight(row),
        ),
    ):
        result = await _search(session)

    assert result == [{"price": 100}, {"price": 200}]
    session.post.assert_awaited_once()
    _, kwargs = session.post.await_args
    assert kwargs["proxies"] == {"all": PROXY}
    assert kwargs["data"].startswith("f.req=")
    assert not session.closed


async def test_search_owned_session_is_closed():
    factory = FakeSessionFactory()
    with (
        patch("collector.providers.google_flights.provider.AsyncSession", factory),
        patch(
            "collector.providers.google_flights.provider.parse_first_wrb_payload",
            return_value=None,
        ),
    ):
        result = await _provider().search(
            SIN, KUL, "2026-12-01", proxy_url=PROXY, session=None
        )

    assert result is None
    assert len(factory.created) == 1
    assert factory.created[0].closed


async def test_search_returns_none_when_payload_empty():
    session = FakeSession(response=FakeResponse(status_code=200, text=""))
    with patch(
        "collector.providers.google_flights.provider.parse_first_wrb_payload",
        return_value=None,
    ):
        assert await _search(session) is None


async def test_search_returns_none_when_no_flights_parsed():
    session = FakeSession(response=FakeResponse(status_code=200, text=""))
    inner = [[], [], [[]], None]
    with (
        patch(
            "collector.providers.google_flights.provider.parse_first_wrb_payload",
            return_value=inner,
        ),
        patch(
            "collector.providers.google_flights.provider.parse_flight_row",
            side_effect=Exception("bad row"),
        ),
    ):
        assert await _search(session) is None


async def test_search_requires_proxy():
    session = FakeSession()
    with pytest.raises(ProviderConnectionError):
        await _provider().search(SIN, KUL, "2026-12-01", session=session)
