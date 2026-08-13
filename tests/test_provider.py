from datetime import datetime
from typing import Any, cast

from unittest.mock import patch

import pytest
from curl_cffi import requests
from fli.models import Airport
from fli.search import _decoders as _fli_decoders

from collector._fli_airports import _extend_airport_enum, _patch_fli_airports
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


def _provider(**kwargs) -> GoogleFlightsProvider:
    return GoogleFlightsProvider(**kwargs)


def test_rt_expand_top_n_defaults_to_three():
    assert GoogleFlightsProvider()._rt_expand_top_n == 3


def test_fli_airport_patch_resolves_extra_codes():
    assert _fli_decoders._AIRPORT_BY_CODE["YIC"] is Airport["YIC"]
    assert _fli_decoders._AIRPORT_BY_CODE["OKA"] is Airport["NAH"]
    assert _fli_decoders._parse_airport("YIC") is Airport["YIC"]
    assert _fli_decoders._parse_airport("OKA") is Airport["NAH"]


def test_fli_airport_patch_is_idempotent():
    member = Airport["YIC"]
    _patch_fli_airports()
    assert Airport["YIC"] is member
    assert Airport._member_map_["YIC"] is member


def test_extend_airport_enum_skips_existing_members():
    _extend_airport_enum(Airport, {"SIN": "Changi"})
    assert Airport["SIN"] is Airport["SIN"]


async def _search(session: Any):
    return await _provider().search(
        SIN,
        KUL,
        "2026-12-01",
        currency="SGD",
        proxy_url=PROXY,
        session=session,
    )


async def test_search_past_date_raises_data_error():
    with pytest.raises(ProviderDataError):
        await _provider().search(
            SIN,
            KUL,
            "2000-01-01",
            currency="SGD",
            proxy_url=PROXY,
            session=cast(Any, FakeSession(response=FakeResponse(status_code=200, text=""))),
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
    await_args = session.post.await_args
    assert await_args is not None
    _, kwargs = await_args
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
        await _provider().search(SIN, KUL, "2026-12-01", session=cast(Any, session))


class _FakeLeg:
    def __init__(self, origin, dest, dep):
        self.departure_airport = origin
        self.departure_datetime = dep
        self.arrival_airport = dest
        self.airline = "TEST"
        self.flight_number = "001"


class _FakeFlight:
    def __init__(self, price, duration=60, stops=0, legs=None, token=None):
        self.price = price
        self.duration = duration
        self.stops = stops
        self.legs = legs or []
        self.booking_token = token

    def model_copy(self, update: dict | None = None):
        return _FakeFlight(
            price=(update or {}).get("price", self.price),
            duration=(update or {}).get("duration", self.duration),
            stops=(update or {}).get("stops", self.stops),
            legs=(update or {}).get("legs", self.legs),
            token=(update or {}).get("booking_token", self.booking_token),
        )

    def model_dump(self, mode="json"):
        return {
            "price": self.price,
            "duration": self.duration,
            "stops": self.stops,
            "legs": self.legs,
            "booking_token": self.booking_token,
        }


async def test_search_round_trip_expands_return_leg():
    session = FakeSession(response=FakeResponse(status_code=200, text=""))

    def payload(rows):
        return [[], [], [rows], None]

    def make_flight(price):
        return _FakeFlight(
            price,
            legs=[_FakeLeg(SIN, KUL, datetime(2026, 12, 1))],
            token=f"t{price}",
        )

    initial = [100, 200]
    returns = [30]
    with (
        patch(
            "collector.providers.google_flights.provider.parse_first_wrb_payload",
            side_effect=[
                payload(initial),
                payload(returns),
            ],
        ),
        patch(
            "collector.providers.google_flights.provider.parse_flight_row",
            side_effect=lambda row: make_flight(row),
        ),
    ):
        result = await _provider(rt_expand_top_n=1).search(
            SIN,
            KUL,
            "2026-12-01",
            currency="SGD",
            proxy_url=PROXY,
            session=cast(Any, session),
            return_date="2026-12-08",
        )

    assert result is not None
    assert len(result) == 1
    combo = result[0]
    assert combo["price"] == 130
    assert len(combo["legs"]) == 2
    assert combo["duration"] == 120
    assert session.post.await_count == 2
