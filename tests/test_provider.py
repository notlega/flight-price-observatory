from datetime import datetime
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest
from curl_cffi import requests
from fli.models import Airport
from fli.search import _decoders as _fli_decoders

from collector._fli_airports import _extend_airport_enum, init
from collector.errors import (
    ProviderBlockedError,
    ProviderConnectionError,
    ProviderDataError,
    ProviderRateLimitedError,
    ProviderTimeoutError,
)
from collector.providers.google_flights.provider import (
    GoogleFlightsProvider,
    _parse_flights,
)
from tests.libs.fakes import FakeResponse, FakeSession, FakeSessionFactory

SIN = Airport["SIN"]
KUL = Airport["KUL"]
PROXY = "http://p:1"


def _provider(**kwargs) -> GoogleFlightsProvider:
    return GoogleFlightsProvider(**kwargs)


def test_rt_expand_top_n_defaults_to_three():
    assert GoogleFlightsProvider()._rt_expand_top_n == 3


def test_fli_airport_patch_resolves_extra_codes():
    init()
    assert _fli_decoders._AIRPORT_BY_CODE["YIC"] is Airport["YIC"]
    assert _fli_decoders._AIRPORT_BY_CODE["OKA"] is Airport["NAH"]
    assert _fli_decoders._parse_airport("YIC") is Airport["YIC"]
    assert _fli_decoders._parse_airport("OKA") is Airport["NAH"]


def test_fli_airport_patch_is_idempotent():
    member = Airport["YIC"]
    init()
    assert Airport["YIC"] is member
    assert Airport._member_map_["YIC"] is member


def test_init_repairs_missing_oka_patch():
    init()
    saved = _fli_decoders._AIRPORT_BY_CODE.get("OKA")
    _fli_decoders._AIRPORT_BY_CODE.pop("OKA", None)
    try:
        init()
        assert _fli_decoders._AIRPORT_BY_CODE["OKA"] is Airport["NAH"]
    finally:
        if saved is not None:
            _fli_decoders._AIRPORT_BY_CODE["OKA"] = saved


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
            session=cast(
                Any, FakeSession(response=FakeResponse(status_code=200, text=""))
            ),
        )


async def test_search_malformed_date_raises_data_error():
    with pytest.raises(ProviderDataError):
        await _provider().search(
            SIN,
            KUL,
            "2026-99-99",
            currency="SGD",
            proxy_url=PROXY,
            session=cast(
                Any, FakeSession(response=FakeResponse(status_code=200, text=""))
            ),
        )


async def test_search_past_return_date_raises_data_error():
    with pytest.raises(ProviderDataError):
        await _provider().search(
            SIN,
            KUL,
            "2026-12-01",
            currency="SGD",
            proxy_url=PROXY,
            return_date="2000-01-01",
            session=cast(
                Any, FakeSession(response=FakeResponse(status_code=200, text=""))
            ),
        )


@pytest.mark.parametrize(
    "status,expected",
    [
        (429, ProviderRateLimitedError),
        (500, ProviderConnectionError),
        (503, ProviderConnectionError),
        (400, ProviderDataError),
        (403, ProviderRateLimitedError),
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


@pytest.mark.parametrize(
    "body", ["captcha", "unusual traffic", "attention required", "access denied"]
)
async def test_search_block_marker_raises_rate_limited(body):
    session = FakeSession(
        response=FakeResponse(status_code=200, text=f"<title>{body}</title>")
    )
    with pytest.raises(ProviderRateLimitedError):
        await _search(session)


async def test_search_uppercase_block_marker_raises_rate_limited():
    session = FakeSession(
        response=FakeResponse(status_code=200, text="<title>UNUSUAL TRAFFIC</title>")
    )
    with pytest.raises(ProviderRateLimitedError):
        await _search(session)


@pytest.mark.parametrize("status", [500, 503])
async def test_search_server_error_with_block_marker_maps_to_connection(status):
    session = FakeSession(
        response=FakeResponse(status_code=status, text="captcha required")
    )
    with pytest.raises(ProviderConnectionError):
        await _search(session)


async def test_search_429_with_block_marker_maps_to_rate_limited():
    session = FakeSession(
        response=FakeResponse(status_code=429, text="captcha required")
    )
    with pytest.raises(ProviderRateLimitedError):
        await _search(session)


async def test_search_403_with_block_marker_maps_to_rate_limited():
    session = FakeSession(
        response=FakeResponse(status_code=403, text="captcha required")
    )
    with pytest.raises(ProviderRateLimitedError):
        await _search(session)


@pytest.mark.parametrize("status", [400, 404])
async def test_search_client_error_with_block_marker_maps_to_data(status):
    session = FakeSession(
        response=FakeResponse(status_code=status, text="access denied")
    )
    with pytest.raises(ProviderDataError):
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
        pytest.raises(ProviderBlockedError),
    ):
        await _provider().search(SIN, KUL, "2026-12-01", proxy_url=PROXY, session=None)

    assert len(factory.created) == 1
    assert factory.created[0].closed


async def test_search_raises_blocked_on_stub_payload():
    session = FakeSession(response=FakeResponse(status_code=200, text="stub"))
    with (
        patch(
            "collector.providers.google_flights.provider.parse_first_wrb_payload",
            return_value=None,
        ),
        pytest.raises(ProviderBlockedError),
    ):
        await _search(session)


async def test_search_returns_none_when_large_body_has_no_payload():
    session = FakeSession(response=FakeResponse(status_code=200, text="x" * 2000))
    with patch(
        "collector.providers.google_flights.provider.parse_first_wrb_payload",
        return_value=None,
    ):
        assert await _search(session) is None


async def test_search_returns_none_when_payload_empty():
    session = FakeSession(response=FakeResponse(status_code=200, text="x" * 2000))
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


@pytest.mark.parametrize(
    "inner",
    [
        [[], []],
        [[], [], [5]],
    ],
)
async def test_search_returns_none_when_malformed_wrb(inner):
    session = FakeSession(response=FakeResponse(status_code=200, text=""))
    with patch(
        "collector.providers.google_flights.provider.parse_first_wrb_payload",
        return_value=inner,
    ):
        assert await _search(session) is None


def test_parse_flights_keeps_valid_rows_drops_bad():
    def fake_parse(row):
        if row == "bad":
            raise ValueError("nope")
        return row

    with patch(
        "collector.providers.google_flights.provider.parse_flight_row",
        side_effect=fake_parse,
    ):
        assert _parse_flights(["good", "bad", "good2"]) == ["good", "good2"]


async def test_search_generic_exception_escapes_as_other():
    session = FakeSession()
    session.post = cast(
        Any,
        AsyncMock(side_effect=requests.exceptions.RequestException("boom")),
    )
    with pytest.raises(requests.exceptions.RequestException):
        await _search(session)


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


async def test_search_round_trip_keeps_successful_expands_on_partial_failure():
    session = FakeSession(response=FakeResponse(status_code=200, text=""))
    session.post.side_effect = [
        FakeResponse(status_code=200, text=""),
        FakeResponse(status_code=200, text=""),
        ProviderRateLimitedError("HTTP 429"),
        FakeResponse(status_code=200, text=""),
    ]

    def payload(rows):
        return [[], [], [rows], None]

    def make_flight(price):
        return _FakeFlight(
            price,
            legs=[_FakeLeg(SIN, KUL, datetime(2026, 12, 1))],
            token=f"t{price}",
        )

    with (
        patch(
            "collector.providers.google_flights.provider.parse_first_wrb_payload",
            side_effect=[
                payload([100, 200, 300]),
                payload([30]),
                payload([30]),
            ],
        ),
        patch(
            "collector.providers.google_flights.provider.parse_flight_row",
            side_effect=lambda row: make_flight(row),
        ),
    ):
        result = await _provider(rt_expand_top_n=3).search(
            SIN,
            KUL,
            "2026-12-01",
            currency="SGD",
            proxy_url=PROXY,
            session=cast(Any, session),
            return_date="2026-12-08",
        )

    assert result is not None
    assert len(result) == 2
    assert {combo["price"] for combo in result} == {130, 330}
    assert session.post.await_count == 4


async def test_search_round_trip_raises_when_all_expands_fail():
    session = FakeSession(response=FakeResponse(status_code=200, text=""))
    session.post.side_effect = [
        FakeResponse(status_code=200, text=""),
        ProviderRateLimitedError("HTTP 429"),
        ProviderRateLimitedError("HTTP 429"),
        ProviderRateLimitedError("HTTP 429"),
    ]

    def payload(rows):
        return [[], [], [rows], None]

    def make_flight(price):
        return _FakeFlight(
            price,
            legs=[_FakeLeg(SIN, KUL, datetime(2026, 12, 1))],
            token=f"t{price}",
        )

    with (
        patch(
            "collector.providers.google_flights.provider.parse_first_wrb_payload",
            side_effect=[
                payload([100, 200, 300]),
                payload([30]),
                payload([30]),
                payload([30]),
            ],
        ),
        patch(
            "collector.providers.google_flights.provider.parse_flight_row",
            side_effect=lambda row: make_flight(row),
        ),
        pytest.raises(ProviderRateLimitedError),
    ):
        await _provider(rt_expand_top_n=3).search(
            SIN,
            KUL,
            "2026-12-01",
            currency="SGD",
            proxy_url=PROXY,
            session=cast(Any, session),
            return_date="2026-12-08",
        )

    assert session.post.await_count == 4


async def test_search_round_trip_raises_when_empty_inbound_and_error_mixed():
    session = FakeSession(response=FakeResponse(status_code=200, text=""))
    session.post.side_effect = [
        FakeResponse(status_code=200, text=""),
        FakeResponse(status_code=200, text=""),
        ProviderRateLimitedError("HTTP 429"),
    ]

    def payload(rows):
        return [[], [], [rows], None]

    def make_flight(price):
        return _FakeFlight(
            price,
            legs=[_FakeLeg(SIN, KUL, datetime(2026, 12, 1))],
            token=f"t{price}",
        )

    with (
        patch(
            "collector.providers.google_flights.provider.parse_first_wrb_payload",
            side_effect=[
                payload([100, 200]),
                None,
                payload([30]),
            ],
        ),
        patch(
            "collector.providers.google_flights.provider.parse_flight_row",
            side_effect=lambda row: make_flight(row),
        ),
        pytest.raises(ProviderBlockedError),
    ):
        await _provider(rt_expand_top_n=2).search(
            SIN,
            KUL,
            "2026-12-01",
            currency="SGD",
            proxy_url=PROXY,
            session=cast(Any, session),
            return_date="2026-12-08",
        )

    assert session.post.await_count == 3


async def test_search_round_trip_returns_none_when_all_expands_empty():
    session = FakeSession(response=FakeResponse(status_code=200, text=""))

    def payload(rows):
        return [[], [], [rows], None]

    def make_flight(price):
        return _FakeFlight(
            price,
            legs=[_FakeLeg(SIN, KUL, datetime(2026, 12, 1))],
            token=f"t{price}",
        )

    with (
        patch(
            "collector.providers.google_flights.provider.parse_first_wrb_payload",
            side_effect=[payload([100, 200]), None, None],
        ),
        patch(
            "collector.providers.google_flights.provider.parse_flight_row",
            side_effect=lambda row: make_flight(row),
        ),
        pytest.raises(ProviderBlockedError),
    ):
        await _provider(rt_expand_top_n=2).search(
            SIN,
            KUL,
            "2026-12-01",
            currency="SGD",
            proxy_url=PROXY,
            session=cast(Any, session),
            return_date="2026-12-08",
        )

    assert session.post.await_count == 3


@pytest.mark.parametrize(
    "error",
    [
        ProviderRateLimitedError("HTTP 429"),
        ProviderTimeoutError("timeout"),
        ProviderConnectionError("conn"),
    ],
)
async def test_search_round_trip_all_expands_fail_raises_same_error(error):
    session = FakeSession(response=FakeResponse(status_code=200, text=""))
    session.post.side_effect = [
        FakeResponse(status_code=200, text=""),
        error,
        error,
    ]

    def payload(rows):
        return [[], [], [rows], None]

    def make_flight(price):
        return _FakeFlight(
            price,
            legs=[_FakeLeg(SIN, KUL, datetime(2026, 12, 1))],
            token=f"t{price}",
        )

    with (
        patch(
            "collector.providers.google_flights.provider.parse_first_wrb_payload",
            side_effect=[payload([100, 200]), payload([30]), payload([30])],
        ),
        patch(
            "collector.providers.google_flights.provider.parse_flight_row",
            side_effect=lambda row: make_flight(row),
        ),
        pytest.raises(type(error)),
    ):
        await _provider(rt_expand_top_n=2).search(
            SIN,
            KUL,
            "2026-12-01",
            currency="SGD",
            proxy_url=PROXY,
            session=cast(Any, session),
            return_date="2026-12-08",
        )

    assert session.post.await_count == 3


async def test_search_round_trip_keeps_single_successful_expand():
    session = FakeSession(response=FakeResponse(status_code=200, text=""))
    session.post.side_effect = [
        FakeResponse(status_code=200, text=""),
        ProviderRateLimitedError("HTTP 429"),
        FakeResponse(status_code=200, text=""),
        ProviderRateLimitedError("HTTP 429"),
    ]

    def payload(rows):
        return [[], [], [rows], None]

    def make_flight(price):
        return _FakeFlight(
            price,
            legs=[_FakeLeg(SIN, KUL, datetime(2026, 12, 1))],
            token=f"t{price}",
        )

    with (
        patch(
            "collector.providers.google_flights.provider.parse_first_wrb_payload",
            side_effect=[payload([100, 200, 300]), payload([30])],
        ),
        patch(
            "collector.providers.google_flights.provider.parse_flight_row",
            side_effect=lambda row: make_flight(row),
        ),
    ):
        result = await _provider(rt_expand_top_n=3).search(
            SIN,
            KUL,
            "2026-12-01",
            currency="SGD",
            proxy_url=PROXY,
            session=cast(Any, session),
            return_date="2026-12-08",
        )

    assert result is not None
    assert [combo["price"] for combo in result] == [230]
    assert session.post.await_count == 4


async def test_search_round_trip_preserves_outbound_order():
    session = FakeSession(response=FakeResponse(status_code=200, text=""))

    def payload(rows):
        return [[], [], [rows], None]

    def make_flight(price):
        return _FakeFlight(
            price,
            legs=[_FakeLeg(SIN, KUL, datetime(2026, 12, 1))],
            token=f"t{price}",
        )

    with (
        patch(
            "collector.providers.google_flights.provider.parse_first_wrb_payload",
            side_effect=[
                payload([300, 100, 200]),
                payload([10]),
                payload([10]),
                payload([10]),
            ],
        ),
        patch(
            "collector.providers.google_flights.provider.parse_flight_row",
            side_effect=lambda row: make_flight(row),
        ),
    ):
        result = await _provider(rt_expand_top_n=3).search(
            SIN,
            KUL,
            "2026-12-01",
            currency="SGD",
            proxy_url=PROXY,
            session=cast(Any, session),
            return_date="2026-12-08",
        )

    assert result is not None
    assert [combo["price"] for combo in result] == [310, 110, 210]


async def test_search_round_trip_zero_expand_top_n_returns_none():
    session = FakeSession(response=FakeResponse(status_code=200, text=""))

    def payload(rows):
        return [[], [], [rows], None]

    with (
        patch(
            "collector.providers.google_flights.provider.parse_first_wrb_payload",
            return_value=payload([100, 200]),
        ),
        patch(
            "collector.providers.google_flights.provider.parse_flight_row",
            return_value=_FakeFlight(
                100,
                legs=[_FakeLeg(SIN, KUL, datetime(2026, 12, 1))],
                token="t100",
            ),
        ),
    ):
        result = await _provider(rt_expand_top_n=0).search(
            SIN,
            KUL,
            "2026-12-01",
            currency="SGD",
            proxy_url=PROXY,
            session=cast(Any, session),
            return_date="2026-12-08",
        )

    assert result is None
    assert session.post.await_count == 1


async def test_search_round_trip_single_expand_empty_returns_none():
    session = FakeSession(response=FakeResponse(status_code=200, text=""))

    def payload(rows):
        return [[], [], [rows], None]

    def make_flight(price):
        return _FakeFlight(
            price,
            legs=[_FakeLeg(SIN, KUL, datetime(2026, 12, 1))],
            token=f"t{price}",
        )

    with (
        patch(
            "collector.providers.google_flights.provider.parse_first_wrb_payload",
            side_effect=[payload([100]), None],
        ),
        patch(
            "collector.providers.google_flights.provider.parse_flight_row",
            side_effect=lambda row: make_flight(row),
        ),
        pytest.raises(ProviderBlockedError),
    ):
        await _provider(rt_expand_top_n=1).search(
            SIN,
            KUL,
            "2026-12-01",
            currency="SGD",
            proxy_url=PROXY,
            session=cast(Any, session),
            return_date="2026-12-08",
        )

    assert session.post.await_count == 2
