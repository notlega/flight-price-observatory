import asyncio
import logging
from copy import deepcopy

from curl_cffi import requests
from curl_cffi.requests import AsyncSession
from fli.models import (
    Airport,
    FlightResult,
    FlightSearchFilters,
    FlightSegment,
    MaxStops,
    PassengerInfo,
    SeatType,
    SortBy,
    TripType,
)
from fli.search._decoders import parse_flight_row
from fli.search._urls import with_locale_params
from fli.search._wire import parse_first_wrb_payload

from collector import _fli_airports  # noqa: F401
from collector.errors import (
    ProviderConnectionError,
    ProviderDataError,
    ProviderRateLimitedError,
    ProviderTimeoutError,
)
from collector.providers.base import BaseProvider
from collector.routes import RouteCatalog

logger = logging.getLogger(__name__)

_SHOPPING_URL = (
    "https://www.google.com/_/FlightsFrontendUi/data/"
    "travel.frontend.flights.FlightsFrontendService/GetShoppingResults"
)

_HEADERS = {
    "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
}

_REQUEST_TIMEOUT = 30


class GoogleFlightsProvider(BaseProvider):
    name = "google_flights"

    def __init__(self, rt_expand_top_n: int = 3):
        self._rt_expand_top_n = rt_expand_top_n

    @property
    def supports(self) -> set[tuple[str, str]]:
        return {(route.origin, route.dest) for route in RouteCatalog.one_way_routes()}

    def _build_filters(
        self,
        origin: Airport,
        dest: Airport,
        date_str: str,
        return_date: str | None,
    ) -> FlightSearchFilters:
        segments = [
            FlightSegment(
                departure_airport=[[origin, 0]],
                arrival_airport=[[dest, 0]],
                travel_date=date_str,
            )
        ]
        trip_type = TripType.ONE_WAY
        if return_date:
            segments.append(
                FlightSegment(
                    departure_airport=[[dest, 0]],
                    arrival_airport=[[origin, 0]],
                    travel_date=return_date,
                )
            )
            trip_type = TripType.ROUND_TRIP

        return FlightSearchFilters(
            passenger_info=PassengerInfo(adults=1),
            flight_segments=segments,
            seat_type=SeatType.ECONOMY,
            stops=MaxStops.ANY,
            sort_by=SortBy.CHEAPEST,
            trip_type=trip_type,
        )

    async def _post_shopping(
        self,
        filters: FlightSearchFilters,
        currency: str,
        proxy_url: str,
        session: AsyncSession,
    ) -> list[FlightResult] | None:
        encoded = filters.encode()
        url = with_locale_params(_SHOPPING_URL, currency, None, None)

        try:
            response = await session.post(
                url,
                data=f"f.req={encoded}",
                impersonate="chrome",
                allow_redirects=True,
                proxies={"all": proxy_url},
                timeout=_REQUEST_TIMEOUT,
            )
        except requests.exceptions.Timeout as e:
            raise ProviderTimeoutError(str(e)) from e
        except requests.exceptions.ProxyError as e:
            raise ProviderConnectionError(str(e)) from e
        except requests.exceptions.ConnectionError as e:
            raise ProviderConnectionError(str(e)) from e

        if response.status_code == 429:
            raise ProviderRateLimitedError(f"HTTP 429 from {url}")
        if response.status_code >= 500:
            raise ProviderConnectionError(f"HTTP {response.status_code} from {url}")
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise ProviderDataError(f"HTTP {response.status_code} from {url}") from e

        inner = parse_first_wrb_payload(response.text)
        if inner is None:
            return None

        try:
            flights_raw = [
                item
                for i in (2, 3)
                if isinstance(inner[i], list)
                for item in inner[i][0]
            ]
        except IndexError, TypeError:
            return None

        flights: list[FlightResult] = []
        for row in flights_raw:
            try:
                flights.append(parse_flight_row(row))
            except Exception:
                continue

        return flights or None

    @staticmethod
    def _merge_round_trip(
        outbound: FlightResult, inbound: FlightResult
    ) -> FlightResult:
        total_price = (outbound.price or 0) + (inbound.price or 0)
        return outbound.model_copy(
            update={
                "legs": outbound.legs + inbound.legs,
                "price": total_price or None,
                "duration": outbound.duration + inbound.duration,
                "stops": outbound.stops + inbound.stops,
                "booking_token": inbound.booking_token or outbound.booking_token,
            }
        )

    async def search(
        self,
        origin: Airport,
        dest: Airport,
        date_str: str,
        currency: str = "SGD",
        proxy_url: str | None = None,
        session: AsyncSession | None = None,
        return_date: str | None = None,
    ) -> list[dict] | None:
        self._require_proxy(proxy_url)

        filters = self._build_filters(origin, dest, date_str, return_date)

        owns_session = session is None
        session = session or AsyncSession()
        try:
            session.headers.update(_HEADERS)
            outbound = await self._post_shopping(filters, currency, proxy_url, session)
            if not outbound:
                return None

            if not return_date:
                return [f.model_dump(mode="json") for f in outbound]

            selected = outbound[: self._rt_expand_top_n]

            async def expand(out: FlightResult) -> list[FlightResult]:
                next_filters = deepcopy(filters)
                next_filters.flight_segments[0].selected_flight = out
                inbound = await self._post_shopping(
                    next_filters, currency, proxy_url, session
                )
                if not inbound:
                    return []
                return [self._merge_round_trip(out, rt) for rt in inbound]

            results = await asyncio.gather(*(expand(o) for o in selected))

            combos = [r for batch in results for r in batch]
            if not combos:
                return None
            return [c.model_dump(mode="json") for c in combos]
        finally:
            if owns_session:
                await session.close()
