import logging

from curl_cffi import requests
from curl_cffi.requests import AsyncSession
from fli.models import (
    Airport,
    FlightSearchFilters,
    FlightSegment,
    MaxStops,
    PassengerInfo,
    SeatType,
    SortBy,
)
from fli.search._decoders import parse_flight_row
from fli.search._urls import with_locale_params
from fli.search._wire import parse_first_wrb_payload

from collector.errors import (
    ProviderConnectionError,
    ProviderDataError,
    ProviderRateLimitedError,
    ProviderTimeoutError,
)
from collector.providers.base import BaseProvider

logger = logging.getLogger(__name__)

ROUTES = [
    (Airport["SIN"], Airport["KUL"]),
    (Airport["SIN"], Airport["CGK"]),
    (Airport["SIN"], Airport["BKK"]),
    (Airport["SIN"], Airport["HKT"]),
    (Airport["SIN"], Airport["DPS"]),
    (Airport["SIN"], Airport["MNL"]),
    (Airport["SIN"], Airport["SGN"]),
    (Airport["SIN"], Airport["HAN"]),
    (Airport["SIN"], Airport["NRT"]),
]

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

    @property
    def routes(self) -> list[tuple[Airport, Airport]]:
        return list(ROUTES)

    async def search(
        self,
        origin: Airport,
        dest: Airport,
        date_str: str,
        currency: str = "SGD",
        proxy_url: str | None = None,
        session: AsyncSession | None = None,
    ) -> list[dict] | None:
        self._require_proxy(proxy_url)

        filters = FlightSearchFilters(
            passenger_info=PassengerInfo(adults=1),
            flight_segments=[
                FlightSegment(
                    departure_airport=[[origin, 0]],
                    arrival_airport=[[dest, 0]],
                    travel_date=date_str,
                )
            ],
            seat_type=SeatType.ECONOMY,
            stops=MaxStops.ANY,
            sort_by=SortBy.CHEAPEST,
        )
        encoded = filters.encode()
        url = with_locale_params(_SHOPPING_URL, currency, None, None)

        owns_session = session is None
        session = session or AsyncSession()
        try:
            session.headers.update(_HEADERS)
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
                raise ProviderConnectionError(
                    f"HTTP {response.status_code} from {url}"
                )
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                raise ProviderDataError(f"HTTP {response.status_code} from {url}") from e
        finally:
            if owns_session:
                await session.close()

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
        except (IndexError, TypeError):
            return None

        flights = []
        for row in flights_raw:
            try:
                flights.append(parse_flight_row(row))
            except Exception:
                continue

        if not flights:
            return None

        return [f.model_dump(mode="json") for f in flights]
