# Type stub for `fli.models` — mirrors the actual fli source surface.
# Upstream ships no py.typed, so this stub enables strict checking.
# Airport/Airline are dynamically built enums from CSV data; the members
# listed here are the subset this project references plus the standard
# Enum machinery for `Airport[code]` / iteration.
#
# NOTE: This retrieves strongly typed Pydantic models matching
# fli.models.google_flights.base / .flights / .dates.

from datetime import date, datetime

from enum import Enum
from typing import ClassVar

from pydantic import BaseModel


class Airport(Enum):
    """IATA airport codes (subset referenced by this project)."""

    value: str
    _name_: str
    _value_: str
    _member_map_: ClassVar[dict[str, "Airport"]]
    _value2member_map_: ClassVar[dict[str, "Airport"]]
    _member_names_: ClassVar[list[str]]

    SIN: "Airport"
    KUL: "Airport"
    CGK: "Airport"
    BKK: "Airport"
    HKT: "Airport"
    DPS: "Airport"
    MNL: "Airport"
    SGN: "Airport"
    HAN: "Airport"
    NRT: "Airport"
    KIX: "Airport"
    HND: "Airport"
    PVG: "Airport"
    PEK: "Airport"
    ICN: "Airport"
    PUS: "Airport"
    TPE: "Airport"
    OKA: "Airport"
    NAH: "Airport"
    YIC: "Airport"


class Airline(Enum):
    """IATA airline codes."""

    value: str
    _name_: str
    _value_: str
    _member_map_: ClassVar[dict[str, "Airline"]]
    _value2member_map_: ClassVar[dict[str, "Airline"]]
    _member_names_: ClassVar[list[str]]


class SeatType(Enum):
    ECONOMY: "SeatType"
    PREMIUM_ECONOMY: "SeatType"
    BUSINESS: "SeatType"
    FIRST: "SeatType"


class SortBy(Enum):
    TOP_FLIGHTS: "SortBy"
    BEST: "SortBy"
    CHEAPEST: "SortBy"
    DEPARTURE_TIME: "SortBy"
    ARRIVAL_TIME: "SortBy"
    DURATION: "SortBy"
    EMISSIONS: "SortBy"


class TripType(Enum):
    ROUND_TRIP: "TripType"
    ONE_WAY: "TripType"
    MULTI_CITY: "TripType"


class MaxStops(Enum):
    ANY: "MaxStops"
    NON_STOP: "MaxStops"
    ONE_STOP_OR_FEWER: "MaxStops"
    TWO_OR_FEWER_STOPS: "MaxStops"


class EmissionsFilter(Enum):
    ALL: "EmissionsFilter"
    LESS: "EmissionsFilter"


class Currency(Enum):
    AED: "Currency"
    ARS: "Currency"
    AUD: "Currency"
    BGN: "Currency"
    BRL: "Currency"
    CAD: "Currency"
    CHF: "Currency"
    CLP: "Currency"
    CNY: "Currency"
    COP: "Currency"
    CZK: "Currency"
    DKK: "Currency"
    EGP: "Currency"
    EUR: "Currency"
    GBP: "Currency"
    HKD: "Currency"
    HUF: "Currency"
    IDR: "Currency"
    ILS: "Currency"
    INR: "Currency"
    JPY: "Currency"
    KRW: "Currency"
    MXN: "Currency"
    MYR: "Currency"
    NOK: "Currency"
    NZD: "Currency"
    PEN: "Currency"
    PHP: "Currency"
    PLN: "Currency"
    QAR: "Currency"
    RON: "Currency"
    SAR: "Currency"
    SEK: "Currency"
    SGD: "Currency"
    THB: "Currency"
    TRY: "Currency"
    TWD: "Currency"
    UAH: "Currency"
    USD: "Currency"
    VND: "Currency"
    ZAR: "Currency"


class Alliance(Enum):
    ONEWORLD: "Alliance"
    SKYTEAM: "Alliance"
    STAR_ALLIANCE: "Alliance"


class BagsFilter(BaseModel):
    checked_bags: int = 0
    carry_on: bool = False


class TimeRestrictions(BaseModel):
    earliest_departure: int | None = None
    latest_departure: int | None = None
    earliest_arrival: int | None = None
    latest_arrival: int | None = None


class PassengerInfo(BaseModel):
    adults: int = 1
    children: int = 0
    infants_in_seat: int = 0
    infants_on_lap: int = 0


class PriceLimit(BaseModel):
    max_price: int
    currency: Currency | None = Currency.USD


class LayoverRestrictions(BaseModel):
    airports: list[Airport] | None = None
    min_duration: int | None = None
    max_duration: int | None = None


class Amenities(BaseModel):
    wifi: bool | None = None
    power: bool | None = None
    usb_power: bool | None = None
    in_seat_video: bool | None = None
    on_demand_video: bool | None = None
    legroom_rating: int | None = None


class Layover(BaseModel):
    airport: Airport
    duration: int
    overnight: bool = False
    change_of_airport: bool = False
    city: str | None = None
    airport_name: str | None = None


class FlightLeg(BaseModel):
    airline: Airline
    flight_number: str
    departure_airport: Airport
    arrival_airport: Airport
    departure_datetime: datetime
    arrival_datetime: datetime
    duration: int
    departure_airport_name: str | None = None
    arrival_airport_name: str | None = None
    operating_airline: Airline | None = None
    operating_flight_number: str | None = None
    aircraft: str | None = None
    legroom: str | None = None
    legroom_short: str | None = None
    amenities: Amenities | None = None
    overnight: bool = False
    co2_emissions_g: int | None = None


class BookingOption(BaseModel):
    vendor_code: str | None = None
    vendor_name: str | None = None
    is_airline_direct: bool = False
    price: float | None = None
    currency: str | None = None
    fare_name: str | None = None
    booking_url: str | None = None
    google_click_url: str | None = None
    flights: list[tuple[str, str]] | None = None


class FlightResult(BaseModel):
    legs: list[FlightLeg]
    price: float | None = None
    currency: str | None = None
    duration: int
    stops: int
    layovers: list[Layover] | None = None
    co2_emissions_g: int | None = None
    co2_emissions_typical_g: int | None = None
    co2_emissions_delta_pct: int | None = None
    emissions_tag: str | None = None
    self_transfer: bool | None = None
    mixed_cabin: bool | None = None
    primary_airline: Airline | None = None
    primary_airline_name: str | None = None
    booking_token: str | None = None

    @property
    def price_unknown(self) -> bool: ...


class FlightSegment(BaseModel):
    departure_airport: list[list[Airport | int]]
    arrival_airport: list[list[Airport | int]]
    travel_date: str
    time_restrictions: TimeRestrictions | None = None
    selected_flight: FlightResult | None = None

    @property
    def parsed_travel_date(self) -> datetime: ...


class FlightSearchFilters(BaseModel):
    trip_type: TripType = TripType.ONE_WAY
    passenger_info: PassengerInfo
    flight_segments: list[FlightSegment]
    stops: MaxStops = MaxStops.ANY
    seat_type: SeatType = SeatType.ECONOMY
    price_limit: PriceLimit | None = None
    airlines: list[Airline] | None = None
    airlines_exclude: list[Airline] | None = None
    alliances: list[Alliance] | None = None
    alliances_exclude: list[Alliance] | None = None
    max_duration: int | None = None
    layover_restrictions: LayoverRestrictions | None = None
    sort_by: SortBy = SortBy.BEST
    exclude_basic_economy: bool = False
    emissions: EmissionsFilter = EmissionsFilter.ALL
    bags: BagsFilter | None = None
    show_all_results: bool = True

    def format(self) -> list: ...
    def encode(self) -> str: ...


class DateSearchFilters(BaseModel):
    trip_type: TripType = TripType.ONE_WAY
    passenger_info: PassengerInfo
    flight_segments: list[FlightSegment]
    stops: MaxStops = MaxStops.ANY
    seat_type: SeatType = SeatType.ECONOMY
    price_limit: PriceLimit | None = None
    airlines: list[Airline] | None = None
    airlines_exclude: list[Airline] | None = None
    alliances: list[Alliance] | None = None
    alliances_exclude: list[Alliance] | None = None
    max_duration: int | None = None
    layover_restrictions: LayoverRestrictions | None = None
    emissions: EmissionsFilter = EmissionsFilter.ALL
    bags: BagsFilter | None = None
    from_date: str
    to_date: str
    duration: int | None = None

    @property
    def parsed_from_date(self) -> datetime: ...
    @property
    def parsed_to_date(self) -> datetime: ...
    def format(self) -> list: ...
    def encode(self) -> str: ...
