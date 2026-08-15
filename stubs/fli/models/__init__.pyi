# Minimal type stub for `fli.models` — covers only symbols used by this
# project. Upstream ships no py.typed; this stub lets strict checking work.

from enum import Enum
from typing import Any, ClassVar

from pydantic import BaseModel


class Airport(Enum):
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
    OKA: "Airport"
    NAH: "Airport"
    YIC: "Airport"


class SeatType(Enum):
    ECONOMY: "SeatType"
    BUSINESS: "SeatType"


class SortBy(Enum):
    BEST: "SortBy"
    CHEAPEST: "SortBy"


class TripType(Enum):
    ROUND_TRIP: "TripType"
    ONE_WAY: "TripType"


class MaxStops(Enum):
    ANY: "MaxStops"


class PassengerInfo(BaseModel):
    adults: int = 1


class FlightLeg(BaseModel):
    departure_airport: Airport
    arrival_airport: Airport
    duration: int
    departure_airport_name: str | None = None
    arrival_airport_name: str | None = None


class FlightSegment(BaseModel):
    departure_airport: list[list[Airport | int]]
    arrival_airport: list[list[Airport | int]]
    travel_date: str
    selected_flight: FlightResult | None = None


class FlightResult(BaseModel):
    legs: list[FlightLeg]
    price: float | None = None
    currency: str | None = None
    duration: int
    stops: int
    layovers: list[Any] | None = None
    co2_emissions_g: int | None = None
    co2_emissions_typical_g: int | None = None
    co2_emissions_delta_pct: int | None = None
    emissions_tag: str | None = None
    self_transfer: bool | None = None
    mixed_cabin: bool | None = None
    primary_airline: Any | None = None
    primary_airline_name: str | None = None
    booking_token: str | None = None


class FlightSearchFilters(BaseModel):
    trip_type: TripType = TripType.ONE_WAY
    passenger_info: PassengerInfo
    flight_segments: list[FlightSegment]
    stops: MaxStops = MaxStops.ANY
    seat_type: SeatType = SeatType.ECONOMY
    price_limit: Any | None = None
    sort_by: SortBy = SortBy.BEST

    def encode(self) -> str: ...
