"""Serialised flight result shapes as produced by ``model_dump(mode="json")``."""

from typing import Any, TypedDict


class FlightLegDict(TypedDict, total=False):
    """JSON-serialised form of a fli ``FlightLeg`` (airports as codes)."""

    departure_airport: str
    arrival_airport: str
    duration: int
    departure_airport_name: str
    arrival_airport_name: str


class FlightResultDict(TypedDict, total=False):
    """JSON-serialised form of a fli ``FlightResult``."""

    legs: list[FlightLegDict]
    price: float | None
    currency: str | None
    duration: int
    stops: int
    layovers: list[Any] | None
    co2_emissions_g: int | None
    co2_emissions_typical_g: int | None
    co2_emissions_delta_pct: int | None
    emissions_tag: str | None
    self_transfer: bool | None
    mixed_cabin: bool | None
    primary_airline: Any | None
    primary_airline_name: str | None
    booking_token: str | None


class SearchResultRow(TypedDict):
    """One search_results row; ``flights`` is parsed or the raw JSON string."""

    route: str
    dep_date: str
    return_date: str
    flight_type: str
    origin: str
    destination: str
    flights: list[FlightResultDict] | str
    searched_at: str
