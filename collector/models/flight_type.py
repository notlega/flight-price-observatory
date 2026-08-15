"""Supported flight search types."""

from enum import StrEnum


class FlightType(StrEnum):
    ONE_WAY = "ONE_WAY"
    ROUND_TRIP = "ROUND_TRIP"
