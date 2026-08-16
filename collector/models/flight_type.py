"""Supported flight search types."""

from enum import StrEnum


class FlightType(StrEnum):
    """Supported flight search types."""

    ONE_WAY = "ONE_WAY"
    ROUND_TRIP = "ROUND_TRIP"
