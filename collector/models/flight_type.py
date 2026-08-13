"""Supported flight search types."""

from enum import Enum


class FlightType(str, Enum):
    ONE_WAY = "ONE_WAY"
    ROUND_TRIP = "ROUND_TRIP"
