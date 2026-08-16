# Minimal type stub for `fli.search._decoders`.

from fli.models import Airport, FlightResult

_AIRPORT_BY_CODE: dict[str, Airport]


def parse_flight_row(row: list[object]) -> FlightResult: ...
