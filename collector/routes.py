"""Route catalog: one-way and round-trip route definitions."""

from dataclasses import dataclass
from typing import ClassVar

from fli.models import Airport


@dataclass(frozen=True)
class Route:
    """One origin-destination pair in the catalog."""

    origin: str
    dest: str


class RouteCatalog:
    """Static catalog of hub/destination codes and route builders."""

    HUB = "SIN"
    DESTINATIONS = (
        "KUL",
        "CGK",
        "BKK",
        "HKT",
        "DPS",
        "MNL",
        "SGN",
        "HAN",
        "NRT",
        "KIX",
        "HND",
        "TPE",
        "PVG",
        "PEK",
        "ICN",
        "PUS",
    )
    ROUND_TRIP_OFFSETS = (7, 14, 21)

    @classmethod
    def one_way_routes(cls) -> list[Route]:
        """Return all hub-to-destination routes in both directions."""
        routes: list[Route] = []
        for dest in cls.DESTINATIONS:
            routes.append(Route(cls.HUB, dest))
            routes.append(Route(dest, cls.HUB))
        return routes

    @classmethod
    def round_trip_routes(cls) -> list[Route]:
        """Return outbound-only hub routes for round-trip searches."""
        return [Route(cls.HUB, dest) for dest in cls.DESTINATIONS]

    iri_map: ClassVar[dict[str, Airport]] = {
        code: Airport[code] for code in (HUB, *DESTINATIONS)
    }

    @classmethod
    def resolve(cls, code: str) -> Airport:
        """Return the fli Airport enum member for ``code``."""
        return cls.iri_map[code]
