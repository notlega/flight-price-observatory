"""Route catalog: one-way and round-trip route definitions."""

from dataclasses import dataclass
from typing import ClassVar

from fli.models import Airport


@dataclass(frozen=True)
class Route:
    origin: str
    dest: str


class RouteCatalog:
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
        "PVG",
        "PEK",
        "ICN",
        "PUS",
    )
    ROUND_TRIP_OFFSETS = (7, 14, 21)

    @classmethod
    def one_way_routes(cls) -> list[Route]:
        routes: list[Route] = []
        for dest in cls.DESTINATIONS:
            routes.append(Route(cls.HUB, dest))
            routes.append(Route(dest, cls.HUB))
        return routes

    @classmethod
    def round_trip_routes(cls) -> list[Route]:
        return [Route(cls.HUB, dest) for dest in cls.DESTINATIONS]

    iri_map: ClassVar[dict[str, Airport]] = {
        code: Airport[code] for code in (HUB, *DESTINATIONS)
    }

    @classmethod
    def resolve(cls, code: str) -> Airport:
        return cls.iri_map[code]
