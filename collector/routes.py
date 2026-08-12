from dataclasses import dataclass
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
    )
    ROUND_TRIP_OFFSETS = (7, 14, 21)

    @classmethod
    def one_way_routes(cls) -> list[Route]:
        routes = []
        for dest in cls.DESTINATIONS:
            routes.append(Route(cls.HUB, dest))
            routes.append(Route(dest, cls.HUB))
        return routes

    @classmethod
    def round_trip_routes(cls) -> list[Route]:
        return [Route(cls.HUB, dest) for dest in cls.DESTINATIONS]

    iri_map = {
        "SIN": Airport["SIN"],
        "KUL": Airport["KUL"],
        "CGK": Airport["CGK"],
        "BKK": Airport["BKK"],
        "HKT": Airport["HKT"],
        "DPS": Airport["DPS"],
        "MNL": Airport["MNL"],
        "SGN": Airport["SGN"],
        "HAN": Airport["HAN"],
        "NRT": Airport["NRT"],
        "KIX": Airport["KIX"],
        "HND": Airport["HND"],
        "PVG": Airport["PVG"],
        "PEK": Airport["PEK"],
    }

    @classmethod
    def resolve(cls, code: str) -> Airport:
        return cls.iri_map[code]
