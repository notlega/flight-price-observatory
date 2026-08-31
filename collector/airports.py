"""Airport full name → IATA code mapping.

Maps the human-readable airport names returned by Google Flights to
three-letter IATA codes used in the silver tier and dashboard queries.
"""

AIRPORT_NAME_TO_IATA: dict[str, str] = {
    "Singapore Changi International Airport": "SIN",
    "Singapore Changi Airport": "SIN",
    "Kuala Lumpur International Airport": "KUL",
    "Soekarno-Hatta International Airport": "CGK",
    "Suvarnabhumi Airport": "BKK",
    "Phuket International Airport": "HKT",
    "Ngurah Rai International Airport": "DPS",
    "Ngurah Rai (Bali) International Airport": "DPS",
    "Ninoy Aquino International Airport": "MNL",
    "Tan Son Nhat International Airport": "SGN",
    "Noi Bai International Airport": "HAN",
    "Narita International Airport": "NRT",
    "Kansai International Airport": "KIX",
    "Tokyo Haneda Airport": "HND",
    "Tokyo International Airport": "HND",
    "Shanghai Pudong International Airport": "PVG",
    "Beijing Capital International Airport": "PEK",
    "Beijing Daxing International Airport": "PKX",
    "Incheon International Airport": "ICN",
    "Gimhae International Airport": "PUS",
}


def resolve_iata(name: str) -> str | None:
    """Return the IATA code for an airport full name, or None if unknown."""
    return AIRPORT_NAME_TO_IATA.get(name)
