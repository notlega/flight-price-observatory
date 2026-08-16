"""Runtime patch adding airport codes missing from the fli package.

The patch deliberately reaches into private ``fli`` internals (the Airport
enum member maps and the decoder code lookup), so private-usage checks are
suppressed per site.
"""

from fli.models import Airport
from fli.search import _decoders as _fli_decoders  # type: ignore[reportPrivateUsage]

_EXTRA_AIRPORTS = {
    "YIC": "Yichun Mingyueshan Airport",
}


def _extend_airport_enum(cls: type[Airport], members: dict[str, str]) -> None:
    for name, value in members.items():
        if name in cls._member_map_:  # type: ignore[reportPrivateUsage]
            continue
        member: Airport = object.__new__(cls)
        member._name_ = name  # type: ignore[reportPrivateUsage]
        member._value_ = value  # type: ignore[reportPrivateUsage]
        setattr(cls, name, member)
        cls._member_map_[name] = member  # type: ignore[reportPrivateUsage]
        cls._value2member_map_[value] = member  # type: ignore[reportPrivateUsage]
        cls._member_names_.append(name)  # type: ignore[reportPrivateUsage]


def init() -> None:
    """Apply the Airport patch; idempotent and robust to partial patches."""
    missing = {
        name: value
        for name, value in _EXTRA_AIRPORTS.items()
        if name not in Airport.__members__
    }
    if missing:
        _extend_airport_enum(Airport, missing)
    for code in _EXTRA_AIRPORTS:
        if code in Airport.__members__:
            _fli_decoders._AIRPORT_BY_CODE[code] = Airport[code]  # type: ignore[reportPrivateUsage]
    if "OKA" in Airport.__members__:
        _fli_decoders._AIRPORT_BY_CODE["OKA"] = Airport["OKA"]  # type: ignore[reportPrivateUsage]
