from fli.models import Airport
from fli.search import _decoders as _fli_decoders

_EXTRA_AIRPORTS = {
    "YIC": "Yichun Mingyueshan Airport",
}


def _extend_airport_enum(cls: type, members: dict[str, str]) -> None:
    for name, value in members.items():
        if name in cls._member_map_:
            continue
        member = object.__new__(cls)
        member._name_ = name
        member._value_ = value
        setattr(cls, name, member)
        cls._member_map_[name] = member
        cls._value2member_map_[value] = member
        cls._member_names_.append(name)


def _patch_fli_airports() -> None:
    if "YIC" in _fli_decoders._AIRPORT_BY_CODE:
        return
    _extend_airport_enum(Airport, _EXTRA_AIRPORTS)
    for code in _EXTRA_AIRPORTS:
        _fli_decoders._AIRPORT_BY_CODE[code] = Airport[code]
    if "OKA" in Airport.__members__:
        _fli_decoders._AIRPORT_BY_CODE["OKA"] = Airport["OKA"]


_patch_fli_airports()
