"""Provider error taxonomy and task failure classification."""

from enum import StrEnum


class ProviderError(Exception):
    pass


class ProviderRateLimitedError(ProviderError):
    pass


class ProviderTimeoutError(ProviderError):
    pass


class ProviderConnectionError(ProviderError):
    pass


class ProviderDataError(ProviderError):
    pass


class ProviderBlockedError(ProviderDataError):
    pass


class ErrorType(StrEnum):
    NO_PROXY = "no_proxy"
    RATE_LIMITED = "429"
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    DATA = "data"
    OTHER = "other"
