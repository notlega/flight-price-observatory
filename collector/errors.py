"""Provider error taxonomy and task failure classification."""


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


class ErrorType:
    NO_PROXY = "no_proxy"
    RATE_LIMITED = "429"
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    DATA = "data"
    OTHER = "other"

