"""Provider error taxonomy and task failure classification."""

from enum import StrEnum


class ProviderError(Exception):
    """Base class for all provider search failures."""


class ProviderRateLimitedError(ProviderError):
    """The provider throttled the request (HTTP 429 or block page)."""


class ProviderTimeoutError(ProviderError):
    """The provider request timed out."""


class ProviderConnectionError(ProviderError):
    """The provider connection failed (network or proxy error)."""


class ProviderDataError(ProviderError):
    """The provider returned unusable or invalid data."""


class ProviderBlockedError(ProviderDataError):
    """The provider served a stub page indicating a blocked request."""


class RepositoryStateError(Exception):
    """Invalid or missing database state for a continue run."""


class ErrorType(StrEnum):
    """Failure classification stored per failed search task."""

    NO_PROXY = "no_proxy"
    RATE_LIMITED = "429"
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    DATA = "data"
    OTHER = "other"
