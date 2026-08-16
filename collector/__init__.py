"""Flight Price Observatory collector package."""

from .manager import CollectorManager
from .registry import ProviderRegistry

__all__ = [
    "CollectorManager",
    "ProviderRegistry",
]
