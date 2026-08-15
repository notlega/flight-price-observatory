import pytest

from collector.errors import ProviderConnectionError
from collector.providers.base import BaseProvider


def test_require_proxy_raises_without_url():
    with pytest.raises(ProviderConnectionError):
        BaseProvider._require_proxy(None)


def test_require_proxy_accepts_url():
    assert BaseProvider._require_proxy("http://p:1") == "http://p:1"
