from collector.registry import ProviderRegistry
from tests.libs.fakes import FakeProvider


def test_providers_returns_copy():
    r = ProviderRegistry()
    providers = r.providers
    providers["bogus"] = FakeProvider
    assert "bogus" not in r.providers


def test_register_and_unregister():
    r = ProviderRegistry()
    r.register("custom", FakeProvider)
    assert r.providers["custom"] is FakeProvider
    r.unregister("custom")
    assert "custom" not in r.providers


def test_unregister_missing_is_noop():
    r = ProviderRegistry()
    r.unregister("never-registered")


def test_builtin_google_flights_registered():
    r = ProviderRegistry()
    assert "google_flights" in r.providers
