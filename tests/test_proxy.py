import asyncio
import time
from unittest.mock import AsyncMock, Mock, patch


from collector.models.proxy import ProxyInfo
from collector.proxy import (
    ProxyRotator,
    _load_cache,
    _normalise_url,
    _parse_all_sources,
    _parse_source,
    _save_cache,
    _validate_proxy,
)

from tests.libs.factories import make_proxy
from tests.libs.fakes import FakeCurlSession


def test_normalise_url_bare_ip_port():
    assert _normalise_url("http", "1.2.3.4:8080") == "http://1.2.3.4:8080"


def test_normalise_url_keeps_scheme():
    assert _normalise_url("http", "socks5://1.2.3.4:1080") == "socks5://1.2.3.4:1080"


def test_normalise_url_rejects_bad_port():
    assert _normalise_url("http", "1.2.3.4:abc") is None


def test_normalise_url_rejects_missing_port():
    assert _normalise_url("http", "1.2.3.4") is None


def test_proxy_roundtrip_dict():
    p = ProxyInfo(url="http://a:1", protocol="http", quality_score=0.8, latency_ms=12.0)
    d = p.to_dict()
    q = ProxyInfo.from_dict(d)
    assert q.url == p.url
    assert q.protocol == p.protocol
    assert q.quality_score == p.quality_score
    assert q.latency_ms == p.latency_ms


def test_proxy_roundtrip_preserves_last_validated():
    p = ProxyInfo(url="http://a:1", protocol="http", last_validated=1234.5)
    q = ProxyInfo.from_dict(p.to_dict())
    assert q.last_validated == 1234.5


async def test_validate_proxy_scores_reflect_latency():
    with patch("collector.proxy._test_http_echo", return_value=100.0):
        fast = await _validate_proxy(make_proxy(url="http://f:1"), None)
    with patch("collector.proxy._test_http_echo", return_value=700.0):
        slow = await _validate_proxy(make_proxy(url="http://s:1"), None)
    with patch("collector.proxy._test_http_echo", return_value=0.0):
        dead = await _validate_proxy(make_proxy(url="http://d:1"), None)

    assert fast.quality_score == 1.3
    assert slow.quality_score == 1.1
    assert dead is None


def _fake_client(body: str = "", status: int = 200) -> AsyncMock:
    client = AsyncMock()
    resp = Mock()
    resp.status_code = status
    resp.text = body
    if status >= 400:
        resp.raise_for_status.side_effect = RuntimeError("http")
    client.get = AsyncMock(return_value=resp)
    return client


async def test_parse_source_mixed_lines():
    body = "1.2.3.4:8080\nsocks5://5.6.7.8:1080\nbad-line\n\n"
    proxies = await _parse_source("http", "https://s", _fake_client(body))
    assert [p.url for p in proxies] == ["http://1.2.3.4:8080", "socks5://5.6.7.8:1080"]
    assert [p.protocol for p in proxies] == ["http", "socks5"]


async def test_parse_source_http_error_returns_empty():
    proxies = await _parse_source("http", "https://s", _fake_client(status=500))
    assert proxies == []


async def test_parse_all_sources_dedups_and_caps():
    a = make_proxy(url="http://a:1")
    b = make_proxy(url="http://b:2")
    c = make_proxy(url="http://c:3")
    with patch(
        "collector.proxy._parse_source",
        side_effect=[[a, b], [b, c], [a]],
    ), patch(
        "collector.proxy._PROXY_SOURCES",
        [("http", "s1"), ("http", "s2"), ("http", "s3")],
    ):
        proxies = await _parse_all_sources(max_per_source=5)
    assert {p.url for p in proxies} == {"http://a:1", "http://b:2", "http://c:3"}


async def test_validate_bounded_workers():
    proxies = [make_proxy(url=f"http://{i}:1") for i in range(5)]
    rot = ProxyRotator(max_concurrent=2)

    async def fake_validate(proxy, session):
        if proxy.url.startswith(("http://0", "http://3")):
            proxy.quality_score = 1.3
            return proxy
        return None

    with (
        patch("collector.proxy._validate_proxy", side_effect=fake_validate),
        patch("collector.proxy.AsyncSession", new=FakeCurlSession),
    ):
        valid = await rot._validate(proxies)

    assert [p.url for p in valid] == ["http://0:1", "http://3:1"]


async def test_save_load_cache_roundtrip(tmp_path):
    path = tmp_path / "cache.json"
    with patch("collector.proxy._PROXY_CACHE_PATH", str(path)):
        p = make_proxy(url="http://a:1", quality_score=1.2, last_validated=42.0)
        _save_cache([p])
        cached_at, proxies = _load_cache()
    assert proxies[0].url == p.url
    assert proxies[0].quality_score == 1.2
    assert proxies[0].last_validated == 42.0
    assert cached_at <= time.time()


async def test_load_cache_missing_file_returns_none(tmp_path):
    with patch("collector.proxy._PROXY_CACHE_PATH", str(tmp_path / "absent.json")):
        assert _load_cache() is None


async def test_rotator_returns_none_when_empty():
    rot = ProxyRotator()
    assert await rot.get_proxy() is None


async def test_rotator_picks_from_pool_and_removes_failure():
    rot = ProxyRotator()
    rot._proxies = [make_proxy(url="http://a:1"), make_proxy(url="http://b:2")]
    seen = set()
    for _ in range(50):
        p = await rot.get_proxy()
        assert p is not None
        seen.add(p.url)
    assert seen == {"http://a:1", "http://b:2"}
    assert rot.working_count() == 2

    await rot.report_failure(rot._proxies[0])
    assert rot.working_count() == 1


async def test_rotator_weights_favor_high_quality():
    rot = ProxyRotator()
    rot._proxies = [
        make_proxy(url="http://fast:1", quality_score=1.3),
        make_proxy(url="http://slow:1", quality_score=0.1),
    ]
    rot._recompute_weights()
    picks = [await rot.get_proxy() for _ in range(200)]
    fast = sum(1 for p in picks if p.url == "http://fast:1")
    assert fast > 150


async def test_get_proxy_empty_triggers_auto_refresh():
    rot = ProxyRotator()
    with patch.object(rot, "_auto_refresh", new=AsyncMock()) as refresh:
        assert await rot.get_proxy() is None
        await asyncio.sleep(0)
        refresh.assert_awaited_once()


async def test_refresh_uses_fresh_cache():
    rot = ProxyRotator()
    cached = [make_proxy(url="http://a:1"), make_proxy(url="http://b:2")]
    with (
        patch("collector.proxy._load_cache", return_value=(time.time() - 60, cached)),
        patch("collector.proxy._parse_all_sources") as fetch,
    ):
        await rot.refresh(force=False)
    assert rot.working_count() == 2
    fetch.assert_not_called()


async def test_refresh_force_skips_cache():
    rot = ProxyRotator()
    with (
        patch("collector.proxy._load_cache") as load,
        patch("collector.proxy._parse_all_sources", return_value=[make_proxy(url="http://x:1")]),
        patch("collector.proxy.ProxyRotator._validate", side_effect=lambda proxies: proxies),
        patch("collector.proxy._save_cache"),
    ):
        await rot.refresh(force=True)
    load.assert_not_called()


async def test_refresh_stale_cache_all_dead_fetches_fresh():
    rot = ProxyRotator()
    stale = [make_proxy(url="http://old:1")]
    fresh = [make_proxy(url="http://new:1")]
    with (
        patch("collector.proxy._load_cache", return_value=(time.time() - 2000, stale)),
        patch("collector.proxy.ProxyRotator._validate", side_effect=[[], fresh]),
        patch("collector.proxy._parse_all_sources", return_value=fresh),
        patch("collector.proxy._save_cache"),
    ):
        await rot.refresh(force=False)
    assert rot.working_count() == 1
    assert rot._proxies[0].url == "http://new:1"
