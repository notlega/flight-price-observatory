import asyncio
import logging
import time
from contextlib import suppress
from unittest.mock import AsyncMock, Mock, patch

import pytest
from curl_cffi.requests import exceptions as curl_exceptions

from collector.models.proxy import ProxyInfo
from collector.proxy.blacklist import Blacklist
from collector.proxy.cache import (
    CACHE_FRESH_TTL,
    CACHE_MAX_AGE,
    load_cache,
    save_cache,
)
from collector.proxy.refresh_state import RefreshState
from collector.proxy.rotator import ProxyRotator
from collector.proxy.sources import (
    build_sources,
    normalise_url,
    parse_all_sources,
    parse_source,
)
from collector.proxy.validation import (
    ALIVE_TO_VALID_MULTIPLIER,
    extract_ip,
    prefilter_tcp_until,
    probe_url,
    tcp_alive,
    validate_proxy,
)
from collector.proxy.validation import (
    test_http_echo as echo_probe,
)
from tests.libs.factories import make_proxy
from tests.libs.fakes import FakeCurlSession, FakeResponse


def testnormalise_url_bare_ip_port():
    assert normalise_url("http", "1.2.3.4:8080") == "http://1.2.3.4:8080"


def testnormalise_url_keeps_scheme():
    assert normalise_url("http", "socks5://1.2.3.4:1080") == "socks5://1.2.3.4:1080"


def testnormalise_url_rejects_bad_port():
    assert normalise_url("http", "1.2.3.4:abc") is None


def testnormalise_url_rejects_missing_port():
    assert normalise_url("http", "1.2.3.4") is None


@pytest.mark.parametrize(
    "raw", ["1.2.3.4:0", "1.2.3.4:65536", "1.2.3.4:-1", "1.2.3.4:99999"]
)
def testnormalise_url_rejects_out_of_range_port(raw):
    assert normalise_url("http", raw) is None


def testnormalise_url_rejects_empty_host():
    assert normalise_url("http", ":8080") is None


@pytest.mark.parametrize("port", [1, 65535])
def testnormalise_url_accepts_port_boundaries(port):
    assert normalise_url("http", f"1.2.3.4:{port}") == f"http://1.2.3.4:{port}"


def testnormalise_url_rejects_invalid_protocol_arg():
    assert normalise_url("ftp", "1.2.3.4:80") is None


def testnormalise_url_rejects_slash_only():
    assert normalise_url("http", "http:///") is None


def testnormalise_url_rejects_unclosed_ipv6_bracket():
    assert normalise_url("http", "[::1:8080") is None


def testnormalise_url_rejects_embedded_colon_port():
    assert normalise_url("http", "1.2.3.4:8080:extra") is None


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


async def testvalidate_proxy_scores_reflect_latency():
    with patch("collector.proxy.validation.test_http_echo", return_value=100.0):
        fast = await validate_proxy(make_proxy(url="http://f:1"), AsyncMock())
    with patch("collector.proxy.validation.test_http_echo", return_value=700.0):
        slow = await validate_proxy(make_proxy(url="http://s:1"), AsyncMock())
    with patch("collector.proxy.validation.test_http_echo", return_value=0.0):
        dead = await validate_proxy(make_proxy(url="http://d:1"), AsyncMock())

    assert fast is not None
    assert fast.quality_score == 1.3
    assert slow is not None
    assert slow.quality_score == 1.1
    assert dead is None


def testextract_ip_from_json_origin():
    assert extract_ip('{"origin": "1.2.3.4"}') == "1.2.3.4"


def testextract_ip_from_json_query():
    assert extract_ip('{"query": "5.6.7.8"}') == "5.6.7.8"


def testextract_ip_from_plain_text():
    assert extract_ip("1.2.3.4\n") == "1.2.3.4"


def testextract_ip_rejects_non_ip_body():
    assert extract_ip("<html>attention required</html>") is None


def testextract_ip_rejects_out_of_range_octets():
    assert extract_ip("999.1.1.1") is None


def testextract_ip_splits_comma_joined_origin():
    assert extract_ip('{"origin": "1.2.3.4, 5.6.7.8"}') == "1.2.3.4"


def testextract_ip_json_list_not_dict():
    assert extract_ip('["1.2.3.4"]') == "1.2.3.4"


def testextract_ip_ipv6_via_json():
    assert extract_ip('{"origin": "::1"}') == "::1"


async def testprobe_url_rejects_transparent_proxy():
    session = AsyncMock()
    session.get = AsyncMock(return_value=FakeResponse(200, "9.9.9.9"))
    assert await probe_url("http://a:1", "https://u", session, 5.0, "9.9.9.9") is None


async def testprobe_url_rejects_non_ip_body():
    session = AsyncMock()
    session.get = AsyncMock(return_value=FakeResponse(200, "<html>blocked</html>"))
    assert await probe_url("http://a:1", "https://u", session, 5.0) is None


async def testprobe_url_rejects_non_200():
    session = AsyncMock()
    session.get = AsyncMock(return_value=FakeResponse(403, "1.2.3.4"))
    assert await probe_url("http://a:1", "https://u", session, 5.0) is None


async def testprobe_url_rejects_http_error():
    session = AsyncMock()
    session.get = AsyncMock(side_effect=curl_exceptions.Timeout("timed out"))
    assert await probe_url("http://a:1", "https://u", session, 5.0) is None


async def testprobe_url_returns_latency_on_success():
    session = AsyncMock()
    session.get = AsyncMock(return_value=FakeResponse(200, "1.2.3.4"))
    latency = await probe_url("http://a:1", "https://u", session, 5.0, "9.9.9.9")
    assert latency is not None
    assert latency >= 0


async def testprobe_url_rejects_block_marker():
    session = AsyncMock()
    session.get = AsyncMock(
        return_value=FakeResponse(200, "attention required 1.2.3.4")
    )
    assert await probe_url("http://a:1", "https://u", session, 5.0) is None


@pytest.mark.parametrize(
    "exc",
    [
        curl_exceptions.ProxyError("proxy"),
        curl_exceptions.SSLError("tls"),
        curl_exceptions.ConnectionError("conn"),
        RuntimeError("boom"),
    ],
)
async def testprobe_url_failure_handlers(exc):
    session = AsyncMock()
    session.get = AsyncMock(side_effect=exc)
    assert await probe_url("http://a:1", "https://u", session, 5.0) is None


@pytest.mark.parametrize("status", [407, 429])
async def testprobe_url_rejects_auth_and_rate_limited(status):
    session = AsyncMock()
    session.get = AsyncMock(return_value=FakeResponse(status, "1.2.3.4"))
    assert await probe_url("http://a:1", "https://u", session, 5.0) is None


async def test_echo_uses_median_with_even_probes():
    with patch(
        "collector.proxy.validation.probe_url",
        side_effect=[100.0, 300.0],
    ):
        assert await echo_probe("http://a:1", AsyncMock()) == 300.0


async def test_echo_requires_two_probes():
    with patch(
        "collector.proxy.validation.probe_url",
        side_effect=[100.0, None, None],
    ):
        assert await echo_probe("http://a:1", AsyncMock()) == 0.0


async def test_echo_uses_median_latency():
    with patch(
        "collector.proxy.validation.probe_url",
        side_effect=[100.0, 700.0, 900.0],
    ):
        assert await echo_probe("http://a:1", AsyncMock()) == 700.0


def _fake_client(body: str = "", status: int = 200) -> AsyncMock:
    client = AsyncMock()
    resp = Mock()
    resp.status_code = status
    resp.text = body
    if status >= 400:
        resp.raise_for_status.side_effect = RuntimeError("http")
    client.get = AsyncMock(return_value=resp)
    return client


async def testparse_source_mixed_lines():
    body = "1.2.3.4:8080\nsocks5://5.6.7.8:1080\nbad-line\n\n"
    proxies = await parse_source("http", "https://s", _fake_client(body))
    assert [p.url for p in proxies] == ["http://1.2.3.4:8080", "socks5://5.6.7.8:1080"]
    assert [p.protocol for p in proxies] == ["http", "socks5"]


async def testparse_source_http_error_returns_empty():
    proxies = await parse_source("http", "https://s", _fake_client(status=500))
    assert proxies == []


async def testparse_source_skips_comment_lines():
    body = (
        "# Free proxy list by Databay - https://databay.com/free-proxy-list\n"
        "1.2.3.4:8080\n"
        "# plain comment\n"
        "5.6.7.8:8081\n"
    )
    proxies = await parse_source("http", "https://s", _fake_client(body))
    assert [p.url for p in proxies] == ["http://1.2.3.4:8080", "http://5.6.7.8:8081"]


async def testparse_source_comment_only_returns_empty():
    body = "# header with url https://databay.com/free-proxy-list\n# plain\n"
    proxies = await parse_source("http", "https://s", _fake_client(body))
    assert proxies == []


def testbuild_sources_no_duplicate_urls():
    urls = [url for _, url in build_sources()]
    assert len(urls) == len(set(urls))


def testbuild_sources_entries_well_formed():
    allowed = {"http", "https", "socks4", "socks5"}
    for protocol, url in build_sources():
        assert protocol in allowed
        assert url.startswith(("http://", "https://"))


def testbuild_sources_count():
    assert len(build_sources()) == 15


def testbuild_sources_keeps_only_high_yield_sources():
    urls = [url for _, url in build_sources()]
    assert any("databay.com/api/v1" in u for u in urls)
    assert any("ClearProxy" in u for u in urls)
    assert not any("hproxy" in u for u in urls)
    assert not any("proxifly" in u for u in urls)
    assert not any("openproxylist" in u for u in urls)


def testnormalise_url_bare_ipv6():
    assert normalise_url("http", "[::1]:8080") == "http://[::1]:8080"


async def testparse_source_canonicalises_uppercase_scheme():
    proxies = await parse_source("http", "https://s", _fake_client("HTTP://1.2.3.4:80"))
    assert [(p.url, p.protocol) for p in proxies] == [("http://1.2.3.4:80", "http")]


async def testparse_source_canonicalises_duplicates():
    body = "http://9.9.9.9:9/\n9.9.9.9:9\nHTTP://9.9.9.9:9"
    proxies = await parse_source("http", "https://s", _fake_client(body))
    assert [p.url for p in proxies] == ["http://9.9.9.9:9"] * 3


async def testparse_source_rejects_invalid_scheme():
    proxies = await parse_source("http", "https://s", _fake_client("ftp://1.2.3.4:21"))
    assert proxies == []


async def testparse_all_sources_first_wins_for_same_proxy():
    a = make_proxy(url="http://9.9.9.9:9")
    with (
        patch("collector.proxy.sources.parse_source", side_effect=[[a], [a]]),
        patch(
            "collector.proxy.sources.PROXY_SOURCES",
            [("http", "s1"), ("socks5", "s2")],
        ),
    ):
        proxies = await parse_all_sources()
    assert [(p.url, p.protocol) for p in proxies] == [("http://9.9.9.9:9", "http")]


async def testparse_all_sources_isolates_failed_source():
    a = make_proxy(url="http://a:1")
    with (
        patch(
            "collector.proxy.sources.parse_source",
            side_effect=[[a], RuntimeError("boom")],
        ),
        patch(
            "collector.proxy.sources.PROXY_SOURCES",
            [("http", "s1"), ("http", "s2")],
        ),
    ):
        proxies = await parse_all_sources()
    assert [p.url for p in proxies] == ["http://a:1"]


async def testparse_all_sources_caps_per_source():
    a = make_proxy(url="http://a:1")
    b = make_proxy(url="http://b:2")
    c = make_proxy(url="http://c:3")
    with (
        patch("collector.proxy.sources.parse_source", side_effect=[[a, b, c]]),
        patch(
            "collector.proxy.sources.PROXY_SOURCES",
            [("http", "s1")],
        ),
    ):
        proxies = await parse_all_sources(max_per_source=2)
    assert len(proxies) == 2
    assert {p.url for p in proxies} <= {"http://a:1", "http://b:2", "http://c:3"}


async def testparse_all_sources_dedups_and_caps():
    a = make_proxy(url="http://a:1")
    b = make_proxy(url="http://b:2")
    c = make_proxy(url="http://c:3")
    with (
        patch(
            "collector.proxy.sources.parse_source",
            side_effect=[[a, b], [b, c], [a]],
        ),
        patch(
            "collector.proxy.sources.PROXY_SOURCES",
            [("http", "s1"), ("http", "s2"), ("http", "s3")],
        ),
    ):
        proxies = await parse_all_sources(max_per_source=5)
    assert {p.url for p in proxies} == {"http://a:1", "http://b:2", "http://c:3"}


async def test_validate_uses_per_worker_sessions():
    class SessionFactory:
        def __init__(self):
            self.calls = 0

        def __call__(self):
            self.calls += 1
            return FakeCurlSession()

    proxies = [make_proxy(url=f"http://{i}:1") for i in range(5)]
    rot = ProxyRotator(max_concurrent=2)
    factory = SessionFactory()

    async def fake_validate(proxy, session, real_ip=""):
        return proxy

    with (
        patch("collector.proxy.validation.validate_proxy", side_effect=fake_validate),
        patch("collector.proxy.rotator.AsyncSession", factory),
        patch("collector.proxy.validation.fetch_real_ip", return_value=""),
        patch("collector.proxy.validation.prefilter_tcp", side_effect=lambda ps: ps),
    ):
        await rot._validate(proxies)

    assert factory.calls == 5


async def test_validate_bounded_by_validate_max_concurrent():
    class SessionFactory:
        def __init__(self):
            self.calls = 0

        def __call__(self):
            self.calls += 1
            return FakeCurlSession()

    proxies = [make_proxy(url=f"http://{i}:1") for i in range(20)]
    rot = ProxyRotator(max_concurrent=100)
    factory = SessionFactory()

    async def fake_validate(proxy, session, real_ip=""):
        return proxy

    with (
        patch("collector.proxy.validation.validate_proxy", side_effect=fake_validate),
        patch("collector.proxy.rotator.AsyncSession", factory),
        patch("collector.proxy.validation.fetch_real_ip", return_value=""),
        patch("collector.proxy.validation.prefilter_tcp", side_effect=lambda ps: ps),
        patch("collector.proxy.validation.VALIDATE_MAX_CONCURRENT", 3),
    ):
        await rot._validate(proxies)

    assert factory.calls == 3


async def test_validate_bounded_workers():
    proxies = [make_proxy(url=f"http://{i}:1") for i in range(5)]
    rot = ProxyRotator(max_concurrent=2)

    async def fake_validate(proxy, session, real_ip=""):
        if proxy.url.startswith(("http://0", "http://3")):
            proxy.quality_score = 1.3
            return proxy
        return None

    with (
        patch("collector.proxy.validation.validate_proxy", side_effect=fake_validate),
        patch("collector.proxy.rotator.AsyncSession", new=FakeCurlSession),
        patch("collector.proxy.validation.prefilter_tcp", side_effect=lambda ps: ps),
    ):
        valid = await rot._validate(proxies)

    assert [p.url for p in valid] == ["http://0:1", "http://3:1"]


async def test_validate_early_exit_hits_target():
    proxies = [make_proxy(url=f"http://{i}:1") for i in range(5)]
    rot = ProxyRotator(max_concurrent=2)

    async def fake_validate(proxy, session, real_ip=""):
        proxy.quality_score = 1.3
        return proxy

    with (
        patch("collector.proxy.validation.validate_proxy", side_effect=fake_validate),
        patch("collector.proxy.rotator.AsyncSession", new=FakeCurlSession),
        patch("collector.proxy.validation.prefilter_tcp", side_effect=lambda ps: ps),
    ):
        valid = await rot._validate(proxies, target=2)

    assert len(valid) == 2


async def test_validate_zero_max_concurrent_processes_all():
    proxies = [make_proxy(url=f"http://{i}:1") for i in range(3)]
    rot = ProxyRotator(max_concurrent=0)

    async def fake_validate(proxy, session, real_ip=""):
        proxy.quality_score = 1.3
        return proxy

    with (
        patch("collector.proxy.validation.validate_proxy", side_effect=fake_validate),
        patch("collector.proxy.rotator.AsyncSession", new=FakeCurlSession),
        patch("collector.proxy.validation.prefilter_tcp", side_effect=lambda ps: ps),
    ):
        valid = await rot._validate(proxies)

    assert [p.url for p in valid] == ["http://0:1", "http://1:1", "http://2:1"]


async def test_validate_empty_input_returns_empty():
    rot = ProxyRotator()
    assert await rot._validate([]) == []


async def test_validate_prefilter_kills_all_returns_empty():
    rot = ProxyRotator()
    proxies = [make_proxy(url="http://0:1"), make_proxy(url="http://1:1")]
    with (
        patch("collector.proxy.validation.prefilter_tcp", side_effect=lambda ps: []),
        patch("collector.proxy.validation.validate_proxy"),
    ):
        valid = await rot._validate(proxies)
    assert valid == []


async def test_validate_prefilter_drops_dead_tcp():
    proxies = [make_proxy(url=f"http://{i}:1") for i in range(4)]
    rot = ProxyRotator(max_concurrent=2)

    async def fake_validate(proxy, session, real_ip=""):
        proxy.quality_score = 1.3
        return proxy

    with (
        patch("collector.proxy.validation.validate_proxy", side_effect=fake_validate),
        patch("collector.proxy.rotator.AsyncSession", new=FakeCurlSession),
        patch(
            "collector.proxy.validation.prefilter_tcp",
            side_effect=lambda ps: ps[2:],
        ),
    ):
        valid = await rot._validate(proxies)

    assert [p.url for p in valid] == ["http://2:1", "http://3:1"]


async def testprefilter_tcp_until_stops_at_target():
    seen: list[str] = []

    async def fake(batch):
        seen.extend(p.url for p in batch)
        return batch[:1]

    proxies = [make_proxy(url=f"http://{i}:1") for i in range(5000)]
    with patch("collector.proxy.validation.prefilter_tcp", side_effect=fake):
        alive = await prefilter_tcp_until(proxies, 3)

    assert len(alive) == 3
    assert len(seen) == 1500


async def testprefilter_tcp_until_exhausts_when_target_unreachable():
    proxies = [make_proxy(url=f"http://{i}:1") for i in range(1500)]
    with patch("collector.proxy.validation.prefilter_tcp", side_effect=lambda ps: ps):
        alive = await prefilter_tcp_until(proxies, 5000)

    assert len(alive) == 1500


async def test_validate_prefilters_to_alive_target():
    rot = ProxyRotator()
    proxies = [make_proxy(url=f"http://{i}:1") for i in range(300)]

    with (
        patch(
            "collector.proxy.validation.prefilter_tcp_until",
            new=AsyncMock(return_value=[]),
        ) as prefilter,
        patch("collector.proxy.validation.validate_proxy"),
    ):
        await rot._validate(proxies, target=10)

    prefilter.assert_awaited_once_with(proxies, 10 * ALIVE_TO_VALID_MULTIPLIER)


async def test_validate_prefilter_early_exit_respects_alive_target():
    rot = ProxyRotator()
    proxies = [make_proxy(url=f"http://{i}:1") for i in range(20)]

    async def fake_validate(proxy, session, real_ip=""):
        proxy.quality_score = 1.3
        return proxy

    with (
        patch("collector.proxy.validation.validate_proxy", side_effect=fake_validate),
        patch("collector.proxy.rotator.AsyncSession", new=FakeCurlSession),
        patch("collector.proxy.validation.prefilter_tcp", side_effect=lambda ps: ps),
    ):
        valid = await rot._validate(proxies, target=2)

    assert len(valid) == 2


async def test_validate_logs_source_yield(caplog):
    rot = ProxyRotator(max_concurrent=2)
    proxies = [
        make_proxy(url="http://0:1", source="a.example"),
        make_proxy(url="http://1:1", source="a.example"),
        make_proxy(url="http://2:1", source="b.example"),
    ]

    async def fake_validate(proxy, session, real_ip=""):
        proxy.quality_score = 1.3
        return proxy

    with (
        patch("collector.proxy.validation.validate_proxy", side_effect=fake_validate),
        patch("collector.proxy.rotator.AsyncSession", new=FakeCurlSession),
        patch("collector.proxy.validation.prefilter_tcp", side_effect=lambda ps: ps),
        caplog.at_level(logging.INFO, logger="collector.proxy"),
    ):
        await rot._validate(proxies, target=10)

    assert "Proxy source yield" in caplog.text
    assert "'a.example': (2, 2, 2)" in caplog.text
    assert "'b.example': (1, 1, 1)" in caplog.text


async def testtcp_alive_true_for_reachable():
    writer = Mock()
    writer.close = Mock()
    writer.wait_closed = AsyncMock()
    with patch(
        "collector.proxy.validation.asyncio.open_connection",
        new=AsyncMock(return_value=(Mock(), writer)),
    ) as conn:
        assert await tcp_alive("http://1.2.3.4:8080") is True
    conn.assert_awaited_once_with("1.2.3.4", 8080)


async def testtcp_alive_false_on_error():
    with patch(
        "collector.proxy.validation.asyncio.open_connection",
        new=AsyncMock(side_effect=OSError("refused")),
    ):
        assert await tcp_alive("http://1.2.3.4:8080") is False


async def testtcp_alive_strips_brackets_ipv6():
    writer = Mock()
    writer.close = Mock()
    writer.wait_closed = AsyncMock()
    with patch(
        "collector.proxy.validation.asyncio.open_connection",
        new=AsyncMock(return_value=(Mock(), writer)),
    ) as conn:
        assert await tcp_alive("http://[::1]:8080") is True
    conn.assert_awaited_once_with("::1", 8080)


async def test_saveload_cache_roundtrip(tmp_path):
    path = tmp_path / "cache.json"
    with patch("collector.proxy.cache.PROXY_CACHE_PATH", str(path)):
        p = make_proxy(url="http://a:1", quality_score=1.2, last_validated=42.0)
        save_cache([p])
        loaded = load_cache()
        assert loaded is not None
        cached_at, proxies = loaded
    assert proxies[0].url == p.url
    assert proxies[0].quality_score == 1.2
    assert proxies[0].last_validated == 42.0
    assert cached_at <= time.time()


async def testload_cache_missing_file_returns_none(tmp_path):
    with patch(
        "collector.proxy.cache.PROXY_CACHE_PATH",
        str(tmp_path / "absent.json"),
    ):
        assert load_cache() is None


@pytest.mark.parametrize(
    "payload",
    [
        "{not json",
        '{"proxies": []}',
        '{"cached_at": "soon", "proxies": [{"url": "http://a:1"}]}',
        '{"cached_at": 123, "proxies": "oops"}',
    ],
)
def testload_cache_corrupt_payload_returns_none(tmp_path, payload):
    path = tmp_path / "cache.json"
    path.write_text(payload)
    with patch("collector.proxy.cache.PROXY_CACHE_PATH", str(path)):
        assert load_cache() is None


def testsave_cache_write_failure_propagates(tmp_path):
    d = tmp_path / "ro"
    d.mkdir()
    d.chmod(0o500)
    try:
        with (
            patch("collector.proxy.cache.PROXY_CACHE_PATH", str(d / "cache.json")),
            pytest.raises(OSError),
        ):
            save_cache([make_proxy(url="http://a:1")])
    finally:
        d.chmod(0o700)


async def test_rotator_returns_none_when_empty():
    rot = ProxyRotator()
    with patch.object(rot, "_auto_refresh", new=AsyncMock()):
        assert await rot.get_proxy() is None


async def test_apply_valid_empty_keeps_pool_and_cache():
    rot = ProxyRotator()
    await rot._set_pool([make_proxy(url="http://a:1") for _ in range(3)])
    with patch("collector.proxy.rotator.cache.save_cache") as save:
        await rot._apply_valid([], 10)
    assert rot.working_count() == 3
    save.assert_not_called()


async def test_apply_valid_swaps_pool_and_saves_cache():
    rot = ProxyRotator()
    await rot._set_pool([make_proxy(url="http://a:1")])
    fresh = [make_proxy(url=f"http://n{i}:1") for i in range(2)]
    with patch("collector.proxy.rotator.cache.save_cache") as save:
        await rot._apply_valid(fresh, 10)
    assert rot.working_count() == 2
    save.assert_called_once_with(fresh)


async def test_get_proxy_waits_for_refresh_when_empty():
    rot = ProxyRotator()
    proxy = make_proxy(url="http://a:1")

    async def fake_auto_refresh():
        await rot._set_pool([proxy])

    with patch.object(rot, "_auto_refresh", side_effect=fake_auto_refresh):
        p = await rot.get_proxy()
    assert p is not None
    assert p.url == "http://a:1"


async def test_get_proxy_concurrent_calls_single_refresh():
    rot = ProxyRotator()
    proxy = make_proxy(url="http://a:1")
    started = asyncio.Event()

    async def fake_auto_refresh():
        started.set()
        await asyncio.sleep(0.01)
        await rot._set_pool([proxy])

    with patch.object(rot, "_auto_refresh", side_effect=fake_auto_refresh) as refresh:
        calls = [asyncio.create_task(rot.get_proxy()) for _ in range(10)]
        await started.wait()
        await asyncio.sleep(0.02)
        results = await asyncio.gather(*calls)

    assert refresh.await_count == 1
    assert all(p is not None for p in results)


async def test_get_proxy_timeout_returns_none_without_cancelling_refresh():
    rot = ProxyRotator()
    started = asyncio.Event()

    async def hanging_refresh():
        started.set()
        await asyncio.sleep(3600)

    with patch.object(rot, "_auto_refresh", hanging_refresh):
        proxy = await rot.get_proxy(await_timeout=0.01)
        task = rot._refresh_task
        try:
            assert proxy is None
            assert task is not None
            assert not task.cancelled()
        finally:
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task


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
    picks: list[ProxyInfo] = []
    for _ in range(200):
        p = await rot.get_proxy()
        assert p is not None
        picks.append(p)
    fast = sum(1 for p in picks if p.url == "http://fast:1")
    assert fast > 150


async def test_report_rate_limited_parks_proxy_and_skips_it():
    rot = ProxyRotator()
    rot._proxies = [
        make_proxy(url="http://a:1"),
        make_proxy(url="http://b:2"),
    ]
    victim = rot._proxies[0]
    await rot.report_rate_limited(victim, seconds=60)

    picks: list[ProxyInfo] = []
    for _ in range(30):
        p = await rot.get_proxy()
        assert p is not None
        picks.append(p)
    assert all(p.url != "http://a:1" for p in picks)
    assert rot.working_count() == 2
    assert "http://b:2" in {p.url for p in picks}


async def test_report_rate_limited_evicts_after_threshold():
    rot = ProxyRotator()
    rot._proxies = [make_proxy(url="http://a:1"), make_proxy(url="http://b:2")]
    victim = rot._proxies[0]
    for _ in range(3):
        await rot.report_rate_limited(victim, seconds=60)
    assert rot.working_count() == 1
    assert rot._proxies[0].url == "http://b:2"
    assert "http://a:1" in rot._blacklist.active()


async def test_report_rate_limited_after_eviction_ignored():
    rot = ProxyRotator()
    rot._proxies = [make_proxy(url="http://a:1"), make_proxy(url="http://b:2")]
    victim = rot._proxies[0]
    for _ in range(3):
        await rot.report_rate_limited(victim, seconds=60)
    assert victim not in rot._proxies
    await rot.report_rate_limited(victim, seconds=60)
    assert rot.working_count() == 1


async def test_report_rate_limited_count_roundtrips_cache(tmp_path):
    path = tmp_path / "cache.json"
    with patch("collector.proxy.cache.PROXY_CACHE_PATH", str(path)):
        p = make_proxy(url="http://a:1", rate_limited_count=2)
        save_cache([p])
        loaded = load_cache()
        assert loaded is not None
        assert loaded[1][0].rate_limited_count == 2


async def test_pick_returns_none_when_all_proxies_parked():
    rot = ProxyRotator()
    rot._proxies = [make_proxy(url="http://a:1")]
    await rot.report_rate_limited(rot._proxies[0], seconds=60)
    with patch.object(rot, "_auto_refresh", new=AsyncMock()):
        assert await rot.get_proxy() is None


async def test_parked_proxy_returns_after_cooldown_expires():
    rot = ProxyRotator()
    proxy = make_proxy(url="http://a:1")
    rot._proxies = [proxy]
    await rot.report_rate_limited(proxy, seconds=0.05)
    with patch.object(rot, "_auto_refresh", new=AsyncMock()):
        assert await rot.get_proxy() is None
    await asyncio.sleep(0.1)
    assert await rot.get_proxy() is proxy


async def test_get_proxy_empty_triggers_auto_refresh():
    rot = ProxyRotator()
    with patch.object(rot, "_auto_refresh", new=AsyncMock()) as refresh:
        assert await rot.get_proxy() is None
        await asyncio.sleep(0)
        refresh.assert_awaited_once()


async def test_auto_refresh_prefers_cache():
    rot = ProxyRotator()
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)
        if not kwargs.get("force"):
            await rot._set_pool([make_proxy(url=f"http://p{i}:1") for i in range(20)])

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert calls == [{"max_per_source": 750}]
    assert rot.working_count() == 20


async def test_auto_refresh_falls_back_when_cache_dead():
    rot = ProxyRotator()
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)
        if kwargs.get("force"):
            await rot._set_pool([make_proxy(url="http://b:1")])

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert calls == [
        {"max_per_source": 750},
        {"force": True, "max_per_source": 750},
    ]
    assert rot.working_count() == 1


async def test_auto_refresh_partial_pool_skips_refill():
    rot = ProxyRotator()
    await rot._set_pool([make_proxy(url=f"http://p{i}:1") for i in range(15)])
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)
        if kwargs.get("force"):
            await rot._set_pool([make_proxy(url=f"http://n{i}:1") for i in range(22)])

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert calls == [{"max_per_source": 750}]
    assert rot.working_count() == 15


async def test_auto_refresh_skips_refill_when_pool_ok():
    rot = ProxyRotator()
    await rot._set_pool([make_proxy(url=f"http://p{i}:1") for i in range(25)])
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert calls == [{"max_per_source": 750}]


async def test_auto_refresh_cooldown_blocks_refill():
    rot = ProxyRotator()
    await rot._set_pool([make_proxy(url=f"http://p{i}:1") for i in range(15)])
    rot._refresh_state.last_force_refresh = time.monotonic()
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert calls == [{"max_per_source": 750}]


async def test_auto_refresh_gap_skips_repeated_attempts():
    rot = ProxyRotator()
    await rot._set_pool([make_proxy(url=f"http://p{i}:1") for i in range(20)])
    rot._refresh_state.last_auto_refresh = time.monotonic()
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert calls == []


async def test_auto_refresh_gap_bypassed_when_pool_starved():
    rot = ProxyRotator()
    await rot._set_pool([make_proxy(url="http://p1:1")])
    rot._refresh_state.last_auto_refresh = time.monotonic()
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert calls == [{"max_per_source": 750}]


async def test_auto_refresh_empty_pool_escalates_backoff():
    rot = ProxyRotator()
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        for _ in range(3):
            await rot._auto_refresh()
            rot._refresh_state.last_auto_refresh = float("-inf")
            rot._refresh_state.last_force_refresh = float("-inf")

    assert calls == [
        {"max_per_source": 750},
        {"force": True, "max_per_source": 750},
        {"max_per_source": 750},
        {"force": True, "max_per_source": 750},
        {"max_per_source": 750},
        {"force": True, "max_per_source": 750},
    ]
    assert rot._refresh_state.consecutive_force_refetches == 3


async def test_auto_refresh_backoff_cooldown_blocks_escalated_refetch():
    rot = ProxyRotator()
    rot._refresh_state.last_force_refresh = time.monotonic()
    rot._refresh_state.consecutive_force_refetches = 3
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert calls == [{"max_per_source": 750}]
    assert rot._refresh_state.consecutive_force_refetches == 3


async def test_auto_refresh_resets_backoff_when_pool_recovers():
    rot = ProxyRotator()
    rot._refresh_state.consecutive_force_refetches = 5
    await rot._set_pool([make_proxy(url=f"http://p{i}:1") for i in range(25)])
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert calls == [{"max_per_source": 750}]
    assert rot._refresh_state.consecutive_force_refetches == 0


async def test_auto_refresh_empty_pool_uses_short_cooldown():
    rot = ProxyRotator()
    rot._refresh_state.last_force_refresh = time.monotonic() - 61
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)
        if kwargs.get("force"):
            await rot._set_pool([make_proxy(url="http://n:1")])

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert any(c.get("force") for c in calls)


async def test_auto_refresh_low_pool_keeps_long_cooldown():
    rot = ProxyRotator()
    await rot._set_pool([make_proxy(url=f"http://p{i}:1") for i in range(5)])
    rot._refresh_state.last_force_refresh = time.monotonic() - 61
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert not any(c.get("force") for c in calls)


async def test_auto_refresh_partial_recovery_resets_backoff():
    rot = ProxyRotator()
    rot._refresh_state.consecutive_force_refetches = 5
    await rot._set_pool([make_proxy(url=f"http://p{i}:1") for i in range(5)])
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert calls == [{"max_per_source": 750}]
    assert rot._refresh_state.consecutive_force_refetches == 0


async def test_auto_refresh_backoff_caps_at_longest_interval():
    rot = ProxyRotator()
    rot._refresh_state.last_force_refresh = float("-inf")
    rot._refresh_state.consecutive_force_refetches = 10
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert calls == [
        {"max_per_source": 750},
        {"force": True, "max_per_source": 750},
    ]
    assert rot._refresh_state.consecutive_force_refetches == 11


async def test_auto_refresh_refresh_error_keeps_state():
    rot = ProxyRotator()
    rot._refresh_state.consecutive_force_refetches = 2

    async def boom(**kwargs):
        raise RuntimeError("fetch failed")

    with patch.object(rot, "refresh", side_effect=boom):
        await rot._auto_refresh()

    assert rot._refresh_state.consecutive_force_refetches == 2
    assert rot._refresh_task is None


async def test_auto_refresh_starved_pool_respects_refresh_gap():
    rot = ProxyRotator()
    rot._refresh_state.last_auto_refresh = time.monotonic()
    rot._refresh_state.consecutive_auto_refills = 1
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert calls == []


async def test_auto_refresh_starved_pool_escalates_on_repeated_refills():
    rot = ProxyRotator()
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        for _ in range(3):
            await rot._auto_refresh()
            rot._refresh_state.last_auto_refresh = float("-inf")
            rot._refresh_state.last_force_refresh = float("-inf")

    assert rot._refresh_state.consecutive_auto_refills == 3
    assert rot._refresh_state.consecutive_force_refetches == 3


async def test_auto_refresh_parked_pool_then_revive_resets_backoff():
    rot = ProxyRotator()
    parked = make_proxy(url="http://p1:1")
    parked.rate_limit_until = time.monotonic() + 3600
    await rot._set_pool([parked])
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()
        rot._refresh_state.last_auto_refresh = float("-inf")
        rot._refresh_state.last_force_refresh = float("-inf")
        parked.rate_limit_until = 0.0
        await rot._auto_refresh()

    assert rot._refresh_state.consecutive_force_refetches == 0
    assert any(c.get("force") for c in calls)


async def test_cache_fresh_keeps_larger_live_pool():
    rot = ProxyRotator()
    await rot._set_pool([make_proxy(url=f"http://live{i}:1") for i in range(5)])
    cached = [make_proxy(url="http://cached:1")]
    with (
        patch("collector.proxy.cache.MIN_CACHE_POOL", 0),
        patch(
            "collector.proxy.cache.load_cache",
            return_value=(time.time() - 10, cached),
        ),
        patch("collector.proxy.sources.parse_all_sources") as fetch,
    ):
        await rot.refresh(force=False)
    assert rot.working_count() == 5
    assert all(p.url.startswith("http://live") for p in rot._proxies)
    fetch.assert_not_called()


async def test_cache_fresh_replaces_all_parked_pool():
    rot = ProxyRotator()
    parked = make_proxy(url="http://parked:1")
    parked.rate_limit_until = time.monotonic() + 60
    parked.rate_limited_count = 2
    await rot._set_pool([parked])
    cached = [make_proxy(url="http://cached:1")]
    with (
        patch("collector.proxy.cache.MIN_CACHE_POOL", 0),
        patch(
            "collector.proxy.cache.load_cache",
            return_value=(time.time() - 10, cached),
        ),
        patch("collector.proxy.sources.parse_all_sources") as fetch,
    ):
        await rot.refresh(force=False)
    assert rot.usable_count() == 1
    assert rot._proxies[0].url == "http://cached:1"
    fetch.assert_not_called()


async def test_auto_refresh_all_parked_uses_short_cooldown():
    rot = ProxyRotator()
    parked = make_proxy(url="http://p:1")
    parked.rate_limit_until = time.monotonic() + 60
    await rot._set_pool([parked])
    rot._refresh_state.last_force_refresh = time.monotonic() - 61
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)
        if kwargs.get("force"):
            await rot._set_pool([make_proxy(url="http://n:1")])

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert any(c.get("force") for c in calls)


async def test_auto_refresh_first_call_not_gap_blocked():
    rot = ProxyRotator()
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert calls[0] == {"max_per_source": 750}


async def test_auto_refresh_gap_resets_after_elapsed():
    rot = ProxyRotator()
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()
        first = len(calls)
        rot._refresh_state.last_auto_refresh = time.monotonic() - 10
        await rot._auto_refresh()
        second = len(calls)
    assert second == first + 1


async def test_auto_refresh_at_threshold_no_force():
    rot = ProxyRotator()
    await rot._set_pool([make_proxy(url=f"http://p{i}:1") for i in range(20)])
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        await rot._auto_refresh()

    assert calls == [{"max_per_source": 750}]


async def test_get_proxy_storm_throttled():
    rot = ProxyRotator()
    rot._refresh_state.last_force_refresh = time.monotonic()
    calls = []

    async def fake_refresh(**kwargs):
        calls.append(kwargs)
        await rot._set_pool([make_proxy(url="http://ok:1")])

    with patch.object(rot, "refresh", side_effect=fake_refresh):
        results = await asyncio.gather(*[rot.get_proxy() for _ in range(5)])

    assert len(calls) == 1
    assert all(r is not None for r in results)


async def test_get_proxy_retries_after_refresh_failure():
    rot = ProxyRotator()
    calls = []

    async def flaky(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise RuntimeError("down")
        if kwargs.get("force"):
            await rot._set_pool([make_proxy(url="http://ok:1")])

    with patch.object(rot, "refresh", side_effect=flaky):
        first = await rot.get_proxy()
        rot._refresh_state.last_auto_refresh = time.monotonic() - 10
        second = await rot.get_proxy()

    assert first is None
    assert second is not None
    assert len(calls) == 3


async def test_report_failure_already_removed_no_crash():
    rot = ProxyRotator()
    proxy = make_proxy(url="http://a:1")
    await rot._set_pool([proxy])
    rot._proxies.clear()
    await rot.report_failure(proxy)
    assert proxy.url in rot._blacklist.active()


async def test_refresh_uses_fresh_cache():
    rot = ProxyRotator()
    cached = [make_proxy(url="http://a:1"), make_proxy(url="http://b:2")]
    with (
        patch("collector.proxy.cache.MIN_CACHE_POOL", 0),
        patch(
            "collector.proxy.cache.load_cache",
            return_value=(time.time() - 60, cached),
        ),
        patch("collector.proxy.sources.parse_all_sources") as fetch,
    ):
        await rot.refresh(force=False)
    assert rot.working_count() == 2
    fetch.assert_not_called()


async def test_evicted_proxy_excluded_from_cache_fresh():
    rot = ProxyRotator()
    rot._blacklist.park("http://bad:1", 600)
    cached = [make_proxy(url="http://bad:1"), make_proxy(url="http://good:2")]
    with (
        patch("collector.proxy.cache.MIN_CACHE_POOL", 0),
        patch(
            "collector.proxy.cache.load_cache",
            return_value=(time.time() - 10, cached),
        ),
        patch("collector.proxy.sources.parse_all_sources") as fetch,
    ):
        await rot.refresh(force=False)
    assert [p.url for p in rot._proxies] == ["http://good:2"]
    fetch.assert_not_called()


async def test_blacklisted_proxy_excluded_from_force_refresh():
    rot = ProxyRotator()
    rot._blacklist.park("http://bad:1", 600)
    fresh = [make_proxy(url="http://bad:1"), make_proxy(url="http://good:2")]
    with (
        patch("collector.proxy.sources.parse_all_sources", return_value=fresh),
        patch("collector.proxy.cache.save_cache"),
        patch.object(
            rot,
            "_validate",
            new=AsyncMock(side_effect=lambda proxies, **kw: proxies),
        ),
    ):
        await rot.refresh(force=True)
    assert [p.url for p in rot._proxies] == ["http://good:2"]


async def test_dead_proxy_excluded_from_cache_fresh():
    rot = ProxyRotator()
    await rot._set_pool([make_proxy(url="http://dead:1")])
    await rot.report_failure(rot._proxies[0])
    assert rot.working_count() == 0
    cached = [make_proxy(url="http://dead:1"), make_proxy(url="http://good:2")]
    with (
        patch("collector.proxy.cache.MIN_CACHE_POOL", 0),
        patch(
            "collector.proxy.cache.load_cache",
            return_value=(time.time() - 10, cached),
        ),
        patch("collector.proxy.sources.parse_all_sources") as fetch,
    ):
        await rot.refresh(force=False)
    assert [p.url for p in rot._proxies] == ["http://good:2"]
    fetch.assert_not_called()


async def test_blacklist_expiry_readmits_proxy():
    rot = ProxyRotator()
    rot._blacklist.park("http://bad:1", -1)
    cached = [make_proxy(url="http://bad:1")]
    with (
        patch("collector.proxy.cache.MIN_CACHE_POOL", 0),
        patch(
            "collector.proxy.cache.load_cache",
            return_value=(time.time() - 10, cached),
        ),
        patch("collector.proxy.sources.parse_all_sources") as fetch,
    ):
        await rot.refresh(force=False)
    assert [p.url for p in rot._proxies] == ["http://bad:1"]
    assert rot._blacklist.active() == set()
    fetch.assert_not_called()


async def test_refresh_stale_cache_revalidates_successfully():
    rot = ProxyRotator()
    cached = [make_proxy(url="http://a:1"), make_proxy(url="http://b:2")]
    with (
        patch("collector.proxy.cache.MIN_CACHE_POOL", 0),
        patch(
            "collector.proxy.cache.load_cache",
            return_value=(time.time() - (CACHE_FRESH_TTL + 60), cached),
        ),
        patch.object(
            rot,
            "_validate",
            new=AsyncMock(side_effect=lambda proxies, **kw: proxies),
        ),
        patch("collector.proxy.sources.parse_all_sources") as fetch,
        patch("collector.proxy.cache.save_cache"),
    ):
        await rot.refresh(force=False)
    assert [p.url for p in rot._proxies] == ["http://a:1", "http://b:2"]
    fetch.assert_not_called()


async def test_refresh_all_sources_empty_keeps_existing_pool():
    rot = ProxyRotator()
    existing = [make_proxy(url="http://keep:1")]
    await rot._set_pool(existing)
    with (
        patch("collector.proxy.cache.load_cache", return_value=None),
        patch("collector.proxy.sources.parse_all_sources", return_value=[]),
    ):
        await rot.refresh(force=False)
    assert [p.url for p in rot._proxies] == ["http://keep:1"]
    assert rot.working_count() == 1


async def test_refresh_cache_stale_at_exact_ttl_boundary():
    rot = ProxyRotator()
    cached = [make_proxy(url="http://a:1")]
    with (
        patch("collector.proxy.cache.MIN_CACHE_POOL", 0),
        patch(
            "collector.proxy.cache.load_cache",
            return_value=(time.time() - CACHE_FRESH_TTL, cached),
        ),
        patch.object(
            rot,
            "_validate",
            new=AsyncMock(side_effect=lambda proxies, **kw: proxies),
        ),
        patch("collector.proxy.sources.parse_all_sources") as fetch,
        patch("collector.proxy.cache.save_cache"),
    ):
        await rot.refresh(force=False)
    assert rot.working_count() == 1
    fetch.assert_not_called()


async def test_refresh_cache_expired_at_max_age_fetches_fresh():
    rot = ProxyRotator()
    cached = [make_proxy(url="http://a:1")]
    with (
        patch(
            "collector.proxy.cache.load_cache",
            return_value=(time.time() - CACHE_MAX_AGE, cached),
        ),
        patch.object(
            rot,
            "_validate",
            new=AsyncMock(side_effect=lambda proxies, **kw: proxies),
        ),
        patch("collector.proxy.sources.parse_all_sources", return_value=cached),
        patch("collector.proxy.cache.save_cache"),
    ):
        await rot.refresh(force=False)
    assert rot.working_count() == 1


async def test_refresh_cache_fresh_not_replaced_when_smaller():
    rot = ProxyRotator()
    await rot._set_pool(
        [make_proxy(url="http://big:1"), make_proxy(url="http://big:2")]
    )
    cached = [make_proxy(url="http://small:1")]
    with (
        patch("collector.proxy.cache.MIN_CACHE_POOL", 0),
        patch(
            "collector.proxy.cache.load_cache",
            return_value=(time.time() - 10, cached),
        ),
        patch("collector.proxy.sources.parse_all_sources") as fetch,
    ):
        await rot.refresh(force=False)
    assert [p.url for p in rot._proxies] == ["http://big:1", "http://big:2"]
    fetch.assert_not_called()


async def test_cache_fresh_below_min_pool_fetches_fresh():
    rot = ProxyRotator()
    cached = [make_proxy(url=f"http://c{i}:1") for i in range(10)]
    fresh = [make_proxy(url=f"http://n{i}:1") for i in range(60)]
    with (
        patch(
            "collector.proxy.cache.load_cache",
            return_value=(time.time() - 10, cached),
        ),
        patch("collector.proxy.sources.parse_all_sources", return_value=fresh),
        patch(
            "collector.proxy.ProxyRotator._validate",
            side_effect=lambda ps, target=None: ps,
        ),
        patch("collector.proxy.cache.save_cache"),
    ):
        await rot.refresh(force=False)
    assert rot.working_count() == 60
    assert all(p.url.startswith("http://n") for p in rot._proxies)


async def test_cache_stale_below_min_pool_fetches_fresh():
    rot = ProxyRotator()
    cached = [make_proxy(url=f"http://c{i}:1") for i in range(10)]
    fresh = [make_proxy(url=f"http://n{i}:1") for i in range(60)]
    with (
        patch(
            "collector.proxy.cache.load_cache",
            return_value=(time.time() - (CACHE_FRESH_TTL + 60), cached),
        ),
        patch("collector.proxy.sources.parse_all_sources", return_value=fresh),
        patch(
            "collector.proxy.ProxyRotator._validate",
            side_effect=lambda ps, target=None: ps,
        ),
        patch("collector.proxy.cache.save_cache"),
    ):
        await rot.refresh(force=False)
    assert rot.working_count() == 60
    assert all(p.url.startswith("http://n") for p in rot._proxies)


async def test_cache_stale_above_min_pool_uses_cache():
    rot = ProxyRotator()
    cached = [make_proxy(url=f"http://c{i}:1") for i in range(60)]
    with (
        patch(
            "collector.proxy.cache.load_cache",
            return_value=(time.time() - (CACHE_FRESH_TTL + 60), cached),
        ),
        patch.object(
            rot,
            "_validate",
            new=AsyncMock(side_effect=lambda proxies, **kw: proxies),
        ),
        patch("collector.proxy.sources.parse_all_sources") as fetch,
        patch("collector.proxy.cache.save_cache"),
    ):
        await rot.refresh(force=False)
    assert rot.working_count() == 60
    fetch.assert_not_called()


async def test_report_stub_below_threshold_keeps_proxy():
    rot = ProxyRotator()
    proxy = make_proxy(url="http://s:1")
    await rot._set_pool([proxy])
    await rot.report_stub(proxy)
    assert proxy.stub_count == 1
    assert rot.working_count() == 1
    assert proxy.url not in rot._blacklist.active()


async def test_report_stub_evicts_at_threshold():
    rot = ProxyRotator()
    proxy = make_proxy(url="http://s:1")
    await rot._set_pool([proxy])
    for _ in range(3):
        await rot.report_stub(proxy)
    assert rot.working_count() == 0
    assert proxy.url in rot._blacklist.active()


async def test_report_stub_count_roundtrips_cache(tmp_path):
    path = tmp_path / "cache.json"
    with patch("collector.proxy.cache.PROXY_CACHE_PATH", str(path)):
        p = make_proxy(url="http://a:1", stub_count=2)
        save_cache([p])
        loaded = load_cache()
        assert loaded is not None
        assert loaded[1][0].stub_count == 2


def test_last_force_refresh_init_allows_immediate_refill():
    assert ProxyRotator()._refresh_state.last_force_refresh == float("-inf")


def test_blacklist_tracks_and_prunes_expired_entries():
    bl = Blacklist()
    assert bl.active() == set()
    bl.park("http://a:1", 600)
    assert bl.active() == {"http://a:1"}
    bl.park("http://b:2", -1)
    assert bl.active() == {"http://a:1"}
    assert bl.active() == {"http://a:1"}


def test_refresh_state_gap_blocks_refill_on_healthy_pool():
    state = RefreshState()
    state.last_auto_refresh = time.monotonic()
    assert not state.should_refill(usable=20, refill_threshold=20)


def test_refresh_state_starved_pool_bypasses_gap():
    state = RefreshState()
    state.last_auto_refresh = time.monotonic()
    assert state.should_refill(usable=1, refill_threshold=20)


def test_refresh_state_recovery_resets_counters():
    state = RefreshState()
    state.consecutive_auto_refills = 3
    state.consecutive_force_refetches = 5
    assert not state.refill_result(usable=1)
    assert state.consecutive_auto_refills == 0
    assert state.consecutive_force_refetches == 0


def test_refresh_state_force_refetch_due_on_empty_pool():
    state = RefreshState()
    assert state.refill_result(usable=0)
    assert state.consecutive_auto_refills == 1
    assert state.consecutive_force_refetches == 1


async def test_refresh_force_skips_cache():
    rot = ProxyRotator()
    with (
        patch("collector.proxy.cache.load_cache") as load,
        patch(
            "collector.proxy.sources.parse_all_sources",
            return_value=[make_proxy(url="http://x:1")],
        ),
        patch(
            "collector.proxy.ProxyRotator._validate",
            side_effect=lambda ps, target=None: ps,
        ),
        patch("collector.proxy.cache.save_cache"),
    ):
        await rot.refresh(force=True)
    load.assert_not_called()


async def test_refresh_stale_cache_all_dead_fetches_fresh():
    rot = ProxyRotator()
    stale = [make_proxy(url="http://old:1")]
    fresh = [make_proxy(url="http://new:1")]
    with (
        patch(
            "collector.proxy.cache.load_cache",
            return_value=(time.time() - 2000, stale),
        ),
        patch("collector.proxy.ProxyRotator._validate", side_effect=[[], fresh]),
        patch("collector.proxy.sources.parse_all_sources", return_value=fresh),
        patch("collector.proxy.cache.save_cache"),
    ):
        await rot.refresh(force=False)
    assert rot.working_count() == 1
    assert rot._proxies[0].url == "http://new:1"
