from collector.models.proxy import ProxyInfo
from tests.libs.factories import make_proxy


def test_proxy_defaults():
    p = make_proxy()
    assert p.quality_score == 1.0
    assert p.latency_ms == 0.0
    assert p.last_validated == 0.0


def test_proxy_to_dict_keys():
    p = make_proxy(
        url="http://a:1", quality_score=0.8, latency_ms=12.0, last_validated=9.0
    )
    assert p.to_dict() == {
        "url": "http://a:1",
        "protocol": "http",
        "quality_score": 0.8,
        "latency_ms": 12.0,
        "last_validated": 9.0,
        "rate_limit_until": 0.0,
        "rate_limited_count": 0,
        "stub_count": 0,
        "source": "",
    }


def test_proxy_from_dict_defaults():
    q = ProxyInfo.from_dict({"url": "http://a:1", "protocol": "http"})
    assert q.quality_score == 1.0
    assert q.latency_ms == 0.0
    assert q.last_validated == 0.0
    assert q.rate_limit_until == 0.0
