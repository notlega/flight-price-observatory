from collector.models.proxy import ProxyInfo


def make_proxy(
    url: str = "http://a:1",
    protocol: str = "http",
    quality_score: float = 1.0,
    latency_ms: float = 0.0,
    last_validated: float = 0.0,
    rate_limited_count: int = 0,
    stub_count: int = 0,
) -> ProxyInfo:
    return ProxyInfo(
        url=url,
        protocol=protocol,
        quality_score=quality_score,
        latency_ms=latency_ms,
        last_validated=last_validated,
        rate_limited_count=rate_limited_count,
        stub_count=stub_count,
    )


def make_flight(price: int, airline: str = "SQ") -> dict:
    return {"price": price, "airline": airline}


def make_flights(*prices: int) -> list[dict]:
    return [make_flight(p) for p in prices]
