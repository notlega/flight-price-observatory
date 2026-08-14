"""Proxy pool entry model and cache serialisation."""

from dataclasses import dataclass


@dataclass
class ProxyInfo:
    url: str
    protocol: str
    quality_score: float = 1.0
    latency_ms: float = 0.0
    last_validated: float = 0.0
    rate_limit_until: float = 0.0
    rate_limited_count: int = 0
    stub_count: int = 0
    source: str = ""

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "protocol": self.protocol,
            "quality_score": self.quality_score,
            "latency_ms": self.latency_ms,
            "last_validated": self.last_validated,
            "rate_limit_until": self.rate_limit_until,
            "rate_limited_count": self.rate_limited_count,
            "stub_count": self.stub_count,
            "source": self.source,
        }

    @staticmethod
    def from_dict(data: dict) -> "ProxyInfo":
        return ProxyInfo(
            url=data["url"],
            protocol=data["protocol"],
            quality_score=float(data.get("quality_score", 1.0)),
            latency_ms=float(data.get("latency_ms", 0.0)),
            last_validated=float(data.get("last_validated", 0.0)),
            rate_limit_until=float(data.get("rate_limit_until", 0.0)),
            rate_limited_count=int(data.get("rate_limited_count", 0)),
            stub_count=int(data.get("stub_count", 0)),
            source=data.get("source", ""),
        )
