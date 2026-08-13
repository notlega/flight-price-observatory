# ADR-0004: Proxy reliability hardening

## Status

Accepted

## Context

Long search runs over a shared proxy pool produced several failure modes:

1. **429 spiral / pool lock-in.** Repeated 429s were only parked (cooldown), never removed. A small bad pool froze the whole run.
2. **Mass blacklist from data errors.** Any error mapped to `OTHER` (including pydantic validation failures for past travel dates at midnight rollover) was reported to the rotator as a proxy failure, blacklisting innocent proxies until the pool emptied.
3. **Endless auto-refresh loop.** With 0 proxies, every `get_proxy()` spawned an auto-refresh that was a cache no-op, spinning at ~15 ms per cycle (4000+ cycles in minutes) while the 30-min refill cooldown blocked real recovery.
4. **Cache re-population defeated eviction.** The cache-fresh path reloaded the disk snapshot wholesale, re-adding evicted and dead proxies every refresh.
5. **Cache-fresh clobbering.** Replacing the live pool with a smaller filtered cache snapshot wiped surviving usable proxies.

## Decision

- **Body/anonymity validation** (v1): proxies validated against the real target; success requires the body to echo a known probe IP (transparent proxies rejected), 2/3 probes pass with median latency.
- **Eviction:** 3×429 evicts a proxy and blacklists it for 30 min. Dead (timeout/connection) proxies blacklisted for 10 min.
- **Blame only real faults:** `Timeout`/`Connection` errors trigger `report_failure`. `OTHER` and `DATA` errors never blame the proxy.
- **Past-date guard:** task build skips `dep_date < today`; retry loop skips past-departure failures; provider maps stale-date validation to `ProviderDataError`.
- **Refill:** auto-refresh throttled to 1 attempt / 5 s. 0 usable → force-refetch after 60 s; low-but-nonempty → after 30 min. "Usable" excludes rate-limit-parked proxies.
- **Cache safety:** blacklisted proxies excluded from cache re-population; cache-fresh path only adopts the filtered pool if larger than the current usable pool.
- **Preflight refill:** `run()` force-refetches once before aborting on zero working proxies.

## Consequences

**Positive**

- Runs complete (94.8% success in a 20k-task run vs. earlier storms).
- Proxy pool self-recovers from total exhaustion in ≤ 60 s instead of stalling or spinning.
- Eviction is durable across cache reloads; dead/evicted proxies stay out for their blacklist TTL.
- Data errors (e.g. midnight rollover) no longer poison the proxy pool.

**Negative**

- Stricter validation (body echo, real target) rejects some working-but-transparent proxies — yield drops (~0.7–1.2% of fresh lists pass).
- All-parked pools now force-refetch sooner — slightly more source fetches.
- Retrying is skipped for past departures; such rows remain recorded as failed.
