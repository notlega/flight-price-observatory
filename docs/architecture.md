# Architecture

## Layers

Six layers, one Python codebase.

| Layer | Responsibility | Implementation |
|-------|----------------|----------------|
| Scheduler | Periodic trigger | (planned) GitHub Actions cron |
| Collection | Provider-agnostic search | `BulkSearchPipeline` |
| Validation/transformation | Schema, dedup, normalise | `collector/convert.py` |
| Data lake | Immutable raw tier | (future) R2 bronze |
| Analytics | SQL over Parquet | (future) DuckDB |
| Presentation | Interactive dashboard | (future) Streamlit + Plotly |

## Collection pipeline

Entry point: `python -m cli search` -> `cli/search.py` -> `CollectorManager` -> `BulkSearchPipeline`.

```mermaid
flowchart TD
    CM["CollectorManager"]
    REG["ProviderRegistry.providers<br/>google_flights"]
    PIPE["BulkSearchPipeline"]

    CM --> REG
    CM --> PIPE
    PIPE --> G["GoogleFlightsProvider.search()"]
    G --> ROT["ProxyRotator.get_proxy()"]
    G --> POST["POST Google Flights internal API"]
    POST --> PARSE["parse wrb payload -> flight rows -> dicts"]

    PIPE --> RL["RateLimiter.acquire()<br/>per request"]
    PIPE --> REPO["repository.upsert()<br/>async write queue, WAL"]
    PIPE --> RETRY["_retry_loop()<br/>3 rounds on 429 / no_proxy / data"]
    PIPE --> STORE["_store_result() / _record_failure()"]

    REPO --> STORE
    RETRY --> G
```

### Provider flow

1. `BulkSearchPipeline.run()` enumerates `[start_date, start_date+max_days_ahead]`.
2. Each date is a route task: `origin -> dest` for every provider route.
3. `_run_batch()` fans out tasks with `asyncio.Semaphore(max_concurrent)`, each worker loops over its assigned `(route, date)` pairs.
4. `_attempt_once()`:
   - rate-limit acquire,
   - pick proxy (`_get_provider_map` -> `rotator.get_proxy()`; empty -> `_auto_refresh`),
   - `provider.search(...)`, map exceptions to `AttemptResult`.
5. Success -> `_store_result()` -> `repository.upsert()`. Failure -> `_record_failure()` increments retry counters.
6. `_retry_loop(3)` re-runs failures, distributing them round-robin across providers; failed attempts with retries < 3 are rejected (429 excluded from retry exhaust).
7. `convert()` writes SQLite -> JSONL, then deletes the DB (unless `--keep-db`).

### Rate limiting

Adaptive token bucket (`collector/services/rate_limiter.py`):

- `acquire()` waits until `now - last_request >= 1/rate`.
- `report_429()` halves the rate (min 0.5/s) when a 429 burst exceeds 20% of the expected rate in a 30s window.
- `report_success()` doubles it back, but only after a clean 60s window.

### Proxy rotation (`collector/proxy.py`)

- 2-phase validation: TCP prefilter -> HTTP echo latency (concurrent).
- Quality score = f(latency), proxies weighted by score, dead ones removed.
- Sources fetched from 27 list endpoints; results cached to `storage/proxy_cache.json` (fresh 30 min, revalidated up to 24 h).
- `refresh()` uses cache when fresh; revalidates stale cache up to 24 h; fetches fresh when cache missing, expired, or all cached proxies dead.
- **Eviction & blacklist:** a proxy blacklisted after 3×429 (30 min) or one dead-proxy failure (10 min) is excluded from cache re-population; cache-fresh path only adopts the cached pool if it has more *usable* proxies than the live pool (never clobbers live survivors).
- **Refill:** auto-refresh throttled to one attempt / 5 s. Empty pool (0 usable) force-refetches after 60 s; low-but-nonempty pool after 30 min.
- **Usable vs working:** "usable" excludes rate-limit-parked proxies, so an all-parked pool triggers fast refill instead of blocking on the 30-min cooldown.

## Persistence

- Intermediary: SQLite via `aiosqlite`, WAL journal, async write queue with batching, flush/stop sentinels.
- Final: JSONL in `storage/raw/`, one line per successful route search. Flights embedded as JSON string.

See [data-model.md](data-model.md).

## Design decisions

- ADR-0001 Cloudflare R2 as lake tier
- ADR-0002 Parquet for processed tier
- ADR-0003 provider abstraction
- ADR-0004 proxy reliability hardening

Full records: [docs/decisions/](decisions/)
