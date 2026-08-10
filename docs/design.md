# Design

## Principles

1. **Provider-agnostic core.** Pipeline never imports a concrete provider. New provider = new class implementing `BaseProvider` + register in `ProviderRegistry`. No pipeline changes.
2. **Async-first.** Everything runs on `asyncio`: bounded concurrent searches, async SQLite, async proxy validation.
3. **External I/O at boundaries.** All network access confined to providers (`curl_cffi`), proxy fetcher, and session handling. Unit tests inject fakes at those seams.
4. **Intermediary before final write.** Raw results land in SQLite first (retry-friendly, durable), then are converted to JSONL. Converted DB is deleted to avoid drift between state and output.
5. **Failure is data.** 429 / timeout / no-proxy / data errors are recorded in SQLite with retry counts, not swallowed. `get_failed()` drives the retry loop; `count_by_error()` gives the failure taxonomy.
6. **Explicit error taxonomy.** `collector/errors.py` maps transport errors to typed exceptions (`ProviderTimeoutError`, `ProviderConnectionError`, `ProviderRateLimitedError`, `ProviderDataError`) so the pipeline can act on them without string matching.

## Retry semantics

- 3 retry rounds.
- Retryable: `ProviderRateLimitedError` (fresh proxies), `ProviderConnectionError`, `no_proxy`.
- `ProviderDataError` marks data bad; not worth retrying.
- Failed routes rejected from retry once retry count >= 3, except 429 which is always re-queued to the next round.

## Testing strategy

- **Isolation:** each test gets a fresh `tmp_path` repo; no shared mutable state.
- **Fakes over mocks:** `tests/libs/fakes.py` provides in-memory doubles (`FakeRepo`, `FakeRotator`, `FakeProvider`, fake sessions) injected post-construction. Mocks only at network boundaries.
- **Factories:** `tests/libs/factories.py` — `make_proxy`, `make_flights` for data setup.
- **Lifecycle:** `conftest.py` owns the opened repo fixture with teardown; tests never open/close manually.
- **Determinism:** `pytest-randomly` shuffles order each run; `filterwarnings=error` fails on leaked resources; `--strict-markers` catches typo'd markers.
- **Gate:** `fail_under = 80` coverage; current 92%.

## Cost control

- Single free tier infra: GitHub Actions cron, Cloudflare R2 (10 GB free).
- Rate limiter guards Google Flights endpoint from 429-spiral; adaptive halving/doubling protects the provider.
- Proxy cache (TTL 30 min) avoids re-fetching 16 sources every run.
