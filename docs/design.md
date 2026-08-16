# Design

## Principles

1. **Provider-agnostic core.** Pipeline never imports a concrete provider. New provider = new class implementing `BaseProvider` + register in `ProviderRegistry`. No pipeline changes.
2. **Async-first.** Everything runs on `asyncio`: bounded concurrent searches, async SQLite, async proxy validation.
3. **External I/O at boundaries.** All network access confined to providers (`curl_cffi`), proxy fetcher, and session handling. Unit tests inject fakes at those seams.
4. **Intermediary before final write.** Raw results land in SQLite first (retry-friendly, durable), then are exported to JSONL on demand via an explicit `cli convert`. The DB is always retained — it holds retry counts and failure state, and `--continue` runs read from it; JSONL is the raw archive, never the mutable state.
5. **Failure is data.** 429 / timeout / no-proxy / data errors are recorded in SQLite with retry counts, not swallowed. `get_failed()` drives the retry loop; `count_by_error()` gives the failure taxonomy.
6. **Explicit error taxonomy.** `collector/errors.py` maps transport errors to typed exceptions (`ProviderTimeoutError`, `ProviderConnectionError`, `ProviderRateLimitedError`, `ProviderDataError`) so the pipeline can act on them without string matching.

## Code structure

- **`SearchTask`** (`collector/services/search_pipeline.py`) is a `@dataclass(slots=True, kw_only=True)` describing one provider + route + date combination; it flows through building, batching, and retries instead of 6-arg tuple unpacking.
- **`SeedRow`** (`collector/repository.py`) is a `NamedTuple` for DB seeding writes.
- **`repository.upsert()`** is keyword-only — 11 positional args would be unreadable at call sites.
- **Shared defaults** live in `collector/config.py` (currency, horizon, rate, workers, DB path) and are imported by the CLI, manager, and pipeline.
- **`ProgressLogger`** (`collector/services/progress.py`) is the single percent-step log implementation used by both the pipeline batch loop and proxy validation.
- **Worker isolation:** batch workers run under `asyncio.gather(..., return_exceptions=True)`; a crashing worker is logged without abandoning remaining tasks.

## Retry semantics

- 3 retry rounds.
- Retryable: `ProviderRateLimitedError` (fresh proxies), `ProviderTimeoutError`, `ProviderConnectionError`, `ProviderDataError`, `no_proxy` (see `_RETRY_ERROR_TYPES`). Data errors may be transient (e.g. parse hiccups) so they are retried, but they never trigger proxy `report_failure`.
- `ProviderDataError` marks data bad; only `Timeout`/`Connection` errors blame (remove + blacklist) the proxy.
- **Stub backoff:** a `ProviderBlockedError` (Google stub shell, status 200) sleeps `_STUB_BACKOFF_S` before the next in-flight attempt, letting a throttled proxy IP cool instead of hammering the pool during degraded windows.
- **No query-level stub cooldown:** a run-persistent per-query cooldown was tried (threshold 4/6, skip rest of run) and reverted — it killed recoverable queries whenever the pool degraded: 603/21515 (2.8%) failed in the 330-day run vs 29 real failures (0.13%). Backoff + retry rounds already cap harm; a throttled query gets a fresh 3-attempt budget each round, and the upsert overwrite means only the final round's result persists.
- **Round-1 forced refresh:** the first retry round always runs `refresh(force=True)` before processing. Retry queries failed on a first-pass pool Google has throttled, and new IPs are the recovery lever — measured: pool 3 -> 62 recovered 226/240 in round 3 vs 0/240 in rounds 1-2 on the stale pool. Round 2+ falls back to the low-pool threshold (`_MIN_POOL_BEFORE_RETRY`).
- **Mid-round collapse refresh:** during retry batches, if the working pool drops below `_MIN_POOL_BEFORE_RETRY` mid-batch, a single-flight `refresh(force=True)` runs (one worker takes the refresh; the rest keep draining on whatever pool remains). Prevents NO_PROXY floods on a pool that dies between round-start checks.
- Past-departure failures are skipped entirely — once `dep_date < today` no amount of retries can succeed. Build-time already skips past dates; retry skips them too.
- `retries` stores **cumulative attempts consumed** (1-based). A success at attempt *n* records `retries = n`; a failure after all 3 in-flight attempts records `retries = round * 3`. Round *r* re-queries failures with `retries <= r * 3`, so each round grants a fresh 3-attempt budget (`get_failed(max_retries=r * 3)`).
- Failed routes not covered by any provider are skipped with a WARNING (route is either removed from the catalog or the provider's `supports` map changed).

## Midnight rollover contract

- A run that starts before midnight builds tasks for "today". At 00:00 those dates become invalid: the provider maps the validation failure to `ProviderDataError` (no proxy blame, no blacklist churn) and the failure is recorded as `DATA`.
- Any rebuild after rollover emits only future dates (both one-way and round-trip) — past dates are invalid and cannot be searched.
- Rationale: correctness over completeness. Losing up to a day of "today" tasks at the boundary is cheaper than blacklisting healthy proxies.

## Testing strategy

- **Isolation:** each test gets a fresh `tmp_path` repo; no shared mutable state.
- **Fakes over mocks:** `tests/libs/fakes.py` provides in-memory doubles (`FakeRepo`, `FakeRotator`, `FakeProvider`, fake sessions) injected post-construction. Mocks only at network boundaries.
- **Factories:** `tests/libs/factories.py` — `make_proxy`, `make_flights` for data setup.
- **Lifecycle:** `conftest.py` owns the opened repo fixture with teardown; tests never open/close manually.
- **Determinism:** `pytest-randomly` shuffles order each run; `filterwarnings=error` fails on leaked resources; `--strict-markers` catches typo'd markers.
- **Gate:** `fail_under = 80` coverage; current 97% (338 tests).

## Cost control

- Single free tier infra: GitHub Actions cron, Cloudflare R2 (10 GB free).
- Rate limiter guards Google Flights endpoint from 429-spiral; adaptive halving/doubling protects the provider.
- Proxy cache (fresh 30 min) avoids re-fetching 15 high-yield sources every run. Cache below `_MIN_CACHE_POOL` (50 working proxies) is ignored and a fresh fetch runs — a tiny cached pool cannot starve the whole search.
- **Stub eviction:** a 200 response whose body is a small block shell (`len < _STUB_MAX_LEN`, no `wrb.fr` payload) raises `ProviderBlockedError`; the pipeline blames the proxy (`report_stub`), evicting it after 3 stubs instead of silently recording DATA and letting the whole pool burn.
- **Seed hygiene:** at run start `purge_abandoned_seeds()` deletes `success = 0` / NULL-error placeholder rows left by interrupted runs, so failure breakdowns reflect real errors, not zombie seeds.
- **Streaming prefilter:** `_prefilter_tcp_until` probes candidates in batches of `_TCP_FILTER_LIMIT` and stops once `_ALIVE_TO_VALID_MULTIPLIER` × validation target survive, instead of TCP-scanning all ~10-40k candidates. `_build_sources` orders checked/clean/google-targeted lists first so early batches are highest yield and the early-exit triggers soonest. Only 15 high-yield sources are configured (pruned from 64 via the validation yield log: the rest passed TCP yet yielded ~0 valid, wasting prefilter + echo work).
- **Validation concurrency:** echo validation caps workers at `_VALIDATE_MAX_CONCURRENT` (50), each owning its own `AsyncSession`. Higher concurrency (the old `max_concurrent` default 200) floods ~600 concurrent echo probes and roughly halves the valid proxy count (measured: 28 vs 54 on the same alive set). `_ALIVE_TO_VALID_MULTIPLIER` (30) sizes the prefilter target from measured TCP-alive rates (~7%) against ~2% echo-pass of alive.
- **Per-source caps:** `max_per_source` (default 2500) caps the initial fetch, `_REFILL_MAX_PER_SOURCE` (750) mid-run refills. Raise both to lean on the streaming prefilter; the caps bound candidate volume, not pool depth.
- **Validation yield log:** every validation cycle logs `Proxy source yield (candidates/alive/valid)` per source (INFO), the data source for pruning near-zero-yield proxy lists.
