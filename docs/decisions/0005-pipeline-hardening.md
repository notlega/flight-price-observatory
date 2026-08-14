# ADR-0005: Pipeline hardening — retries, CLI logging, persistence safety

Status: Accepted

## Context

Reliability passes exposed several correctness issues:

1. **Retry budget ambiguity.** `retries` was per-round, and the "rejected once retries >= 3 except 429" rule was both obscure and unenforceable. Rounds leaked tasks or over-retried.
2. **Flush race.** The writer called `task_done()` *before* executing the queued work, so `flush()` returned before rows were committed. Readers could observe pre-commit state.
3. **Corrupt DB thread leak.** On a failed `aiosqlite.connect()`, aiosqlite (CPython 3.14) leaves its worker thread alive — the connection handle is lost (`_connection is None`), so `__del__` refuses to stop the thread. The interpreter then hangs at shutdown.
4. **Config drift.** `RateLimiter` accepted invalid rates silently; `max_concurrent=0` spawned zero workers.
5. **Scattered logging.** Each subcommand configured its own verbosity; `tqdm` progress bars were unfit for CI logs.

## Decision

- **Cumulative attempt semantics.** `retries` = attempts consumed, 1-based. Success at attempt *n* -> `n`; exhaustion within a round -> `round * 3`. Round *r* selects `retries <= r * 3` (a fresh 3-attempt budget per round). `data` errors are retryable (transient parse hiccups) but never blame the proxy.
- **Flush awaits commit.** Writer calls `task_done()` after executing each item, so `Queue.join()` in `flush()` returns only once queued writes are committed.
- **Fail-fast DB open.** `SearchRepository.open()` runs a synchronous `sqlite3` probe (`SELECT 1`) before starting aiosqlite. Corrupt/unreadable DB raises `sqlite3.DatabaseError` immediately — no worker thread is spawned. Post-connect setup failures close the connection.
- **Validation.** `RateLimiter.__init__` raises `ValueError` for non-positive rates or `min_rate > max_rate`; zero `max_concurrent` clamps to 1 worker.
- **Central CLI logging.** `cli/__main__.py` owns `-v/--verbose`; `httpx` stays at WARNING. Progress reported via periodic log lines (5% steps, rate/ETA), replacing `tqdm`.
- **Dead code removed.** Unreachable refresh-cooldown branch, no-op guard, unused parameter, and the dead `[tool.ruff]` pyproject section (effective line length is 88 via `ruff.toml`).

## Consequences

- `get_failed()` is the single retry entry point with a monotonic budget; no per-error-type exceptions.
- Tests pin boundaries: flush sizes (499/500/501, 999/1000/1001), threshold at 19/20 working proxies, retry escalation `[3, 6, 9]`.
- 288 tests, 97% coverage, `fail_under = 80` gate.
