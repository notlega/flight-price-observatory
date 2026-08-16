# ADR-0006: Scheduling and storage of the automated collection cycle

Status: Accepted

## Context

The collector runs ~20k searches per full 270-day pass (~2.5-3.5 h wall) and needs
to run unattended on free infrastructure. Requirements:

- Capture both near-horizon (daily) and far-horizon (weekly-ish) price movement
  without burning 20k searches every day.
- Store raw data durably and cheaply, with a path to analytic (Parquet) access.
- Resume from a failed run (`--continue`) with the same-day task state intact.

## Decision

### Schedule: 4-day cycle anchored to 2026-08-19, run at 05:30 SGT

- `cycle_day = (days_since_epoch - anchor_epoch_day) % 4`, anchor = 2026-08-19.
- Day 0: full 270-day pass (~20,325 tasks, ~2.5-3.5 h).
- Day 1: 0-30 days ahead (~2,325 tasks, ~25 min).
- Day 2: 0-60 days ahead (~4,525 tasks, ~45 min).
- Day 3: 0-90 days ahead (~6,725 tasks, ~1 h 5 min).

Rationale: booking-window research shows near-horizon prices move daily while
far-horizon prices move slowly. Daily re-collection of the near window plus a
staged widening of the window each cycle captures both at bounded cost. 05:30 SGT
(21:30 UTC, cron `30 21 * * *`) was chosen so a run never crosses midnight — a
midnight rollover invalidates "today" tasks at build time (see the midnight
caveat in README/design).

### Storage

- **Bronze (raw).** GitHub Releases. One release per cycle
  (`bronze-YYYYMMDD`, tag `cycle-YYYYMMDD`) carrying the gzipped JSONL bundle
  from each of the 4 runs as separate assets
  (`search_YYYYMMDD_053000_{full,w30,w60,w90}.jsonl.gz`). The release is created
  as a draft on day 0 and published on day 3. Releases are retained
  indefinitely; no pruning (risk: unbounded growth, ~23 GB/year at current
  bundle size — accepted; GitHub caps assets at 2 GiB each, 1000 per release,
  with no total-size or bandwidth limit).
- **Silver (processed).** DuckDB transform of JSONL -> Parquet,
  Hive-partitioned by route, uploaded to Cloudflare R2 via boto3. R2's 10 GB
  free Standard tier covers roughly 20 months at ~0.5 GB/month. Parquet remains
  derived data (ADR-0002) — bronze releases stay the source of truth.

### Raw storage

The SQLite state DB is kept indefinitely as operational state (retries,
failures, `--continue`). The raw archive is gzipped JSONL (measured ~4.8%
compression on real output; ~150 MB per full run). SQLite gzips poorly and
Parquet is a derived format, so neither replaces JSONL as the raw archive.

### CLI model

- `search` never auto-converts and always retains the DB; `--keep-db` removed.
- `convert <db>` is the explicit DB -> JSONL export and never deletes the DB.
- `transform --input <jsonl>` is the JSONL -> Parquet (silver) step (new).
- `publish --input <silver>` uploads silver to R2 (new; creds via env
  `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET`).

### CI / runner economics

Repo is public, so GitHub-hosted standard runners are free and unlimited
(4 vCPU/16 GB, 6 h job limit). The collect workflow runs `search -> convert ->
gzip -> release asset -> transform -> publish` with `concurrency: collect`
(cancel-in-progress false) and `timeout-minutes: 240`. The full-pass day is the
only long job (~3.5 h) and fits comfortably. No third-party runner.

## Consequences

**Positive**

- Near-window data refreshes daily; far-window refreshes weekly per cycle.
- Bronze lives on GitHub infra (no egress fees, versioned, retained); silver
  analytics on R2 free tier.
- Failed runs resume via `--continue` (state DB cached per cycle under
  `cycle-<date>-search-state`).

**Negative**

- Release retention is unbounded (~23 GB/year) — a pruning policy may be needed
  later.
- 4-day cadence means far-horizon rows age up to 4 days before refresh.
- `--continue` in CI requires the state cache; a cold re-dispatch starts fresh.

## Alternatives considered

- **Daily full pass** — 4x the searches/cost for marginal far-horizon gain.
- **Daily 0-30d + monthly full** — misses mid-horizon movement for up to a
  month; the staged widening is simpler and fully covered.
- **R2 as bronze (gzip JSONL directly)** — fine, but GitHub Releases is
  zero-extra-infra for raw archival and keeps raw + release metadata together;
  R2 stays the silver tier.
- **Third-party runners (Blacksmith.sh)** — no gain: repo is public and
  standard runners are already free; paid runners only matter on private repos.
