# Flight Price Observatory

Auto-collect, store, analyse historical airfare data. SIN -> 13 Asian destinations. Build longitudinal dataset for trend analysis + ML.

Unlike normal flight search (show current price), this snapshots prices over time -> spot patterns across booking windows, seasons, airlines, routes.

## Table of Contents

- [Objectives](#objectives)
- [System Architecture](#system-architecture)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Installation](#installation)
- [Running Locally](#running-locally)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

## Objectives

**Build historical airfare dataset.** Automated pipeline, periodic snapshots, growing dataset for long-term analysis.

**Scalable pipeline.** Modular ingestion + validation + storage. Modern data engineering.

**Cloud data lake.** Raw + processed datasets in private R2 bucket. Immutable, queryable, versioned.

**Exploratory analysis.** Price trends, seasonal patterns, airline comparison, booking window behaviour.

**Predictive analytics.** Prep for ML: price forecasting, anomaly detection, booking recommendations.

**Extensible design.** Provider-agnostic. Add routes, providers, analytics without rewiring core.

## System Architecture

Six layers:

**Scheduler.** (Planned) GitHub Actions cron trigger — no manual intervention.

**Data collection.** Provider abstraction layer. Each provider implements `BaseProvider` interface. Currently: `GoogleFlightsProvider` (SIN->KUL/CGK/BKK/HKT/DPS/MNL/SGN/HAN/NRT/KIX/HND/PVG/PEK). Swap or add providers without touching pipeline.

**Routes.** `RouteCatalog` (`collector/routes.py`) defines 13 destinations out of SIN. Each date in the search window generates 26 one-way tasks (SIN->dest and dest->SIN) plus 3 round-trip tasks per route (return offsets 7, 14, 21 days) — up to 4 searches per route/date.

> **Midnight caveat.** Runs crossing midnight keep only future dates after rollover: a run started 23:50 builds today's tasks, which become invalid at 00:00 and fail as `DATA` (no proxy blame); any rebuild after rollover skips past dates entirely. Past dates are unsearchable by definition — losing up to a day at the boundary is by design.

```mermaid
flowchart TD
    CLI["python -m cli search --start YYYY-MM-DD --max-days 30"]
    REG["ProviderRegistry"]
    PROV["GoogleFlightsProvider"]
    PIPE["BulkSearchPipeline"]

    CLI --> REG --> PROV --> PIPE

    PIPE --> ROT["ProxyRotator<br/>2-phase validate TCP|HTTP echo, weighted select"]
    PIPE --> RL["RateLimiter<br/>adaptive token bucket, halve on 429 burst, double on clean 60s"]
    PIPE --> SQL["SQLite aiosqlite<br/>upsert intermediary, track retries"]
    PIPE --> RETRY["Retry loop x3<br/>cumulative attempt budget (retries <= r x 3)"]
    PIPE --> CONV["Convert to JSONL<br/>write, delete SQLite DB"]

    SQL --> CONV
    CONV --> OUT["storage/raw/search_YYYYMMDD_HHMMSS.jsonl"]
    OUT -. future .-> R2["gzip -> R2 bronze"]
```

Details: [docs/architecture.md](docs/architecture.md), [docs/design.md](docs/design.md), [docs/data-model.md](docs/data-model.md).

**Validation + transformation.** Schema validation, type checking, dedup, normalisation, enrichment. Separate raw from processed.

**Data lake.** R2 buckets: bronze (raw gzipped JSONL) -> silver (cleaned Parquet) -> gold (aggregated analytics). Immutable raw tier enables reprocessing.

**Analytics.** DuckDB SQL queries against Parquet. Route comparisons, seasonal trends, booking window analysis.

**Presentation.** (Future) Dashboard -- Streamlit + Plotly. Decoupled from pipeline. Consumes processed datasets only.

## Technology Stack

| Component       | Technology        | Why                                         |
|-----------------|-------------------|---------------------------------------------|
| Language        | Python 3.14+      | One language, entire stack                  |
| HTTP            | curl_cffi         | TLS fingerprint spoofing, browser impersonation |
| Proxy fetch     | httpx             | Pull proxy lists from 64 sources            |
| SQLite          | aiosqlite         | Async intermediary storage, upsert + retry  |
| Flights API     | fli               | Google Flights internal API wrapper         |
| Progress        | log lines         | Periodic %/rate/ETA progress in logs       |
| Scheduler       | GitHub Actions    | Cron, no infra                              |
| Package mgmt    | uv                | Fast, reproducible                          |
| Testing         | pytest + ruff + basedpyright + coverage | 289 tests, 97% cov |
| Storage         | Cloudflare R2     | S3-compatible, free 10 GB, pay after        |
| Query           | DuckDB            | SQL over Parquet, no server                 |
| Viz             | Streamlit + Plotly| (Future) interactive dashboard              |

## Project Structure

```
flight-price-observatory/
+-- .github/
|   +-- workflows/          # (planned) CI: lint + test + coverage gate
+-- cli/                    # CLI entry points (search, convert)
+-- collector/              # core package: providers, services, models
|   +-- providers/          #   one dir per flight data source (BaseProvider)
|   +-- services/           #   pipeline + rate limiter
|   +-- models/             #   domain objects
+-- storage/                # runtime data (gitignored outputs)
|   +-- raw/                #   final JSONL
|   +-- db/                 #   transient SQLite state
|   +-- logs/               #   run logs (gitignored)
|   +-- proxy_cache.json    #   proxy pool cache (gitignored)
+-- docs/                   # architecture, design, data model, ADRs
+-- tests/                  # mirrors collector/ module structure
|   +-- libs/               #   factories + fakes (shared test helpers)
+-- pyproject.toml
+-- README.md
```

### cli/

CLI commands. `__main__.py` dispatches via argparse subparsers. Add commands by adding module + registering subparser.

### collector/

Core collection logic. Provider-agnostic pipeline. New provider = new file under `providers/` implementing `BaseProvider`.

### storage/

`raw/` -- final JSONL output. `db/` -- transient SQLite state, deleted after JSONL produced.

## Getting Started

### Prerequisites

- Python 3.14+
- Git
- uv

```
git clone https://github.com/notlega/flight-price-observatory.git
cd flight-price-observatory
uv sync
```

No env vars needed for local collection. R2 upload requires credentials when configured.

## Installation

```
uv sync
```

## Running Locally

```
# Search next 270 days
uv run python -m cli search

# Custom window
uv run python -m cli search --start 2026-07-11 --max-days 90

# Currency + rate/concurrency tuning
uv run python -m cli search --currency USD --rate 5 --workers 10

# Verbose debug (global flag, before or after subcommand)
uv run python -m cli -v search
uv run python -m cli search --verbose

# Convert existing SQLite to JSONL (default: delete DB after)
uv run python -m cli convert

# Keep the SQLite DB after conversion
uv run python -m cli convert --keep-db

# Run tests + lint
uv run pytest
uv run lint
```

`search` flags:

| Flag        | Default | Description          |
|-------------|---------|----------------------|
| `--start`   | today   | Start date (YYYY-MM-DD) |
| `--max-days`| 270     | Days ahead from start |
| `--currency`| SGD     | Currency code for pricing |
| `--rate`    | 200     | Requests per second  |
| `--workers` | 50     | Max concurrent searches |
| `--keep-db` | False   | Keep SQLite state file after JSONL export |
| `-v`        | False   | Debug logging (global, also after subcommand) |

`convert` flags:

| Flag        | Default | Description          |
|-------------|---------|----------------------|
| `--db`      | `storage/db/search_state.db` | SQLite state file |
| `--output`  | auto    | Output JSONL path (`storage/raw/search_YYYYMMDD_HHMMSS.jsonl`) |
| `--keep-db` | False   | Keep SQLite file after conversion (default: delete) |

## Roadmap

- [x] Provider abstraction layer
- [x] Automated data collection with proxy rotation
- [x] SQLite intermediary + retry loop
- [x] Google Flights provider (13 SIN<->Asia routes)
- [ ] Adaptive collection schedule (daily 0-30d, weekly 31-90d, etc.)
- [ ] R2 upload with gzip compression
- [ ] Bronze->silver Parquet transformation
- [ ] DuckDB analytical queries
- [ ] Interactive dashboard
- [ ] Price forecasting ML models
- [ ] Additional providers

## Contributing

1. Open issue proposing change
2. Discuss architecture changes before code
3. Feature branch from main
4. `uv run pytest` pass before PR
5. Match code style (ruff)
6. ADR in `docs/decisions/` for major decisions

## License

See `LICENSE` file. Contributors agree to same terms.
