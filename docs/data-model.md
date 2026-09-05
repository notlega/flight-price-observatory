---
description: Data model reference — SQLite intermediary schema, silver Parquet columns, and R2 storage layout.
author: "@notlega"
---

# Data Model

## SQLite intermediary (`search_results`)

Intermediary store for collected searches. Async writes, WAL journal, `INSERT OR REPLACE` keyed on `(route, dep_date, return_date, flight_type)`.

| Column      | Type   | Notes |
|-------------|--------|-------|
| `route`     | TEXT   | `"{origin}|{dest}"`, PK part |
| `dep_date`  | TEXT   | `YYYY-MM-DD`, PK part |
| `return_date`| TEXT  | `YYYY-MM-DD`, empty for one-way, PK part |
| `flight_type`| TEXT  | `"ONE_WAY"` or `"ROUND_TRIP"`, PK part |
| `origin`    | TEXT   | IATA code |
| `destination`| TEXT  | IATA code |
| `flights`   | TEXT   | JSON array of flight rows (serialized string) |
| `error_type`| TEXT   | ErrorType enum value, NULL on success |
| `retries`   | INTEGER| Cumulative attempts consumed (1-based), default 0 |
| `success`   | BOOL   | 1 if search returned flights |
| `searched_at`| TEXT  | ISO timestamp |

Semantics:

- **Success row:** `success=1`, `flights` holds JSON. Absent if no flights found. `retries` = attempt that succeeded (1..3).
- **Failure row:** `success=0`, `error_type` set, `retries` = `round * 3` after a round's attempts are exhausted. `get_failed(max_retries)` feeds the retry loop with `retries <= max_retries`.
- **Retryable errors:** `rate_limited`, `timeout`, `connection`, `no_proxy`, `data` (see `_RETRY_ERROR_TYPES`). Round *r* queries `retries <= r * 3`, giving every round a fresh 3-attempt budget.

## JSONL output (`storage/raw/search_*.jsonl`)

One line per successful route search:

```json
{
  "route": "SIN|KUL",
  "dep_date": "2026-08-01",
  "return_date": "",
  "flight_type": "ONE_WAY",
  "origin": "SIN",
  "destination": "KUL",
  "flights": "[{\"price\": 120, \"airline\": \"SQ\"}]",
  "searched_at": "2026-01-01T00:00:00Z"
}
```

- `flights` remains a JSON **string** (raw fidelity, lossless round-trip through SQLite). Consumers parse it.
- Produced by `collector/convert.py`, which deletes the SQLite DB afterwards by default.

## Route catalog (`collector/routes.py`)

- **Destinations:** KUL, CGK, BKK, HKT, DPS, MNL, SGN, HAN, NRT, KIX, HND, TPE, PVG, PEK, ICN, PUS (16).
- **One-way:** every date in the search window x both directions (`RouteCatalog.one_way_routes()`) — 32 tasks/date.
- **Round-trip:** `RouteCatalog.round_trip_routes()` (SIN->dest only), return offsets `ROUND_TRIP_OFFSETS = (7, 14, 21)` days — 3 extra tasks/route/date.
- Encoded in `search_results` as `return_date` (empty = one-way) + `flight_type` (`ONE_WAY` / `ROUND_TRIP`).

## Flight row shape (Google Flights)

Output of `GoogleFlightsProvider.search()`, from `fli` lib `parse_flight_row(...).model_dump(mode="json")`. Fields per the `fli` `Flight` model (price, airline, departure/arrival times, stops, etc.). Pipelines treat flight dicts as opaque JSON.

## Silver tier (Parquet on R2)

Optimised Parquet files stored in Cloudflare R2, partitioned by route.

### Schema

| Column | Type | Partition | Notes |
|--------|------|-----------|-------|
| `origin` | VARCHAR(3) | Yes | IATA code (SIN) |
| `destination` | VARCHAR(3) | Yes | IATA code (KUL) |
| `dep_date` | DATE | Yes | ISO date |
| `return_date` | DATE | No | NULL for one-way |
| `flight_type` | VARCHAR(9) | No | ONE_WAY / ROUND_TRIP |
| `searched_at` | TIMESTAMP | No | UTC timestamp |
| `price` | FLOAT | No | NULL if unknown |
| `currency` | VARCHAR(3) | No | SGD, USD, etc. |
| `duration_minutes` | INT | No | Total flight duration |
| `stops` | INT | No | Number of stops |
| `airline` | VARCHAR(3) | No | Primary airline code |
| `airline_name` | VARCHAR | No | Full airline name |
| `co2_emissions_g` | INT | No | NULL if unknown |
| `emissions_tag` | VARCHAR(6) | No | lower/typical/higher |
| `booking_token` | VARCHAR | No | For future booking lookup |

### Partition strategy

`origin/destination`
- Query: "SIN→KUL" → reads 1 partition
- ~166 partitions max (16 destinations × both directions)

### Compression

zstd (default Parquet) — expect 10-20x reduction from JSONL

### Dashboard access

DuckDB WASM reads Parquet directly from R2 via HTTP range requests.
R2 is private; Cloudflare Worker proxy handles CORS + Range headers.

## Proxy cache (`storage/proxy_cache.json`)

Persisted `(timestamp, [ProxyInfo dicts])`. Each proxy: `url`, `protocol`, `quality_score`, `latency_ms`, `last_validated`. Fresh for 30 min, revalidated up to 24 h; `refresh(force=True)` bypasses.
