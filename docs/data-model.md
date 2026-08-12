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
| `retries`   | INTEGER| Retry count, default 0 |
| `success`   | BOOL   | 1 if search returned flights |
| `searched_at`| TEXT  | ISO timestamp |

Semantics:

- **Success row:** `success=1`, `flights` holds JSON. Absent if no flights found.
- **Failure row:** `success=0`, `error_type` set, `retries` tracks attempts. Used by `get_failed(max_retries)` to feed retry loop.
- **Retryable errors:** `rate_limited`, `timeout`, `connection`, `no_proxy` (see `_RETRY_ERROR_TYPES`). `data` errors are not retried; 429 never exhausts retries within a run.

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

- **Destinations:** KUL, CGK, BKK, HKT, DPS, MNL, SGN, HAN, NRT, KIX, HND, PVG, PEK (13).
- **One-way:** every date in the search window x both directions (`RouteCatalog.one_way_routes()`) — 26 tasks/date.
- **Round-trip:** `RouteCatalog.round_trip_routes()` (SIN->dest only), return offsets `ROUND_TRIP_OFFSETS = (7, 14, 21)` days — 3 extra tasks/route/date.
- Encoded in `search_results` as `return_date` (empty = one-way) + `flight_type` (`ONE_WAY` / `ROUND_TRIP`).

## Flight row shape (Google Flights)

Output of `GoogleFlightsProvider.search()`, from `fli` lib `parse_flight_row(...).model_dump(mode="json")`. Fields per the `fli` `Flight` model (price, airline, departure/arrival times, stops, etc.). Pipelines treat flight dicts as opaque JSON.

## Proxy cache (`storage/proxy_cache.json`)

Persisted `(timestamp, [ProxyInfo dicts])`. Each proxy: `url`, `protocol`, `quality_score`, `latency_ms`, `last_validated`. Fresh for 15 min, revalidated up to 24 h; `refresh(force=True)` bypasses.
