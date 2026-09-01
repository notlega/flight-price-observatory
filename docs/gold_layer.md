# Gold Layer — Implementation Runbook

Owner: **@josephyqf** (consumes Silver, never writes bronze/silver). Gold tables + alert thresholds owned by @josephyqf. Silver source contract owned by @notlega — breaking schema changes require ADR + notice to the gold owner.

Bronze + Silver are owned by the pipeline. Gold sits on top of Silver only.

## Data contract (Silver v2, read-only)

- Location: R2 bucket `flights`, prefix `silver/<RUN_TS>/origin=<IATA>/destination=<IATA>/*.parquet` (zstd). ~55 runs, ~740 MB, ~23.6M rows.
- Every file has `origin`, `destination`, `dep_date`, `return_date`, `flight_type`, `searched_at`, `price` (`DECIMAL(12,2)`), `price_present`, `currency`, `duration_minutes`, `stops`, `airline`, `booking_token`, `run_ts`, `lead_days`, `direction`, `itinerary_id`.
- Ingest patterns:
  - DuckDB `httpfs` with presigned URLs (S3 API), or
  - `aws s3 sync` (region `auto`, endpoint `https://<account>.r2.cloudflarestorage.com`), or
  - full rip to local Parquet then `read_parquet('**/*.parquet')` (partition columns materialized, `hive_partitioning` not required).

## Golden rules

1. **Price stats only where `price_present = true`** (null fares ≈10%). Averages over null prices are wrong.
2. **Split `direction`** — `OUTBOUND` (SIN→X) ≠ `RETURN` (X→SIN) fares; `ROUND_TRIP` rows carry the paired outbound+return fare (one row, `return_date` populated). Never mix directions in a "route price" metric.
3. **Dedup by `itinerary_id`** before min/max aggregates (same fare re-appears across `searched_at` ticks day of run). `itinerary_id` is MD5(searched_at, booking_token, flight_type) and is the outbound↔return pairing key.
4. **`lead_days`** = `dep_date − searched_at::date` (0..270). It is the advance-purchase dimension; bucket it.
5. Money stays `DECIMAL`; round to 2dp at output, never float.
6. Respect known gaps — see [Known Data Gaps](index.md#known-data-gaps). No 0818/0826/0831 data, ever.

## Deliverables (proposed gold tables, DuckDB)

Store under `gold/` in same R2 bucket, zstd, partitioned `origin=X/destination=Y/`.

### 1. `route_daily_profile`
Per `(origin, destination, dep_date)` over a 7-day trailing window:
- `n_offers`, `n_fares` (price_present), `min/p25/median/p75/max price`
- `airline_share` top-3, `stops_distribution`
- Builder: filter `direction = 'OUTBOUND'`, `price_present`, dedup `itinerary_id`; group by `(origin, destination, dep_date)`.

### 2. `lead_time_curve`
Per `(route, lead_days_bucket)` where bucket = 7-day bands over 0–270:
- median fare by lead band → *"when to book"* chart (U-curve: cheapest far out, rises near date).
- Split one-way vs round-trip.

### 3. `forward_curve`
Per `(route, dep_date)`: series of `min fare` over `searched_at` ascending → cheapest-ever price for a given travel date; slope/flatness = purchase-now signal.

### 4. `price_spike_warning`
Per `(route, dep_date)`: current median vs trailing-90-day rolling median (same week-of-departure); flag when > +X% (start X=25). Surface as alert table; suppress during festive known peaks initially.

## Suggested first query (sanity)

```sql
SELECT origin, destination, dep_date,
       count(*)                      AS n_offers,
       count(*) FILTER (WHERE price_present) AS n_fares,
       median(price)                 AS price_median
FROM read_parquet('silver/**/*.parquet')
WHERE direction = 'OUTBOUND' AND price_present
  AND run_ts = (SELECT max(run_ts) FROM read_parquet('silver/**/*.parquet'))
GROUP BY 1, 2, 3
LIMIT 20;
```

## Notes / caveats
- Full-window ("full") runs cover 270d; windowed runs (w30/w60/w90) cover subsets — lead-time coverage per `run_ts` varies. Compare within same `run_ts` family for fair curves.
- One run/day; samples are sparse after 90d lead for daily windows.
- Reverse legs (`RETURN`) are legitimate X→SIN fares returned by the engine — keep them for X→SIN analytics, exclude from SIN→X route metrics.