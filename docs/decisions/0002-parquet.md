# ADR-0002: Parquet for processed data

## Status

Accepted

## Context

Need a columnar format for the processed (silver) tier, enabling efficient analytical queries over historical airfare data. Options: Parquet, CSV, JSONL, DuckDB native storage.

## Decision

Use Parquet for the processed tier. Bronze stays gzipped JSONL (immutable raw), Parquet is derived.

## Consequences

**Positive**

- Columnar: fast aggregations over price/route/date slices.
- DuckDB queries Parquet directly, no server.
- Schema-typed, compresses well.

**Negative**

- Immutable raw must be preserved: Parquet is derived, never the source of truth.

## Alternatives considered

- **CSV/JSONL for processed** — slow analytical scans.
- **DuckDB native files** — format locks to DuckDB; Parquet is portable.
