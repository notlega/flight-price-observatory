# ADR-0003: Provider abstraction

## Status

Accepted

## Context

Need to support multiple flight data sources (currently Google Flights) without coupling the collection pipeline to any one vendor's API.

## Decision

Introduce `BaseProvider` interface. Pipeline depends only on the abstraction. Concrete providers live under `collector/providers/` and register in `ProviderRegistry`.

## Consequences

**Positive**

- Add providers without touching pipeline (`CollectorManager`, `BulkSearchPipeline` unchanged).
- Providers own their session/error mapping; `errors.py` gives shared typed exception taxonomy.
- Tests inject `FakeProvider` — no network in unit tests.

**Negative**

- Slight indirection: provider contract must stay stable.
- Providers share no search-URL config; each owns its own.

## Alternatives considered

- **Concrete pipeline per provider** — duplication, hard to add routes/providers.
- **Plugin system** — overkill at current scale.
