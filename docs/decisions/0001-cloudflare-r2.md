# ADR-0001: Cloudflare R2 as data lake storage

## Status

Accepted

## Context

Need cheap, immutable, versioned object storage for raw + processed airfare datasets. Options: AWS S3, GCS, Azure Blob, Cloudflare R2, MinIO self-host.

## Decision

Use Cloudflare R2 (S3-compatible API).

## Consequences

**Positive**

- S3-compatible: reuse existing SDK/tooling.
- 10 GB free, no egress fees.
- Zero-infra managed object store.

**Negative**

- Cloud provider lock-in (S3 API mitigates).
- Free tier only up to 10 GB.

## Alternatives considered

- **S3** — egress costs, more setup.
- **MinIO self-host** — operational burden, needs a server.
