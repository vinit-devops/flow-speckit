# ADR-0003 — PostgreSQL-only core storage (embedded server for the quickstart)

**Status:** Accepted · 2026-07-03

## Context

The spec mandated PostgreSQL + Redis + S3 + Qdrant + Neo4j + OpenSearch. Six services
before "hello world" is how OSS frameworks die (Backstage's adoption tax is the
cautionary tale). Meanwhile v1-scale data is small: thousands of artifacts, lineage
depth < 20, blobs in KB–MB, single-team concurrency.

## Decision

**One storage dependency: PostgreSQL 15+.** Each spec role maps to a Postgres feature
behind a port where substitution is plausible:

| Spec system | Postgres mechanism | Port |
|---|---|---|
| Redis (queue/pubsub) | `FOR UPDATE SKIP LOCKED` queue + `LISTEN/NOTIFY` | none — concrete |
| S3 (blobs) | `blobs` table (bytea) | `BlobStore` |
| Qdrant (vectors) | pgvector (optional extra; unused in v0.1) | `VectorStore` |
| Neo4j (graph) | `artifact_edges` + recursive CTEs | none — concrete |
| OpenSearch (FTS) | `tsvector` over artifacts | none — concrete |

**Quickstart:** `flow-speckit init` provisions an embedded PostgreSQL via the `pgserver`
wheel (data dir `.flow-speckit/pg`) — no Docker, no brew, no compose. Production/team mode
sets `FLOW_SPECKIT_DATABASE_URL` to a real server: same dialect, same Alembic migrations,
zero dual-database code.

## Alternatives rejected

- **SQLite for local, Postgres for teams** (stress-test analysis favored this): two SQL
  dialects is a permanent tax on every feature — JSONB vs JSON1, no `SKIP LOCKED`
  parity, no `LISTEN/NOTIFY`, divergent migration testing. The embedded-Postgres wheel
  delivers SQLite's zero-install feel with one dialect.
- **Six-service stack per the spec:** rejected on adoption grounds; scale-out plugins
  (`flow-speckit-storage-s3`, Qdrant) can arrive post-v1 behind the existing ports if real
  deployments demand them.

## Risks & mitigations

- `pgserver` wheel coverage/maintenance (macOS arm64/x86, Linux, Windows) is the main
  bet — mitigated by a documented `docker-compose` fallback and the standard
  `DATABASE_URL` path; the embedded server is a convenience layer, never a requirement.
- Blob growth (bytea) is bounded by retention policy (`flow-speckit gc`) before an S3 plugin
  is ever needed.
