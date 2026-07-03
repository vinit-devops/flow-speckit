# ADR-0005 — Postgres is the artifact system of record; git materialization is a projection

**Status:** Accepted · 2026-07-03

## Context

Two credible artifact-storage philosophies emerged during design:

- **DB-authoritative:** artifacts as immutable JSONB rows — transactional with the
  workflow event log, queryable, schema-validated, graph-linked.
- **Git-native:** artifacts as markdown files in `.sdlc/` in the target repo — reviewed
  where reviewers live, diffed by git, interoperable with GitHub Spec Kit, zero extra
  storage.

Each is genuinely right about something; picking only one loses either durability
integration or human ergonomics.

## Decision

**Postgres is the system of record** ([doc 02](../02-artifact-engine.md)): validation,
versioning, lineage edges, status transitions, and step checkpoints commit in one
database transaction with the workflow event log — durability and audit are only
coherent if artifacts and events share a transaction boundary.

**Git materialization is a first-class projection** (designed now, built v0.2): rendered
markdown + frontmatter files under `.sdlc/`, committed to the target repo — for PR-
embedded review, Spec Kit-style interop, and cold-start recovery via `flow-speckit import`.

Reconciliation rule: materialized files are never parsed back implicitly. A hand-edited
file re-enters the store only through explicit `flow-speckit import`, which creates a **new
version** authored by the human (with a `supersedes` edge). Immutability holds; human
edits get a first-class, attributed path back in; drift is detectable via
`content_hash`.

## Consequences

- The wedge demo's `kill -9` durability and one-transaction step commits are preserved.
- Reviewers see design docs in the PR diff without tooling; Spec Kit users get a
  migration story.
- Cost: a rendering/import layer and hash-based drift checks — bounded, and deferred to
  v0.2 without blocking the wedge.
