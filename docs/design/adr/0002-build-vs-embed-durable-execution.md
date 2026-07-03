# ADR-0002 — Build a small durable-execution engine (with a DBOS tripwire)

**Status:** Accepted · 2026-07-03

## Context

The spec asked for a "Temporal-style" workflow engine — ambiguous between (a) depending
on Temporal, (b) embedding a durable-execution library (DBOS Transact, Restate SDK), or
(c) building our own. Durable execution is the technical heart and the classic place to
lose a year; the choice is strategic.

## Options

1. **Temporal cluster dependency.** Battle-tested durability for free — but every
   adopter must run a Temporal cluster, killing the `pip install` wedge; human gates map
   awkwardly to signals; the project becomes "a Temporal app", undercutting its own
   positioning.
2. **Embed DBOS Transact.** Library-only durable execution over Postgres — closest to
   our constraints. But its decorator model, queue semantics, and upgrade cadence would
   shape our public `ctx` API from underneath, and gates/artifact-native steps/YAML
   compilation want engine-level integration we'd be building around someone else's core.
3. **Build a small engine** (chosen): step-memoization over an event log — the
   DBOS/Restate *model*, not the Temporal full-history-replay model. Estimated
   1,500–2,500 LOC given our narrow requirements: single language, Postgres-only, tens
   of steps per run, human-scale latency, 9 event types, no infinite workflows, no
   cross-language determinism.

## Decision

Build option 3 exactly as specified in [doc 03](../03-workflow-engine.md), with scope
discipline (one execution model; at-least-once stated honestly; versioning by pinning,
not patching) and week-one chaos/replay tests.

**Tripwire (binding):** if golden-replay, crash-injection, and queue-contention tests
are not rock-solid by the end of the v0.1 engine build phase, we adopt DBOS Transact
underneath and keep `ctx`/gates/YAML as the facade. The `ctx` API is designed so this
swap does not change any workflow, skill, or template code.

## Consequences

- We own gate semantics natively (release-worker-while-waiting, rejection-as-data) —
  the flagship feature is not contorted through someone else's signal mechanism.
- We accept engine-bug risk, mitigated by the tiny surface, the test regime, and the
  tripwire.
- A Temporal *backend adapter* remains possible post-1.0 for shops that already run it;
  nothing in the public API assumes our engine.
