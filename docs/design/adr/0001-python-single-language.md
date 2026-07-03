# ADR-0001 — Python as the single implementation language

**Status:** Accepted · 2026-07-03

## Context

The originating spec demanded SDKs in both Python and TypeScript. A framework's language
determines its contributor pool, dependency ecosystem, and maintenance surface. Dual
first-party SDKs double every contract change forever.

## Decision

Flow SpecKit is **Python 3.11+ only** through v1.0: kernel, plugins, skill authoring, and
workflow authoring. Non-Python consumers use the CLI and (from v0.4) the REST/SSE API;
a TypeScript client is *generated* from OpenAPI post-1.0 if demand exists.

## Rationale

| Dimension | Python | TypeScript |
|---|---|---|
| AI ecosystem | LiteLLM, Pydantic, provider SDKs, eval tooling — the gravity well | Good SDKs, thinner orchestration ecosystem |
| Target authors | Platform/AI engineers building SDLC tooling — Python-first | Consume via CLI/API anyway |
| Schema/validation | Pydantic v2: runtime validation + JSON Schema export drives artifacts, skills, REST, and docs from one definition | Zod adequate, weaker export ergonomics |
| Durable-execution prior art | DBOS, Hatchet, Prefect, Temporal-py — designs to learn from (and the ADR-0002 fallback is Python) | SaaS-oriented options |
| Packaging | Solved by `uv` + hatchling + entry points; entry points also power the whole plugin system | Better DX, but npm-installed daemons are a hard sell to platform teams |

Async: the workload is I/O-bound (LLM calls, subprocess waits, Postgres); asyncio is
sufficient and uniform (`async def` everywhere in the kernel).

## Consequences

- One contract definition (Pydantic) → JSON Schema → OpenAPI → generated clients.
- TypeScript-native teams author skills only via a future generated client + REST, or
  by shelling out — accepted cost until post-1.0.
- Kernel is mypy-strict; Python 3.11 floor for `asyncio` task groups and perf.
