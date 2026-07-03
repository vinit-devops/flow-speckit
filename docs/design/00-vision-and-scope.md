# 00 — Vision & Scope

> Phase 1 design document. Companion to the [README](../../README.md); this doc is the
> authoritative statement of what Flow SpecKit is, what it deliberately is not, and the order
> in which it gets built.

## 1. Problem

AI coding tools solve the coding step. Modern software development starts far earlier —
idea, discovery, shaping, technical design — and continues after code lands: review,
merge, release. Today:

- **Prompt-first agents** (Claude Code, Cursor, Copilot) produce excellent code from a
  prompt but preserve nothing upstream of the prompt. There is no record of *why*.
- **Spec-driven tools** (GitHub Spec Kit, Amazon Kiro) prove that artifact-driven
  development works — specs, plans, and tasks as markdown driving an agent — but they
  are stateless scripts: no durability, no resumability, no enforced approval gates, no
  provenance from PR back to idea.
- **Agent frameworks** (CrewAI, AutoGen, LangGraph) orchestrate model calls but have no
  artifact model, no human accountability layer, and their high-level abstractions
  invert control away from teams that need to own the process.
- **Durable-execution engines** (Temporal, DBOS, Restate) solve resumable workflows in
  general but know nothing about artifacts, skills, coding agents, or SDLC semantics.

Nobody owns the intersection: **durable, artifact-driven SDLC workflows with human
approval gates driving pluggable coding agents.** That intersection is Flow SpecKit.

## 2. Product definition

Flow SpecKit is a Python framework + CLI that executes SDLC workflows as durable state
machines. Each workflow step either:

1. runs a **skill** (a stateless AI function that consumes artifacts and produces a new
   versioned artifact),
2. opens a **gate** (a first-class pause where a named human approves, rejects, or
   comments — the run consumes zero resources while waiting), or
3. dispatches an **execution task** (delegates implementation work to a coding-agent
   backend such as Claude Code in an isolated git worktree, capturing the result as an
   artifact).

Every event is appended to a run log that simultaneously provides crash recovery, audit
trail, cost accounting, and live streaming. Every artifact is immutable, versioned,
schema-validated, and linked into a lineage graph.

### The wedge demo (v0.1 acceptance criterion)

```console
$ pip install flow-speckit && flow-speckit init
$ flow-speckit run feature --idea "Add CSV export to the reports page"
```

executes `FrameBrief → [gate] → ProductArtifact → TechnicalDesign → [gate] →
implementation via Claude Code in a worktree → PR opened`, and:

1. `kill -9` mid-implementation + `flow-speckit resume <run-id>` continues from the last
   completed step — **durability**.
2. `flow-speckit trace <pr-url>` prints PR → CodeChange → TaskPlan → TechnicalDesign →
   ProductArtifact → FrameBrief with approvers, timestamps, and per-step cost —
   **lineage**.
3. `flow-speckit runs show <run-id> --events` shows the event log proving the workflow owned
   every transition — **accountability**.

Install requirement is `pip install` plus an API key or an authenticated coding-agent
CLI. If the demo needs Docker or a database server, the wedge has failed.

## 3. Core principles (non-negotiable)

1. **Artifact-driven.** Every phase consumes artifacts and produces a new artifact.
   Artifacts are the source of truth — never chat history, never prompts.
2. **Workflow-first.** The framework owns state transitions. AI never decides the next
   step. Sequential, parallel, conditional, retry, timeout, cancellation, sub-workflow —
   all engine primitives.
3. **Stateless skills.** No long-running agents. Every capability is an independently
   executable function whose only inputs are artifacts + configuration.
4. **Human accountability.** Approval gates are first-class states. Humans own
   decisions; AI owns artifacts. Every decision is recorded with actor and timestamp.
5. **Context preservation.** Skills reconstruct context exclusively from persisted
   artifacts via deterministic context assembly. LLM conversation memory is never a
   dependency.
6. **Pluggable execution.** Coding agents are interchangeable adapters behind one small
   port. Flow SpecKit orchestrates; backends execute.

## 4. What Flow SpecKit is NOT (explicit non-goals)

Cut or deferred deliberately — revisiting any of these requires a new ADR:

| Non-goal | Why | Disposition |
|---|---|---|
| Repo intelligence (AST indexing, knowledge graphs, call graphs, vector repo search) | Coding-agent backends already do in-repo retrieval, better, and keep improving | Cut from core; possible future plugin ([ADR-0004](adr/0004-cut-context-engine.md)) |
| Hand-rolled LLM provider routing | LiteLLM already covers 100+ providers, fallbacks, budgets | Reuse LiteLLM behind a ~200-line tier wrapper |
| Multi-datastore stack (Redis, S3, Qdrant, Neo4j, OpenSearch) | Six services kill OSS adoption; Postgres covers every role at v1 scale | Postgres-only core; ports allow scale-out plugins ([ADR-0003](adr/0003-postgres-only-storage.md)) |
| GraphQL / gRPC / WebSocket APIs | No consumer exists yet; REST + SSE covers CLI, CI, bots, and a future UI | Cut (GraphQL likely forever) |
| TypeScript SDK | Dual-SDK maintenance is death by surface area | Post-1.0, generated from OpenAPI ([ADR-0001](adr/0001-python-single-language.md)) |
| Plugin marketplace | A marketplace with no community is negative signal | Entry-point plugins only until an ecosystem exists |
| RBAC / OAuth / OIDC / policy engine / compliance dashboards | v0.x is local-first, single team; enterprise auth is a post-1.0 (possibly commercial) surface | Deferred; audit log ships from day one for free via the event log |
| Reference React UI | Gates work in the CLI (v0.1) and as GitHub PR reviews (v0.3) with zero UI built | Read-only viewer ~v0.5 |
| 13 bespoke integrations | MCP is becoming the universal integration mechanism | GitHub + Slack webhook only; the rest via MCP/plugins |
| Autonomous multi-agent anything | Contradicts principles 2–4 | Never |

## 5. Kernel and extension surface

Four kernel subsystems (the only required install):

1. **Artifact Engine** — types, versioning, lineage graph, validation, diff,
   materialization ([doc 02](02-artifact-engine.md)).
2. **Workflow Engine** — durable step-memoization runtime, event log, gates, timers,
   queue, Python DSL + YAML templates ([doc 03](03-workflow-engine.md)).
3. **Skill Engine** — `@skill` contract, registry, discovery, testing harness
   ([doc 04](04-skill-engine.md)).
4. **Execution Port** — `ExecutionBackend` adapter interface + workspace lifecycle
   ([doc 05](05-execution-engine.md)).

Supporting kernel services (thin, not "modules"): LLM wrapper + `ContextAssembler`
([doc 06](06-llm-and-context.md)), `GitProvider`/`BlobStore`/`VectorStore`/`Notifier`
ports, config, CLI, optional REST/SSE server ([doc 07](07-api-and-cli.md)).

Everything else is a plugin discovered via entry points
([doc 01 §6](01-architecture-overview.md)): skill packs (`flow-speckit-skills-product`,
`flow-speckit-skills-engineering` — the spec's "product engine" and "engineering engine"),
execution backends, git providers, notifiers, storage backends.

### Built-in workflow templates (4, not 12)

| Template | Flow |
|---|---|
| `full-sdlc` | Idea → FrameBrief → gate → ProductArtifact → TechnicalDesign → gate → TaskPlan → execute → review → PR |
| `feature` | Idea-to-PR: FrameBrief → gate → TechnicalDesign → gate → TaskPlan → Implement → ReviewReport → PR (the wedge demo) |
| `bugfix` | Reproduce/diagnose → TaskPlan → gate → Implement → ReviewReport → PR |
| `review-only` | Existing diff/PR → ReviewReport → sign-off gate (3 steps; the ceremony-free on-ramp) |

Templates are YAML compiled onto the same engine as Python-defined workflows; more
templates are cheap once primitives exist.

## 6. Roadmap

Each milestone is independently demo-able. Phase gates: stop for human approval after
each phase, per the originating spec.

| Milestone | Deliverable | Demo |
|---|---|---|
| **Phase 1 (now)** | This design-doc set | Docs reviewed & approved |
| **v0.1 — wedge** | Scaffold; Artifact Engine; Workflow Engine (events, memoization, gates, timers, queue); Skill Engine + LLM wrapper; Execution Port + `local_shell` + Claude Code adapter; CLI; `feature` + `review-only` templates; quickstart | §2 wedge demo, end to end |
| **v0.2 — programmable** | User-authored workflows (Python DSL) and project-local skills; `.sdlc/` git materialization; cost/audit reports; `full-sdlc` + `bugfix` templates | A user writes a custom 3-step gated workflow in ~50 lines |
| **v0.3 — pluggable + async gates** | Second execution backend (Cursor CLI) proving the port; gates resolvable via GitHub PR review; conformance suite public | Same workflow, two engines; approve from your phone |
| **v0.4 — team mode** | `flow-speckit server` (REST + SSE) + `flow-speckit worker`; external Postgres; concurrent runs; Slack webhook notifications on gates | Shared control plane, two simultaneous runs, Slack approval ping |
| **v0.5 — visibility** | Read-only web UI (runs, artifact viewer, lineage graph, approve/reject); OpenTelemetry GenAI spans | Lineage graph in a browser; traces in any OTel backend |
| **v0.6–0.9 — ecosystem** | Plugin packaging conventions; GitLab adapter; embedded artifact search (Postgres FTS/pgvector); policy hooks (e.g. "design gate needs 2 approvers"); hardening | Third-party plugin installed from PyPI works untouched |
| **v1.0 — stability contract** | Frozen artifact schemas + adapter API v1; migration guarantees; docs site; ≥3 external teams in production | Upgrade v0.9 → v1.0 with zero workflow changes |

Post-1.0: RBAC/OIDC, policy engine, hosted control plane (candidate commercial
boundary), TypeScript client.

## 7. Open decisions flagged for the product owner

Non-blocking for Phase 1, needed before v0.1 ships:

1. **Name** — RESOLVED (2026-07-03): **flow-speckit** (distribution + CLI), Python
   module `flow_speckit`, prose name "Flow SpecKit". PyPI availability still to be
   verified before first publish.
2. **License** — Apache-2.0 assumed (repo LICENSE); confirm vs MIT.
3. **v0.1 primary user** — individual developer laptop vs Appfire-internal platform
   dogfooding; affects quickstart defaults and which gate channel gets polish first.
4. **OSS/commercial boundary** — decides whether governance features are deferred
   (commercial) or cut (forever-free); needed before v0.4 server mode.
