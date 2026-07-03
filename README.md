# Flow SpecKit — AI-Native SDLC Orchestration Framework

> **Status: Phase 2 complete — Artifact Engine shipped.** The design documents live in
> [`docs/design/`](docs/design/); the kernel package is at
> [`packages/flow-speckit`](packages/flow-speckit). Next up: Phase 3, the Workflow Engine.

Flow SpecKit is an open-source framework that orchestrates the **complete AI-assisted software
development lifecycle** — not just the coding step:

```
Business Idea → Discovery → Product Shaping → Technical Design
             → Implementation → Review → Merge → Release
```

Think **"Temporal for AI Software Engineering"**: durable, resumable workflows drive
stateless AI skills and pluggable coding agents (Claude Code, Cursor, …), with human
approval gates owning every consequential transition and every phase producing a
versioned, lineage-linked artifact.

## Why

AI coding tools start from a prompt and end at code. Real software development starts
much earlier — and no existing tool preserves the chain of reasoning from business idea
to merged pull request. Spec-driven tools (GitHub Spec Kit, Amazon Kiro) prove the
artifact idea but have no durability, no approval gates, and no lineage. Agent frameworks
(CrewAI, AutoGen) have no artifacts and no human accountability. Flow SpecKit owns exactly
that gap.

## The wedge (v0.1 target demo)

In any existing repository, with nothing installed but Python and a coding-agent CLI:

```console
$ pip install flow-speckit
$ flow-speckit init
$ flow-speckit run feature --idea "Add CSV export to the reports page"
```

which durably executes:

```
FrameBrief → [human gate: approve brief] → ProductArtifact → TechnicalDesign
          → [human gate: approve design] → Claude Code implements in an isolated
            git worktree → Pull request opened
```

Three properties no prompt-first tool can offer:

1. **Durability** — `kill -9` the process mid-implementation, then `flow-speckit resume`
   continues exactly where it stopped.
2. **Lineage** — `flow-speckit trace <pr-url>` prints the full provenance of the PR back to
   the business idea: every artifact version, who approved what and when, and per-step
   token cost.
3. **Accountability** — the append-only event log proves the *workflow*, not the model,
   decided every transition; humans resolved every gate.

## Core principles

| Principle | Meaning |
|---|---|
| **Artifact-driven** | Every phase consumes an artifact and produces a new, versioned, schema-validated artifact. Artifacts — never chat history — are the source of truth. |
| **Workflow-first** | A durable state machine owns all transitions: sequential, parallel, conditional, retries, timeouts, sub-workflows. AI never picks the next step. |
| **Stateless skills** | Every AI capability is a stateless, independently executable skill. No long-running autonomous agents. |
| **Human gates** | Approval gates are first-class workflow states. Humans own decisions; AI owns artifacts. |
| **Pluggable execution** | Coding work delegates to interchangeable backends (Claude Code first) behind a small adapter port. |
| **Context from artifacts** | Skills reconstruct context purely from persisted artifacts — never LLM conversation memory. |

## Architecture at a glance

- **Python 3.11+**, single package, `pip install` — no Docker, no service stack.
- **PostgreSQL-only core** (embedded server for the quickstart): the work queue, event
  log, artifact graph, blobs, and full-text search all live in one database.
- **Four kernel subsystems**: Artifact Engine, Workflow Engine, Skill Engine, Execution
  Port. Everything else — product/engineering skill packs, GitHub, Slack, storage
  backends — is a plugin behind an entry point.
- **LiteLLM** for multi-provider LLM access with tier-based policy routing.

See [`docs/design/00-vision-and-scope.md`](docs/design/00-vision-and-scope.md) for the
full vision, the explicit non-goals, and the roadmap from v0.1 to v1.0.

## Design documents

| Doc | Contents |
|---|---|
| [00 — Vision & Scope](docs/design/00-vision-and-scope.md) | Revised spec, wedge demo, roadmap, non-goals |
| [01 — Architecture Overview](docs/design/01-architecture-overview.md) | Kernel boundaries, C4 diagrams, ports & adapters, repo layout, plugin system |
| [02 — Artifact Engine](docs/design/02-artifact-engine.md) | Type system, ER model, versioning, lineage graph, `.sdlc/` materialization |
| [03 — Workflow Engine](docs/design/03-workflow-engine.md) | Event log, step memoization, determinism, gates, timers, queue, DSL |
| [04 — Skill Engine](docs/design/04-skill-engine.md) | `@skill` contract, `SkillContext`, discovery, testing harness |
| [05 — Execution Engine](docs/design/05-execution-engine.md) | `ExecutionBackend` port, workspace lifecycle, Claude Code adapter, conformance suite |
| [06 — LLM & Context](docs/design/06-llm-and-context.md) | LiteLLM wrapper, tiers, cost metering, `ContextAssembler` |
| [07 — API & CLI](docs/design/07-api-and-cli.md) | Full CLI spec as the wedge-demo transcript, REST + SSE surface |
| [ADRs](docs/design/adr/) | Language, build-vs-embed durable execution, Postgres-only storage, context-engine cut, artifact system of record |

## License

Apache-2.0 (see [LICENSE](LICENSE)).
