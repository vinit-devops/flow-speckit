# Flow SpecKit

Durable, artifact-driven AI-SDLC workflow orchestration — think **"Temporal for AI
Software Engineering"**.

Flow SpecKit orchestrates the **complete AI-assisted software development lifecycle**,
not just the coding step:

```
Business Idea → Discovery → Product Shaping → Technical Design
             → Implementation → Review → Merge → Release
```

Durable, resumable workflows drive stateless AI skills and pluggable coding agents
(Claude Code, Cursor, …), with human approval gates owning every consequential
transition and every phase producing a versioned, lineage-linked artifact.

> **Status: early development.** Phase 2 (Artifact Engine) is complete; Phase 3
> (Workflow Engine) is in progress. The full wedge demo below is the v0.1 target,
> not yet functional end to end.

## Install

```console
pip install flow-speckit
```

## The wedge (v0.1 target demo)

In any existing repository, with nothing installed but Python and a coding-agent CLI:

```console
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
2. **Lineage** — `flow-speckit trace <pr-url>` prints the full provenance of the PR back
   to the business idea: every artifact version, who approved what and when, and
   per-step token cost.
3. **Accountability** — the append-only event log proves the *workflow*, not the model,
   decided every transition; humans resolved every gate.

## Core principles

- **Artifact-driven** — every phase consumes an artifact and produces a new, versioned,
  schema-validated artifact. Artifacts, never chat history, are the source of truth.
- **Workflow-first** — a durable state machine owns all transitions. AI never picks the
  next step.
- **Stateless skills** — every AI capability is a stateless, independently executable
  skill. No long-running autonomous agents.
- **Human gates** — approval gates are first-class workflow states. Humans own
  decisions; AI owns artifacts.
- **Pluggable execution** — coding work delegates to interchangeable backends
  (Claude Code first) behind a small adapter port.

## Architecture at a glance

- **Python 3.11+**, single package — no Docker, no service stack.
- **PostgreSQL-only core** (embedded server via the `embedded-pg` extra): the work
  queue, event log, artifact graph, blobs, and full-text search all live in one
  database.
- **Four kernel subsystems**: Artifact Engine, Workflow Engine, Skill Engine,
  Execution Port. Everything else is a plugin behind an entry point.

Full design documents live in the repository:
[github.com/vinit-devops/flow-speckit](https://github.com/vinit-devops/flow-speckit/tree/main/docs/design).

## License

Apache-2.0.
