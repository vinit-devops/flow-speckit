# ADR-0004 — Cut the repo-intelligence context engine; keep artifact context assembly

**Status:** Accepted · 2026-07-03

## Context

The spec demanded a Context Engine: repository index, knowledge graph, vector + hybrid +
AST + semantic search, dependency/call graphs, file history. That is a Sourcegraph/
Greptile-scale product, and it drags in tree-sitter, embedding pipelines, and two of the
six datastores rejected in ADR-0003.

## Decision

**Cut repo intelligence from the kernel entirely.** Split the spec's "context" concern
into its two actual jobs:

1. **Repo context for coding steps** — delegated wholly to execution backends. Claude
   Code, Cursor, and successors ship sophisticated in-repo retrieval (agentic search,
   their own indexes) that improves on their vendors' schedules. A core reimplementation
   would be duplicative, worse, and permanently rotting — and would violate the
   framework's own principle that backends execute while Flow SpecKit orchestrates.
2. **Artifact context for skills** — kernel-owned `ContextAssembler`
   ([doc 06 §5](../06-llm-and-context.md)): a deterministic, token-budgeted, lineage-
   ordered packing of upstream artifacts into skill prompts, with the included set
   recorded per step. This — not repo indexing — is what "context preservation" means
   in an artifact-driven framework, and it is ~200 lines.

## Consequences

- Six spec features (repo index, KG, AST search, call graph, hybrid search, dependency
  graph) leave the roadmap; no tree-sitter/Qdrant/Neo4j in the kernel.
- Skill prompts are reproducible and auditable ("what did the model see?" is a stored
  fact, not an inference).
- If cross-project artifact retrieval is demanded later, pgvector behind the existing
  `VectorStore` port ships as a plugin without reopening this decision.
- Non-agentic execution backends (bare `local_shell` scripts) receive only the
  instructions + artifact context — accepted; agentic backends are the product bet.
