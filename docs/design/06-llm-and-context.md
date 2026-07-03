# 06 — LLM Wrapper & Context Assembly

> Two deliberately small components: a ~200-line policy layer over LiteLLM, and the
> deterministic artifact-context packer that embodies "context from artifacts, never
> chat history". Related ADR: [0004 — context engine cut](adr/0004-cut-context-engine.md).

## 1. What we do NOT build

- No provider SDK wrangling — **LiteLLM** gives 100+ providers, retries, fallbacks,
  budget caps, and usage accounting behind one `completion()` call.
- No repo indexing, AST parsing, embeddings pipelines, or knowledge graphs — coding
  backends do in-repo retrieval themselves (ADR-0004).
- No prompt-management product — prompts are versioned files in skill packages.

## 2. `LLMClient`: tier-based policy routing

Skills declare **tiers**, never model names. Users map tiers to models in `flow-speckit.toml`:

```toml
[llm.tiers]
fast      = "anthropic/claude-haiku-4-5"        # cheap classification, summaries
standard  = "anthropic/claude-sonnet-5"         # default drafting
reasoning = "anthropic/claude-opus-4-8"         # design, review, shaping

[llm.tiers.overrides]                            # optional per-skill pin
code_review = "openai/gpt-5"

[llm.budget]
default_max_usd_per_run = 25.0
```

```python
class LLMClient:
    async def complete(self, prompt: str, *, context: AssembledContext | None = None,
                       response_model: type[BaseModel] | None = None,
                       tier: Tier | None = None) -> BaseModel | str
```

Behavior per call:

1. Resolve tier → model (skill `LLMSpec` → per-skill override → tier map).
2. Render prompt + assembled context into messages.
3. `response_model` → structured output via provider-native structured/tool-call modes
   (LiteLLM `response_format`), validated by Pydantic; one repair round-trip on
   validation failure, then raise `SkillOutputError` (retryable per step policy).
4. Record usage: tokens in/out and USD (LiteLLM's cost map) accumulated into the step's
   `step_completed.cost` and checked against `LLMSpec.max_cost_usd` and the run budget —
   breach raises `BudgetExceededError`, failing the step (and run, unless the workflow
   catches it and opens a gate).

This wrapper is also the seam: if LiteLLM's dependency weight ever becomes a problem,
`LLMClient` is the only file that knows it exists.

## 3. Policy routing examples (the spec's "LLM Router", collapsed)

"Research → Claude, review → GPT, security → local model" is just configuration:
tier maps + per-skill overrides, e.g. `security_review = "ollama/qwen3-coder"`. Policy
beyond static mapping (cost-aware fallback chains) is LiteLLM router config passed
through verbatim — we add no routing logic of our own.

## 4. Cost accounting

Every `step_completed` event carries `cost {tokens_in, tokens_out, usd}` (skills: from
LiteLLM; execution steps: from backend report, flagged `estimated` when the vendor CLI
doesn't report). Therefore:

- `flow-speckit runs show <id>` prints per-step and total cost.
- `flow-speckit trace <pr>` attributes cost along the lineage.
- The event log is the cost ledger — no separate metering system. OTel GenAI span
  attributes mirror the same numbers at v0.5.

## 5. `ContextAssembler` — context from artifacts

The one piece of "context engine" that survives: deterministic packing of upstream
artifacts into a skill prompt.

```python
async def assemble(self, *primary: ArtifactModel, budget_tokens: int = 24_000,
                   include: Literal["lineage", "primary-only"] = "lineage") -> AssembledContext
```

Algorithm:

1. **Walk lineage upward** from the primary inputs (`derived_from`/`informs` edges,
   latest approved versions only) up to depth 8.
2. **Order deterministically**: lineage depth descending (oldest reasoning first),
   then artifact `created_at` — so prompts are reproducible run-to-run.
3. **Budget by token count** (tokenizer via LiteLLM): full `body_md` for primaries;
   ancestors degrade gracefully — full body → summary field → one-line title+ref —
   until the budget fits. Never silently drop a primary.
4. **Cite**: every included artifact is rendered under a header carrying its ref
   (`design/csv-export@3`) so skill outputs can reference provenance, and skills are
   prompted to cite refs when they rely on an ancestor.

`AssembledContext` records exactly what was included at which fidelity — persisted in
the step event payload, so "what did the model actually see?" is always answerable
(prompt-audit requirement from the governance spec, satisfied structurally).

Future (post-v1, plugin): pgvector semantic search over artifact bodies for
cross-project retrieval ("find prior designs touching auth"). The `VectorStore` port
exists; nothing in the kernel calls it.
