# 04 — Skill Engine

> Skills are stateless AI functions with typed artifact I/O. This doc defines the
> contract every skill author codes against, discovery, and the testing harness.

## 1. The contract

```python
from flow_speckit.skills import skill, SkillContext, LLMSpec
from flow_speckit_skills_product.artifacts import FrameBrief, ProductArtifact

@skill(
    name="product_shaping",
    input=FrameBrief,                    # ArtifactModel type, tuple of types, or dict spec
    output=ProductArtifact,
    llm=LLMSpec(tier="reasoning", max_cost_usd=2.0),
    version="1.0",
)
async def product_shaping(brief: FrameBrief, ctx: SkillContext) -> ProductArtifact:
    context = await ctx.artifacts.assemble(brief)            # lineage-aware prompt context
    return await ctx.llm.complete(
        SHAPING_PROMPT,
        context=context,
        response_model=ProductArtifact,                      # structured output, validated
    )
```

Rules the decorator + engine enforce:

1. **Stateless.** A skill is a pure async function of `(input artifacts, ctx)`. It holds
   no memory between invocations; there is no session object to reach for.
2. **Typed I/O.** `input`/`output` declare artifact types. The engine fetches and
   validates inputs, and persists the returned model as a new artifact with
   `derived_from` edges wired automatically — skills never touch the store's write path.
3. **Idempotent.** Retries are engine policy (`ctx.run_skill(retry=...)`), never skill
   logic. Re-running a skill with identical inputs must produce an equivalent artifact
   (identical `content_hash` dedups).
4. **Declared LLM needs.** `LLMSpec(tier=..., max_cost_usd=...)` — tiers, never model
   names (doc 06 §2). A skill is portable across providers by construction.
5. **Bounded surface.** Everything a skill may do arrives via `ctx`; anything not on
   `ctx` (network, filesystem, subprocess) is out of contract. v0.1 enforces by review +
   lint; sandboxing is a later hardening step.

### `SkillContext` — the whole capability surface

| Member | Capability |
|---|---|
| `ctx.llm` | Tier-routed LiteLLM client: `complete()`, `complete_streaming()`; structured output via `response_model`; cost metered against run budget (doc 06) |
| `ctx.artifacts` | **Read-only** store: `get`, `versions`, `lineage`, `search`, `assemble` (doc 02 §8, doc 06 §5) |
| `ctx.config` | Skill-scoped config (from `flow-speckit.toml` `[skills.<name>]`) |
| `ctx.log` | structlog logger pre-bound with run_id/step_key/skill |
| `ctx.emit_progress(msg)` | Progress line into the run's event stream (not a checkpoint) |

Multi-input skills declare `input=(TechnicalDesign, ProductArtifact)` and receive them
as ordered arguments; optional reference inputs (lineage-fetched context) come through
`assemble`, not the signature.

## 2. Skill packs — the demoted "engines"

The spec's product/engineering "engines" ship as ordinary skill packs:

| Package | Skills |
|---|---|
| `flow-speckit-skills-product` | `frame` (idea → FrameBrief), `research` (FrameBrief → GenericArtifact research memo), `product_shaping` (FrameBrief → ProductArtifact: scope, MVP cut, acceptance criteria, risks) |
| `flow-speckit-skills-engineering` | `technical_design` (ProductArtifact → TechnicalDesign), `task_planning` (TechnicalDesign → TaskPlan), `code_review` (CodeChange → ReviewReport), `fetch_change` (PR URL → CodeChange) |

Each skill = prompt template (package data, versioned with the skill) + the function
above. Prompts are ordinary files a team can fork; `version` bumps when prompt or
contract changes meaningfully.

## 3. Discovery & registration

Two mechanisms, one registry:

1. **Entry points** — installed packages declare `[project.entry-points."flow_speckit.skills"]`;
   `pip install flow-speckit-skills-product` and the skills appear.
2. **Project-local** — `./skills/*.py` in the target repo auto-loads at startup; a
   team's first custom skill needs zero packaging.

`flow-speckit skills list` shows every registered skill with name, version, I/O types, tier,
and provenance (package vs local path). Name collisions: local overrides installed, with
a startup warning; two installed packages colliding is a hard error naming both.

Registry lookup is by `name` (optionally `name@version`); workflows reference skills by
name, so swapping a skill implementation is a registry change, not a workflow edit.

## 4. Testing harness (`flow_speckit.skills.testing`)

Shipped with the kernel so skill quality is testable without a live workflow:

```python
from flow_speckit.skills.testing import SkillHarness

async def test_shaping_produces_mvp_cut(harness: SkillHarness):
    brief = FrameBrief(problem="...", constraints=[...])
    result = await harness.run("product_shaping", brief,
                               llm=RecordedLLM("fixtures/shaping-1.json"))
    assert result.mvp_scope
    assert result.acceptance_criteria
```

- `SkillHarness` provides an in-memory artifact store and a fake `ctx`.
- `RecordedLLM` replays recorded completions (record mode hits the real provider and
  saves fixtures) — deterministic CI without API keys.
- Contract checks run automatically: output validates against the declared type,
  no out-of-contract capability was touched, cost stayed under `max_cost_usd` in
  record mode.
- The same harness backs a future eval story (scored fixtures per skill); explicitly
  out of scope for v0.1 beyond the fixture mechanism.
