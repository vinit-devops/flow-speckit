"""Tier-based LLM specification (doc 06 §§2, 3).

Skills declare tier-names, never model names. Users map tiers to models
in `flow-speckit.toml` under ``[llm.tiers]``.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

Tier = Literal["fast", "standard", "reasoning"]


class LLMSpec(BaseModel):
    """Declared LLM needs of a skill (doc 04 §1, doc 06 §2)."""

    model_config = ConfigDict(frozen=True)

    tier: Tier = "standard"
    max_cost_usd: float = 5.0


def resolve_tier(
    spec: LLMSpec,
    tier_map: dict[str, str],
    *,
    skill_name: str | None = None,
    overrides: dict[str, str] | None = None,
) -> str:
    """Resolve a tier → model string.

    Priority: per-skill override → tier map key.
    """
    overrides = overrides or {}
    if skill_name is not None and skill_name in overrides:
        return overrides[skill_name]
    if spec.tier in tier_map:
        return tier_map[spec.tier]
    raise KeyError(
        f"No model configured for tier {spec.tier!r}. "
        "Add it to [llm.tiers] in flow-speckit.toml."
    )
