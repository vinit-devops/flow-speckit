"""LLM wrapper + context assembly (doc 06).

The two deliberately small components: a ~200-line policy layer over LiteLLM
and the deterministic artifact-context packer.
"""

from __future__ import annotations

from flow_speckit.llm.assemble import AssembledContext, ContextAssembler
from flow_speckit.llm.client import LLMClient
from flow_speckit.llm.tiers import LLMSpec, Tier, resolve_tier

__all__ = [
    "AssembledContext",
    "ContextAssembler",
    "LLMClient",
    "LLMSpec",
    "Tier",
    "resolve_tier",
]
