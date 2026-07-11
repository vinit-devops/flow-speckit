"""``@skill`` decorator and ``SkillContext`` — the contract (doc 04 §1).

Skills are stateless async functions of ``(input artifacts, ctx)``. The
decorator records metadata; the engine fetches input(s), validates, invokes,
and persists the output as a new artifact.

``SkillContext`` is the bounded capability surface handed to every skill
invocation. The engine wires the real LLM client, read-only artifact store,
config, logger and progress emitter — skills never touch the store's write
path or reach outside this surface.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

import structlog
from pydantic import BaseModel, ConfigDict, Field

from flow_speckit.llm.tiers import LLMSpec


class SkillDefinition(BaseModel):
    """Recorded metadata for one registered skill (doc 04 §3)."""

    model_config = ConfigDict(frozen=True)

    name: str
    version: str
    fn: Any = Field(default=None, repr=False)
    input_types: list[str] = Field(default_factory=list)
    output_type: str | None = None
    llm: LLMSpec | None = None
    provenance: str = "unknown"  # "local:<path>" or "package:<name>"


# ---------------------------------------------------------------------------
# SkillContext — the bounded capability surface
# ---------------------------------------------------------------------------


class SkillContext:
    """The ``ctx`` handed to every skill invocation (doc 04 §1).

    | Member          | Capability |
    |-----------------|------------|
    | ``ctx.llm``     | Tier-routed LiteLLM client |
    | ``ctx.artifacts``| **Read-only** store: ``get``, ``versions``, |
    |                 |   ``lineage``, ``search``, ``assemble`` |
    | ``ctx.config``  | Skill-scoped config from the workflow engine |
    | ``ctx.log``     | structlog logger pre-bound with run_id/step_key/skill |
    | ``ctx.emit_progress(msg)`` | Progress line into the run's event stream |
    """

    def __init__(
        self,
        *,
        skill_name: str,
        run_id: str = "",
        step_key: str = "",
        llm: Any = None,
        artifacts: Any = None,
        config: Mapping[str, Any] | None = None,
    ) -> None:
        self.skill_name = skill_name
        self.run_id = run_id
        self.step_key = step_key
        self.llm = llm  # LLMClient instance wired by engine
        self.artifacts = artifacts  # ArtifactStore (read-only) or SkillArtifactsHandle
        self.config: Mapping[str, Any] = MappingProxyType(dict(config or {}))
        self.log = structlog.get_logger(
            __name__,
            skill_name=skill_name,
            run_id=run_id,
            step_key=step_key,
        )

    def emit_progress(self, msg: str) -> None:
        """Progress line into the run's event stream (not a checkpoint)."""
        self.log.info("skill_progress", msg=msg)


# ---------------------------------------------------------------------------
# The @skill decorator
# ---------------------------------------------------------------------------


def skill(
    *,
    name: str,
    input: type[BaseModel] | tuple[type[BaseModel], ...] | None = None,
    output: type[BaseModel] | None = None,
    llm: LLMSpec | None = None,
    version: str = "0.1.0",
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Decorator that records skill contract metadata (doc 04 §1).

    ``input`` may be a single ``ArtifactModel`` subclass, a tuple of types
    (multi-input skills), or ``None`` (the skill receives no typed input
    artifact; optional context comes through ``ctx.artifacts.assemble``).
    """

    def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        input_names: list[str] = []
        output_name: str | None = None
        if input is not None:
            if isinstance(input, type):
                input_names = [getattr(input, "artifact_type", input.__name__)]
            else:
                input_names = [
                    getattr(t, "artifact_type", t.__name__) for t in input
                ]
        if output is not None:
            output_name = getattr(output, "artifact_type", output.__name__)

        definition = SkillDefinition(
            name=name,
            version=version,
            fn=fn,
            input_types=input_names,
            output_type=output_name,
            llm=llm,
            provenance="local",
        )
        fn._flow_speckit_skill = True  # type: ignore[attr-defined]
        fn._skill_definition = definition  # type: ignore[attr-defined]

        return fn

    return decorator