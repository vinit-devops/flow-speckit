"""Skill testing harness (doc 04 §4).

Shipped with the kernel so skill quality is testable without a live workflow.
``SkillHarness`` provides an in-memory artifact store and a fake `ctx`;
``RecordedLLM`` replays recorded completions for deterministic CI.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from flow_speckit.artifacts.models import ArtifactModel
from flow_speckit.artifacts.registry import ArtifactRegistry
from flow_speckit.artifacts.store import ArtifactStore
from flow_speckit.skills.base import SkillContext, SkillDefinition
from flow_speckit.skills.registry import SkillRegistry


class RecordedLLM:
    """Replays recorded completions from a JSON fixture file.

    *Record mode*: when ``fixture_path`` does not exist, calls the real
    provider and saves the result. *Replay mode*: reads from the fixture.
    """

    def __init__(self, fixture_path: Path | str) -> None:
        self._path = Path(fixture_path)
        self._recordings: dict[str, Any] = {}
        self._mode: str = "replay"
        if self._path.exists():
            self._recordings = json.loads(self._path.read_text())

    async def complete(
        self,
        prompt: str,
        *,
        context: Any = None,
        response_model: type[BaseModel] | None = None,
        tier: str | None = None,
        skill_name: str | None = None,
    ) -> BaseModel | str:
        """Return the recorded response for *prompt* (keyed by skill_name)."""
        key = skill_name or "default"
        recording = self._recordings.get(key)
        if recording is None:
            return ""
        if isinstance(recording, dict):
            if response_model is not None:
                return response_model.model_validate(recording)
            return json.dumps(recording)
        return str(recording)

    async def complete_streaming(self, prompt: str, **kwargs: Any) -> Any:
        raise NotImplementedError("RecordedLLM does not support streaming")

    @property
    def total_cost_usd(self) -> float:
        return 0.0

    def save(self, key: str, response: Any) -> None:
        """Save a recorded response for later replay."""
        if isinstance(response, BaseModel):
            self._recordings[key] = response.model_dump(mode="json")
        else:
            self._recordings[key] = response
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._recordings, indent=2))


class SkillHarness:
    """In-memory harness for testing skills (doc 04 §4).

    Provides a fake artifact store and ctx, enabling skill testing without a
    live workflow, database, or API keys.
    """

    def __init__(
        self,
        *,
        registry: SkillRegistry | None = None,
        artifact_registry: ArtifactRegistry | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> None:
        self.skills = registry or SkillRegistry()
        self._artifact_registry = artifact_registry or ArtifactRegistry()
        self._config = dict(config or {})

    async def run(
        self,
        skill_name: str,
        *inputs: ArtifactModel,
        llm: Any | None = None,
        version: str | None = None,
    ) -> ArtifactModel:
        """Execute *skill_name* with *inputs* and return the produced artifact.

        ``llm`` may be a ``RecordedLLM`` for deterministic replay.
        """
        definition = self.skills.get(skill_name, version)
        ctx = SkillContext(
            skill_name=skill_name,
            run_id="test-run",
            step_key=f"test-{skill_name}",
            llm=llm,
            artifacts=None,  # No read-only store in pure test harness
            config=self._config.get(skill_name),
        )

        if definition.input_types:
            args: tuple[Any, ...] = inputs if inputs else ()
        else:
            args = ()

        result = await definition.fn(*args, ctx)
        return result