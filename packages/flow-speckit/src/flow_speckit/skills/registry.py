"""SkillRegistry — lookup by name (optionally ``name@version``) (doc 04 §3).

Two sources feed the registry:
1. Entry points — installed packages declare ``[project.entry-points."flow_speckit.skills"]``.
2. Project-local — ``./skills/*.py`` in the target repo auto-loads at startup.

Uses ``flow_speckit.shared.local_override`` for collision policy (shared with
WorkflowRegistry and ArtifactRegistry).
"""

from __future__ import annotations

from typing import Any

import structlog

from flow_speckit.shared import RegistryCollision, local_override
from flow_speckit.skills.base import SkillDefinition

logger = structlog.get_logger(__name__)


class UnknownSkill(LookupError):
    """Raised when a requested skill name (and optionally version) is not
    registered."""

    def __init__(self, name: str, version: str | None = None) -> None:
        key = f"{name}@{version}" if version else name
        super().__init__(f"No registered skill {key!r}")


class SkillRegistry:
    """In-memory registry of all discovered skills."""

    def __init__(self) -> None:
        self._by_name: dict[str, list[SkillDefinition]] = {}

    def register(self, skill_fn: Any, *, provenance: str = "unknown") -> SkillDefinition:
        """Register a ``@skill``-decorated function."""
        raw: SkillDefinition | None = getattr(skill_fn, "_skill_definition", None)
        if raw is None:
            raise TypeError(
                f"{skill_fn!r} is not a @skill-decorated function. "
                "Use the @skill decorator before registering."
            )
        definition = SkillDefinition(
            name=raw.name,
            version=raw.version,
            fn=raw.fn,
            input_types=raw.input_types,
            output_type=raw.output_type,
            llm=raw.llm,
            provenance=provenance,
        )
        entries = self._by_name.setdefault(raw.name, [])
        for existing in entries:
            if existing.version == definition.version:
                key = f"{raw.name}@{definition.version}"
                decision = local_override(
                    existing,
                    definition,
                    key=key,
                    provenance=provenance,
                    existing_provenance=existing.provenance,
                )
                if decision is existing:
                    logger.warning(
                        "skill_override_ignored",
                        skill_name=raw.name,
                        version=definition.version,
                        attempted_from=provenance,
                        kept_from=existing.provenance,
                    )
                    return existing
                if decision is None:
                    if provenance == "local" or provenance.startswith("local"):
                        logger.warning(
                            "skill_local_override",
                            skill_name=raw.name,
                            version=definition.version,
                            replaced_from=existing.provenance,
                        )
                        entries.remove(existing)
                        break
                    # non-local over non-local → RegistryCollision raised
                    raise RegistryCollision(
                        f"Skill name collision: {raw.name!r} version "
                        f"{definition.version!r} from {provenance!r} conflicts "
                        f"with version {existing.version!r} from "
                        f"{existing.provenance!r}"
                    )
                # decision is definition (unreachable for registries)
        entries.append(definition)
        return definition

    def get(self, name: str, version: str | None = None) -> SkillDefinition:
        entries = self._by_name.get(name)
        if entries is None or len(entries) == 0:
            raise UnknownSkill(name, version)
        if version is None:
            return max(entries, key=lambda d: d.version)
        for entry in entries:
            if entry.version == version:
                return entry
        raise UnknownSkill(name, version)

    def latest(self, name: str) -> SkillDefinition:
        return self.get(name)

    def list_all(self) -> list[SkillDefinition]:
        result: list[SkillDefinition] = []
        for entries in self._by_name.values():
            result.extend(entries)
        result.sort(key=lambda d: (d.name, d.version))
        return result