"""SkillRegistry — lookup by name (optionally ``name@version``) (doc 04 §3).

Two sources feed the registry:
1. Entry points — installed packages declare ``[project.entry-points."flow_speckit.skills"]``.
2. Project-local — ``./skills/*.py`` in the target repo auto-loads at startup.

Uses ``flow_speckit.shared.resolve_collision`` for collision policy (shared
with WorkflowRegistry and ArtifactRegistry).
"""

from __future__ import annotations

from typing import Any

import structlog

from flow_speckit.shared import RegistryCollision, resolve_collision
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
        definition = raw.model_copy(update={"provenance": provenance})
        entries = self._by_name.setdefault(raw.name, [])
        for existing in entries:
            if existing.version != definition.version:
                continue
            decision = resolve_collision(provenance, existing.provenance)
            if decision == "keep_existing":
                logger.warning(
                    "skill_override_ignored",
                    skill_name=raw.name,
                    version=definition.version,
                    attempted_from=provenance,
                    kept_from=existing.provenance,
                )
                return existing
            if decision == "replace":
                logger.warning(
                    "skill_local_override",
                    skill_name=raw.name,
                    version=definition.version,
                    replaced_from=existing.provenance,
                )
                entries.remove(existing)
                break
            raise RegistryCollision(
                f"Skill {raw.name!r} version {definition.version!r} is "
                f"already registered from {existing.provenance!r}; cannot "
                f"re-register from {provenance!r}"
            )
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
