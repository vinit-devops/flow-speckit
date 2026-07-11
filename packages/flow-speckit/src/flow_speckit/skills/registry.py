"""SkillRegistry — lookup by name (optionally ``name@version``) (doc 04 §3).

Two sources feed the registry:
1. Entry points — installed packages declare ``[project.entry-points."flow_speckit.skills"]``.
2. Project-local — ``./skills/*.py`` in the target repo auto-loads at startup.

Name collisions: local overrides installed with a startup warning; two
installed packages colliding is a hard error naming both.
"""

from __future__ import annotations

from typing import Any

import structlog

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

    # -- registration -----------------------------------------------------------

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
                if definition.provenance.startswith("local") and not existing.provenance.startswith("local"):
                    # Local overrides installed — replace with warning
                    logger.warning(
                        "skill_local_override",
                        skill_name=raw.name,
                        version=definition.version,
                        replaced_from=existing.provenance,
                    )
                    entries.remove(existing)
                    break
                if not definition.provenance.startswith("local") and existing.provenance.startswith("local"):
                    # Installed version cannot override a local skill — keep local, warn
                    logger.warning(
                        "skill_override_ignored",
                        skill_name=raw.name,
                        version=definition.version,
                        attempted_from=definition.provenance,
                        kept_from=existing.provenance,
                    )
                    return existing
                # Two identical provenance levels — hard error (doc 04 §3)
                raise RuntimeError(
                    f"Skill name collision: {raw.name!r} version {definition.version!r} "
                    f"from {definition.provenance!r} conflicts with "
                    f"version {existing.version!r} from {existing.provenance!r}"
                )
        entries.append(definition)
        return definition

    # -- lookup ---------------------------------------------------------------

    def get(self, name: str, version: str | None = None) -> SkillDefinition:
        """Return the skill definition matching *name* and optionally *version*.

        ``None`` version returns the latest registered version (highest
        version string via lexicographic compare — simple for v0.1; full
        semver sorting at v0.2).
        """
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
        """Convenience: return the latest version of *name*."""
        return self.get(name)

    def list_all(self) -> list[SkillDefinition]:
        """Return every registered skill, sorted by name then version."""
        result: list[SkillDefinition] = []
        for entries in self._by_name.values():
            result.extend(entries)
        result.sort(key=lambda d: (d.name, d.version))
        return result