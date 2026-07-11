"""Shared collision-policy helper for registries (review follow-up).

SkillRegistry, WorkflowRegistry and ArtifactRegistry all implement the same
local-overrides-installed precedence with a hard error on identical provenance.
This module extracts the shared logic so the three registries share one policy.
"""

from __future__ import annotations

from typing import TypeVar

_KT = TypeVar("_KT")
_LOCAL = "local"


class RegistryCollision(RuntimeError):
    """Two installed packages registered the same key."""


def local_override(
    existing: _KT,
    definition: _KT,
    *,
    key: str,
    provenance: str,
    existing_provenance: str,
) -> _KT | None:
    """Apply the local-overrides-installed collision policy.

    Returns ``None`` if the definition should be kept and appended normally
    (it does not collide with an existing entry at this name/version).
    Returns the *existing* definition if the new one should be silently
    dropped.  Raises ``RegistryCollision`` when two non-local sources collide.

    The caller is responsible for any replacement (removing existing + adding
    definition) when this function returns ``None`` and the newer provenance
    is local.
    """
    definition_is_local = provenance == _LOCAL or provenance.startswith("local")
    existing_is_local = existing_provenance == _LOCAL or existing_provenance.startswith("local")

    if definition_is_local and not existing_is_local:
        return None  # local overrides installed

    if not definition_is_local and existing_is_local:
        return existing  # installed must not clobber local

    if definition_is_local and existing_is_local:
        return None  # two locals: last-wins

    raise RegistryCollision(
        f"Key {key!r} already registered from {existing_provenance!r}; "
        f"cannot re-register from {provenance!r}"
    )