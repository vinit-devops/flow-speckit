"""Shared registry collision policy (doc 04 §3, doc 01 §6).

SkillRegistry, WorkflowRegistry and ArtifactRegistry apply the same
precedence when two registrations collide on the same key: local overrides
installed, installed never clobbers local, and two installed packages
colliding is a hard error. ``resolve_collision`` is that policy in one
place; each registry maps the ``"collision"`` outcome to its own error type
and does its own logging.
"""

from __future__ import annotations

from typing import Literal

_LOCAL = "local"

CollisionDecision = Literal["replace", "keep_existing", "collision"]


class RegistryCollision(RuntimeError):
    """Two installed packages registered the same key."""


def _is_local(provenance: str) -> bool:
    # "local" exactly, or a "local:<path>" provenance — but not an installed
    # package that happens to be named "local-something".
    return provenance == _LOCAL or provenance.startswith("local:")


def resolve_collision(
    new_provenance: str, existing_provenance: str
) -> CollisionDecision:
    """Decide what happens when *new* collides with *existing* at one key.

    - local over anything → ``"replace"`` (the last local registration wins)
    - installed over local → ``"keep_existing"`` (never clobber a local override)
    - installed over installed → ``"collision"`` (caller raises its error type)
    """
    if _is_local(new_provenance):
        return "replace"
    if _is_local(existing_provenance):
        return "keep_existing"
    return "collision"
