"""Plugin / entry-point discovery for all groups (doc 01 §6).

Scans the project-local directories (``./skills/``, ``./workflows/`` in the
target repo) and the installed package entry points at startup, populating
the ArtifactRegistry, SkillRegistry, WorkflowRegistry and backend/provider
maps that every other subsystem depends on.
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Project-local discovery
# ---------------------------------------------------------------------------


def discover_local_skills(root: Path) -> Iterator[Callable[..., Any]]:
    """Yield ``@skill``-decorated functions from ``<root>/skills/*.py``."""
    skills_dir = root / "skills"
    if not skills_dir.is_dir():
        return
    for py_file in sorted(skills_dir.glob("*.py")):
        if py_file.name.startswith("_"):
            continue
        try:
            mod = _import_file(py_file)
        except Exception:
            logger.warning("skill_load_failed", file=str(py_file), exc_info=True)
            continue
        for name in dir(mod):
            obj = getattr(mod, name)
            if callable(obj) and getattr(obj, "_flow_speckit_skill", False):
                yield obj


def discover_local_workflows(root: Path) -> Iterator[Any]:
    """Yield workflow definitions from ``<root>/workflows/*.{py,yaml}``."""
    wf_dir = root / "workflows"
    if not wf_dir.is_dir():
        return
    for wf_file in sorted(wf_dir.glob("*")):
        if wf_file.name.startswith("_"):
            continue
        if wf_file.suffix == ".py":
            try:
                mod = _import_file(wf_file)
            except Exception:
                logger.warning("workflow_load_failed", file=str(wf_file), exc_info=True)
                continue
            for name in dir(mod):
                obj = getattr(mod, name)
                if callable(obj) and getattr(obj, "_flow_speckit_workflow", False):
                    yield obj
        elif wf_file.suffix in (".yaml", ".yml"):
            # YAML workflows are loaded by the registry itself
            yield wf_file


# ---------------------------------------------------------------------------
# Entry-point discovery (all groups)
# ---------------------------------------------------------------------------

_ENTRY_POINT_GROUPS = {
    "flow_speckit.artifacts",
    "flow_speckit.skills",
    "flow_speckit.workflows",
    "flow_speckit.backends",
    "flow_speckit.git_providers",
    "flow_speckit.notifiers",
    "flow_speckit.storage",
}


def discover_entry_points(group: str) -> Iterator[tuple[str, Any]]:
    """Yield ``(name, loaded_object)`` for every entry point in *group*.

    Wraps ``ep.load()`` failures with the entry-point name so callers get
    actionable diagnostics (phase3-followups item).
    """
    if group not in _ENTRY_POINT_GROUPS:
        return
    if sys.version_info >= (3, 12):
        from importlib.metadata import entry_points

        eps = entry_points(group=group)
    else:
        from importlib.metadata import entry_points  # type: ignore[no-redef]

        eps = entry_points().get(group, ())
    for ep in eps:
        try:
            yield ep.name, ep.load()
        except Exception:
            logger.warning(
                "entry_point_load_failed",
                group=group,
                entry_point=ep.name,
                exc_info=True,
            )
            # Re-raise with the entry-point name for actionable errors
            raise RuntimeError(
                f"Failed to load entry point {ep.name!r} from group {group!r}"
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _import_file(path: Path) -> Any:
    """Import a .py file as a module, keyed by its absolute path."""
    name = f"_flow_speckit_local_{path.stem}_{hash(str(path.absolute())) & 0xFFFFFFFF}"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module