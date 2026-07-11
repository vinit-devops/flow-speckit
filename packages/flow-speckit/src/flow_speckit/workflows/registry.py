from __future__ import annotations

import importlib.util
import sys
from dataclasses import replace
from importlib.metadata import entry_points
from pathlib import Path

import structlog

from flow_speckit.shared import resolve_collision
from flow_speckit.workflows.dsl import WorkflowDefinition

logger = structlog.get_logger(__name__)

_LOCAL = "local"

ENTRY_POINT_GROUP = "flow_speckit.workflows"

_LOCAL_MODULE_PREFIX = "flow_speckit_local_workflows"


class UnknownWorkflow(KeyError):
    """Raised when looking up a workflow that has not been registered."""


class WorkflowCollisionError(RuntimeError):
    """Raised when two installed packages register the same (name, version)."""


class WorkflowRegistry:
    def __init__(self) -> None:
        self._workflows: dict[tuple[str, str], WorkflowDefinition] = {}

    def register(
        self, definition: WorkflowDefinition, source_package: str | None = None
    ) -> None:
        if source_package is not None and source_package != definition.source_package:
            definition = replace(definition, source_package=source_package)

        key = definition.key
        name, version = key
        existing = self._workflows.get(key)
        if existing is None:
            self._workflows[key] = definition
            return

        if existing.fn is definition.fn:
            return  # re-registering the identical workflow is a no-op

        decision = resolve_collision(
            definition.source_package, existing.source_package
        )
        if decision == "replace":
            # A local registration always overrides whatever was there before.
            logger.warning(
                "workflow_local_override",
                workflow=name,
                version=version,
                previous_source=existing.source_package,
                new_source=definition.source_package,
            )
            self._workflows[key] = definition
            return

        if decision == "keep_existing":
            # An installed package must never silently clobber a local override.
            logger.warning(
                "workflow_local_override_kept",
                workflow=name,
                version=version,
                local_source=existing.source_package,
                ignored_source=definition.source_package,
            )
            return

        raise WorkflowCollisionError(
            f"Workflow {name!r} version {version!r} is already registered by package "
            f"{existing.source_package!r}; cannot re-register from package "
            f"{definition.source_package!r}"
        )

    def get(self, name: str, version: str) -> WorkflowDefinition:
        definition = self._workflows.get((name, version))
        if definition is None:
            raise UnknownWorkflow(f"{name} (version={version})")
        return definition

    def latest(self, name: str) -> WorkflowDefinition:
        """Return the highest-versioned definition for ``name``.

        Purely-numeric versions (the documented convention: "1", "2", ...)
        rank above and among themselves numerically; any non-numeric versions
        rank below, ordered lexicographically.
        """
        candidates = [d for (n, _), d in self._workflows.items() if n == name]
        if not candidates:
            raise UnknownWorkflow(name)

        def sort_key(definition: WorkflowDefinition) -> tuple[int, int, str]:
            v = definition.version
            if v.isdigit():
                return (1, int(v), "")
            return (0, 0, v)

        return max(candidates, key=sort_key)

    def load_entry_points(self, group: str = ENTRY_POINT_GROUP) -> None:
        for ep in entry_points(group=group):
            try:
                loaded = ep.load()
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to load workflow entry point {ep.name!r} ({ep.value!r})"
                ) from exc
            if not isinstance(loaded, WorkflowDefinition):
                raise RuntimeError(
                    f"Workflow entry point {ep.name!r} ({ep.value!r}) did not resolve "
                    f"to a WorkflowDefinition, got {type(loaded).__name__}"
                )
            source_package = ep.dist.name if ep.dist is not None else ep.module
            self.register(loaded, source_package=source_package)

    def discover_local(self, root: Path) -> None:
        """Import ``<root>/workflows/*.py`` and register every definition found.

        Registrations carry local provenance, so they override installed
        packages per the collision rules above.
        """
        workflows_dir = root / "workflows"
        if not workflows_dir.is_dir():
            return
        for path in sorted(workflows_dir.glob("*.py")):
            module_name = f"{_LOCAL_MODULE_PREFIX}.{path.stem}"
            spec = importlib.util.spec_from_file_location(module_name, path)
            if spec is None or spec.loader is None:
                raise RuntimeError(f"Cannot import local workflow module {path}")
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            try:
                spec.loader.exec_module(module)
            except Exception as exc:
                del sys.modules[module_name]
                raise RuntimeError(
                    f"Failed to import local workflow module {path}"
                ) from exc
            for value in vars(module).values():
                if isinstance(value, WorkflowDefinition):
                    self.register(value, source_package=_LOCAL)

    def all(self) -> list[WorkflowDefinition]:
        return list(self._workflows.values())


registry = WorkflowRegistry()
