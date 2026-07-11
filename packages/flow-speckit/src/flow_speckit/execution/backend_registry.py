"""BackendRegistry — discovers and resolves ExecutionBackend implementations.

Discovers backends from the ``flow_speckit.backends`` entry-point group and
the in-core ``local_shell`` reference. Resolves ``settings.execution.backend``
by name so doctor, the worker, and the workflow engine all get the same
backend without hardcoding.
"""

from __future__ import annotations

from typing import Any

import structlog

from flow_speckit.execution.base import ExecutionBackend
from flow_speckit.execution.local_shell import LocalShellBackend
from flow_speckit.plugins import discover_entry_points

logger = structlog.get_logger(__name__)


class BackendRegistry:
    """In-memory registry of available execution backends."""

    def __init__(self) -> None:
        self._backends: dict[str, ExecutionBackend] = {}

    def discover(self) -> None:
        """Load in-core backends and entry-point backends."""
        # In-core reference is always available
        local = LocalShellBackend()
        self._backends[local.name] = local

        # Entry-point backends
        for name, obj in discover_entry_points("flow_speckit.backends"):
            backend: ExecutionBackend
            if isinstance(obj, ExecutionBackend):
                backend = obj
            elif callable(obj):
                backend = obj()
            else:
                logger.warning(
                    "backend_entry_point_skipped",
                    name=name,
                    reason=f"expected ExecutionBackend, got {type(obj).__name__}",
                )
                continue
            self._backends[name] = backend

    def get(self, name: str) -> ExecutionBackend:
        """Resolve a backend by name. Raises ``KeyError`` if not found."""
        if not self._backends:
            self.discover()
        backend = self._backends.get(name)
        if backend is None:
            raise KeyError(
                f"No backend named {name!r}. Available: "
                f"{', '.join(sorted(self._backends))}. "
                f"Run `flow-speckit backends list` to see installed backends."
            )
        return backend

    def list_all(self) -> list[ExecutionBackend]:
        if not self._backends:
            self.discover()
        return list(self._backends.values())