"""The ``@workflow`` authoring DSL (doc 03 §8).

Python ``@workflow`` is the canonical authoring path: the decorator pins a
``(name, version)`` pair on an ``async def fn(ctx, ...)`` and produces an
immutable :class:`WorkflowDefinition` that the registry and engine consume.
``workflow_version`` is pinned per run at ``run_started``, so a definition is
identified by ``(name, version)`` and never mutated in place.
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

WorkflowFn = Callable[..., Awaitable[Any]]

_LOCAL = "local"


@dataclass(frozen=True)
class WorkflowDefinition:
    """An immutable, versioned workflow definition.

    ``source_package`` records provenance, mirroring the artifact registry:
    ``"local"`` for project-local definitions, the distribution name for
    definitions loaded from installed packages.
    """

    name: str
    version: str
    fn: WorkflowFn
    source_package: str = _LOCAL

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("workflow name must be a non-empty string")
        if not isinstance(self.version, str) or not self.version:
            raise ValueError("workflow version must be a non-empty string")

    @property
    def key(self) -> tuple[str, str]:
        return (self.name, self.version)

    def __call__(self, *args: Any, **kwargs: Any) -> Awaitable[Any]:
        return self.fn(*args, **kwargs)


def workflow(
    *, name: str, version: str, source_package: str = _LOCAL
) -> Callable[[WorkflowFn], WorkflowDefinition]:
    """Declare an ``async def fn(ctx, ...)`` as a workflow.

    Returns a decorator producing a frozen :class:`WorkflowDefinition` whose
    ``fn`` preserves the wrapped function's metadata (``functools.wraps``);
    the original function stays reachable via ``definition.fn.__wrapped__``.
    """

    def decorate(fn: WorkflowFn) -> WorkflowDefinition:
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(f"@workflow requires an async def function, got {fn!r}")

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await fn(*args, **kwargs)

        return WorkflowDefinition(
            name=name, version=version, fn=wrapper, source_package=source_package
        )

    return decorate
