"""Execution Engine — delegation port (doc 05).

The ``ExecutionBackend`` protocol, ``ExecutionTask`` / ``Workspace`` /
``ExecutionResult`` models, and the conformance suite. Core owns the
workspace lifecycle; adapters only edit files (and optionally commit).
"""

from __future__ import annotations

from flow_speckit.execution.base import (
    BackendHealth,
    ExecutionBackend,
    ExecutionConstraints,
    ExecutionResult,
    ExecutionTask,
    Workspace,
)
from flow_speckit.execution.workspace import WorkspaceManager

__all__ = [
    "BackendHealth",
    "ExecutionBackend",
    "ExecutionConstraints",
    "ExecutionResult",
    "ExecutionTask",
    "Workspace",
    "WorkspaceManager",
]