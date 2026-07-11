"""Execution port: protocol, task/result models, and backend health (doc 05 §1).

Every coding-agent backend implements the ``ExecutionBackend`` protocol.
Adapters live in separate packages (e.g., ``flow-speckit-backend-claude-code``)
or the in-core ``local_shell`` reference.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict


class BackendHealth(BaseModel):
    """Preflight check result: binary on PATH? authenticated? version-supported?"""

    model_config = ConfigDict(frozen=True)

    available: bool
    message: str = ""
    version: str | None = None


class ExecutionConstraints(BaseModel):
    """Limits on a single execution task (doc 05 §1)."""

    model_config = ConfigDict(frozen=True)

    timeout_s: int = 600
    max_cost_usd: float = 100.0
    allowed_paths: list[str] | None = None
    network: Literal["inherit", "none"] = "inherit"


class Workspace(BaseModel):
    """A prepared git worktree handed to the backend adapter (doc 05 §1)."""

    model_config = ConfigDict(frozen=True)

    path: Path
    repo: str  # RepoRef placeholder — full RepoRef model at v0.2
    base_branch: str
    target_branch: str


class ExecutionTask(BaseModel):
    """One task for a backend to execute (doc 05 §1)."""

    model_config = ConfigDict(frozen=True)

    instructions: str
    task_plan_ref: str = ""  # ArtifactRef address string
    constraints: ExecutionConstraints = ExecutionConstraints()


class CommitInfo(BaseModel):
    """One commit captured by core post-run."""

    model_config = ConfigDict(frozen=True)

    sha: str
    message: str
    author: str


class CostReport(BaseModel):
    """Token and USD cost (as reported or estimated)."""

    model_config = ConfigDict(frozen=True)

    tokens_in: int = 0
    tokens_out: int = 0
    usd: float = 0.0
    estimated: bool = True


class ExecutionResult(BaseModel):
    """The backend's result after executing a task (doc 05 §1)."""

    model_config = ConfigDict(frozen=True)

    status: Literal["completed", "failed", "partial"]
    summary: str = ""
    commits: list[CommitInfo] = []
    diff_ref: str = ""  # BlobRef — blob-stored unified diff
    logs_ref: str = ""  # BlobRef — full backend transcript
    cost: CostReport = CostReport()


# Event sink: the backend may stream progress lines during execution.
ExecutionEventSink = Callable[[str], Awaitable[None]]
"""``async def sink(message: str) -> None`` — streamed progress line."""


class ExecutionBackend(Protocol):
    """The port every coding-agent adapter implements (doc 05 §1).

    ``name`` is the backend identifier used in workflow DSL step
    ``backend:`` keys and ``flow-speckit backends list``.
    """

    name: str

    async def check_available(self) -> BackendHealth:
        """Preflight: binary on PATH? authenticated? version-supported?

        Returns actionable diagnostics; never raises.
        """
        ...

    async def execute(
        self,
        task: ExecutionTask,
        workspace: Workspace,
        events: ExecutionEventSink,
    ) -> ExecutionResult:
        """Run the task inside the prepared workspace.

        May stream progress via ``events``. Must respect task.constraints.
        Must be cancellable (SIGTERM → grace period → SIGKILL).
        """
        ...
