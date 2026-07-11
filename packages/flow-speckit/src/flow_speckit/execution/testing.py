"""Execution conformance suite (doc 05 §4).

Functional checks any backend adapter must pass against a prepared
workspace. Implemented today:

1. ``check_available`` reports honestly
2. Trivial task → completed, file created
3. Timeout respected (long task → failed/partial, workspace intact)
4. Cost report present on every result

Still to come (v0.2): allowed_paths enforcement, cancellation mid-run,
crash-rerun convergence.
"""

from __future__ import annotations

from flow_speckit.execution.base import (
    BackendHealth,
    CostReport,
    ExecutionBackend,
    ExecutionConstraints,
    ExecutionResult,
    ExecutionTask,
    Workspace,
)


async def _null_sink(message: str) -> None:
    """Event sink that discards progress lines."""


async def check_backend_available(backend: ExecutionBackend) -> None:
    """1. check_available reports honestly (present case)."""
    health = await backend.check_available()
    assert isinstance(health, BackendHealth), (
        "check_available must return BackendHealth"
    )
    assert health.available is True, (
        f"Backend {backend.name} not available: {health.message}"
    )


async def check_trivial_task(
    backend: ExecutionBackend, workspace: Workspace
) -> None:
    """2. Trivial task ("create FILE with CONTENT") → completed."""
    task = ExecutionTask(
        instructions="printf hello > conformance-output.txt",
        constraints=ExecutionConstraints(timeout_s=30),
    )
    result = await backend.execute(task, workspace, _null_sink)
    assert isinstance(result, ExecutionResult)
    assert result.status == "completed", result.summary
    out_file = workspace.path / "conformance-output.txt"
    assert out_file.exists(), "task must create conformance-output.txt"
    assert out_file.read_text().strip() == "hello"


async def check_timeout(
    backend: ExecutionBackend, workspace: Workspace
) -> None:
    """3. Respects timeout (long task terminated, failed/partial, workspace intact)."""
    task = ExecutionTask(
        instructions="sleep 9999",
        constraints=ExecutionConstraints(timeout_s=2),
    )
    result = await backend.execute(task, workspace, _null_sink)
    assert result.status in ("failed", "partial"), result.status
    assert workspace.path.exists(), "workspace must survive a timeout"


async def check_cost_report(
    backend: ExecutionBackend, workspace: Workspace
) -> None:
    """4. Cost report present on every result."""
    task = ExecutionTask(
        instructions="true",
        constraints=ExecutionConstraints(timeout_s=30),
    )
    result = await backend.execute(task, workspace, _null_sink)
    assert isinstance(result.cost, CostReport)


async def run_conformance_suite(
    backend: ExecutionBackend, workspace: Workspace
) -> None:
    """Run every implemented conformance check against *backend*."""
    await check_backend_available(backend)
    await check_trivial_task(backend, workspace)
    await check_timeout(backend, workspace)
    await check_cost_report(backend, workspace)
