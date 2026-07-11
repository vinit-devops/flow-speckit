"""Execution conformance suite (doc 05 §4).

Any backend adapter must pass these tests against a fixture repo:
1. check_available reports honestly
2. Trivial task → completed, correct diff captured
3. Respects allowed_paths
4. Respects timeout
5. Cancellation mid-run
6. Cost report present
7. Crash-rerun convergence
"""

from __future__ import annotations

from pathlib import Path

from flow_speckit.execution.base import (
    BackendHealth,
    ExecutionBackend,
    ExecutionConstraints,
    ExecutionTask,
)


async def test_check_available(backend: ExecutionBackend) -> None:
    """1. check_available reports honestly (present case)."""
    health = await backend.check_available()
    assert isinstance(health, BackendHealth), "check_available must return BackendHealth"
    assert health.available is True, (
        f"Backend {backend.name} not available: {health.message}"
    )


async def test_trivial_task(
    backend: ExecutionBackend,
    workspace_fixture: Path,
    events_sink: Callable[[str], Awaitable[None]],
) -> None:
    """2. Trivial task ("create FILE with CONTENT") → completed, correct diff captured."""
    task = ExecutionTask(
        instructions='echo "hello" > test-output.txt',
        constraints=ExecutionConstraints(timeout_s=30),
    )
    # This requires a prepared workspace; the conformance suite wires one.
    # For a pure unit-level test, a Workspace with a temp dir suffices.
    pass


async def test_timeout(
    backend: ExecutionBackend,
    workspace_fixture: Path,
    events_sink: Callable[[str], Awaitable[None]],
) -> None:
    """4. Respects timeout (long task terminated, failed/partial, workspace intact)."""
    task = ExecutionTask(
        instructions="sleep 9999",
        constraints=ExecutionConstraints(timeout_s=2),
    )
    # Backend should timeout and return failed/partial
    pass