"""local_shell backend — the in-core reference (doc 05 §3).

~60-line adapter that runs a configured command in a workspace. The
always-working demo/CI backend and the template for adapter authors.
"""

from __future__ import annotations

import asyncio

from flow_speckit.execution.base import (
    BackendHealth,
    CostReport,
    ExecutionBackend,
    ExecutionEventSink,
    ExecutionResult,
    ExecutionTask,
    Workspace,
)


class LocalShellBackend:
    """Runs a shell command inside the prepared workspace.

    Dispatches the task's ``instructions`` as a command via ``sh -c``.
    Configuration key ``[execution.local_shell]`` may supply a ``command``
    override (e.g. ``make ai-task`` or any CLI).
    """

    name = "local_shell"

    def __init__(
        self, *, command: str | None = None, shell: str = "/bin/sh"
    ) -> None:
        self._command = command
        self._shell = shell

    async def check_available(self) -> BackendHealth:
        """Always available — it's the shell on PATH."""
        proc = await asyncio.create_subprocess_exec(
            self._shell,
            "--version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        version = stdout.decode().strip().split("\n")[0] if stdout else self._shell
        return BackendHealth(available=True, version=version)

    async def execute(
        self,
        task: ExecutionTask,
        workspace: Workspace,
        events: ExecutionEventSink,
    ) -> ExecutionResult:
        """Execute the task's instructions as a shell command in the workspace."""
        cmd = self._command or task.instructions
        await events(f"[local_shell] executing: {cmd[:200]}")

        try:
            proc = await asyncio.create_subprocess_exec(
                self._shell,
                "-c",
                cmd,
                cwd=str(workspace.path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(),
                timeout=task.constraints.timeout_s,
            )
            logs = []
            if stdout_bytes:
                logs.append(f"STDOUT:\n{stdout_bytes.decode(errors='replace')}")
            if stderr_bytes:
                logs.append(f"STDERR:\n{stderr_bytes.decode(errors='replace')}")
            logs_text = "\n".join(logs)

            status: str = "completed" if proc.returncode == 0 else "failed"
            return ExecutionResult(
                status=status,
                summary=logs_text[:2000],
                logs_ref="",  # blob-stored at caller level
                cost=CostReport(estimated=True),
            )
        except asyncio.TimeoutError:
            return ExecutionResult(
                status="failed",
                summary=f"Command timed out after {task.constraints.timeout_s}s",
                cost=CostReport(estimated=True),
            )
        except Exception as exc:
            return ExecutionResult(
                status="failed",
                summary=str(exc),
                cost=CostReport(estimated=True),
            )