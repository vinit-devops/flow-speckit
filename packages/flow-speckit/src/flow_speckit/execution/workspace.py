"""WorkspaceManager — git worktree lifecycle (doc 05 §2).

Core owns the workspace: prepare (worktree add + branch), capture (commit
uncommitted changes, diff, collect commits), and cleanup. Adapters only edit
files.
"""

from __future__ import annotations

import asyncio
import shutil
from pathlib import Path
from typing import ClassVar

from flow_speckit.execution.base import Workspace


_RUN_WORKTREES = ".flow-speckit/wt"


class WorkspaceManager:
    """Manages git worktrees for execution tasks (doc 05 §2).

    All git operations are subprocess-based via ``asyncio.create_subprocess_exec``,
    keeping the dependency surface minimal.
    """

    _GIT: ClassVar[str] = "git"

    async def prepare(
        self,
        repo_root: Path,
        base_branch: str,
        run_id: str,
    ) -> Workspace:
        """Create a git worktree on a new branch for *run_id*.

        Branch name: ``flow-speckit/run-<run_id>``.
        Worktree path: ``<repo_root>/.flow-speckit/wt/<run_id>``.
        """
        branch = f"flow-speckit/run-{run_id}"
        wt_path = repo_root / _RUN_WORKTREES / run_id
        wt_path.parent.mkdir(parents=True, exist_ok=True)

        # Create branch from base
        await self._run_git(
            repo_root, "checkout", "-b", branch, base_branch
        )
        # Add worktree
        await self._run_git(
            repo_root, "worktree", "add", str(wt_path), branch
        )

        return Workspace(
            path=wt_path,
            repo=str(repo_root),
            base_branch=base_branch,
            target_branch=branch,
        )

    async def capture(self, workspace: Workspace) -> tuple[list[str], str]:
        """Capture uncommitted changes and return (commit_shas, unified_diff).

        1. Add and commit any uncommitted changes (message: "flow-speckit: checkpoint").
        2. Collect all commits on the branch since base.
        3. Generate unified diff `base...target`.
        """
        wt = workspace.path
        # Check for uncommitted changes
        status_proc = await asyncio.create_subprocess_exec(
            self._GIT, "-C", str(wt), "status", "--porcelain",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        status_out, _ = await status_proc.communicate()
        if status_out.strip():
            await self._run_git(wt, "add", "-A")
            await self._run_git(
                wt, "commit", "-m", "flow-speckit: checkpoint"
            )

        # Collect commits
        proc = await asyncio.create_subprocess_exec(
            self._GIT, "-C", str(wt), "log", "--format=%H",
            f"{workspace.base_branch}..{workspace.target_branch}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        log_out, _ = await proc.communicate()
        commits = [
            sha for sha in log_out.decode().strip().split("\n") if sha
        ]

        # Unified diff
        diff_proc = await asyncio.create_subprocess_exec(
            self._GIT, "-C", str(wt), "diff",
            workspace.base_branch,
            workspace.target_branch,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        diff_out, _ = await diff_proc.communicate()
        diff_text = diff_out.decode()

        return commits, diff_text

    async def cleanup(self, workspace: Workspace, *, keep_on_failure: bool = True) -> None:
        """Remove the worktree and optionally the branch.

        ``keep_on_failure``: always cleanup the worktree directory but
        preserve the branch for inspection when ``True`` (default).
        """
        # Prune the worktree metadata
        await self._run_git(
            workspace.path.parent,
            "worktree", "prune",
        )

        # Remove the worktree directory
        if workspace.path.exists():
            await asyncio.to_thread(shutil.rmtree, str(workspace.path))

        # Remove the worktree from git's list
        try:
            await self._run_git(
                Path(workspace.repo),
                "worktree", "remove", str(workspace.path),
                "--force",
            )
        except Exception:
            pass  # worktree already doesn't exist in git's view

    @staticmethod
    async def _run_git(cwd: Path, *args: str) -> tuple[int, str, str]:
        proc = await asyncio.create_subprocess_exec(
            WorkspaceManager._GIT, *args,
            cwd=str(cwd) if cwd.is_dir() else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            msg = stderr.decode().strip() or f"git {' '.join(args)} failed"
            raise RuntimeError(msg)
        return proc.returncode or 0, stdout.decode(), stderr.decode()