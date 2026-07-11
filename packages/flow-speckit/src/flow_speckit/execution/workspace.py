"""WorkspaceManager — git worktree lifecycle (doc 05 §2).

Core owns the workspace: prepare (worktree add + branch), capture (commit
uncommitted changes, diff, collect commits), and cleanup. Adapters only edit
files.
"""

from __future__ import annotations

import asyncio
import contextlib
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

        Uses ``git worktree add -b <branch> <path> <base>`` in a single
        command — never touches the main working tree.
        """
        branch = f"flow-speckit/run-{run_id}"
        wt_path = repo_root / _RUN_WORKTREES / run_id
        wt_path.parent.mkdir(parents=True, exist_ok=True)

        await self._run_git(
            repo_root,
            "worktree",
            "add",
            "-b",
            branch,
            str(wt_path),
            base_branch,
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
        # Commit any uncommitted changes as a checkpoint
        _, status_out, _ = await self._run_git(wt, "status", "--porcelain")
        if status_out.strip():
            await self._run_git(wt, "add", "-A")
            await self._run_git(wt, "commit", "-m", "flow-speckit: checkpoint")

        # Commit list and merge-base diff are independent reads of the same
        # finalized branch state — run the two git processes concurrently.
        # Three-dot diff so commits added to base after divergence never show
        # up as spurious removals.
        (_, log_out, _), (_, diff_text, _) = await asyncio.gather(
            self._run_git(
                wt,
                "log",
                "--format=%H",
                f"{workspace.base_branch}..{workspace.target_branch}",
            ),
            self._run_git(
                wt,
                "diff",
                f"{workspace.base_branch}...{workspace.target_branch}",
            ),
        )
        commits = [sha for sha in log_out.strip().split("\n") if sha]
        return commits, diff_text

    async def cleanup(
        self, workspace: Workspace, *, keep_on_failure: bool = True
    ) -> None:
        """Remove the worktree and optionally the branch.

        ``git worktree remove --force`` deletes the directory itself, so no
        separate ``rmtree`` or pre-emptive ``prune`` is needed. ``prune`` on
        the main repo afterward is safety-only for stale metadata.

        ``keep_on_failure=True`` (default) preserves the run branch for
        inspection; ``False`` deletes it too.
        """
        repo = Path(workspace.repo)
        # Suppressed failures below mean the worktree/branch is already gone.
        with contextlib.suppress(Exception):
            await self._run_git(
                repo,
                "worktree",
                "remove",
                str(workspace.path),
                "--force",
            )

        # Prune stale metadata (defensive)
        with contextlib.suppress(Exception):
            await self._run_git(repo, "worktree", "prune")

        if not keep_on_failure:
            with contextlib.suppress(Exception):
                await self._run_git(
                    repo, "branch", "-D", workspace.target_branch
                )

    @staticmethod
    async def _run_git(cwd: Path, *args: str) -> tuple[int, str, str]:
        proc = await asyncio.create_subprocess_exec(
            WorkspaceManager._GIT,
            *args,
            cwd=str(cwd) if cwd.is_dir() else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            msg = stderr.decode().strip() or f"git {' '.join(args)} failed"
            raise RuntimeError(msg)
        return proc.returncode or 0, stdout.decode(), stderr.decode()
