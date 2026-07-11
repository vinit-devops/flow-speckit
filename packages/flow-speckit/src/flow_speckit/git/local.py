"""LocalGitProvider — local git operations (doc 05 §6).

Handles branches, worktrees, and diffs locally. PR operations raise
``NotImplementedError`` with guidance to install a remote provider plugin.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from flow_speckit.git.provider import GitProvider, PullRequestInfo, RepoRef, ReviewInfo


class LocalGitProvider:
    """Local-git-only provider. PR / review operations are not supported —
    install ``flow-speckit-github`` (or another provider plugin) for those."""

    name = "local"

    async def push_branch(self, workspace: Any) -> None:
        """Push the workspace's target branch to origin."""
        path = getattr(workspace, "path", workspace)
        proc = await asyncio.create_subprocess_exec(
            "git", "-C", str(path), "push", "origin", workspace.target_branch,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(stderr.decode().strip())

    async def open_pr(
        self,
        repo: RepoRef,
        head: str,
        base: str,
        title: str,
        body_md: str,
    ) -> PullRequestInfo:
        raise NotImplementedError(
            "PR creation requires a remote GitProvider plugin. "
            "Install flow-speckit-github or another provider."
        )

    async def get_pr(self, ref: str) -> PullRequestInfo:
        raise NotImplementedError(
            "PR fetching requires a remote GitProvider plugin."
        )

    async def list_reviews(self, pr: PullRequestInfo) -> list[ReviewInfo]:
        raise NotImplementedError(
            "PR review listing requires a remote GitProvider plugin."
        )