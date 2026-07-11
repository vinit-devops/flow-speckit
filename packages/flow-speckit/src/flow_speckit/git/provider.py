"""GitProvider port (doc 05 §6).

The kernel ships local-git operations; ``flow-speckit-github`` implements the
remote surface via ``gh`` CLI or PyGithub. GitLab/Bitbucket: post-v1 plugins
against this same protocol.
"""

from __future__ import annotations

from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict


class RepoRef(BaseModel):
    """Identity of a git repository."""

    model_config = ConfigDict(frozen=True)

    owner: str = ""
    name: str = ""
    url: str = ""


class PullRequestInfo(BaseModel):
    """A pull request as returned by the provider."""

    model_config = ConfigDict(frozen=True)

    number: int
    url: str
    title: str
    head_branch: str
    base_branch: str
    state: str = "open"


class ReviewInfo(BaseModel):
    """One review on a PR (v0.3 gates)."""

    model_config = ConfigDict(frozen=True)

    reviewer: str
    state: str  # "approved", "changes_requested", "commented"
    body: str = ""


class GitProvider(Protocol):
    """The port every git host adapter implements (doc 05 §6).

    ``name`` is the provider identifier (e.g. ``"github"``, ``"gitlab"``).
    """

    name: str

    async def push_branch(self, workspace: Any) -> None:
        """Push the workspace's target branch to the remote."""
        ...

    async def open_pr(
        self,
        repo: RepoRef,
        head: str,
        base: str,
        title: str,
        body_md: str,
    ) -> PullRequestInfo:
        """Open a pull request with the given title and markdown body."""
        ...

    async def get_pr(self, ref: str) -> PullRequestInfo:
        """Fetch PR details by URL or ``owner/repo#number``."""
        ...

    async def list_reviews(self, pr: PullRequestInfo) -> list[ReviewInfo]:
        """List all reviews on a PR (v0.3 gates)."""
        ...