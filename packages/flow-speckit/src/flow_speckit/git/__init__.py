"""Git provider port and local implementation (doc 05 §6)."""

from __future__ import annotations

from flow_speckit.git.local import LocalGitProvider
from flow_speckit.git.provider import GitProvider, PullRequestInfo, RepoRef, ReviewInfo

__all__ = [
    "GitProvider",
    "LocalGitProvider",
    "PullRequestInfo",
    "RepoRef",
    "ReviewInfo",
]
