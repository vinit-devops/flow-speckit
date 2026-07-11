"""Integration context builder — constructs the handler map from settings + registries.

This is the single seam called by CLI/open_workflow_env and tests to build
a fully wired WorkflowEngine with skill, execute, and open_pr handlers.
Previously, ``open_workflow_env`` created a bare engine with no handlers,
so any workflow step of kind ``skill``, ``execute`` or ``open_pr`` would
fail with ``StepKindUnavailableError``.

Call :func:`build_handlers` from ``open_workflow_env`` (or any caller) and
pass the result to ``WorkflowEngine(handlers=...)``.
"""

from __future__ import annotations

from typing import Any

import structlog

from flow_speckit.config import FlowSpeckitSettings
from flow_speckit.execution.backend_registry import BackendRegistry
from flow_speckit.llm.client import LLMClient
from flow_speckit.skills.registry import SkillRegistry
from flow_speckit.workflows.context import StepHandler
from flow_speckit.workflows.handlers import build_handler_map

logger = structlog.get_logger(__name__)


def _build_llm_client(settings: FlowSpeckitSettings) -> LLMClient | None:
    """Construct an LLMClient from settings, or return None if no tiers are
    configured. When no model tiers are set, skills that call ctx.llm will
    get a descriptive error via the null client."""
    llm_config = settings.llm
    if not llm_config.tiers:
        return None
    return LLMClient(
        tier_map=llm_config.tiers,
        overrides=llm_config.overrides,
        default_max_usd_per_run=llm_config.default_max_usd_per_run,
    )


def build_handlers(
    settings: FlowSpeckitSettings,
    *,
    skill_registry: SkillRegistry | None = None,
    backend_registry: BackendRegistry | None = None,
    artifact_store: Any = None,
    git_provider: Any = None,
    config: dict[str, Any] | None = None,
) -> dict[str, StepHandler]:
    """Build the complete step-handler map from settings and registries.

    Args:
        settings: Loaded flow-speckit settings (from env + flow-speckit.toml).
        skill_registry: Populated SkillRegistry; if None, skill steps will fail.
        backend_registry: Populated BackendRegistry; if None, execute steps fail.
        artifact_store: Full ArtifactStore for skills; if None, artifact read/write is skipped.
        git_provider: GitProvider instance; if None, open_pr steps get a descriptive error.
        config: Additional config passed to SkillContext (e.g. skill-scoped config).

    Returns:
        A handler map suitable for ``WorkflowEngine(handlers=...)``.
    """
    llm_client = _build_llm_client(settings)

    if skill_registry is None:
        skill_registry = SkillRegistry()

    if backend_registry is None:
        backend_registry = BackendRegistry()
        backend_registry.discover()

    return build_handler_map(
        skill_registry=skill_registry,
        backend_registry=backend_registry,
        llm_client=llm_client,
        artifact_store=artifact_store,
        workspace_manager=None,  # Phase 5 follow-up: wire WorkspaceManager
        git_provider=git_provider,
        config=config,
    )