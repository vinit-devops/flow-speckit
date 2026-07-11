"""Step handlers — the Phase 5 integration layer (doc 03 §5).

Three StepHandler implementations bridge the workflow engine to the Phase 4
subsystems: the skill engine (run_skill), the execution engine (execute), and
the git provider (open_pr).

Each handler is a stateless callable implementing the ``StepHandler`` protocol;
the handler map is built by :func:`build_handler_map` from settings + registries
at engine startup and passed through ``WorkflowEngine`` → ``WorkflowContext``
→ ``_run_handler_step``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog
from pydantic_core import to_jsonable_python

from flow_speckit.workflows.context import StepHandler, StepInvocation, StepResult

if TYPE_CHECKING:
    from flow_speckit.artifacts.store import ArtifactStore
    from flow_speckit.execution.backend_registry import BackendRegistry
    from flow_speckit.llm.client import LLMClient
    from flow_speckit.skills.registry import SkillRegistry

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Skill step handler
# ---------------------------------------------------------------------------


class SkillStepHandler:
    """Executes a ``run_skill`` step: resolves the skill from the registry,
    assembles the input artifacts via the read-only artifact store, wires a
    ``SkillContext`` with the LLM client, and invokes the skill function.

    The skill's output is stored as a new artifact via the write path (the
    store passed to this handler is the full ArtifactStore — the skill itself
    sees only the read-only surface via ``SkillContext.artifacts``).
    """

    def __init__(
        self,
        skill_registry: SkillRegistry,
        llm_client: LLMClient | None,
        artifact_store: ArtifactStore | None = None,
        config: dict[str, Any] | None = None,
    ) -> None:
        self._registry = skill_registry
        self._llm = llm_client
        self._store = artifact_store
        self._config = config or {}

    async def __call__(self, step: StepInvocation) -> StepResult:
        from flow_speckit.skills.base import SkillContext

        payload = step.payload
        input_value = payload.get("input")
        skill_name = step.label

        # If the label is the skill name directly, or extract from payload
        # The DSL step label is the skill name per convention (doc 04 §3)
        try:
            definition = self._registry.get(skill_name)
        except Exception as exc:
            return StepResult(
                result={
                    "error": f"Unknown skill {skill_name!r}: {exc}",
                    "status": "failed",
                }
            )

        # Build the read-only artifact surface
        read_only_store: Any = None
        if self._store is not None:
            read_only_store = SkillArtifactsHandle(self._store)

        # Build SkillContext
        ctx = SkillContext(
            skill_name=skill_name,
            run_id=str(step.run_id),
            step_key=step.step_key,
            llm=self._llm,
            artifacts=read_only_store,
            config=self._config,
        )

        # Resolve input artifacts from refs in the payload
        fn_args: tuple[Any, ...] = ()
        if definition.input_types:
            # Input value may be a ref address string or a dict of refs
            input_refs = self._resolve_input_refs(input_value)
            fn_args = (input_refs,)

        try:
            output = await definition.fn(*fn_args, ctx)
        except Exception as exc:
            logger.error(
                "skill_execution_failed",
                skill_name=skill_name,
                run_id=str(step.run_id),
                step_key=step.step_key,
                error=str(exc),
            )
            return StepResult(
                result={
                    "error": str(exc),
                    "status": "failed",
                    "skill_name": skill_name,
                }
            )

        # Persist the output if the store is available AND the output is an
        # artifact model
        output_ref: str | None = None
        if self._store is not None and hasattr(output, "artifact_type"):
            try:
                stored = await self._store.save(output)
                output_ref = stored.ref_address
            except Exception as exc:
                logger.warning(
                    "artifact_save_failed",
                    skill_name=skill_name,
                    error=str(exc),
                )

        result = to_jsonable_python(output)
        if output_ref:
            result = {"artifact_ref": output_ref, "data": result}

        return StepResult(
            result={
                "status": "completed",
                "skill_name": skill_name,
                "output": result,
                "artifact_ref": output_ref,
            }
        )

    def _resolve_input_refs(self, value: Any) -> Any:
        """If value is an artifact ref address string, resolve it; otherwise
        pass through as-is (the skill function handles further resolution)."""
        if isinstance(value, str) and self._store is not None:
            try:
                from flow_speckit.artifacts.refs import ArtifactRef

                ref = ArtifactRef.from_address(value)
                return self._store.get(ref)
            except Exception:
                pass
        return value


class SkillArtifactsHandle:
    """Read-only artifact store surface handed to a skill's ``SkillContext``.

    Exposes only read methods: ``get``, ``versions``, ``lineage``, ``search``,
    ``assemble``. The write path is never exposed.
    """

    def __init__(self, store: ArtifactStore) -> None:
        self._store = store

    async def get(self, *args: Any, **kwargs: Any) -> Any:
        return await self._store.get(*args, **kwargs)

    async def versions(self, *args: Any, **kwargs: Any) -> Any:
        return await self._store.versions(*args, **kwargs)

    async def lineage(self, *args: Any, **kwargs: Any) -> Any:
        return await self._store.lineage(*args, **kwargs)

    async def search(self, *args: Any, **kwargs: Any) -> Any:
        return await self._store.search(*args, **kwargs)

    async def assemble(self, *args: Any, **kwargs: Any) -> Any:
        return await self._store.assemble(*args, **kwargs)


# ---------------------------------------------------------------------------
# Execute step handler
# ---------------------------------------------------------------------------


class ExecuteStepHandler:
    """Dispatches a coding task to an :class:`ExecutionBackend` (doc 05 §1).

    Resolves the backend by name from the :class:`BackendRegistry`, prepares a
    workspace via :class:`WorkspaceManager`, and runs the execution task.
    Constraints from the step payload are applied to the execution.
    """

    def __init__(
        self,
        backend_registry: BackendRegistry,
        workspace_manager: Any = None,
    ) -> None:
        self._registries = backend_registry
        self._workspace_mgr = workspace_manager

    async def __call__(self, step: StepInvocation) -> StepResult:
        from flow_speckit.execution.base import (
            ExecutionConstraints,
            ExecutionTask,
            Workspace,
        )

        payload = step.payload
        backend_name: str = payload.get("backend", "local_shell")
        plan: Any = payload.get("plan", {})
        constraints_raw: Any = payload.get("constraints")

        # Resolve the backend
        try:
            backend = self._registries.get(backend_name)
        except KeyError as exc:
            return StepResult(
                result={
                    "error": str(exc),
                    "status": "failed",
                    "backend": backend_name,
                }
            )

        # Build constraints
        constraints = ExecutionConstraints()
        if isinstance(constraints_raw, dict):
            constraints = ExecutionConstraints.model_validate(constraints_raw)

        # Extract instructions from the plan
        instructions = ""
        if isinstance(plan, dict):
            instructions = plan.get("instructions", plan.get("prompt", str(plan)))
        elif isinstance(plan, str):
            instructions = plan

        task = ExecutionTask(
            instructions=instructions,
            constraints=constraints,
        )

        # Prepare workspace if manager is available
        import tempfile

        workspace = Workspace(
            path=tempfile.mkdtemp(prefix="flow-speckit-"),  # guaranteed to exist
            repo="",
            base_branch="main",
            target_branch=f"flow-speckit/{step.run_id}",
        )

        if self._workspace_mgr is not None:
            try:
                workspace = await self._workspace_mgr.create(
                    base_branch="main",
                    target_branch=f"flow-speckit/{step.run_id}",
                )
            except Exception as exc:
                logger.warning(
                    "workspace_create_failed",
                    run_id=str(step.run_id),
                    error=str(exc),
                )

        # Execute
        health = await backend.check_available()
        if not health.available:
            return StepResult(
                result={
                    "error": f"Backend {backend_name!r} is not available: {health.message}",
                    "status": "failed",
                }
            )

        async def progress_sink(msg: str) -> None:
            logger.info(
                "backend_progress",
                run_id=str(step.run_id),
                step_key=step.step_key,
                backend=backend_name,
                msg=msg,
            )

        try:
            exec_result = await backend.execute(task, workspace, progress_sink)
        except Exception as exc:
            return StepResult(
                result={
                    "error": f"Execution failed: {exc}",
                    "status": "failed",
                    "backend": backend_name,
                }
            )

        return StepResult(
            result={
                "status": exec_result.status,
                "summary": exec_result.summary,
                "commits": [
                    {"sha": c.sha, "message": c.message, "author": c.author}
                    for c in exec_result.commits
                ],
                "diff_ref": exec_result.diff_ref,
                "cost": {
                    "tokens_in": exec_result.cost.tokens_in,
                    "tokens_out": exec_result.cost.tokens_out,
                    "usd": exec_result.cost.usd,
                },
            }
        )


# ---------------------------------------------------------------------------
# Open PR step handler
# ---------------------------------------------------------------------------


class OpenPRStepHandler:
    """Opens a pull request through the GitProvider port (doc 05 §6).

    The GitHub provider plugin does not exist yet — this handler raises a
    descriptive error when no provider is available, explaining how to install
    or register one.
    """

    def __init__(self, git_provider: Any = None) -> None:
        self._provider = git_provider

    async def __call__(self, step: StepInvocation) -> StepResult:
        from flow_speckit.git.provider import (
            RepoRef,
        )

        if self._provider is None:
            return StepResult(
                result={
                    "error": (
                        "No Git provider registered. Install a provider plugin "
                        "(e.g. flow-speckit-github) or register one via the "
                        "flow_speckit.git_providers entry point group. "
                        "The open_pr step requires a remote git provider."
                    ),
                    "status": "unimplemented",
                }
            )

        payload = step.payload
        change_raw = payload.get("change", {})
        review_raw = payload.get("review")

        # Extract PR details from the change payload
        title = "Flow Speckit — Automated Changes"
        body = ""
        head_branch = ""
        base_branch = ""

        if isinstance(change_raw, dict):
            title = change_raw.get("title", title)
            body = change_raw.get("body", change_raw.get("description", ""))
            head_branch = change_raw.get("head_branch", change_raw.get("head", ""))
            base_branch = change_raw.get(
                "base_branch", change_raw.get("base", "main")
            )

        if isinstance(review_raw, str):
            body = f"{body}\n\n{review_raw}".strip()

        repo = RepoRef()
        try:
            pr_info = await self._provider.open_pr(
                repo=repo,
                head=head_branch,
                base=base_branch,
                title=title,
                body_md=body,
            )
        except NotImplementedError:
            return StepResult(
                result={
                    "error": (
                        "open_pr is not implemented by the registered provider. "
                        "The GitHub plugin (flow-speckit-github) is expected for "
                        "this step kind."
                    ),
                    "status": "unimplemented",
                }
            )
        except Exception as exc:
            return StepResult(
                result={
                    "error": f"Failed to open PR: {exc}",
                    "status": "failed",
                }
            )

        return StepResult(
            result={
                "status": "completed",
                "pr_number": pr_info.number,
                "pr_url": pr_info.url,
                "pr_title": pr_info.title,
                "head_branch": pr_info.head_branch,
                "base_branch": pr_info.base_branch,
                "state": pr_info.state,
            }
        )


# ---------------------------------------------------------------------------
# Handler map builder — the single seam that wires Phase 4→5
# ---------------------------------------------------------------------------


def build_handler_map(
    *,
    skill_registry: SkillRegistry,
    backend_registry: BackendRegistry,
    llm_client: LLMClient | None = None,
    artifact_store: Any = None,
    workspace_manager: Any = None,
    git_provider: Any = None,
    config: dict[str, Any] | None = None,
) -> dict[str, StepHandler]:
    """Build the complete handler map for a :class:`WorkflowEngine`.

    This is the single integration point where the Phase 4 subsystems (skill
    engine, LLM client, execution backends, git providers) are wired into the
    Phase 3 workflow engine. Every ``StepKindUnavailableError`` previously
    raised for "skill", "execute" or "open_pr" steps is resolved by passing
    the result of this function to ``WorkflowEngine(handlers=...)``.
    """
    handlers: dict[str, StepHandler] = {}

    # Skill handler — always available when a registry is provided
    if skill_registry is not None:
        handlers["skill"] = SkillStepHandler(
            skill_registry=skill_registry,
            llm_client=llm_client,
            artifact_store=artifact_store,
            config=config,
        )

    # Execute handler — always available; local_shell is the in-core fallback
    if backend_registry is not None:
        handlers["execute"] = ExecuteStepHandler(
            backend_registry=backend_registry,
            workspace_manager=workspace_manager,
        )

    # Open PR handler — only registered when a provider is available;
    # without one the step still executes (and the handler surfaces the
    # "no provider" message as a step result rather than
    # StepKindUnavailableError).
    handlers["open_pr"] = OpenPRStepHandler(git_provider=git_provider)

    return handlers