"""Phase 5 handler integration tests — validates the three step handlers
wired into a WorkflowEngine via build_handler_map / build_handlers.

These tests exercise the full path: step invocation → handler resolution →
subsystem execution → result checkpointing. They use the in-memory skill
registry and BackendRegistry (local_shell), no live LLM or git provider.
"""

from __future__ import annotations

from uuid import uuid4

from flow_speckit.config import FlowSpeckitSettings
from flow_speckit.execution.backend_registry import BackendRegistry
from flow_speckit.skills.base import skill
from flow_speckit.skills.registry import SkillRegistry
from flow_speckit.workflows.builder import build_handlers
from flow_speckit.workflows.context import (
    StepInvocation,
)
from flow_speckit.workflows.dsl import workflow
from flow_speckit.workflows.engine import WorkflowEngine
from flow_speckit.workflows.handlers import build_handler_map
from flow_speckit.workflows.registry import WorkflowRegistry

# ---------------------------------------------------------------------------
# Handler contract: each handler is a StepHandler callable
# ---------------------------------------------------------------------------


class TestHandlerContract:
    """Each handler instance implements the StepHandler protocol."""

    def test_skill_handler_is_callable(self):
        reg = SkillRegistry()

        @skill(name="echo")
        async def echo_fn(ctx):
            return {"ok": True}

        reg.register(echo_fn)
        backend_reg = BackendRegistry()
        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )
        handler = handler_map["skill"]
        # StepHandler is a Protocol (not runtime_checkable) — verify by
        # calling it, not via isinstance.
        assert callable(handler)

    def test_execute_handler_is_callable(self):
        reg = SkillRegistry()
        backend_reg = BackendRegistry()
        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )
        handler = handler_map["execute"]
        assert callable(handler)

    def test_open_pr_handler_is_callable(self):
        reg = SkillRegistry()
        backend_reg = BackendRegistry()
        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )
        handler = handler_map["open_pr"]
        assert callable(handler)


# ---------------------------------------------------------------------------
# Skill handler: resolves and executes a registered skill
# ---------------------------------------------------------------------------


class TestSkillHandlerExecution:
    async def test_skill_runs_and_returns_result(self):
        reg = SkillRegistry()

        @skill(name="greet")
        async def greet_fn(ctx):
            return {"greeting": "hello from skill"}

        reg.register(greet_fn)
        backend_reg = BackendRegistry()
        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )
        handler = handler_map["skill"]

        inv = StepInvocation(
            run_id=uuid4(),
            step_key="greet",
            step_kind="skill",
            label="greet",
            attempt=1,
            payload={"input": None},
        )
        result = await handler(inv)
        assert result.result["status"] == "completed"
        assert result.result["skill_name"] == "greet"
        assert result.result["output"]["greeting"] == "hello from skill"

    async def test_unknown_skill_returns_error(self):
        reg = SkillRegistry()
        backend_reg = BackendRegistry()
        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )
        handler = handler_map["skill"]

        inv = StepInvocation(
            run_id=uuid4(),
            step_key="nonexistent",
            step_kind="skill",
            label="nonexistent",
            attempt=1,
            payload={"input": None},
        )
        result = await handler(inv)
        assert "error" in result.result
        assert result.result["status"] == "failed"

    async def test_skill_with_no_input_types(self):
        """Skill that takes only ctx (no typed input model) — the common case."""
        reg = SkillRegistry()

        @skill(name="no_input")
        async def no_input_fn(ctx):
            return {"value": 123}

        reg.register(no_input_fn)
        backend_reg = BackendRegistry()
        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )
        handler = handler_map["skill"]

        inv = StepInvocation(
            run_id=uuid4(),
            step_key="no_input",
            step_kind="skill",
            label="no_input",
            attempt=1,
            payload={"input": {"extra": "ignored"}},
        )
        result = await handler(inv)
        assert result.result["status"] == "completed"
        assert result.result["output"]["value"] == 123


# ---------------------------------------------------------------------------
# Execute handler: dispatches to local_shell
# ---------------------------------------------------------------------------


class TestExecuteHandlerExecution:
    async def test_execute_trivial_command(self, tmp_path):
        reg = SkillRegistry()
        backend_reg = BackendRegistry()
        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )
        handler = handler_map["execute"]

        inv = StepInvocation(
            run_id=uuid4(),
            step_key="run_echo",
            step_kind="execute",
            label="run_echo",
            attempt=1,
            payload={
                "plan": {"instructions": "echo hello"},
                "backend": "local_shell",
                "constraints": {"timeout_s": 10},
            },
        )
        result = await handler(inv)
        assert result.result["status"] == "completed"

    async def test_execute_unknown_backend_returns_error(self):
        reg = SkillRegistry()
        backend_reg = BackendRegistry()
        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )
        handler = handler_map["execute"]

        inv = StepInvocation(
            run_id=uuid4(),
            step_key="bad_backend",
            step_kind="execute",
            label="bad_backend",
            attempt=1,
            payload={
                "plan": {"instructions": "echo x"},
                "backend": "nonexistent_backend",
            },
        )
        result = await handler(inv)
        assert "error" in result.result
        assert result.result["status"] == "failed"

    async def test_execute_timeout(self, tmp_path):
        reg = SkillRegistry()
        backend_reg = BackendRegistry()
        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )
        handler = handler_map["execute"]

        inv = StepInvocation(
            run_id=uuid4(),
            step_key="timeout_test",
            step_kind="execute",
            label="timeout_test",
            attempt=1,
            payload={
                "plan": {"instructions": "sleep 60"},
                "backend": "local_shell",
                "constraints": {"timeout_s": 1},
            },
        )
        result = await handler(inv)
        assert result.result["status"] == "failed"


# ---------------------------------------------------------------------------
# Open PR handler: descriptive error when no provider exists
# ---------------------------------------------------------------------------


class TestOpenPRHandler:
    async def test_no_provider_returns_unimplemented(self):
        reg = SkillRegistry()
        backend_reg = BackendRegistry()
        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )
        handler = handler_map["open_pr"]

        inv = StepInvocation(
            run_id=uuid4(),
            step_key="pr",
            step_kind="open_pr",
            label="pr",
            attempt=1,
            payload={
                "change": {
                    "title": "Test PR",
                    "body": "Automated changes",
                },
            },
        )
        result = await handler(inv)
        assert result.result["status"] == "unimplemented"
        assert "No Git provider" in result.result["error"]


# ---------------------------------------------------------------------------
# Builder: from settings
# ---------------------------------------------------------------------------


class TestBuilder:
    def test_build_handlers_from_settings(self):
        settings = FlowSpeckitSettings()
        backend_reg = BackendRegistry()
        handlers = build_handlers(
            settings, backend_registry=backend_reg
        )
        assert "skill" in handlers
        assert "execute" in handlers
        assert "open_pr" in handlers

    def test_build_handlers_with_llm_tiers(self):
        settings = FlowSpeckitSettings(
            llm={
                "tiers": {"fast": "anthropic/claude-haiku-4-5"},
                "overrides": {},
                "default_max_usd_per_run": 10.0,
            }
        )
        backend_reg = BackendRegistry()
        handlers = build_handlers(
            settings, backend_registry=backend_reg
        )
        assert "skill" in handlers
        # LLM client is constructed with tier map but no live provider
        assert callable(handlers["skill"])

    def test_build_handler_map_minimal(self):
        reg = SkillRegistry()
        backend_reg = BackendRegistry()
        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )
        assert set(handler_map) == {"skill", "execute", "open_pr"}


# ---------------------------------------------------------------------------
# End-to-end: workflow with skill + execute steps
# ---------------------------------------------------------------------------


class TestEndToEndWorkflow:
    """A workflow using skill + execute + intrinsic steps, driven through
    the engine with a real database-backed event log."""

    async def test_workflow_runs_skill_step(self, db_session_factory):
        """Register a skill and a workflow that calls it, then execute."""
        reg = SkillRegistry()

        @skill(name="add_numbers")
        async def add_numbers_fn(ctx):
            return {"sum": 42}

        reg.register(add_numbers_fn)
        backend_reg = BackendRegistry()

        wf_reg = WorkflowRegistry()

        @workflow(name="test_wf", version="1.0")
        async def test_wf_body(ctx):
            result = await ctx.run_skill("add_numbers", input={})
            return {"skill_result": result}

        wf_reg.register(test_wf_body)

        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )

        engine = WorkflowEngine(
            db_session_factory,
            wf_reg,
            handlers=handler_map,
            auto_approve=True,
        )

        run_id = await engine.start_run(
            "test_wf", "1.0", {}, actor="test"
        )

        outcome = await engine.execute_run(run_id)
        # The workflow should complete with the skill result
        if outcome.status == "waiting_timer":
            # local_shell may execute fast enough, but handle timing edge
            outcome = await engine.execute_run(run_id)

        if outcome.status == "pending":
            outcome = await engine.execute_run(run_id)

        assert outcome.status == "completed", (
            f"Expected completed, got {outcome.status}: {outcome.error}"
        )

    async def test_workflow_runs_execute_step(self, db_session_factory):
        """Register a workflow that dispatches an execute step."""
        reg = SkillRegistry()
        backend_reg = BackendRegistry()

        wf_reg = WorkflowRegistry()

        @workflow(name="exec_test", version="1.0")
        async def exec_test_body(ctx):
            result = await ctx.execute(
                "run_echo",
                plan={"instructions": "echo done"},
                backend="local_shell",
                constraints={"timeout_s": 10},
            )
            return {"exec_result": result}

        wf_reg.register(exec_test_body)

        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )

        engine = WorkflowEngine(
            db_session_factory,
            wf_reg,
            handlers=handler_map,
            auto_approve=True,
        )

        run_id = await engine.start_run(
            "exec_test", "1.0", {}, actor="test"
        )

        outcome = await engine.execute_run(run_id)
        if outcome.status == "pending":
            outcome = await engine.execute_run(run_id)

        assert outcome.status == "completed", (
            f"Expected completed, got {outcome.status}: {outcome.error}"
        )

    async def test_workflow_with_mixed_steps(self, db_session_factory):
        """A workflow with skill + execute + intrinsic + gate — full loop."""
        reg = SkillRegistry()

        @skill(name="analyze")
        async def analyze_fn(ctx):
            return {"analysis": "all good"}

        reg.register(analyze_fn)
        backend_reg = BackendRegistry()

        wf_reg = WorkflowRegistry()

        @workflow(name="full_loop", version="1.0")
        async def full_loop_body(ctx):
            # Step 1: intrinsic
            now = await ctx.now()
            # Step 2: skill
            analysis = await ctx.run_skill("analyze", input={})
            # Step 3: execute
            exec_result = await ctx.execute(
                "run_echo",
                plan={"instructions": "echo deployed"},
                backend="local_shell",
                constraints={"timeout_s": 10},
            )
            return {
                "now": str(now),
                "analysis": analysis,
                "exec_result": exec_result,
            }

        wf_reg.register(full_loop_body)

        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )

        engine = WorkflowEngine(
            db_session_factory,
            wf_reg,
            handlers=handler_map,
            auto_approve=True,
        )

        run_id = await engine.start_run(
            "full_loop", "1.0", {}, actor="test"
        )

        outcome = await engine.execute_run(run_id)
        # Drive through any pending states
        for _ in range(5):
            if outcome.status in ("completed", "failed", "cancelled"):
                break
            outcome = await engine.execute_run(run_id)

        assert outcome.status == "completed", (
            f"Expected completed, got {outcome.status}: {outcome.error}"
        )
        assert outcome.output is not None


# ---------------------------------------------------------------------------
# Handler map is accepted by WorkflowContext
# ---------------------------------------------------------------------------


class TestHandlerIntegration:
    async def test_skill_handler_resolves_in_context(self, db_session_factory):
        reg = SkillRegistry()

        @skill(name="ping")
        async def ping_fn(ctx):
            return {"pong": True}

        reg.register(ping_fn)
        backend_reg = BackendRegistry()

        wf_reg = WorkflowRegistry()

        @workflow(name="ping_test", version="1.0")
        async def ping_body(ctx):
            result = await ctx.run_skill("ping", input={})
            return result

        wf_reg.register(ping_body)

        handler_map = build_handler_map(
            skill_registry=reg, backend_registry=backend_reg
        )

        engine = WorkflowEngine(
            db_session_factory,
            wf_reg,
            handlers=handler_map,
            auto_approve=True,
        )

        run_id = await engine.start_run(
            "ping_test", "1.0", {}, actor="test"
        )

        outcome = await engine.execute_run(run_id)
        if outcome.status == "pending":
            outcome = await engine.execute_run(run_id)

        assert outcome.status == "completed"
        # ctx.run_skill returns the StepResult.result dict, which wraps
        # the skill output under the "output" key with metadata.
        assert outcome.output["status"] == "completed"
        assert outcome.output["output"] == {"pong": True}
