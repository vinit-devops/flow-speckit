"""Replay-engine core tests (doc 03 §§4, 8, 10): memoization, step keys,
intrinsics, non-determinism detection, the step-handler seam, retries,
version pinning, and start_run bookkeeping."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import Row, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from flow_speckit.storage import schema
from flow_speckit.storage.db import session_factory
from flow_speckit.workflows import (
    NonDeterminismError,
    RetryPolicy,
    StepInvocation,
    StepKindUnavailableError,
    StepResult,
    WorkflowContext,
    WorkflowEngine,
    WorkflowRegistry,
    immediate_backoff,
    workflow,
)
from flow_speckit.workflows.events import (
    EventLog,
    RunFailed,
    RunStarted,
    StepCompleted,
    StepFailed,
    project_run,
)


class CountingHandler:
    """Fake "skill" handler counting every live side-effect execution."""

    def __init__(self, fail_times: int = 0) -> None:
        self.calls: list[StepInvocation] = []
        self._fail_times = fail_times

    async def __call__(self, step: StepInvocation) -> StepResult:
        self.calls.append(step)
        if len(self.calls) <= self._fail_times:
            raise RuntimeError(f"transient failure {len(self.calls)}")
        return StepResult(result={"call": len(self.calls), "input": step.payload["input"]})


def make_engine(
    db: AsyncEngine,
    registry: WorkflowRegistry,
    handler: CountingHandler | None = None,
    **kwargs: Any,
) -> WorkflowEngine:
    handlers = {"skill": handler} if handler is not None else {}
    return WorkflowEngine(session_factory(db), registry, handlers=handlers, **kwargs)


async def _run_row(session: AsyncSession, run_id: UUID) -> Row[Any] | None:
    result = await session.execute(
        select(schema.workflow_runs).where(schema.workflow_runs.c.run_id == run_id)
    )
    return result.one_or_none()


async def test_replays_execute_each_side_effect_exactly_once(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    handler = CountingHandler()
    registry = WorkflowRegistry()

    @workflow(name="two-step", version="1")
    async def two_step(ctx: WorkflowContext, idea: str) -> dict[str, Any]:
        a = await ctx.run_skill("frame", input={"idea": idea})
        b = await ctx.run_skill("design", input=a)
        return {"a": a, "b": b}

    registry.register(two_step)
    eng = make_engine(engine, registry, handler)
    run_id = await eng.start_run("two-step", "1", {"idea": "x"}, actor="test")

    outcomes = [await eng.execute_run(run_id) for _ in range(3)]

    assert [o.status for o in outcomes] == ["completed"] * 3
    assert outcomes[0].output == outcomes[1].output == outcomes[2].output
    # Each side effect executed exactly once across all three passes.
    assert [c.step_key for c in handler.calls] == ["frame", "design"]

    row = await _run_row(session, run_id)
    assert row is not None and row.status == "completed"
    queue = (
        await session.execute(
            select(schema.task_queue).where(schema.task_queue.c.run_id == run_id)
        )
    ).all()
    assert queue == []
    # The maintained row matches what folding the log computes.
    events = await EventLog(session).list(run_id)
    assert project_run(run_id, events).status == "completed"


async def test_repeated_labels_get_ordinal_step_keys(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    handler = CountingHandler()
    registry = WorkflowRegistry()

    @workflow(name="loop", version="1")
    async def loop_wf(ctx: WorkflowContext) -> list[Any]:
        return [await ctx.run_skill("poll", input={"i": i}) for i in range(3)]

    registry.register(loop_wf)
    eng = make_engine(engine, registry, handler)
    run_id = await eng.start_run("loop", "1", {}, actor="test")

    first = await eng.execute_run(run_id)
    events = await EventLog(session).list(run_id)
    completed = [e.step_key for e in events if isinstance(e, StepCompleted)]
    assert completed == ["poll", "poll#2", "poll#3"]

    replay = await eng.execute_run(run_id)
    assert replay.status == "completed"
    assert replay.output == first.output
    assert len(handler.calls) == 3  # no re-execution on replay


async def test_intrinsics_are_memoized_across_replays(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    registry = WorkflowRegistry()

    @workflow(name="intrinsics", version="1")
    async def intrinsics(ctx: WorkflowContext) -> dict[str, Any]:
        return {
            "now": (await ctx.now()).isoformat(),
            "now2": (await ctx.now()).isoformat(),
            "random": await ctx.random(),
            "uuid": str(await ctx.uuid()),
        }

    registry.register(intrinsics)
    eng = make_engine(engine, registry)
    run_id = await eng.start_run("intrinsics", "1", {}, actor="test")

    first = await eng.execute_run(run_id)
    second = await eng.execute_run(run_id)
    assert first.status == second.status == "completed"
    assert first.output == second.output  # values come from the log, not re-generation
    UUID(first.output["uuid"])  # round-trips as a real uuid
    events = await EventLog(session).list(run_id)
    keys = [e.step_key for e in events if isinstance(e, StepCompleted)]
    assert keys == ["now", "now#2", "random", "uuid"]


async def test_mutated_body_raises_non_determinism_naming_the_step(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    handler = CountingHandler()
    registry = WorkflowRegistry()

    @workflow(name="mutating", version="1")
    async def original(ctx: WorkflowContext) -> str:
        await ctx.run_skill("frame", input={})
        await ctx.run_skill("design", input={})
        return "ok"

    registry.register(original)
    eng = make_engine(engine, registry, handler)
    run_id = await eng.start_run("mutating", "1", {}, actor="test")
    assert (await eng.execute_run(run_id)).status == "completed"

    # Extra step appended to the body under the same pinned version.
    extra_registry = WorkflowRegistry()

    @workflow(name="mutating", version="1")
    async def extra(ctx: WorkflowContext) -> str:
        await ctx.run_skill("frame", input={})
        await ctx.run_skill("design", input={})
        await ctx.run_skill("ship", input={})
        return "ok"

    extra_registry.register(extra)
    with pytest.raises(NonDeterminismError, match="'ship'") as excinfo:
        await make_engine(engine, extra_registry, handler).execute_run(run_id)
    assert "version" in str(excinfo.value)  # instructs bumping the workflow version

    # Renamed label.
    renamed_registry = WorkflowRegistry()

    @workflow(name="mutating", version="1")
    async def renamed(ctx: WorkflowContext) -> str:
        await ctx.run_skill("frame", input={})
        await ctx.run_skill("design_v2", input={})
        return "ok"

    renamed_registry.register(renamed)
    with pytest.raises(NonDeterminismError, match="'design_v2'"):
        await make_engine(engine, renamed_registry, handler).execute_run(run_id)

    # Removed step: detected at run completion via the leftover check.
    removed_registry = WorkflowRegistry()

    @workflow(name="mutating", version="1")
    async def removed(ctx: WorkflowContext) -> str:
        await ctx.run_skill("frame", input={})
        return "ok"

    removed_registry.register(removed)
    with pytest.raises(NonDeterminismError, match="'design'"):
        await make_engine(engine, removed_registry, handler).execute_run(run_id)

    # None of the mutated replays re-executed a side effect, and a mismatch
    # against a completed log never poisons its history.
    assert len(handler.calls) == 2
    events = await EventLog(session).list(run_id)
    assert project_run(run_id, events).status == "completed"


async def test_missing_handler_names_subsystem_and_phase(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    registry = WorkflowRegistry()

    @workflow(name="no-handler", version="1")
    async def no_handler(ctx: WorkflowContext) -> Any:
        return await ctx.run_skill("frame", input={})

    registry.register(no_handler)
    eng = make_engine(engine, registry)  # no handlers registered
    run_id = await eng.start_run("no-handler", "1", {}, actor="test")

    with pytest.raises(StepKindUnavailableError, match=r"Skill Engine \(Phase 4\)"):
        await eng.execute_run(run_id)

    events = await EventLog(session).list(run_id)
    assert project_run(run_id, events).status == "failed"
    row = await _run_row(session, run_id)
    assert row is not None and row.status == "failed"


async def test_retry_records_failures_then_succeeds(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    handler = CountingHandler(fail_times=2)
    registry = WorkflowRegistry()
    backoff_calls: list[int] = []

    async def recording_backoff(attempt: int, policy: RetryPolicy) -> None:
        backoff_calls.append(attempt)

    @workflow(name="flaky", version="1")
    async def flaky(ctx: WorkflowContext) -> Any:
        return await ctx.run_skill(
            "frame", input={}, retry=RetryPolicy(max_attempts=3, retry_on=(RuntimeError,))
        )

    registry.register(flaky)
    eng = make_engine(engine, registry, handler, backoff=recording_backoff)
    run_id = await eng.start_run("flaky", "1", {}, actor="test")

    outcome = await eng.execute_run(run_id)
    assert outcome.status == "completed"
    assert len(handler.calls) == 3
    assert backoff_calls == [1, 2]  # the injectable backoff seam was exercised

    events = await EventLog(session).list(run_id)
    failures = [e for e in events if isinstance(e, StepFailed)]
    assert [(f.step_key, f.attempt, f.will_retry) for f in failures] == [
        ("frame", 1, True),
        ("frame", 2, True),
    ]
    assert len([e for e in events if isinstance(e, StepCompleted)]) == 1


async def test_retry_exhaustion_fails_the_run(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    handler = CountingHandler(fail_times=99)
    registry = WorkflowRegistry()

    @workflow(name="doomed", version="1")
    async def doomed(ctx: WorkflowContext) -> Any:
        return await ctx.run_skill(
            "frame", input={}, retry=RetryPolicy(max_attempts=2, retry_on=(RuntimeError,))
        )

    registry.register(doomed)
    # The engine default backoff is durable (timer-based) since wave 4; this
    # test exercises in-process exhaustion, so opt into immediate_backoff.
    eng = make_engine(engine, registry, handler, backoff=immediate_backoff)
    run_id = await eng.start_run("doomed", "1", {}, actor="test")

    outcome = await eng.execute_run(run_id)
    assert outcome.status == "failed"
    assert outcome.error is not None and outcome.error["type"] == "RuntimeError"
    assert len(handler.calls) == 2

    events = await EventLog(session).list(run_id)
    failures = [e for e in events if isinstance(e, StepFailed)]
    assert [(f.attempt, f.will_retry) for f in failures] == [(1, True), (2, False)]
    run_failed = [e for e in events if isinstance(e, RunFailed)]
    assert len(run_failed) == 1 and run_failed[0].failed_step == "frame"
    row = await _run_row(session, run_id)
    assert row is not None and row.status == "failed"


async def test_unknown_pinned_version_fails_with_guidance(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    registry = WorkflowRegistry()

    @workflow(name="pinned", version="1")
    async def pinned(ctx: WorkflowContext) -> str:
        return "ok"

    registry.register(pinned)
    run_id = await make_engine(engine, registry).start_run("pinned", "1", {}, actor="test")

    # Replay in an engine whose registry no longer knows the pinned version.
    outcome = await make_engine(engine, WorkflowRegistry()).execute_run(run_id)
    assert outcome.status == "failed"
    assert outcome.error is not None
    assert "NEW version" in outcome.error["message"]

    events = await EventLog(session).list(run_id)
    run_failed = [e for e in events if isinstance(e, RunFailed)]
    assert len(run_failed) == 1
    assert "NEW version" in run_failed[0].error["message"]
    row = await _run_row(session, run_id)
    assert row is not None and row.status == "failed"


async def test_start_run_creates_pending_run_and_queue_row(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    registry = WorkflowRegistry()

    @workflow(name="fresh", version="1")
    async def fresh(ctx: WorkflowContext) -> str:
        return "ok"

    registry.register(fresh)
    run_id = await make_engine(engine, registry).start_run(
        "fresh", "1", {"idea": "x"}, actor="vinit"
    )

    row = await _run_row(session, run_id)
    assert row is not None
    assert row.status == "pending"
    assert row.workflow_name == "fresh"
    assert row.workflow_version == "1"
    assert row.input == {"idea": "x"}

    queue = (
        await session.execute(
            select(schema.task_queue).where(schema.task_queue.c.run_id == run_id)
        )
    ).one()
    assert queue.available_at is not None and queue.claimed_by is None

    events = await EventLog(session).list(run_id)
    assert len(events) == 1
    started = events[0]
    assert isinstance(started, RunStarted)
    assert started.actor == "vinit"
    assert started.input == {"idea": "x"}
