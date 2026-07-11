"""ctx.parallel, cancellation and child workflows (doc 03 §§5, 9).

- parallel: eager step-key allocation, independent branch memoization, crash
  recovery (fault-hook BaseException = simulated ``kill -9``), and a gate
  suspending the WHOLE run while completed branches' checkpoints survive.
- cancel_run: legal from every non-terminal state, terminal states raise,
  running bodies observe cancellation at the next step boundary.
- child workflows: terminal children settle the waiting parent
  (completed → ``step_completed`` with the child's output ref; failed /
  cancelled → ``step_failed`` and the parent fails), and cancellation
  cascades parent → children.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from flow_speckit.artifacts.models import GenericArtifact
from flow_speckit.artifacts.refs import ArtifactRef
from flow_speckit.artifacts.registry import ArtifactRegistry
from flow_speckit.artifacts.store import ArtifactStore
from flow_speckit.storage import schema
from flow_speckit.storage.db import session_factory
from flow_speckit.workflows import (
    InvalidCancellation,
    StepInvocation,
    StepResult,
    UnknownRun,
    WorkflowContext,
    WorkflowEngine,
    WorkflowRegistry,
    cancel_run,
    child_run_id,
    resolve_gate,
    workflow,
)
from flow_speckit.workflows.events import (
    EventLog,
    RunCancelled,
    RunFailed,
    RunStarted,
    StepCompleted,
    StepFailed,
    StepStarted,
)


class SimulatedCrash(BaseException):
    """BaseException = a killed worker: the engine must record NOTHING."""


class CountingHandler:
    """Fake "skill" handler recording every live execution by label."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __call__(self, step: StepInvocation) -> StepResult:
        self.calls.append(step.label)
        return StepResult(result={"label": step.label})


class NotifyRecorder:
    def __init__(self) -> None:
        self.calls: list[UUID] = []

    async def __call__(self, run_id: UUID) -> None:
        self.calls.append(run_id)


@pytest.fixture()
def store(session: AsyncSession) -> ArtifactStore:
    reg = ArtifactRegistry()
    reg.register(GenericArtifact, source_package="flow-speckit")
    return ArtifactStore(session, reg)


def make_engine(db: AsyncEngine, registry: WorkflowRegistry, **kwargs: Any) -> WorkflowEngine:
    return WorkflowEngine(session_factory(db), registry, **kwargs)


async def _run_row(session: AsyncSession, run_id: UUID) -> Any:
    result = await session.execute(
        select(schema.workflow_runs).where(schema.workflow_runs.c.run_id == run_id)
    )
    row = result.one_or_none()
    await session.rollback()
    return row


async def _queue_rows(session: AsyncSession, run_id: UUID) -> list[Any]:
    result = await session.execute(
        select(schema.task_queue).where(schema.task_queue.c.run_id == run_id)
    )
    rows = list(result.all())
    await session.rollback()
    return rows


async def _timer_rows(session: AsyncSession, run_id: UUID) -> list[Any]:
    result = await session.execute(
        select(schema.timers).where(schema.timers.c.run_id == run_id)
    )
    rows = list(result.all())
    await session.rollback()
    return rows


def _parallel_registry() -> WorkflowRegistry:
    registry = WorkflowRegistry()

    @workflow(name="par", version="1")
    async def par(ctx: WorkflowContext) -> list[Any]:
        return await ctx.parallel(
            [
                ctx.run_skill("a", input={}),
                ctx.run_skill("b", input={}),
                ctx.run_skill("c", input={}),
            ]
        )

    registry.register(par)
    return registry


# ---------------------------------------------------------------------------
# ctx.parallel
# ---------------------------------------------------------------------------


async def test_parallel_branches_memoize_independently(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    handler = CountingHandler()
    eng = make_engine(engine, _parallel_registry(), handlers={"skill": handler})
    run_id = await eng.start_run("par", "1", {}, actor="test")

    first = await eng.execute_run(run_id)
    assert first.status == "completed"
    # Results come back in list order regardless of completion interleaving.
    assert first.output == [{"label": "a"}, {"label": "b"}, {"label": "c"}]
    assert sorted(handler.calls) == ["a", "b", "c"]

    replay = await eng.execute_run(run_id)
    assert (replay.status, replay.output) == ("completed", first.output)
    assert sorted(handler.calls) == ["a", "b", "c"]  # nothing re-executed

    events = await EventLog(session).list(run_id)
    completed = sorted(e.step_key for e in events if isinstance(e, StepCompleted))
    assert completed == ["a", "b", "c"]  # exactly one checkpoint per branch


async def test_parallel_crash_between_side_effect_and_checkpoint(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    """Simulated kill -9 after branch b's side effect: siblings' committed
    checkpoints survive; only b re-executes on recovery (at-least-once)."""
    handler = CountingHandler()
    checkpointed: set[str] = set()
    crashed = False

    async def fault_hook(checkpoint: str, step_key: str) -> None:
        nonlocal crashed
        if checkpoint == "after_checkpoint":
            checkpointed.add(step_key)
        if not crashed and step_key == "b" and checkpoint == "after_side_effect":
            # Deterministic: crash only once a and c have fully checkpointed.
            while not {"a", "c"} <= checkpointed:
                await asyncio.sleep(0.001)
            crashed = True
            raise SimulatedCrash("killed between side effect and checkpoint")

    eng = make_engine(
        engine, _parallel_registry(), handlers={"skill": handler}, fault_hook=fault_hook
    )
    run_id = await eng.start_run("par", "1", {}, actor="test")

    with pytest.raises(SimulatedCrash):
        await eng.execute_run(run_id)

    # Nothing was recorded for the crashed branch beyond step_started.
    events = await EventLog(session).list(run_id)
    assert sorted(e.step_key for e in events if isinstance(e, StepCompleted)) == ["a", "c"]

    outcome = await eng.execute_run(run_id)
    assert outcome.status == "completed"
    assert outcome.output == [{"label": "a"}, {"label": "b"}, {"label": "c"}]
    # a and c executed once; b twice — its first side effect crashed
    # pre-checkpoint (the at-least-once contract, doc 03 §4).
    assert sorted(handler.calls) == ["a", "b", "b", "c"]

    events = await EventLog(session).list(run_id)
    completed = sorted(e.step_key for e in events if isinstance(e, StepCompleted))
    assert completed == ["a", "b", "c"]  # each memoized exactly once


async def test_parallel_gate_suspends_run_and_resumes_without_reexecution(
    engine: AsyncEngine, session: AsyncSession, store: ArtifactStore
) -> None:
    ref: ArtifactRef = await store.create(GenericArtifact(title="Brief"), key="briefs/par")
    checkpointed: set[str] = set()

    async def fault_hook(checkpoint: str, step_key: str) -> None:
        if checkpoint == "after_checkpoint":
            checkpointed.add(step_key)

    class ChoreoHandler(CountingHandler):
        async def __call__(self, step: StepInvocation) -> StepResult:
            if step.label == "pre_gate":
                # Open the gate only after a and c committed their
                # checkpoints, so the suspend deterministically finds them
                # done (a cancelled-mid-flight sibling would merely
                # re-execute on wake — at-least-once — but then this test
                # could not pin down exact call counts).
                while not {"a", "c"} <= checkpointed:
                    await asyncio.sleep(0.001)
            return await super().__call__(step)

    handler = ChoreoHandler()
    registry = WorkflowRegistry()

    @workflow(name="par-gate", version="1")
    async def par_gate(ctx: WorkflowContext) -> list[Any]:
        async def gate_branch() -> bool:
            await ctx.run_skill("pre_gate", input={})
            decision = await ctx.gate("signoff", artifact=ref, approvers=["role:eng"])
            return decision.approved

        return await ctx.parallel(
            [
                ctx.run_skill("a", input={}),
                gate_branch(),
                ctx.run_skill("c", input={}),
            ]
        )

    registry.register(par_gate)
    eng = make_engine(engine, registry, handlers={"skill": handler}, fault_hook=fault_hook)
    run_id = await eng.start_run("par-gate", "1", {}, actor="test")

    outcome = await eng.execute_run(run_id)
    assert outcome.status == "waiting_gate"  # one branch's gate parks the WHOLE run
    row = await _run_row(session, run_id)
    assert row.status == "waiting_gate"
    assert await _queue_rows(session, run_id) == []  # unclaimable while waiting

    # Completed branches' checkpoints survived the suspend.
    events = await EventLog(session).list(run_id)
    completed = sorted(e.step_key for e in events if isinstance(e, StepCompleted))
    assert completed == ["a", "c", "pre_gate"]
    assert sorted(handler.calls) == ["a", "c", "pre_gate"]

    await resolve_gate(session, run_id, "signoff", "approved", "user:vinit")
    woken = await eng.execute_run(run_id)
    assert woken.status == "completed"
    assert woken.output == [{"label": "a"}, True, {"label": "c"}]
    assert sorted(handler.calls) == ["a", "c", "pre_gate"]  # done branches replayed from memo


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


def _quick_registry() -> WorkflowRegistry:
    registry = WorkflowRegistry()

    @workflow(name="quick", version="1")
    async def quick(ctx: WorkflowContext) -> str:
        return "done"

    registry.register(quick)
    return registry


async def test_cancel_pending_run(engine: AsyncEngine, session: AsyncSession) -> None:
    eng = make_engine(engine, _quick_registry())
    run_id = await eng.start_run("quick", "1", {}, actor="test")

    await cancel_run(session, run_id, "user:vinit", "changed my mind")

    row = await _run_row(session, run_id)
    assert row.status == "cancelled"
    assert await _queue_rows(session, run_id) == []
    events = await EventLog(session).list(run_id)
    assert [type(e) for e in events] == [RunStarted, RunCancelled]
    cancelled = events[-1]
    assert isinstance(cancelled, RunCancelled)
    assert (cancelled.actor, cancelled.reason) == ("user:vinit", "changed my mind")

    # A worker that claims a cancelled run executes nothing and appends nothing.
    outcome = await eng.execute_run(run_id)
    assert outcome.status == "cancelled"
    assert await EventLog(session).list(run_id) == events


async def test_cancel_is_illegal_from_terminal_states(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    eng = make_engine(engine, _quick_registry())
    run_id = await eng.start_run("quick", "1", {}, actor="test")
    assert (await eng.execute_run(run_id)).status == "completed"

    with pytest.raises(InvalidCancellation, match="completed"):
        await cancel_run(session, run_id, "user:vinit", "too late")
    with pytest.raises(UnknownRun):
        await cancel_run(session, uuid4(), "user:vinit", "no such run")

    cancelled_id = await eng.start_run("quick", "1", {}, actor="test")
    await cancel_run(session, cancelled_id, "user:vinit", "first")
    with pytest.raises(InvalidCancellation, match="cancelled"):
        await cancel_run(session, cancelled_id, "user:vinit", "again")


async def test_cancel_waiting_gate_run_clears_timers_and_queue(
    engine: AsyncEngine, session: AsyncSession, store: ArtifactStore
) -> None:
    ref = await store.create(GenericArtifact(title="Brief"), key="briefs/cancel")
    registry = WorkflowRegistry()

    @workflow(name="gated", version="1")
    async def gated(ctx: WorkflowContext) -> str:
        await ctx.gate(
            "signoff", artifact=ref, approvers=["role:eng"], timeout=timedelta(days=1)
        )
        return "approved"

    registry.register(gated)
    eng = make_engine(engine, registry)
    run_id = await eng.start_run("gated", "1", {}, actor="test")
    assert (await eng.execute_run(run_id)).status == "waiting_gate"
    assert len(await _timer_rows(session, run_id)) == 1  # armed gate_timeout

    await cancel_run(session, run_id, "user:vinit", "obsolete")

    row = await _run_row(session, run_id)
    assert row.status == "cancelled"
    assert await _timer_rows(session, run_id) == []
    assert await _queue_rows(session, run_id) == []
    assert (await eng.execute_run(run_id)).status == "cancelled"


async def test_cancel_waiting_timer_run_clears_timers(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    registry = WorkflowRegistry()

    @workflow(name="napper", version="1")
    async def napper(ctx: WorkflowContext) -> str:
        await ctx.sleep("cool", timedelta(hours=1))
        return "woke"

    registry.register(napper)
    eng = make_engine(engine, registry)
    run_id = await eng.start_run("napper", "1", {}, actor="test")
    assert (await eng.execute_run(run_id)).status == "waiting_timer"

    await cancel_run(session, run_id, "user:vinit", "no need to wait")

    row = await _run_row(session, run_id)
    assert row.status == "cancelled"
    assert await _timer_rows(session, run_id) == []
    assert await _queue_rows(session, run_id) == []


async def test_cancel_running_run_observed_at_next_step_boundary(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    calls: list[str] = []

    class BlockingHandler:
        async def __call__(self, step: StepInvocation) -> StepResult:
            calls.append(step.label)
            entered.set()
            await release.wait()
            return StepResult(result={"label": step.label})

    registry = WorkflowRegistry()

    @workflow(name="two-step", version="1")
    async def two_step(ctx: WorkflowContext) -> Any:
        await ctx.run_skill("one", input={})
        return await ctx.run_skill("two", input={})

    registry.register(two_step)
    eng = make_engine(engine, registry, handlers={"skill": BlockingHandler()})
    run_id = await eng.start_run("two-step", "1", {}, actor="test")

    task = asyncio.create_task(eng.execute_run(run_id))
    await asyncio.wait_for(entered.wait(), 5)
    # Cancel while step "one" is mid-flight (from a different session, as the
    # CLI would): bounded grace — the in-flight step finishes and checkpoints,
    # then the next step boundary raises CancelledRun instead of running "two".
    await cancel_run(session, run_id, "user:vinit", "abort")
    release.set()

    outcome = await asyncio.wait_for(task, 10)
    assert outcome.status == "cancelled"
    assert calls == ["one"]  # step "two" never executed

    events = await EventLog(session).list(run_id)
    assert not any(isinstance(e, RunFailed) for e in events)  # cancelled, not failed
    assert sum(isinstance(e, RunCancelled) for e in events) == 1
    completed = [e.step_key for e in events if isinstance(e, StepCompleted)]
    assert completed == ["one"]  # the in-flight step's checkpoint still committed
    started = [e.step_key for e in events if isinstance(e, StepStarted)]
    assert "two" not in started
    row = await _run_row(session, run_id)
    assert row.status == "cancelled"  # re-asserted after the late checkpoint
    assert await _queue_rows(session, run_id) == []


# ---------------------------------------------------------------------------
# Child workflows
# ---------------------------------------------------------------------------


def _family_registry(child_body: str = "uuid") -> WorkflowRegistry:
    registry = WorkflowRegistry()

    @workflow(name="parent", version="1")
    async def parent_wf(ctx: WorkflowContext) -> Any:
        return await ctx.child_workflow("spawn", name="child", input={"x": 2})

    if child_body == "uuid":

        @workflow(name="child", version="1")
        async def child_uuid(ctx: WorkflowContext, x: int) -> UUID:
            return await ctx.uuid()

        registry.register(child_uuid)
    elif child_body == "fail":

        @workflow(name="child", version="1")
        async def child_fail(ctx: WorkflowContext, x: int) -> Any:
            raise ValueError(f"boom {x}")

        registry.register(child_fail)
    else:  # "sleep"

        @workflow(name="child", version="1")
        async def child_sleep(ctx: WorkflowContext, x: int) -> str:
            await ctx.sleep("long-nap", timedelta(hours=1))
            return "woke"

        registry.register(child_sleep)

    registry.register(parent_wf)
    return registry


async def test_child_completion_settles_parent_with_output_ref(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    notify = NotifyRecorder()
    eng = make_engine(engine, _family_registry("uuid"), notify=notify)
    parent_id = await eng.start_run("parent", "1", {}, actor="test")
    notify.calls.clear()  # only interested in wakes from here on

    outcome = await eng.execute_run(parent_id)
    assert outcome.status == "running"  # parked, child is doing the work
    assert await _queue_rows(session, parent_id) == []

    child_id = child_run_id(parent_id, "spawn")
    child = await _run_row(session, child_id)
    assert child is not None
    assert child.parent_run_id == parent_id
    assert child.workflow_name == "child" and child.status == "pending"
    child_events = await EventLog(session).list(child_id)
    started = child_events[0]
    assert isinstance(started, RunStarted)
    assert started.actor == f"run:{parent_id}" and started.input == {"x": 2}
    assert len(await _queue_rows(session, child_id)) == 1  # child was enqueued
    assert notify.calls == [child_id]

    child_outcome = await eng.execute_run(child_id)
    assert child_outcome.status == "completed"
    child_output = child_outcome.output
    assert isinstance(child_output, UUID)

    # The terminal child settled the parent: step_completed + re-enqueue + notify.
    parent_events = await EventLog(session).list(parent_id)
    settled = [e for e in parent_events if isinstance(e, StepCompleted)]
    expected_result = {"child_run_id": str(child_id), "output_ref": str(child_output)}
    assert [(e.step_key, e.result) for e in settled] == [("spawn", expected_result)]
    parent_row = await _run_row(session, parent_id)
    assert parent_row.status == "pending"
    assert len(await _queue_rows(session, parent_id)) == 1
    assert notify.calls[-1] == parent_id

    woken = await eng.execute_run(parent_id)
    assert (woken.status, woken.output) == ("completed", expected_result)
    assert await _queue_rows(session, parent_id) == []
    # Replay agrees and appends nothing new.
    assert (await eng.execute_run(parent_id)).output == expected_result


async def test_child_failure_surfaces_as_parent_step_failure(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    eng = make_engine(engine, _family_registry("fail"))
    parent_id = await eng.start_run("parent", "1", {}, actor="test")
    assert (await eng.execute_run(parent_id)).status == "running"

    child_id = child_run_id(parent_id, "spawn")
    child_outcome = await eng.execute_run(child_id)
    assert child_outcome.status == "failed"
    assert child_outcome.error is not None and child_outcome.error["type"] == "ValueError"

    # Settle: parent got step_failed(will_retry=False) and was re-enqueued.
    parent_events = await EventLog(session).list(parent_id)
    failures = [e for e in parent_events if isinstance(e, StepFailed)]
    assert [(f.step_key, f.attempt, f.will_retry) for f in failures] == [("spawn", 1, False)]
    assert failures[0].error["type"] == "ValueError"
    assert len(await _queue_rows(session, parent_id)) == 1

    woken = await eng.execute_run(parent_id)
    assert woken.status == "failed"
    assert woken.error is not None and woken.error["type"] == "ChildWorkflowFailed"
    assert "boom 2" in woken.error["message"]

    parent_events = await EventLog(session).list(parent_id)
    run_failed = [e for e in parent_events if isinstance(e, RunFailed)]
    assert len(run_failed) == 1 and run_failed[0].failed_step == "spawn"
    # No duplicate step_failed was appended by the parent's replay.
    assert sum(isinstance(e, StepFailed) for e in parent_events) == 1
    assert await _queue_rows(session, parent_id) == []


async def test_cancel_of_child_surfaces_to_parent_as_step_failure(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    eng = make_engine(engine, _family_registry("sleep"))
    parent_id = await eng.start_run("parent", "1", {}, actor="test")
    assert (await eng.execute_run(parent_id)).status == "running"

    child_id = child_run_id(parent_id, "spawn")
    assert (await eng.execute_run(child_id)).status == "waiting_timer"

    await cancel_run(session, child_id, "user:vinit", "child not needed")

    parent_events = await EventLog(session).list(parent_id)
    failures = [e for e in parent_events if isinstance(e, StepFailed)]
    assert [(f.step_key, f.will_retry) for f in failures] == [("spawn", False)]
    assert failures[0].error["type"] == "ChildRunCancelled"

    woken = await eng.execute_run(parent_id)
    assert woken.status == "failed"
    assert woken.error is not None and woken.error["type"] == "ChildWorkflowFailed"


async def test_cancel_parent_cascades_to_children(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    eng = make_engine(engine, _family_registry("sleep"))
    parent_id = await eng.start_run("parent", "1", {}, actor="test")
    assert (await eng.execute_run(parent_id)).status == "running"

    child_id = child_run_id(parent_id, "spawn")
    assert (await eng.execute_run(child_id)).status == "waiting_timer"
    assert len(await _timer_rows(session, child_id)) == 1

    await cancel_run(session, parent_id, "user:vinit", "scrapping the feature")

    parent_row = await _run_row(session, parent_id)
    child_row = await _run_row(session, child_id)
    assert parent_row.status == "cancelled"
    assert child_row.status == "cancelled"
    assert await _timer_rows(session, child_id) == []
    assert await _queue_rows(session, parent_id) == []
    assert await _queue_rows(session, child_id) == []

    child_events = await EventLog(session).list(child_id)
    cancelled = [e for e in child_events if isinstance(e, RunCancelled)]
    assert len(cancelled) == 1
    assert f"parent run {parent_id} cancelled" in cancelled[0].reason
    # The parent was NOT settled with a step failure — it is terminal itself.
    parent_events = await EventLog(session).list(parent_id)
    assert not any(isinstance(e, StepFailed) for e in parent_events)

    assert (await eng.execute_run(parent_id)).status == "cancelled"
    assert (await eng.execute_run(child_id)).status == "cancelled"
