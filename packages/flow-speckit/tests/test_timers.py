"""Durable timers (doc 03 §§5, 7): ctx.sleep suspend/wake via fire_due_timers,
the suspend queue-row contract, and durable timer-based retry backoff with
log-derived attempt numbers. Tests never sleep real seconds — zero/negative
durations make fire_at already due against the database clock."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import Row, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from flow_speckit.storage import schema
from flow_speckit.storage.db import session_factory
from flow_speckit.workflows import (
    RetryPolicy,
    StepInvocation,
    StepResult,
    WorkflowContext,
    WorkflowEngine,
    WorkflowRegistry,
    fire_due_timers,
    workflow,
)
from flow_speckit.workflows.events import (
    EventLog,
    RunFailed,
    StepCompleted,
    StepFailed,
    StepStarted,
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
        return StepResult(result={"call": len(self.calls)})


class NotifyRecorder:
    def __init__(self) -> None:
        self.calls: list[UUID] = []

    async def __call__(self, run_id: UUID) -> None:
        self.calls.append(run_id)


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


async def _queue_rows(session: AsyncSession, run_id: UUID) -> list[Row[Any]]:
    result = await session.execute(
        select(schema.task_queue).where(schema.task_queue.c.run_id == run_id)
    )
    return list(result.all())


async def _timer_rows(session: AsyncSession, run_id: UUID) -> list[Row[Any]]:
    result = await session.execute(
        select(schema.timers).where(schema.timers.c.run_id == run_id)
    )
    return list(result.all())


def _sleepy_registry(duration: timedelta) -> WorkflowRegistry:
    registry = WorkflowRegistry()

    @workflow(name="napper", version="1")
    async def napper(ctx: WorkflowContext) -> str:
        await ctx.sleep("cool", duration)
        return "woke"

    registry.register(napper)
    return registry


async def test_sleep_suspends_run_and_parks_it_unclaimable(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    registry = _sleepy_registry(timedelta(hours=1))
    eng = make_engine(engine, registry)
    run_id = await eng.start_run("napper", "1", {}, actor="test")

    outcome = await eng.execute_run(run_id)
    assert outcome.status == "waiting_timer"

    row = await _run_row(session, run_id)
    assert row is not None and row.status == "waiting_timer"
    timers = await _timer_rows(session, run_id)
    assert [(t.kind, t.step_key) for t in timers] == [("sleep", "cool")]
    # Suspend contract: the queue row is deleted — the run is NOT claimable.
    assert await _queue_rows(session, run_id) == []
    # A future timer is not due yet.
    assert await fire_due_timers(session) == 0

    # Re-executing while still waiting is idempotent: no duplicate
    # step_started, no duplicate timer row.
    events_before = await EventLog(session).list(run_id)
    assert (await eng.execute_run(run_id)).status == "waiting_timer"
    assert await EventLog(session).list(run_id) == events_before
    assert len(await _timer_rows(session, run_id)) == 1


async def test_fire_due_timers_completes_sleep_and_run_resumes(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    registry = _sleepy_registry(timedelta(seconds=-1))  # already due
    notify = NotifyRecorder()
    eng = make_engine(engine, registry)
    run_id = await eng.start_run("napper", "1", {}, actor="test")
    assert (await eng.execute_run(run_id)).status == "waiting_timer"

    assert await fire_due_timers(session, notify=notify) == 1
    assert notify.calls == [run_id]

    # The firing path appended the sleep's step_completed itself.
    events = await EventLog(session).list(run_id)
    completed = [e for e in events if isinstance(e, StepCompleted)]
    assert [(e.step_key, e.result) for e in completed] == [("cool", {"slept": True})]
    assert await _timer_rows(session, run_id) == []
    queue = await _queue_rows(session, run_id)
    assert len(queue) == 1 and queue[0].claimed_by is None

    # Woken replay memoizes the sleep and finishes; a second replay agrees.
    outcome = await eng.execute_run(run_id)
    assert (outcome.status, outcome.output) == ("completed", "woke")
    assert (await eng.execute_run(run_id)).output == "woke"
    assert await _queue_rows(session, run_id) == []


async def test_retryable_failure_parks_run_on_retry_timer(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    handler = CountingHandler(fail_times=1)
    registry = WorkflowRegistry()

    @workflow(name="flaky", version="1")
    async def flaky(ctx: WorkflowContext) -> Any:
        return await ctx.run_skill(
            "frame",
            input={},
            retry=RetryPolicy(
                max_attempts=3,
                backoff_base=timedelta(seconds=0),  # due immediately
                retry_on=(RuntimeError,),
            ),
        )

    registry.register(flaky)
    notify = NotifyRecorder()
    eng = make_engine(engine, registry, handler)  # default backoff: durable
    run_id = await eng.start_run("flaky", "1", {}, actor="test")

    outcome = await eng.execute_run(run_id)
    assert outcome.status == "waiting_timer"
    assert len(handler.calls) == 1  # only attempt 1 ran in this pass

    events = await EventLog(session).list(run_id)
    failures = [e for e in events if isinstance(e, StepFailed)]
    assert [(f.step_key, f.attempt, f.will_retry) for f in failures] == [("frame", 1, True)]
    timers = await _timer_rows(session, run_id)
    assert [(t.kind, t.step_key) for t in timers] == [("retry", "frame")]
    assert await _queue_rows(session, run_id) == []
    row = await _run_row(session, run_id)
    assert row is not None and row.status == "waiting_timer"

    # Fire the backoff timer: run re-enqueued, timer gone, notify called.
    assert await fire_due_timers(session, notify=notify) == 1
    assert notify.calls == [run_id]
    assert await _timer_rows(session, run_id) == []
    assert len(await _queue_rows(session, run_id)) == 1

    # Woken replay re-reaches the step; attempt 2 derives from the log.
    outcome = await eng.execute_run(run_id)
    assert outcome.status == "completed"
    assert [c.attempt for c in handler.calls] == [1, 2]


async def test_retry_exhaustion_across_timer_wakes_fails_run(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    handler = CountingHandler(fail_times=99)
    registry = WorkflowRegistry()

    @workflow(name="doomed", version="1")
    async def doomed(ctx: WorkflowContext) -> Any:
        return await ctx.run_skill(
            "frame",
            input={},
            retry=RetryPolicy(
                max_attempts=2,
                backoff_base=timedelta(seconds=0),
                retry_on=(RuntimeError,),
            ),
        )

    registry.register(doomed)
    eng = make_engine(engine, registry, handler)
    run_id = await eng.start_run("doomed", "1", {}, actor="test")

    assert (await eng.execute_run(run_id)).status == "waiting_timer"
    assert await fire_due_timers(session) == 1

    outcome = await eng.execute_run(run_id)
    assert outcome.status == "failed"
    assert outcome.error is not None and outcome.error["type"] == "RuntimeError"
    assert [c.attempt for c in handler.calls] == [1, 2]

    events = await EventLog(session).list(run_id)
    failures = [e for e in events if isinstance(e, StepFailed)]
    assert [(f.attempt, f.will_retry) for f in failures] == [(1, True), (2, False)]
    run_failed = [e for e in events if isinstance(e, RunFailed)]
    assert len(run_failed) == 1 and run_failed[0].failed_step == "frame"
    # Each attempt has its own step_started (one per wake).
    starts = [e for e in events if isinstance(e, StepStarted)]
    assert [s.step_key for s in starts] == ["frame", "frame"]
    assert await _queue_rows(session, run_id) == []
    assert await _timer_rows(session, run_id) == []
