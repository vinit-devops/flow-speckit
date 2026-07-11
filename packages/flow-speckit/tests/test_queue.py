"""Dispatch tests (doc 03 §7): SKIP LOCKED claims, LISTEN/NOTIFY wakeups
beating the poll fallback, stale-claim reaping (crash recovery), worker
heartbeats, and the advisory-lock scheduler singleton. All loops run with
injected ~50ms intervals — no multi-second sleeps."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Row, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from flow_speckit.storage import schema
from flow_speckit.storage.db import session_factory
from flow_speckit.workflows import (
    Scheduler,
    StepInvocation,
    StepResult,
    Worker,
    WorkflowContext,
    WorkflowEngine,
    WorkflowRegistry,
    claim_one,
    fire_due_timers,
    make_notifier,
    reap_stale,
    run_inline,
    upsert_task_queue,
    workflow,
)
from flow_speckit.workflows.events import EventLog, StepCompleted


async def wait_until(
    check: Callable[[], Awaitable[bool]], *, timeout: float = 5.0, interval: float = 0.02
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not await check():
        if loop.time() > deadline:
            pytest.fail(f"condition not met within {timeout}s")
        await asyncio.sleep(interval)


async def _queue_row(session: AsyncSession, run_id: UUID) -> Row[Any] | None:
    result = await session.execute(
        select(schema.task_queue).where(schema.task_queue.c.run_id == run_id)
    )
    row = result.one_or_none()
    await session.rollback()
    return row


async def _run_status(session: AsyncSession, run_id: UUID) -> str | None:
    result = await session.execute(
        select(schema.workflow_runs.c.status).where(schema.workflow_runs.c.run_id == run_id)
    )
    status = result.scalar_one_or_none()
    await session.rollback()
    return status


def _quick_registry() -> WorkflowRegistry:
    registry = WorkflowRegistry()

    @workflow(name="quick", version="1")
    async def quick(ctx: WorkflowContext) -> str:
        return "done"

    registry.register(quick)
    return registry


# ---------------------------------------------------------------------------
# claim_one
# ---------------------------------------------------------------------------


async def test_claim_one_claims_each_row_exactly_once(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    ids = {uuid4(), uuid4()}
    for run_id in ids:
        await upsert_task_queue(session, run_id)

    first = await claim_one(session, "w1")
    second = await claim_one(session, "w1")
    assert {first, second} == ids
    # Claimed rows are skipped; an empty queue claims None.
    assert await claim_one(session, "w1") is None

    result = await session.execute(select(schema.task_queue))
    rows = result.all()
    await session.rollback()
    assert all(
        row.claimed_by == "w1" and row.heartbeat_at is not None and row.attempts == 1
        for row in rows
    )


async def test_concurrent_claims_never_double_claim(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    """SKIP LOCKED property (doc 03 §10): N workers, N runs, no double-claims."""
    ids = {uuid4() for _ in range(4)}
    for run_id in ids:
        await upsert_task_queue(session, run_id)

    factory = session_factory(engine)

    async def claim(worker: str) -> UUID | None:
        async with factory() as own_session:
            return await claim_one(own_session, worker)

    claims = await asyncio.gather(*(claim(f"w{i}") for i in range(4)))
    assert set(claims) == ids  # every run claimed by exactly one worker


# ---------------------------------------------------------------------------
# NOTIFY wake
# ---------------------------------------------------------------------------


async def test_notify_wakes_worker_long_before_poll_would(
    engine: AsyncEngine, session: AsyncSession, migrated_url: str
) -> None:
    factory = session_factory(engine)
    eng = WorkflowEngine(factory, _quick_registry(), notify=make_notifier(factory))
    worker = Worker(eng, factory, listen_dsn=migrated_url, poll_interval=30.0)
    await worker.start()
    try:
        # Enqueued AFTER the worker went idle: with a 30s poll interval, only
        # the pg_notify sent by start_run can explain a fast pickup.
        run_id = await eng.start_run("quick", "1", {}, actor="test")

        async def completed() -> bool:
            return await _run_status(session, run_id) == "completed"

        await wait_until(completed, timeout=2.0)
        assert await _queue_row(session, run_id) is None
    finally:
        await worker.stop()


# ---------------------------------------------------------------------------
# Reaper / heartbeat
# ---------------------------------------------------------------------------


async def test_reap_stale_recovers_killed_claim(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    factory = session_factory(engine)
    eng = WorkflowEngine(factory, _quick_registry())
    run_id = await eng.start_run("quick", "1", {}, actor="test")

    # "Worker" claims, then dies without ever heartbeating again.
    assert await claim_one(session, "dead-worker") == run_id
    assert await claim_one(session, "w2") is None  # claimed rows are invisible

    assert await reap_stale(session, older_than_seconds=0) == 1
    row = await _queue_row(session, run_id)
    assert row is not None and row.claimed_by is None and row.heartbeat_at is None

    # A second worker claims and completes it; replay makes this safe.
    assert await claim_one(session, "w2") == run_id
    outcome = await eng.execute_run(run_id)
    assert outcome.status == "completed"
    assert await _queue_row(session, run_id) is None


class GatedHandler:
    """Skill handler that blocks until released — a controllable 'slow' step."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def __call__(self, step: StepInvocation) -> StepResult:
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        return StepResult(result="slow-done")


async def test_heartbeat_keeps_slow_run_from_being_reaped(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    handler = GatedHandler()
    registry = WorkflowRegistry()

    @workflow(name="slow", version="1")
    async def slow(ctx: WorkflowContext) -> Any:
        return await ctx.run_skill("crunch", input={})

    registry.register(slow)
    factory = session_factory(engine)
    eng = WorkflowEngine(factory, registry, handlers={"skill": handler})
    worker = Worker(eng, factory, poll_interval=0.05, heartbeat_interval=0.05)
    await worker.start()
    try:
        run_id = await eng.start_run("slow", "1", {}, actor="test")
        await asyncio.wait_for(handler.entered.wait(), 5)
        # Long enough that the CLAIM-time heartbeat is stale on its own; only
        # the worker's 50ms heartbeat loop can keep the claim fresh.
        await asyncio.sleep(0.7)
        assert await reap_stale(session, older_than_seconds=0.3) == 0
        row = await _queue_row(session, run_id)
        assert row is not None and row.claimed_by == worker.worker_id

        handler.release.set()

        async def completed() -> bool:
            return await _run_status(session, run_id) == "completed"

        await wait_until(completed)
        assert handler.calls == 1
        assert await _queue_row(session, run_id) is None
    finally:
        await worker.stop()


# ---------------------------------------------------------------------------
# Scheduler singleton
# ---------------------------------------------------------------------------


class NotifyRecorder:
    def __init__(self) -> None:
        self.calls: list[UUID] = []

    async def __call__(self, run_id: UUID) -> None:
        self.calls.append(run_id)


async def test_two_schedulers_fire_each_timer_exactly_once(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    registry = WorkflowRegistry()

    @workflow(name="napper", version="1")
    async def napper(ctx: WorkflowContext) -> str:
        await ctx.sleep("cool", timedelta(seconds=-1))  # already due
        return "woke"

    registry.register(napper)
    factory = session_factory(engine)
    eng = WorkflowEngine(factory, registry)
    run_ids = [await eng.start_run("napper", "1", {}, actor="test") for _ in range(3)]
    for run_id in run_ids:
        assert (await eng.execute_run(run_id)).status == "waiting_timer"

    recorder = NotifyRecorder()
    schedulers = [Scheduler(engine, interval=0.05, notify=recorder) for _ in range(2)]
    for scheduler in schedulers:
        await scheduler.start()
    try:

        async def all_fired() -> bool:
            result = await session.execute(select(schema.timers.c.timer_id))
            remaining = result.all()
            await session.rollback()
            return remaining == []

        await wait_until(all_fired)
        # Let several more ticks (both schedulers) pass to catch double fires.
        await asyncio.sleep(0.3)
    finally:
        for scheduler in schedulers:
            await scheduler.stop()

    # Exactly one wake per run — the advisory lock serialized the loops and
    # the fired timer was deleted before it could fire again.
    assert sorted(str(r) for r in recorder.calls) == sorted(str(r) for r in run_ids)
    for run_id in run_ids:
        events = await EventLog(session).list(run_id)
        completed = [e for e in events if isinstance(e, StepCompleted)]
        assert [(e.step_key, e.result) for e in completed] == [("cool", {"slept": True})]
        row = await _queue_row(session, run_id)
        assert row is not None and row.claimed_by is None  # re-enqueued, claimable


async def test_scheduler_fire_racing_the_suspend_delete_does_not_eat_the_wake(
    engine: AsyncEngine, session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression (F1): a fire_due_timers tick landing between ctx.sleep's
    committed timer insert and the suspend path's queue-row delete used to
    have its re-enqueue eaten — no queue row, no timer row, run stranded in
    waiting_timer forever. The suspend path now reconciles AFTER the delete
    (WorkflowEngine._reconcile_suspend) and must re-enqueue. Deterministic:
    the fire is injected exactly into the race window via the delete hook."""
    registry = WorkflowRegistry()

    @workflow(name="napper", version="1")
    async def napper(ctx: WorkflowContext) -> str:
        await ctx.sleep("beat", timedelta(seconds=-1))  # already due
        return "woke"

    registry.register(napper)
    factory = session_factory(engine)
    eng = WorkflowEngine(factory, registry)

    fired: list[int] = []
    orig = WorkflowEngine._delete_queue_row

    async def racy_delete(racy_session: AsyncSession, run_id: UUID) -> None:
        if not fired:  # race only the suspend's delete, not later bookkeeping
            async with factory() as fire_session:
                fired.append(await fire_due_timers(fire_session))
        await orig(racy_session, run_id)

    monkeypatch.setattr(WorkflowEngine, "_delete_queue_row", staticmethod(racy_delete))

    run_id = await eng.start_run("napper", "1", {}, actor="test")
    outcome = await eng.execute_run(run_id)
    assert fired == [1]  # the timer really fired inside the window
    # The wake was NOT eaten: the suspend reconciled to a claimable pending run.
    assert outcome.status == "pending"
    assert await _run_status(session, run_id) == "pending"
    row = await _queue_row(session, run_id)
    assert row is not None and row.claimed_by is None

    events = await EventLog(session).list(run_id)
    fired_sleep = [e for e in events if isinstance(e, StepCompleted)]
    assert [(e.step_key, e.result) for e in fired_sleep] == [("beat", {"slept": True})]

    # And the woken run completes normally.
    final = await eng.execute_run(run_id)
    assert (final.status, final.output) == ("completed", "woke")
    assert await _queue_row(session, run_id) is None


# ---------------------------------------------------------------------------
# run_inline (CLI-mode composition)
# ---------------------------------------------------------------------------


async def test_run_inline_completes_a_sleeping_workflow_hands_off(
    engine: AsyncEngine, session: AsyncSession, migrated_url: str
) -> None:
    """Worker + Scheduler in-process: enqueue → NOTIFY claim → durable sleep
    parks the run → scheduler fires the due timer → worker resumes → done,
    with no manual execute_run anywhere."""
    handler = _RecordingHandler()
    registry = WorkflowRegistry()

    @workflow(name="nap-then-work", version="1")
    async def nap_then_work(ctx: WorkflowContext) -> Any:
        await ctx.sleep("beat", timedelta(seconds=-1))  # due immediately
        return await ctx.run_skill("work", input={})

    registry.register(nap_then_work)
    factory = session_factory(engine)
    eng = WorkflowEngine(
        factory, registry, handlers={"skill": handler}, notify=make_notifier(factory)
    )
    async with run_inline(
        eng,
        engine,
        listen_dsn=migrated_url,
        poll_interval=30.0,  # NOTIFY (not polling) must drive every wake
        scheduler_interval=0.05,
    ):
        run_id = await eng.start_run("nap-then-work", "1", {}, actor="test")

        async def completed() -> bool:
            return await _run_status(session, run_id) == "completed"

        await wait_until(completed)

    assert handler.calls == ["work"]  # executed exactly once
    assert await _queue_row(session, run_id) is None


class _RecordingHandler:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __call__(self, step: StepInvocation) -> StepResult:
        self.calls.append(step.label)
        return StepResult(result={"label": step.label})
