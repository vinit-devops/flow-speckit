"""Queue-contention property test (doc 03 §10, ADR-0002 tripwire).

8 concurrent claimer tasks — each opening its OWN AsyncSession from the
factory per claim, mirroring ``test_queue.py`` — race over 25 enqueued runs
of a trivial 2-step workflow. Driven with direct ``claim_one``/``execute_run``
loops rather than full ``Worker`` instances: same claim SQL and execution
path, but deterministic termination and no LISTEN/poll timing in the loop
(the Worker loop itself is covered by ``test_queue.py``).

Asserted:

- every run is claimed EXACTLY once (with zero reaps and no releases, the
  claim audit reduces to exactly-once — no run_id is ever claimed by two
  workers without an intervening release/reap, and there are none);
- every run reaches ``completed``;
- every step executed exactly once (per-run handler counters);
- zero rows are left in ``task_queue``;
- a mid-flight ``reap_stale`` pass with a sane threshold reaps NOTHING while
  workers churn (fresh heartbeats are set at claim time), so it can never
  cause double execution.

A second, deterministic test covers the reap-recovery half of the property:
a worker that dies AFTER a step's checkpoint committed is reaped, the run is
re-claimed and finished, and the committed step never re-executes — replay
memoization is the safety net for at-least-once recovery.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from uuid import UUID

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from flow_speckit.storage import schema
from flow_speckit.storage.db import session_factory
from flow_speckit.workflows import (
    StepInvocation,
    StepResult,
    WorkflowContext,
    WorkflowEngine,
    WorkflowRegistry,
    claim_one,
    reap_stale,
    workflow,
)
from flow_speckit.workflows.events import EventLog, RunCompleted, StepCompleted

N_WORKERS = 8
N_RUNS = 25


class PerRunCounter:
    """Fake "skill" handler counting side-effect executions per (run, label)."""

    def __init__(self) -> None:
        self.counts: Counter[tuple[UUID, str]] = Counter()

    async def __call__(self, step: StepInvocation) -> StepResult:
        self.counts[(step.run_id, step.label)] += 1
        return StepResult(result={"label": step.label})


def _pair_registry() -> WorkflowRegistry:
    registry = WorkflowRegistry()

    @workflow(name="pair", version="1")
    async def pair(ctx: WorkflowContext) -> str:
        await ctx.run_skill("one", input={})
        await ctx.run_skill("two", input={})
        return "done"

    registry.register(pair)
    return registry


async def _task_queue_count(session: AsyncSession) -> int:
    result = await session.execute(select(func.count()).select_from(schema.task_queue))
    count = int(result.scalar_one())
    await session.rollback()
    return count


async def test_contended_claims_execute_every_run_exactly_once(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    factory = session_factory(engine)
    handler = PerRunCounter()
    eng = WorkflowEngine(factory, _pair_registry(), handlers={"skill": handler})
    run_ids = [await eng.start_run("pair", "1", {}, actor="test") for _ in range(N_RUNS)]

    claims: list[tuple[UUID, str]] = []  # audit trail: (run_id, worker) in claim order
    outcomes: dict[UUID, str] = {}
    reaped_mid_flight: list[int] = []

    async def claimer(worker_id: str) -> None:
        while len(outcomes) < N_RUNS:
            async with factory() as own_session:  # own session per claim
                run_id = await claim_one(own_session, worker_id)
            if run_id is None:
                await asyncio.sleep(0.01)  # queue momentarily drained; runs in flight
                continue
            claims.append((run_id, worker_id))
            outcome = await eng.execute_run(run_id)
            outcomes[run_id] = outcome.status

    async def mid_flight_reaper() -> None:
        # One reap pass while workers churn: claim_one stamps a fresh
        # heartbeat, so a sane threshold must reap nothing — proving the
        # reaper cannot yank live claims out from under healthy workers.
        while len(outcomes) < N_RUNS // 2:
            await asyncio.sleep(0.002)
        async with factory() as own_session:
            reaped_mid_flight.append(await reap_stale(own_session, older_than_seconds=30.0))

    workers = [claimer(f"w{i}") for i in range(N_WORKERS)]
    await asyncio.wait_for(asyncio.gather(*workers, mid_flight_reaper()), timeout=15.0)

    # The mid-flight reap ran and disturbed nothing.
    assert reaped_mid_flight == [0]

    # Every run claimed exactly once: no double-claims (SKIP LOCKED), and with
    # zero reaps/releases a second claim of any run_id would be a violation.
    claimed_runs = Counter(run_id for run_id, _ in claims)
    assert claimed_runs == {run_id: 1 for run_id in run_ids}

    # Every run completed.
    assert outcomes == {run_id: "completed" for run_id in run_ids}
    result = await session.execute(
        select(schema.workflow_runs.c.run_id, schema.workflow_runs.c.status)
    )
    statuses = {row.run_id: row.status for row in result.all()}
    await session.rollback()
    assert statuses == {run_id: "completed" for run_id in run_ids}

    # Every step executed exactly once, per run.
    assert handler.counts == {
        (run_id, label): 1 for run_id in run_ids for label in ("one", "two")
    }

    # Exactly one checkpoint per step and one run_completed per run.
    log = EventLog(session)
    for run_id in run_ids:
        events = await log.list(run_id)
        step_counts = Counter(e.step_key for e in events if isinstance(e, StepCompleted))
        assert step_counts == {"one": 1, "two": 1}
        assert sum(isinstance(e, RunCompleted) for e in events) == 1

    # Zero rows left in task_queue.
    assert await _task_queue_count(session) == 0


class CrashSignal(BaseException):
    """BaseException = a killed worker: the engine must record NOTHING."""


async def test_reap_after_crash_never_reexecutes_committed_checkpoints(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    """Worker A claims, crashes right AFTER step "one"'s checkpoint committed;
    ``reap_stale`` frees the stale claim; worker B re-claims and finishes.
    Replay memoization keeps the committed step at exactly one execution."""
    factory = session_factory(engine)
    handler = PerRunCounter()
    crashed = {"fired": False}

    async def crash_hook(checkpoint: str, step_key: str) -> None:
        if not crashed["fired"] and (checkpoint, step_key) == ("after_checkpoint", "one"):
            crashed["fired"] = True
            raise CrashSignal("kill -9 just after the checkpoint committed")

    eng = WorkflowEngine(
        factory, _pair_registry(), handlers={"skill": handler}, fault_hook=crash_hook
    )
    run_id = await eng.start_run("pair", "1", {}, actor="test")

    assert await claim_one(session, "wA") == run_id
    with pytest.raises(CrashSignal):
        await eng.execute_run(run_id)

    # The dead worker's claim blocks everyone until the reaper clears it.
    assert await claim_one(session, "wB") is None
    assert await reap_stale(session, older_than_seconds=0) == 1
    assert await claim_one(session, "wB") == run_id

    outcome = await eng.execute_run(run_id)
    assert outcome.status == "completed"

    # "one" checkpointed before the crash: memoized, never re-executed.
    # "two" never started before the crash: executed exactly once after.
    assert handler.counts == {(run_id, "one"): 1, (run_id, "two"): 1}
    events = await EventLog(session).list(run_id)
    step_counts = Counter(e.step_key for e in events if isinstance(e, StepCompleted))
    assert step_counts == {"one": 1, "two": 1}
    assert await _task_queue_count(session) == 0
