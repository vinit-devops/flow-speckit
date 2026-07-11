"""Crash injection at EVERY checkpoint boundary (doc 03 §10, ADR-0002 tripwire).

The fixture workflow covers the skill-step, intrinsic, retry and gate paths.
A clean discovery run first records every ``(hook_point, step_key)`` pair the
engine's ``fault_hook`` seam visits — the full enumeration, not a sample.
Then, for each discovered pair, a FRESH run is executed with a one-shot hook
that raises a ``BaseException``-derived ``CrashSignal`` at exactly that
occurrence (a simulated ``kill -9``: nothing recorded, propagates), the crash
is caught, and the run is resumed via repeated ``execute_run`` until terminal.

Asserted per scenario:

- the run completes and the event log projects to ``completed``;
- every logical step has exactly ONE ``step_completed`` (replay memoization);
- side-effect counters demonstrate at-least-once: every handler-backed step
  ran >= 1 time, and exactly 2 times PRECISELY when the crash hit
  ``after_side_effect``/``before_checkpoint_commit`` for that step (the side
  effect landed but its checkpoint never committed);
- the maintained ``workflow_runs`` projection row equals ``project_run``'s
  pure fold of the log (rebuildability).
"""

from __future__ import annotations

from collections import Counter
from uuid import UUID

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
    FaultHook,
    RetryPolicy,
    RunOutcome,
    StepInvocation,
    StepResult,
    WorkflowContext,
    WorkflowEngine,
    WorkflowRegistry,
    immediate_backoff,
    resolve_gate,
    workflow,
)
from flow_speckit.workflows.events import (
    EventLog,
    GateOpened,
    GateResolved,
    StepCompleted,
    project_run,
)

HOOK_POINTS = ("after_side_effect", "before_checkpoint_commit", "after_checkpoint")
CHECKPOINTED_STEPS = ("fetch", "uuid", "flaky", "ship")
HANDLER_STEPS = ("fetch", "flaky", "ship")
# The at-least-once window: a crash here lands AFTER the side effect but
# BEFORE its checkpoint committed, so recovery re-executes the step.
REEXECUTION_HOOKS = ("after_side_effect", "before_checkpoint_commit")


class CrashSignal(BaseException):
    """BaseException = a killed worker: the engine must record NOTHING."""


class ChaosHandler:
    """Side-effecting fake skill handler: appends every invocation to a list.

    The "flaky" step fails on its FIRST invocation per handler (per run), so
    the retry path is exercised on every clean segment that reaches it.
    """

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __call__(self, step: StepInvocation) -> StepResult:
        self.calls.append(step.label)
        if step.label == "flaky" and self.calls.count("flaky") == 1:
            raise RuntimeError("transient flaky failure")
        return StepResult(result={"label": step.label})


def make_crash_hook(target: tuple[str, str], crashed: dict[str, bool]) -> FaultHook:
    """A one-shot fault hook: simulate ``kill -9`` at exactly one occurrence
    of ``target`` — recovery passes sail through untouched."""

    async def crash_hook(checkpoint: str, key: str) -> None:
        if not crashed["fired"] and (checkpoint, key) == target:
            crashed["fired"] = True
            raise CrashSignal(f"kill -9 at {checkpoint}:{key}")

    return crash_hook


def _chaos_registry() -> WorkflowRegistry:
    registry = WorkflowRegistry()

    @workflow(name="chaos", version="1")
    async def chaos(ctx: WorkflowContext, artifact_id: str) -> dict[str, object]:
        fetched = await ctx.run_skill("fetch", input={})
        token = str(await ctx.uuid())  # intrinsic path
        crunched = await ctx.run_skill(  # retry path (fails once, then succeeds)
            "flaky", input={}, retry=RetryPolicy(max_attempts=3, retry_on=(RuntimeError,))
        )
        decision = await ctx.gate(  # gate path (suspend + resolve_gate resume)
            "signoff", artifact=UUID(artifact_id), approvers=["role:eng"]
        )
        shipped = await ctx.run_skill("ship", input={})
        return {
            "fetched": fetched,
            "token": token,
            "crunched": crunched,
            "approved": decision.approved,
            "shipped": shipped,
        }

    registry.register(chaos)
    return registry


@pytest.fixture()
def store(session: AsyncSession) -> ArtifactStore:
    reg = ArtifactRegistry()
    reg.register(GenericArtifact, source_package="flow-speckit")
    return ArtifactStore(session, reg)


async def _drive_to_terminal(
    eng: WorkflowEngine, session: AsyncSession, run_id: UUID
) -> RunOutcome:
    """Resume via repeated execute_run (catching the injected crash) until
    terminal, resolving the fixture's gate whenever the run parks on it."""
    for _ in range(30):
        try:
            outcome = await eng.execute_run(run_id)
        except CrashSignal:
            continue  # the worker "died"; a fresh pass replays from the log
        if outcome.status in ("completed", "failed", "cancelled"):
            return outcome
        if outcome.status == "waiting_gate":
            await resolve_gate(session, run_id, "signoff", "approved", "user:test")
    pytest.fail(f"run {run_id} never reached a terminal state")


async def _assert_projection_row_matches_fold(session: AsyncSession, run_id: UUID) -> None:
    events = await EventLog(session).list(run_id)
    fold = project_run(run_id, events)
    result = await session.execute(
        select(schema.workflow_runs).where(schema.workflow_runs.c.run_id == run_id)
    )
    row = result.one()
    await session.rollback()
    assert (
        row.workflow_name,
        row.workflow_version,
        row.status,
        row.current_step,
        row.input,
        row.output_ref,
        row.error,
    ) == (
        fold.workflow_name,
        fold.workflow_version,
        fold.status,
        fold.current_step,
        fold.input,
        fold.output_ref,
        fold.error,
    )


async def test_crash_at_every_checkpoint_boundary_recovers(
    engine: AsyncEngine, session: AsyncSession, store: ArtifactStore
) -> None:
    factory = session_factory(engine)
    registry = _chaos_registry()

    # -- discovery: run once cleanly, recording every boundary the engine visits.
    discovered: list[tuple[str, str]] = []

    async def recording_hook(checkpoint: str, step_key: str) -> None:
        discovered.append((checkpoint, step_key))

    clean_handler = ChaosHandler()
    clean_ref: ArtifactRef = await store.create(
        GenericArtifact(title="Chaos clean"), key="briefs/chaos-clean"
    )
    clean_eng = WorkflowEngine(
        factory,
        registry,
        handlers={"skill": clean_handler},
        fault_hook=recording_hook,
        backoff=immediate_backoff,
    )
    clean_id = await clean_eng.start_run(
        "chaos", "1", {"artifact_id": str(clean_ref.id)}, actor="test"
    )
    assert (await _drive_to_terminal(clean_eng, session, clean_id)).status == "completed"

    # The FULL boundary matrix, discovered programmatically — not sampled.
    assert set(discovered) == {
        (hook, step) for step in CHECKPOINTED_STEPS for hook in HOOK_POINTS
    }
    assert len(discovered) == len(CHECKPOINTED_STEPS) * len(HOOK_POINTS)

    # -- crash injection: one FRESH run per discovered boundary.
    for index, target in enumerate(discovered):
        hook_point, step_key = target
        crashed = {"fired": False}
        handler = ChaosHandler()
        eng = WorkflowEngine(
            factory,
            registry,
            handlers={"skill": handler},
            fault_hook=make_crash_hook(target, crashed),
            backoff=immediate_backoff,
        )
        ref = await store.create(
            GenericArtifact(title=f"Chaos {index}"), key=f"briefs/chaos-{index}"
        )
        run_id = await eng.start_run(
            "chaos", "1", {"artifact_id": str(ref.id)}, actor="test"
        )

        outcome = await _drive_to_terminal(eng, session, run_id)
        assert crashed["fired"], f"boundary {target} was never reached"
        assert outcome.status == "completed", f"crash at {target} did not converge"

        events = await EventLog(session).list(run_id)
        assert project_run(run_id, events).status == "completed"

        # Every logical step checkpointed exactly once, whatever the crash.
        completed_counts = Counter(
            e.step_key for e in events if isinstance(e, StepCompleted)
        )
        assert completed_counts == {step: 1 for step in CHECKPOINTED_STEPS}, (
            f"crash at {target}: {completed_counts}"
        )
        # The gate opened and resolved exactly once.
        assert sum(isinstance(e, GateOpened) for e in events) == 1
        assert sum(isinstance(e, GateResolved) for e in events) == 1

        # At-least-once side-effect accounting: "flaky" always runs twice on
        # a clean segment (fail + success); a crash in the post-side-effect /
        # pre-checkpoint window adds exactly one re-execution of THAT step.
        expected = {"fetch": 1, "flaky": 2, "ship": 1}
        if step_key in expected and hook_point in REEXECUTION_HOOKS:
            expected[step_key] += 1
        assert Counter(handler.calls) == expected, f"crash at {target}: {handler.calls}"
        assert all(count >= 1 for count in expected.values())

        # Rebuildability: the maintained row equals the pure fold at rest.
        await _assert_projection_row_matches_fold(session, run_id)

        # Terminal: nothing left claimable.
        queue = (
            await session.execute(
                select(schema.task_queue).where(schema.task_queue.c.run_id == run_id)
            )
        ).all()
        await session.rollback()
        assert queue == []
