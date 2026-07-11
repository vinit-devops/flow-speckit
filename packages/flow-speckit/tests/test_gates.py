"""Human approval gates (doc 03 §6): full open/suspend/resolve/resume
lifecycle, the rejection feedback loop with re-gating under ``label#2``,
timeout policies (fail / approve / escalate), auto-approve mode, and
resolution error paths. Timeouts use negative durations so timers are already
due — tests never sleep real seconds."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import Row, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from flow_speckit.artifacts.models import GenericArtifact
from flow_speckit.artifacts.refs import ArtifactRef
from flow_speckit.artifacts.registry import ArtifactRegistry
from flow_speckit.artifacts.store import ArtifactStore
from flow_speckit.storage import schema
from flow_speckit.storage.db import session_factory
from flow_speckit.workflows import (
    GateDecision,
    GateNotOpenError,
    StepInvocation,
    StepResult,
    WorkflowContext,
    WorkflowEngine,
    WorkflowRegistry,
    fire_due_timers,
    resolve_gate,
    workflow,
)
from flow_speckit.workflows.events import EventLog, GateOpened, GateResolved, RunFailed


class EchoHandler:
    """Fake "skill" handler echoing its input payload."""

    def __init__(self) -> None:
        self.calls: list[StepInvocation] = []

    async def __call__(self, step: StepInvocation) -> StepResult:
        self.calls.append(step)
        return StepResult(result={"echo": step.payload["input"]})


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


def make_engine(
    db: AsyncEngine,
    registry: WorkflowRegistry,
    handler: EchoHandler | None = None,
    **kwargs: Any,
) -> WorkflowEngine:
    handlers = {"skill": handler} if handler is not None else {}
    return WorkflowEngine(session_factory(db), registry, handlers=handlers, **kwargs)


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


async def _run_status(session: AsyncSession, run_id: UUID) -> str:
    result = await session.execute(
        select(schema.workflow_runs.c.status).where(schema.workflow_runs.c.run_id == run_id)
    )
    return str(result.scalar_one())


def _gated_registry(ref: ArtifactRef) -> WorkflowRegistry:
    """One-gate workflow; the gate takes an ArtifactRef (the ``.id`` seam)."""
    registry = WorkflowRegistry()

    @workflow(name="gated", version="1")
    async def gated(ctx: WorkflowContext) -> dict[str, Any]:
        decision = await ctx.gate(
            "brief_approval", artifact=ref, approvers=["role:product"]
        )
        return {
            "approved": decision.approved,
            "actor": decision.actor,
            "comment": decision.comment,
        }

    registry.register(gated)
    return registry


async def test_gate_full_lifecycle_approved(
    engine: AsyncEngine, session: AsyncSession, store: ArtifactStore
) -> None:
    ref = await store.create(GenericArtifact(title="Brief"), key="briefs/x")
    notify = NotifyRecorder()
    eng = make_engine(engine, _gated_registry(ref))
    run_id = await eng.start_run("gated", "1", {}, actor="test")

    outcome = await eng.execute_run(run_id)
    assert outcome.status == "waiting_gate"
    assert await _run_status(session, run_id) == "waiting_gate"
    assert await _queue_rows(session, run_id) == []  # unclaimable while waiting

    events = await EventLog(session).list(run_id)
    opened = [e for e in events if isinstance(e, GateOpened)]
    assert len(opened) == 1
    gate = opened[0]
    assert gate.step_key == "brief_approval"
    assert gate.gate_key == "brief_approval"
    assert gate.artifact_id == ref.id
    assert gate.approvers == ["role:product"]
    assert gate.timeout_at is None and gate.on_timeout == "fail"

    # Re-executing while waiting re-suspends WITHOUT a duplicate gate_opened.
    assert (await eng.execute_run(run_id)).status == "waiting_gate"
    assert await EventLog(session).list(run_id) == events

    decision = await resolve_gate(
        session, run_id, "brief_approval", "approved", "user:vinit",
        comment="lgtm", notify=notify,
    )
    assert isinstance(decision, GateDecision)
    assert decision.approved and not decision.rejected
    assert decision.resolved_at is not None
    assert notify.calls == [run_id]
    # Approval mirrored onto the artifact and the run re-enqueued.
    assert (await store.resolve(ref.id)).status == "approved"
    queue = await _queue_rows(session, run_id)
    assert len(queue) == 1 and queue[0].claimed_by is None
    assert await _run_status(session, run_id) == "pending"

    outcome = await eng.execute_run(run_id)
    assert outcome.status == "completed"
    assert outcome.output == {"approved": True, "actor": "user:vinit", "comment": "lgtm"}


async def test_rejection_feedback_loop_regates_under_new_step_key(
    engine: AsyncEngine, session: AsyncSession, store: ArtifactStore
) -> None:
    a1 = await store.create(GenericArtifact(title="Brief v1"), key="briefs/a")
    a2 = await store.create(GenericArtifact(title="Brief v2"), key="briefs/b")
    handler = EchoHandler()
    registry = WorkflowRegistry()

    @workflow(name="loopy", version="1")
    async def loopy(ctx: WorkflowContext, a1_id: str, a2_id: str) -> dict[str, Any]:
        first = await ctx.gate(
            "approval", artifact=UUID(a1_id), approvers=["role:product"]
        )
        if first.rejected:
            await ctx.run_skill("frame", input={"feedback": first.comment})
            second = await ctx.gate(
                "approval", artifact=UUID(a2_id), approvers=["role:product"]
            )
            return {"first": first.decision, "second": second.decision}
        return {"first": first.decision}

    registry.register(loopy)
    eng = make_engine(engine, registry, handler)
    run_id = await eng.start_run(
        "loopy", "1", {"a1_id": str(a1.id), "a2_id": str(a2.id)}, actor="test"
    )
    assert (await eng.execute_run(run_id)).status == "waiting_gate"

    rejection = await resolve_gate(
        session, run_id, "approval", "rejected", "user:pm", comment="needs work"
    )
    assert rejection.rejected
    assert (await store.resolve(a1.id)).status == "rejected"

    # Woken replay branches on the rejection: the feedback comment becomes
    # skill input, and re-gating allocates step key "approval#2".
    assert (await eng.execute_run(run_id)).status == "waiting_gate"
    assert [c.payload["input"] for c in handler.calls] == [{"feedback": "needs work"}]
    events = await EventLog(session).list(run_id)
    opened = [e for e in events if isinstance(e, GateOpened)]
    assert [(g.step_key, g.gate_key) for g in opened] == [
        ("approval", "approval"),
        ("approval#2", "approval"),
    ]

    # Resolving the shared gate key targets the currently OPEN instance (#2).
    approval = await resolve_gate(session, run_id, "approval", "approved", "user:pm")
    assert approval.approved
    assert (await store.resolve(a2.id)).status == "approved"

    outcome = await eng.execute_run(run_id)
    assert outcome.status == "completed"
    assert outcome.output == {"first": "rejected", "second": "approved"}
    assert len(handler.calls) == 1  # the feedback skill ran exactly once


async def test_gate_timeout_fail_policy_fails_the_run(
    engine: AsyncEngine, session: AsyncSession, store: ArtifactStore
) -> None:
    ref = await store.create(GenericArtifact(title="Brief"), key="briefs/t")
    registry = WorkflowRegistry()

    @workflow(name="timed", version="1")
    async def timed(ctx: WorkflowContext) -> str:
        await ctx.gate(
            "signoff",
            artifact=ref.id,
            approvers=["role:lead"],
            timeout=timedelta(seconds=-1),  # already due
        )
        return "ok"

    registry.register(timed)
    eng = make_engine(engine, registry)
    run_id = await eng.start_run("timed", "1", {}, actor="test")
    assert (await eng.execute_run(run_id)).status == "waiting_gate"
    timers = await _timer_rows(session, run_id)
    assert [(t.kind, t.step_key) for t in timers] == [("gate_timeout", "signoff")]

    assert await fire_due_timers(session) == 1

    events = await EventLog(session).list(run_id)
    failed = [e for e in events if isinstance(e, RunFailed)]
    assert len(failed) == 1
    assert failed[0].failed_step == "signoff"
    assert "timed out" in failed[0].error["message"]
    assert await _run_status(session, run_id) == "failed"
    assert await _timer_rows(session, run_id) == []
    assert await _queue_rows(session, run_id) == []
    # The artifact is untouched by a timeout failure.
    assert (await store.resolve(ref.id)).status == "proposed"


async def test_gate_timeout_approve_policy_resolves_as_timeout_actor(
    engine: AsyncEngine, session: AsyncSession, store: ArtifactStore
) -> None:
    ref = await store.create(GenericArtifact(title="Brief"), key="briefs/u")
    notify = NotifyRecorder()
    registry = WorkflowRegistry()

    @workflow(name="lenient", version="1")
    async def lenient(ctx: WorkflowContext) -> dict[str, Any]:
        decision = await ctx.gate(
            "signoff",
            artifact=ref.id,
            approvers=["role:lead"],
            timeout=timedelta(seconds=-1),
            on_timeout="approve",
        )
        return {"actor": decision.actor, "approved": decision.approved}

    registry.register(lenient)
    eng = make_engine(engine, registry)
    run_id = await eng.start_run("lenient", "1", {}, actor="test")
    assert (await eng.execute_run(run_id)).status == "waiting_gate"

    assert await fire_due_timers(session, notify=notify) == 1
    assert notify.calls == [run_id]
    assert (await store.resolve(ref.id)).status == "approved"
    assert len(await _queue_rows(session, run_id)) == 1

    outcome = await eng.execute_run(run_id)
    assert outcome.status == "completed"
    assert outcome.output == {"actor": "timeout", "approved": True}


async def test_gate_timeout_escalate_rearms_timer_and_notifies(
    engine: AsyncEngine, session: AsyncSession, store: ArtifactStore
) -> None:
    ref = await store.create(GenericArtifact(title="Brief"), key="briefs/v")
    notify = NotifyRecorder()
    registry = WorkflowRegistry()

    @workflow(name="escalating", version="1")
    async def escalating(ctx: WorkflowContext) -> str:
        await ctx.gate(
            "signoff",
            artifact=ref.id,
            approvers=["role:lead"],
            timeout=timedelta(seconds=-1),
            on_timeout="escalate",
            escalate_to=["role:cto"],
        )
        return "ok"

    registry.register(escalating)
    eng = make_engine(engine, registry)
    run_id = await eng.start_run("escalating", "1", {}, actor="test")
    assert (await eng.execute_run(run_id)).status == "waiting_gate"
    (original_timer,) = await _timer_rows(session, run_id)

    assert await fire_due_timers(session, notify=notify) == 1
    assert notify.calls == [run_id]
    # Still waiting; the timer was re-armed (fresh row), nothing resolved.
    assert await _run_status(session, run_id) == "waiting_gate"
    (rearmed,) = await _timer_rows(session, run_id)
    assert rearmed.timer_id != original_timer.timer_id
    assert rearmed.kind == "gate_timeout" and rearmed.step_key == "signoff"
    events = await EventLog(session).list(run_id)
    assert not any(isinstance(e, GateResolved) for e in events)
    assert await _queue_rows(session, run_id) == []  # still parked

    # The escalated gate resolves like any other.
    await resolve_gate(session, run_id, "signoff", "approved", "role:cto")
    assert (await eng.execute_run(run_id)).status == "completed"


async def test_auto_approve_resolves_gates_without_suspending(
    engine: AsyncEngine, session: AsyncSession, store: ArtifactStore
) -> None:
    ref = await store.create(GenericArtifact(title="Brief"), key="briefs/w")
    registry = WorkflowRegistry()

    @workflow(name="demo", version="1")
    async def demo(ctx: WorkflowContext) -> dict[str, Any]:
        decision = await ctx.gate(
            "signoff",
            artifact=ref.id,
            approvers=["role:lead"],
            timeout=timedelta(days=7),
        )
        return {"actor": decision.actor, "comment": decision.comment}

    registry.register(demo)
    eng = make_engine(engine, registry, auto_approve=True)
    run_id = await eng.start_run("demo", "1", {}, actor="test")

    outcome = await eng.execute_run(run_id)  # completes in ONE pass
    assert outcome.status == "completed"
    assert outcome.output == {"actor": "auto", "comment": "auto-approved"}

    events = await EventLog(session).list(run_id)
    opened = [e for e in events if isinstance(e, GateOpened)]
    resolved = [e for e in events if isinstance(e, GateResolved)]
    assert len(opened) == 1 and len(resolved) == 1
    assert resolved[0].actor == "auto" and resolved[0].decision == "approved"
    # No gate_timeout timer is armed for an auto-approved gate.
    assert await _timer_rows(session, run_id) == []
    # Replay of the completed run returns the same decision.
    assert (await eng.execute_run(run_id)).output == outcome.output


async def test_resolve_errors_unknown_gate_and_duplicate_resolve(
    engine: AsyncEngine, session: AsyncSession, store: ArtifactStore
) -> None:
    ref = await store.create(GenericArtifact(title="Brief"), key="briefs/y")
    eng = make_engine(engine, _gated_registry(ref))
    run_id = await eng.start_run("gated", "1", {}, actor="test")
    assert (await eng.execute_run(run_id)).status == "waiting_gate"

    with pytest.raises(GateNotOpenError, match="no open gate 'nope'"):
        await resolve_gate(session, run_id, "nope", "approved", "user:vinit")

    await resolve_gate(session, run_id, "brief_approval", "approved", "user:vinit")
    with pytest.raises(GateNotOpenError, match="already resolved"):
        await resolve_gate(session, run_id, "brief_approval", "approved", "user:vinit")


async def test_resolve_tolerates_artifact_already_in_target_status(
    engine: AsyncEngine, session: AsyncSession, store: ArtifactStore
) -> None:
    ref = await store.create(GenericArtifact(title="Brief"), key="briefs/z")
    await store.set_status(ref.id, "approved", actor="user:pre")  # pre-approved
    eng = make_engine(engine, _gated_registry(ref))
    run_id = await eng.start_run("gated", "1", {}, actor="test")
    assert (await eng.execute_run(run_id)).status == "waiting_gate"

    decision = await resolve_gate(
        session, run_id, "brief_approval", "approved", "user:vinit"
    )
    assert decision.approved
    assert (await store.resolve(ref.id)).status == "approved"
    assert (await eng.execute_run(run_id)).status == "completed"
