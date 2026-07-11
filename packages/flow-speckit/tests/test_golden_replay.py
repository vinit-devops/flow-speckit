"""Golden replay tests (doc 03 §10, ADR-0002 tripwire).

For each fixture workflow — linear-with-intrinsics, loop with repeated
labels, parallel fan-out, gate + rejection feedback loop, child workflow —
the run is driven to completion, the full event log is snapshotted, and two
further ``execute_run`` passes must (a) append ZERO new events (the typed
log compares equal payload-for-payload) and (b) return the same
``RunOutcome`` status/output. Each fixture also pins its ordered
``(event_type, step_key)`` sequence to an inline golden literal — the
regression tripwire that catches accidental event-shape drift.

Non-determinism hardening (T10c) lives here too: mutations of a COMPLETED
run's body (removed / renamed / extra step) raise ``NonDeterminismError``
without recording anything (the sealed-log contract in ``engine.py``); a
pure reorder of distinct labels is deliberately TOLERATED by the key-based
replay cursor (documented below); and a same-version body change mid-flight
(run parked at a gate) fails loudly at completion with ``run_failed``
recorded and the version-bump guidance in the message.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from flow_speckit.artifacts.models import GenericArtifact
from flow_speckit.artifacts.registry import ArtifactRegistry
from flow_speckit.artifacts.store import ArtifactStore
from flow_speckit.storage import schema
from flow_speckit.storage.db import session_factory
from flow_speckit.workflows import (
    NonDeterminismError,
    RunOutcome,
    StepInvocation,
    StepResult,
    WorkflowContext,
    WorkflowEngine,
    WorkflowRegistry,
    child_run_id,
    resolve_gate,
    workflow,
)
from flow_speckit.workflows.events import EventLog, WorkflowEvent, project_run


class RecordingHandler:
    """Fake "skill" handler recording every live side-effect by label."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __call__(self, step: StepInvocation) -> StepResult:
        self.calls.append(step.label)
        return StepResult(result={"label": step.label, "input": step.payload["input"]})


@pytest.fixture()
def store(session: AsyncSession) -> ArtifactStore:
    reg = ArtifactRegistry()
    reg.register(GenericArtifact, source_package="flow-speckit")
    return ArtifactStore(session, reg)


def make_engine(
    db: AsyncEngine,
    registry: WorkflowRegistry,
    handler: RecordingHandler | None = None,
    **kwargs: Any,
) -> WorkflowEngine:
    handlers = {"skill": handler} if handler is not None else {}
    return WorkflowEngine(session_factory(db), registry, handlers=handlers, **kwargs)


def _shape(events: list[WorkflowEvent]) -> list[tuple[str, str | None]]:
    """The ordered (event_type, step_key) sequence — the golden fingerprint."""
    return [(e.event_type, getattr(e, "step_key", None)) for e in events]


async def _assert_golden_replay(
    eng: WorkflowEngine,
    session: AsyncSession,
    run_id: UUID,
    first: RunOutcome,
) -> list[WorkflowEvent]:
    """Replay twice; the log must be byte-for-byte unchanged and the outcome
    identical. Returns the (stable) typed event list."""
    log = EventLog(session)
    before = await log.list(run_id)
    second = await eng.execute_run(run_id)
    third = await eng.execute_run(run_id)
    after = await log.list(run_id)
    # Zero new events, identical payloads (typed-model equality covers every field).
    assert after == before
    for replay in (second, third):
        assert replay.status == first.status
        assert replay.output == first.output
    assert project_run(run_id, after).status == "completed"
    return before


# ---------------------------------------------------------------------------
# Fixture 1: linear multi-step with intrinsics
# ---------------------------------------------------------------------------


async def test_golden_replay_linear_with_intrinsics(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    handler = RecordingHandler()
    registry = WorkflowRegistry()

    @workflow(name="linear", version="1")
    async def linear(ctx: WorkflowContext, idea: str) -> dict[str, Any]:
        frame = await ctx.run_skill("frame", input={"idea": idea})
        stamp = (await ctx.now()).isoformat()
        seed = await ctx.random()
        token = str(await ctx.uuid())
        design = await ctx.run_skill("design", input=frame)
        return {"frame": frame, "stamp": stamp, "seed": seed, "token": token, "design": design}

    registry.register(linear)
    eng = make_engine(engine, registry, handler)
    run_id = await eng.start_run("linear", "1", {"idea": "x"}, actor="test")
    first = await eng.execute_run(run_id)
    assert first.status == "completed"

    events = await _assert_golden_replay(eng, session, run_id, first)
    assert _shape(events) == [
        ("run_started", None),
        ("step_started", "frame"),
        ("step_completed", "frame"),
        ("step_started", "now"),
        ("step_completed", "now"),
        ("step_started", "random"),
        ("step_completed", "random"),
        ("step_started", "uuid"),
        ("step_completed", "uuid"),
        ("step_started", "design"),
        ("step_completed", "design"),
        ("run_completed", None),
    ]
    assert handler.calls == ["frame", "design"]  # zero re-execution across 3 passes


# ---------------------------------------------------------------------------
# Fixture 2: loop with repeated labels
# ---------------------------------------------------------------------------


async def test_golden_replay_loop_with_repeated_labels(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    handler = RecordingHandler()
    registry = WorkflowRegistry()

    @workflow(name="looped", version="1")
    async def looped(ctx: WorkflowContext) -> list[Any]:
        return [await ctx.run_skill("poll", input={"i": i}) for i in range(3)]

    registry.register(looped)
    eng = make_engine(engine, registry, handler)
    run_id = await eng.start_run("looped", "1", {}, actor="test")
    first = await eng.execute_run(run_id)
    assert first.status == "completed"

    events = await _assert_golden_replay(eng, session, run_id, first)
    assert _shape(events) == [
        ("run_started", None),
        ("step_started", "poll"),
        ("step_completed", "poll"),
        ("step_started", "poll#2"),
        ("step_completed", "poll#2"),
        ("step_started", "poll#3"),
        ("step_completed", "poll#3"),
        ("run_completed", None),
    ]
    assert handler.calls == ["poll", "poll", "poll"]


# ---------------------------------------------------------------------------
# Fixture 3: parallel fan-out
# ---------------------------------------------------------------------------


async def test_golden_replay_parallel_fan_out(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    handler = RecordingHandler()
    registry = WorkflowRegistry()

    @workflow(name="fan-out", version="1")
    async def fan_out(ctx: WorkflowContext) -> list[Any]:
        return await ctx.parallel(
            [
                ctx.run_skill("a", input={}),
                ctx.run_skill("b", input={}),
                ctx.run_skill("c", input={}),
            ]
        )

    registry.register(fan_out)
    eng = make_engine(engine, registry, handler)
    run_id = await eng.start_run("fan-out", "1", {}, actor="test")
    first = await eng.execute_run(run_id)
    assert first.status == "completed"
    assert first.output == [
        {"label": "a", "input": {}},
        {"label": "b", "input": {}},
        {"label": "c", "input": {}},
    ]

    events = await _assert_golden_replay(eng, session, run_id, first)
    # Branch interleaving is not deterministic across executions, so the
    # golden literal for the parallel section is CANONICALIZED (sorted); the
    # byte-for-byte stability above is what pins this run's recorded order.
    shape = _shape(events)
    assert shape[0] == ("run_started", None)
    assert shape[-1] == ("run_completed", None)
    assert sorted(shape[1:-1]) == [
        ("step_completed", "a"),
        ("step_completed", "b"),
        ("step_completed", "c"),
        ("step_started", "a"),
        ("step_started", "b"),
        ("step_started", "c"),
    ]
    for key in ("a", "b", "c"):  # each branch's start precedes its checkpoint
        assert shape.index(("step_started", key)) < shape.index(("step_completed", key))
    assert sorted(handler.calls) == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Fixture 4: gate + rejection feedback loop (resolved via resolve_gate)
# ---------------------------------------------------------------------------


async def test_golden_replay_gate_rejection_feedback_loop(
    engine: AsyncEngine, session: AsyncSession, store: ArtifactStore
) -> None:
    a1 = await store.create(GenericArtifact(title="Brief v1"), key="briefs/golden-1")
    a2 = await store.create(GenericArtifact(title="Brief v2"), key="briefs/golden-2")
    handler = RecordingHandler()
    registry = WorkflowRegistry()

    @workflow(name="review-loop", version="1")
    async def review_loop(ctx: WorkflowContext, first_id: str, second_id: str) -> dict[str, Any]:
        d1 = await ctx.gate("signoff", artifact=UUID(first_id), approvers=["role:eng"])
        if d1.approved:
            return {"rounds": 1}
        await ctx.run_skill("revise", input={"feedback": d1.comment})
        d2 = await ctx.gate("signoff", artifact=UUID(second_id), approvers=["role:eng"])
        return {"rounds": 2, "second": d2.decision}

    registry.register(review_loop)
    eng = make_engine(engine, registry, handler)
    run_id = await eng.start_run(
        "review-loop", "1", {"first_id": str(a1.id), "second_id": str(a2.id)}, actor="test"
    )

    assert (await eng.execute_run(run_id)).status == "waiting_gate"
    await resolve_gate(session, run_id, "signoff", "rejected", "user:pm", comment="needs work")
    assert (await eng.execute_run(run_id)).status == "waiting_gate"  # re-gated as signoff#2
    await resolve_gate(session, run_id, "signoff", "approved", "user:pm", comment="lgtm")
    first = await eng.execute_run(run_id)
    assert first.status == "completed"
    assert first.output == {"rounds": 2, "second": "approved"}

    events = await _assert_golden_replay(eng, session, run_id, first)
    assert _shape(events) == [
        ("run_started", None),
        ("step_started", "signoff"),
        ("gate_opened", "signoff"),
        ("gate_resolved", "signoff"),
        ("step_started", "revise"),
        ("step_completed", "revise"),
        ("step_started", "signoff#2"),
        ("gate_opened", "signoff#2"),
        ("gate_resolved", "signoff#2"),
        ("run_completed", None),
    ]
    assert handler.calls == ["revise"]


# ---------------------------------------------------------------------------
# Fixture 5: child workflow
# ---------------------------------------------------------------------------


async def test_golden_replay_child_workflow(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    registry = WorkflowRegistry()

    @workflow(name="leaf", version="1")
    async def leaf(ctx: WorkflowContext, x: int) -> str:
        return str(await ctx.uuid())

    @workflow(name="trunk", version="1")
    async def trunk(ctx: WorkflowContext) -> Any:
        return await ctx.child_workflow("spawn", name="leaf", input={"x": 1})

    registry.register(leaf)
    registry.register(trunk)
    eng = make_engine(engine, registry)
    parent_id = await eng.start_run("trunk", "1", {}, actor="test")

    assert (await eng.execute_run(parent_id)).status == "running"  # parked on the child
    child_id = child_run_id(parent_id, "spawn")
    child_first = await eng.execute_run(child_id)
    assert child_first.status == "completed"
    first = await eng.execute_run(parent_id)  # settle re-enqueued the parent
    assert first.status == "completed"
    assert first.output == {"child_run_id": str(child_id), "output_ref": None}

    parent_events = await _assert_golden_replay(eng, session, parent_id, first)
    assert _shape(parent_events) == [
        ("run_started", None),
        ("step_started", "spawn"),
        ("step_completed", "spawn"),
        ("run_completed", None),
    ]
    child_events = await _assert_golden_replay(eng, session, child_id, child_first)
    assert _shape(child_events) == [
        ("run_started", None),
        ("step_started", "uuid"),
        ("step_completed", "uuid"),
        ("run_completed", None),
    ]


# ---------------------------------------------------------------------------
# T10c: non-determinism hardening
# ---------------------------------------------------------------------------


def _two_step_registry(name: str) -> tuple[WorkflowRegistry, RecordingHandler]:
    handler = RecordingHandler()
    registry = WorkflowRegistry()

    @workflow(name=name, version="1")
    async def original(ctx: WorkflowContext) -> dict[str, Any]:
        frame = await ctx.run_skill("frame", input={})
        design = await ctx.run_skill("design", input={})
        return {"frame": frame, "design": design}

    registry.register(original)
    return registry, handler


async def test_mutations_against_completed_log_raise_and_record_nothing(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    registry, handler = _two_step_registry("mutant")
    eng = make_engine(engine, registry, handler)
    run_id = await eng.start_run("mutant", "1", {}, actor="test")
    first = await eng.execute_run(run_id)
    assert first.status == "completed"
    snapshot = await EventLog(session).list(run_id)

    # Removed step: detected at body-return via the leftover-memo check.
    removed_registry = WorkflowRegistry()

    @workflow(name="mutant", version="1")
    async def removed(ctx: WorkflowContext) -> Any:
        return await ctx.run_skill("frame", input={})

    removed_registry.register(removed)
    with pytest.raises(NonDeterminismError, match="'design'"):
        await make_engine(engine, removed_registry, handler).execute_run(run_id)

    # Renamed step: a key the sealed log cannot contain.
    renamed_registry = WorkflowRegistry()

    @workflow(name="mutant", version="1")
    async def renamed(ctx: WorkflowContext) -> Any:
        await ctx.run_skill("frame", input={})
        return await ctx.run_skill("design_v2", input={})

    renamed_registry.register(renamed)
    with pytest.raises(NonDeterminismError, match="'design_v2'") as renamed_exc:
        await make_engine(engine, renamed_registry, handler).execute_run(run_id)
    assert "NEW workflow version" in str(renamed_exc.value)  # version-bump guidance

    # Extra step inserted: likewise impossible against a sealed log.
    extra_registry = WorkflowRegistry()

    @workflow(name="mutant", version="1")
    async def extra(ctx: WorkflowContext) -> Any:
        await ctx.run_skill("frame", input={})
        await ctx.run_skill("design", input={})
        return await ctx.run_skill("ship", input={})

    extra_registry.register(extra)
    with pytest.raises(NonDeterminismError, match="'ship'"):
        await make_engine(engine, extra_registry, handler).execute_run(run_id)

    # Sealed-log contract (engine.py): a mismatch against a COMPLETED log is
    # re-raised to the caller and records NOTHING — the run's history is
    # never poisoned. No side effect re-executed either.
    assert await EventLog(session).list(run_id) == snapshot
    assert project_run(run_id, snapshot).status == "completed"
    assert handler.calls == ["frame", "design"]


async def test_reordered_distinct_labels_replay_from_memo_by_key(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    """Documented contract: the replay cursor is keyed by step_key, NOT by
    position (context.py — this is what makes ctx.parallel legal), so a pure
    reorder of distinct labels is tolerated: every key is memoized, replay
    completes with zero new events and each call site gets ITS OWN recorded
    result. Reorders that change the key sequence (ordinals, add/remove/
    rename) are the detected mutations covered above."""
    registry, handler = _two_step_registry("shuffle")
    eng = make_engine(engine, registry, handler)
    run_id = await eng.start_run("shuffle", "1", {}, actor="test")
    first = await eng.execute_run(run_id)
    assert first.status == "completed"
    snapshot = await EventLog(session).list(run_id)

    reordered_registry = WorkflowRegistry()

    @workflow(name="shuffle", version="1")
    async def reordered(ctx: WorkflowContext) -> dict[str, Any]:
        design = await ctx.run_skill("design", input={})
        frame = await ctx.run_skill("frame", input={})
        return {"frame": frame, "design": design}

    reordered_registry.register(reordered)
    replay = await make_engine(engine, reordered_registry, handler).execute_run(run_id)
    assert replay.status == "completed"
    assert replay.output == first.output  # per-key memo: same results, either order
    assert await EventLog(session).list(run_id) == snapshot  # zero new events
    assert handler.calls == ["frame", "design"]  # nothing re-executed


async def test_midflight_body_change_fails_loudly_with_version_guidance(
    engine: AsyncEngine, session: AsyncSession, store: ArtifactStore
) -> None:
    """A same-version body change while a run is parked at a gate must fail
    loudly on resume: the engine records ``run_failed`` (the run is NOT
    sealed) and re-raises ``NonDeterminismError`` with bump-your-version
    guidance."""
    ref = await store.create(GenericArtifact(title="Brief"), key="briefs/midflight")
    handler = RecordingHandler()
    registry = WorkflowRegistry()

    @workflow(name="midflight", version="1")
    async def midflight(ctx: WorkflowContext) -> str:
        await ctx.run_skill("frame", input={})
        await ctx.gate("signoff", artifact=ref, approvers=["role:eng"])
        return "shipped"

    registry.register(midflight)
    eng = make_engine(engine, registry, handler)
    run_id = await eng.start_run("midflight", "1", {}, actor="test")
    assert (await eng.execute_run(run_id)).status == "waiting_gate"

    # Body mutated under the SAME version while the run is parked: the
    # "frame" step is dropped. Resume after approval.
    mutated_registry = WorkflowRegistry()

    @workflow(name="midflight", version="1")
    async def mutated(ctx: WorkflowContext) -> str:
        await ctx.gate("signoff", artifact=ref, approvers=["role:eng"])
        return "shipped"

    mutated_registry.register(mutated)
    await resolve_gate(session, run_id, "signoff", "approved", "user:vinit")

    with pytest.raises(NonDeterminismError, match="'frame'") as excinfo:
        await make_engine(engine, mutated_registry, handler).execute_run(run_id)
    assert "NEW workflow version" in str(excinfo.value)

    # Unsealed contract: the failure IS recorded — run_failed with the same
    # guidance, projection failed, and the run is no longer claimable.
    events = await EventLog(session).list(run_id)
    projection = project_run(run_id, events)
    assert projection.status == "failed"
    assert projection.error is not None
    assert projection.error["type"] == "NonDeterminismError"
    assert "NEW workflow version" in projection.error["message"]
    queue = (
        await session.execute(
            select(schema.task_queue).where(schema.task_queue.c.run_id == run_id)
        )
    ).all()
    assert queue == []
    assert handler.calls == ["frame"]  # the mutated replay re-executed nothing
