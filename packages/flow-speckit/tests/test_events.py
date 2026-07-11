import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from flow_speckit.storage.db import session_factory
from flow_speckit.workflows.events import (
    EventLog,
    GateOpened,
    GateResolved,
    RunCompleted,
    RunStarted,
    StepCompleted,
    StepCost,
    StepStarted,
    WorkflowEvent,
    parse_event,
    project_run,
)


@pytest.fixture()
def log(session: AsyncSession) -> EventLog:
    return EventLog(session)


def _started(name: str = "feature") -> RunStarted:
    return RunStarted(
        workflow_name=name, workflow_version="1", input={"idea": "x"}, actor="vinit"
    )


async def test_append_allocates_sequential_seq_per_run(log: EventLog) -> None:
    run_id = uuid4()
    seqs = [
        await log.append(run_id, _started()),
        await log.append(run_id, StepStarted(step_key="frame", step_kind="skill")),
        await log.append(run_id, StepCompleted(step_key="frame", duration_ms=5)),
    ]
    assert seqs == [1, 2, 3]


async def test_two_runs_interleave_independently(log: EventLog) -> None:
    run_a, run_b = uuid4(), uuid4()
    assert await log.append(run_a, _started("a")) == 1
    assert await log.append(run_b, _started("b")) == 1
    assert await log.append(run_a, StepStarted(step_key="s", step_kind="skill")) == 2
    assert await log.append(run_b, StepStarted(step_key="s", step_kind="skill")) == 2
    assert await log.append(run_b, StepCompleted(step_key="s", duration_ms=1)) == 3
    assert len(await log.list(run_a)) == 2
    assert len(await log.list(run_b)) == 3


async def test_concurrent_appends_are_gapless_and_monotonic(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    # Two independent sessions racing to append to the SAME run must
    # serialize on the advisory lock: no (run_id, seq) PK violation, and the
    # allocated seqs are exactly 1..N with no gaps.
    run_id = uuid4()
    factory = session_factory(engine)
    per_log = 5

    async def burst(log: EventLog) -> list[int]:
        # A session is not safe for concurrent use, so each appender is
        # sequential within itself; the two sessions race each other.
        return [
            await log.append(run_id, StepCompleted(step_key=f"s{i}", duration_ms=i))
            for i in range(per_log)
        ]

    async with factory() as s1, factory() as s2:
        log1, log2 = EventLog(s1), EventLog(s2)
        half_a, half_b = await asyncio.gather(burst(log1), burst(log2))
        results = half_a + half_b
    # Each appender saw strictly increasing seqs; together they are gapless.
    assert half_a == sorted(half_a) and half_b == sorted(half_b)
    assert sorted(results) == list(range(1, 2 * per_log + 1))
    events = await EventLog(session).list(run_id)
    assert len(events) == 2 * per_log


async def test_unknown_event_type_rejected_at_parse() -> None:
    with pytest.raises(ValidationError):
        parse_event("run_paused", {"actor": "vinit"})


async def test_payload_not_matching_model_rejected_at_parse() -> None:
    with pytest.raises(ValidationError):
        parse_event("run_started", {"workflow_name": "f"})  # missing fields
    with pytest.raises(ValidationError):
        parse_event("run_cancelled", {"actor": "a", "reason": "r", "extra": 1})


async def test_list_round_trips_typed_payloads(log: EventLog) -> None:
    run_id = uuid4()
    artifact_id = uuid4()
    timeout = datetime(2026, 7, 18, 12, 0, tzinfo=UTC)
    sent: list[WorkflowEvent] = [
        _started(),
        StepStarted(step_key="frame", step_kind="skill"),
        StepCompleted(
            step_key="frame",
            result={"artifact": str(artifact_id)},
            cost=StepCost(tokens_in=100, tokens_out=50, usd=0.25),
            duration_ms=1234,
        ),
        GateOpened(
            step_key="brief_approval",
            gate_key="brief_approval",
            artifact_id=artifact_id,
            approvers=["role:product"],
            timeout_at=timeout,
            # Wave-4 timeout-policy extension fields round-trip too.
            timeout_s=604800.0,
            on_timeout="escalate",
            escalate_to=["role:cto"],
        ),
        GateResolved(
            step_key="brief_approval",
            gate_key="brief_approval",
            decision="approved",
            actor="vinit",
            comment=None,
            resolved_at=datetime(2026, 7, 12, 9, 30, tzinfo=UTC),
        ),
        RunCompleted(output_ref=artifact_id),
    ]
    for event in sent:
        await log.append(run_id, event)
    assert await log.list(run_id) == sent


async def test_project_run_folds_status_through_lifecycle(log: EventLog) -> None:
    run_id = uuid4()
    output = uuid4()
    stages: list[tuple[WorkflowEvent, str]] = [
        (_started(), "pending"),
        (StepStarted(step_key="frame", step_kind="skill"), "running"),
        (StepCompleted(step_key="frame", duration_ms=10), "running"),
        (StepStarted(step_key="brief_approval", step_kind="gate"), "running"),
        (
            GateOpened(
                step_key="brief_approval",
                gate_key="brief_approval",
                artifact_id=uuid4(),
                approvers=["role:product"],
            ),
            "waiting_gate",
        ),
        (
            GateResolved(
                step_key="brief_approval",
                gate_key="brief_approval",
                decision="approved",
                actor="vinit",
            ),
            "pending",
        ),
        (StepStarted(step_key="design", step_kind="skill"), "running"),
        (StepCompleted(step_key="design", duration_ms=20), "running"),
        (RunCompleted(output_ref=output), "completed"),
    ]
    events: list[WorkflowEvent] = []
    for event, expected_status in stages:
        await log.append(run_id, event)
        events.append(event)
        assert project_run(run_id, events).status == expected_status
    # Projection also folds against the persisted, re-parsed log.
    projection = project_run(run_id, await log.list(run_id))
    assert projection.status == "completed"
    assert projection.workflow_name == "feature"
    assert projection.workflow_version == "1"
    assert projection.input == {"idea": "x"}
    assert projection.current_step == "design"
    assert projection.output_ref == output
    assert projection.error is None


async def test_project_run_sleep_and_failure_paths() -> None:
    run_id = uuid4()
    sleeping = [
        _started(),
        StepStarted(step_key="cooldown", step_kind="sleep"),
    ]
    assert project_run(run_id, sleeping).status == "waiting_timer"

    failed = [
        _started(),
        StepStarted(step_key="frame", step_kind="skill"),
        parse_event(
            "step_failed",
            {"step_key": "frame", "error": {"msg": "boom"}, "attempt": 1, "will_retry": True},
        ),
    ]
    assert project_run(run_id, failed).status == "waiting_timer"
    terminal = [
        *failed,
        parse_event(
            "step_failed",
            {"step_key": "frame", "error": {"msg": "boom"}, "attempt": 2, "will_retry": False},
        ),
        parse_event("run_failed", {"error": {"msg": "boom"}, "failed_step": "frame"}),
    ]
    projection = project_run(run_id, terminal)
    assert projection.status == "failed"
    assert projection.error == {"msg": "boom"}
    assert projection.current_step == "frame"

    cancelled = [_started(), parse_event("run_cancelled", {"actor": "vinit", "reason": "nope"})]
    assert project_run(run_id, cancelled).status == "cancelled"


async def test_project_run_empty_log_is_pending() -> None:
    run_id = uuid4()
    projection = project_run(run_id, [])
    assert projection.status == "pending"
    assert projection.run_id == run_id
    assert isinstance(projection.run_id, UUID)
