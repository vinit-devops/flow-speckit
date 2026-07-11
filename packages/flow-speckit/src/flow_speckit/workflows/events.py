"""Typed workflow event log and the run projection folded from it.

``workflow_events`` is **the** source of truth (doc 03 §2): this module owns
the nine-type closed payload set, the append path that allocates gapless
per-run ``seq`` numbers, and the pure fold that rebuilds a ``workflow_runs``
row from an event list.

Storage layout: the ``event_type`` column is canonical; payloads are stored
WITHOUT the discriminator and it is re-injected from the column on read, so
the row never carries the type twice.

Projection folding rules (doc 03 §3 state diagram)
--------------------------------------------------
The projection's ``status`` is folded event-by-event:

- ``run_started``    -> ``pending`` (run_started coincides with enqueue)
- ``step_started``   -> ``running``; except ``step_kind == "sleep"`` ->
  ``waiting_timer`` (a durable sleep releases the worker immediately after
  its start event, and its ``step_completed`` is only appended once the
  timer has fired and the run replays)
- ``step_completed`` -> ``running`` (the worker is live and proceeding)
- ``step_failed``    -> ``waiting_timer`` when ``will_retry`` (retry backoff
  parks the run on a timer row), else ``running`` (a terminal
  ``run_failed`` is expected to follow)
- ``gate_opened``    -> ``waiting_gate``
- ``gate_resolved``  -> ``pending`` (re-enqueued; a worker must re-claim it)
- ``run_completed``  -> ``completed``
- ``run_failed``     -> ``failed``
- ``run_cancelled``  -> ``cancelled``

Transitions the diagram derives from queue state rather than events
(``pending -> running`` on worker claim, ``waiting_timer -> pending`` on
timer fire, crash re-enqueue) have no event of their own; the fold
approximates them via the next step event. ``created_at``/``updated_at``
derive from the first/last ``workflow_events.created_at`` rows and are not
part of the pure fold, which sees only payloads. Later waves may refine
these rules alongside the worker loop.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from flow_speckit.storage import schema
from flow_speckit.storage.locks import WORKFLOWS_LOCK_CLASS_ID

RunStatus = Literal[
    "pending",
    "running",
    "waiting_gate",
    "waiting_timer",
    "completed",
    "failed",
    "cancelled",
]
StepKind = Literal["skill", "execute", "gate", "sleep", "child", "intrinsic"]
GateDecision = Literal["approved", "rejected"]
OnTimeout = Literal["fail", "approve", "escalate"]


class _EventBase(BaseModel):
    """Common config for event payloads: immutable, closed field set."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class StepCost(_EventBase):
    """Cost ledger entry attached to a completed step."""

    tokens_in: int = 0
    tokens_out: int = 0
    usd: float = 0.0


class RunStarted(_EventBase):
    event_type: Literal["run_started"] = "run_started"
    workflow_name: str
    workflow_version: str
    input: dict[str, Any]
    actor: str


class StepStarted(_EventBase):
    event_type: Literal["step_started"] = "step_started"
    step_key: str
    step_kind: StepKind


class StepCompleted(_EventBase):
    event_type: Literal["step_completed"] = "step_completed"
    step_key: str
    result: Any = None
    cost: StepCost | None = None
    duration_ms: int


class StepFailed(_EventBase):
    event_type: Literal["step_failed"] = "step_failed"
    step_key: str
    error: dict[str, Any]
    attempt: int
    will_retry: bool


class GateOpened(_EventBase):
    event_type: Literal["gate_opened"] = "gate_opened"
    step_key: str
    gate_key: str
    artifact_id: UUID
    approvers: list[str]
    timeout_at: datetime | None = None  # None = no timeout
    # Timeout policy fields (wave-4 extension for doc 03 §6 timeout handling;
    # defaults keep pre-extension payloads parseable). ``timeout_s`` records
    # the original duration so the "escalate" policy can re-arm its timer;
    # ``escalate_to`` is the optional second approver list to re-notify.
    timeout_s: float | None = None
    on_timeout: OnTimeout = "fail"
    escalate_to: list[str] | None = None


class GateResolved(_EventBase):
    event_type: Literal["gate_resolved"] = "gate_resolved"
    step_key: str
    gate_key: str
    decision: GateDecision
    actor: str
    comment: str | None = None
    # Wave-4 extension: resolution instant carried into ``GateDecision``
    # (doc 03 §6); optional so pre-extension payloads still parse.
    resolved_at: datetime | None = None


class RunCompleted(_EventBase):
    event_type: Literal["run_completed"] = "run_completed"
    output_ref: UUID | None = None


class RunFailed(_EventBase):
    event_type: Literal["run_failed"] = "run_failed"
    error: dict[str, Any]
    failed_step: str


class RunCancelled(_EventBase):
    event_type: Literal["run_cancelled"] = "run_cancelled"
    actor: str
    reason: str


WorkflowEvent = Annotated[
    RunStarted
    | StepStarted
    | StepCompleted
    | StepFailed
    | GateOpened
    | GateResolved
    | RunCompleted
    | RunFailed
    | RunCancelled,
    Field(discriminator="event_type"),
]

_EVENT_ADAPTER: TypeAdapter[WorkflowEvent] = TypeAdapter(WorkflowEvent)


def parse_event(event_type: str, payload: dict[str, Any]) -> WorkflowEvent:
    """Validate a stored (event_type, payload) row into a typed event.

    The set is closed: an ``event_type`` outside the nine known types (or a
    payload that does not match its model) raises ``pydantic.ValidationError``.
    """
    return _EVENT_ADAPTER.validate_python({**payload, "event_type": event_type})


class EventLog:
    """Append-only façade over the ``workflow_events`` table.

    EventLog owns the session transaction lifecycle exactly like
    ``ArtifactStore``: writes commit, reads roll back; do not share the
    session with an outer transaction.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append(self, run_id: UUID, event: WorkflowEvent) -> int:
        """Append ``event`` for ``run_id``, returning its allocated ``seq``.

        Seq allocation (``COALESCE(max(seq), 0) + 1`` per run) is serialized
        by a transaction-scoped advisory lock on ``hashtext(run_id)`` in the
        workflow namespace, so concurrent appenders produce gapless,
        monotonic sequences instead of racing into the (run_id, seq) PK.
        The lock auto-releases on the commit/rollback below.
        """
        try:
            await self._session.execute(
                text("SELECT pg_advisory_xact_lock(:class_id, hashtext(:run_id))"),
                {"class_id": WORKFLOWS_LOCK_CLASS_ID, "run_id": str(run_id)},
            )
            result = await self._session.execute(
                select(
                    func.coalesce(func.max(schema.workflow_events.c.seq), 0)
                ).where(schema.workflow_events.c.run_id == run_id)
            )
            seq: int = result.scalar_one() + 1
            await self._session.execute(
                schema.workflow_events.insert().values(
                    run_id=run_id,
                    seq=seq,
                    event_type=event.event_type,
                    # The event_type column is canonical; keep the payload
                    # free of the discriminator (parse_event re-injects it).
                    payload=event.model_dump(mode="json", exclude={"event_type"}),
                )
            )
            await self._session.commit()
        except Exception:
            await self._session.rollback()
            raise
        return seq

    async def list(self, run_id: UUID) -> list[WorkflowEvent]:
        """Return the typed event history for ``run_id`` ordered by seq."""
        result = await self._session.execute(
            select(
                schema.workflow_events.c.event_type,
                schema.workflow_events.c.payload,
            )
            .where(schema.workflow_events.c.run_id == run_id)
            .order_by(schema.workflow_events.c.seq.asc())
        )
        rows = result.all()
        # Read-only: release the transaction opened by the SELECT above.
        await self._session.rollback()
        return [parse_event(row.event_type, row.payload) for row in rows]


class RunProjection(BaseModel):
    """Rebuildable ``workflow_runs``-shaped view folded from the event log."""

    model_config = ConfigDict(frozen=True)

    run_id: UUID
    workflow_name: str | None = None
    workflow_version: str | None = None
    status: RunStatus = "pending"
    current_step: str | None = None
    input: dict[str, Any] | None = None
    output_ref: UUID | None = None
    error: dict[str, Any] | None = None


def project_run(run_id: UUID, events: Sequence[WorkflowEvent]) -> RunProjection:
    """Fold ``events`` (in seq order) into a ``RunProjection``.

    Pure and side-effect free — this is the contract that lets
    ``workflow_runs`` be dropped and rebuilt from ``workflow_events`` at any
    time. Status folding rules are documented in the module docstring.
    """
    workflow_name: str | None = None
    workflow_version: str | None = None
    status: RunStatus = "pending"
    current_step: str | None = None
    input_payload: dict[str, Any] | None = None
    output_ref: UUID | None = None
    error: dict[str, Any] | None = None
    for event in events:
        if isinstance(event, RunStarted):
            workflow_name = event.workflow_name
            workflow_version = event.workflow_version
            input_payload = event.input
            status = "pending"
        elif isinstance(event, StepStarted):
            current_step = event.step_key
            status = "waiting_timer" if event.step_kind == "sleep" else "running"
        elif isinstance(event, StepCompleted):
            status = "running"
        elif isinstance(event, StepFailed):
            status = "waiting_timer" if event.will_retry else "running"
        elif isinstance(event, GateOpened):
            status = "waiting_gate"
        elif isinstance(event, GateResolved):
            status = "pending"
        elif isinstance(event, RunCompleted):
            output_ref = event.output_ref
            status = "completed"
        elif isinstance(event, RunFailed):
            error = event.error
            current_step = event.failed_step
            status = "failed"
        else:  # RunCancelled — the union is closed, so this is exhaustive.
            status = "cancelled"
    return RunProjection(
        run_id=run_id,
        workflow_name=workflow_name,
        workflow_version=workflow_version,
        status=status,
        current_step=current_step,
        input=input_payload,
        output_ref=output_ref,
        error=error,
    )
