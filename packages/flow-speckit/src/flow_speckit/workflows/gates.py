"""Human approval gates — flagship semantics (doc 03 §6).

Lifecycle: ``ctx.gate`` (context.py) appends ``gate_opened`` and suspends the
run with status ``waiting_gate``; the run then occupies no process, no memory,
no connection. :func:`resolve_gate` — the service behind CLI/REST/PR-review
resolution channels — appends ``gate_resolved``, mirrors the decision onto the
referenced artifact's status, re-enqueues the run and calls ``notify``; the
woken replay finds the resolution and returns a :class:`GateDecision`.

Rejection is a **first-class branchable outcome**, not an error:
``GateDecision.rejected`` drives feedback loops (doc 03 §5), and a workflow
that re-gates the same label after a rejection gets a NEW step key
(``label#2``) with the same ``gate_key`` — resolution always targets the
currently OPEN instance of a gate key.

Timeout policies (applied by ``timers.fire_due_timers`` via
:func:`handle_gate_timeout`, from the ``on_timeout`` recorded in the
``gate_opened`` payload):

- ``fail`` (default) — append ``run_failed`` explaining the gate timeout.
- ``approve`` — dangerous, explicit: resolve as approved with actor
  ``"timeout"``; loudly logged.
- ``escalate`` — re-arm the timer with the same duration, re-notify (the
  ``escalate_to`` approver list if one was recorded, else the original list;
  the swap is informational — see below) and log a warning. No new event is
  appended (re-opening an open gate is a no-op, doc 03 §4).

Actor identity (v0.1): actors are plain strings (``user:<name>`` /
``role:<name>``). The ``approvers`` list is RECORDED in ``gate_opened`` but
NOT enforced at resolution — any identity may resolve (doc 03 §6; quorum and
enforcement are a v0.6 policy hook).

Artifact status mirroring: approval sets the referenced artifact to
``approved``, rejection to ``rejected``, via ``ArtifactStore.set_status``
(keeping artifact state and audit log consistent). An artifact ALREADY in the
target status is tolerated as an idempotent no-op; any other disallowed
transition (e.g. ``draft -> approved``) raises ``InvalidStatusTransition``
before ``gate_resolved`` is appended, so the log never records a resolution
whose artifact mirror failed.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import UUID

import structlog
from pydantic import BaseModel, ConfigDict
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from flow_speckit.artifacts.registry import ArtifactRegistry
from flow_speckit.artifacts.store import ArtifactStore, InvalidStatusTransition
from flow_speckit.storage import schema
from flow_speckit.workflows.context import apply_event_to_run_row
from flow_speckit.workflows.events import (
    EventLog,
    GateOpened,
    GateResolved,
    RunFailed,
    WorkflowEvent,
)
from flow_speckit.workflows.timers import NotifyFn, upsert_task_queue

__all__ = [
    "GateDecision",
    "GateNotOpenError",
    "decision_from_event",
    "resolve_gate",
]

logger = structlog.get_logger(__name__)

DecisionLiteral = Literal["approved", "rejected"]


class GateDecision(BaseModel):
    """The outcome a workflow body receives from ``ctx.gate`` (doc 03 §6)."""

    model_config = ConfigDict(frozen=True)

    decision: DecisionLiteral
    actor: str
    comment: str | None = None
    resolved_at: datetime | None = None

    @property
    def approved(self) -> bool:
        return self.decision == "approved"

    @property
    def rejected(self) -> bool:
        return self.decision == "rejected"


class GateNotOpenError(LookupError):
    """The run has no OPEN gate with the requested gate key.

    Raised on resolving an unknown gate key, a gate that was already resolved
    (duplicate resolve), or a run that is not waiting on the gate.
    """

    def __init__(self, run_id: UUID, gate_key: str) -> None:
        super().__init__(
            f"Run {run_id} has no open gate {gate_key!r}: either the gate key is "
            "unknown, or the gate was already resolved"
        )
        self.run_id = run_id
        self.gate_key = gate_key


def decision_from_event(event: GateResolved) -> GateDecision:
    """Project a recorded ``gate_resolved`` event into a :class:`GateDecision`."""
    return GateDecision(
        decision=event.decision,
        actor=event.actor,
        comment=event.comment,
        resolved_at=event.resolved_at,
    )


def _open_gate(events: list[WorkflowEvent], gate_key: str) -> GateOpened | None:
    """The latest ``gate_opened`` for ``gate_key`` without a matching resolution."""
    open_by_step: dict[str, GateOpened] = {}
    for event in events:
        if isinstance(event, GateOpened) and event.gate_key == gate_key:
            open_by_step[event.step_key] = event
        elif isinstance(event, GateResolved):
            open_by_step.pop(event.step_key, None)
    if not open_by_step:
        return None
    return list(open_by_step.values())[-1]


async def _mirror_artifact_status(
    session: AsyncSession, artifact_id: UUID, decision: DecisionLiteral, actor: str
) -> None:
    # set_status never consults the registry, so an empty one suffices here.
    store = ArtifactStore(session, ArtifactRegistry())
    target: DecisionLiteral = decision
    try:
        await store.set_status(artifact_id, target, actor=actor)
    except InvalidStatusTransition as exc:
        if exc.from_status == target:
            # Already in the desired status (e.g. resolved via another
            # channel, or a gate opened on an approved artifact): idempotent.
            logger.info(
                "gate_artifact_status_already_set",
                artifact_id=str(artifact_id),
                status=target,
                actor=actor,
            )
            return
        raise


async def _resolve_on_session(
    session: AsyncSession,
    run_id: UUID,
    gate_key: str,
    decision: DecisionLiteral,
    actor: str,
    comment: str | None,
    notify: NotifyFn | None,
) -> GateDecision:
    log = EventLog(session)
    events = await log.list(run_id)
    opened = _open_gate(events, gate_key)
    if opened is None:
        raise GateNotOpenError(run_id, gate_key)
    # Mirror the artifact status FIRST: a disallowed transition must surface
    # before gate_resolved is appended (module docstring).
    await _mirror_artifact_status(session, opened.artifact_id, decision, actor)
    resolved = GateResolved(
        step_key=opened.step_key,
        gate_key=gate_key,
        decision=decision,
        actor=actor,
        comment=comment,
        resolved_at=datetime.now(UTC),
    )
    await log.append(run_id, resolved)
    await apply_event_to_run_row(session, run_id, resolved)  # -> pending
    await upsert_task_queue(session, run_id)
    if notify is not None:
        await notify(run_id)
    logger.info(
        "gate_resolved",
        run_id=str(run_id),
        gate_key=gate_key,
        step_key=opened.step_key,
        decision=decision,
        actor=actor,
    )
    return decision_from_event(resolved)


async def resolve_gate(
    session_or_factory: AsyncSession | async_sessionmaker[AsyncSession],
    run_id: UUID,
    gate_key: str,
    decision: DecisionLiteral,
    actor: str,
    comment: str | None = None,
    *,
    notify: NotifyFn | None = None,
) -> GateDecision:
    """Resolve the OPEN gate ``gate_key`` on ``run_id`` (doc 03 §6).

    Validates that the run is waiting on that gate (``gate_opened`` without a
    ``gate_resolved``) and raises :class:`GateNotOpenError` otherwise; appends
    ``gate_resolved``; mirrors the decision onto the referenced artifact's
    status; sets the run projection to ``pending``; re-enqueues the run; and
    awaits ``notify(run_id)``. Any recorded identity may resolve — approvers
    are not enforced in v0.1 (module docstring).
    """
    if isinstance(session_or_factory, AsyncSession):
        return await _resolve_on_session(
            session_or_factory, run_id, gate_key, decision, actor, comment, notify
        )
    async with session_or_factory() as session:
        return await _resolve_on_session(
            session, run_id, gate_key, decision, actor, comment, notify
        )


async def handle_gate_timeout(
    session: AsyncSession, *, run_id: UUID, step_key: str, notify: NotifyFn | None
) -> None:
    """Apply a fired ``gate_timeout`` timer's ``on_timeout`` policy.

    Called by ``timers.fire_due_timers`` (which deletes the fired timer row
    afterwards). A stale timer — gate never opened, or already resolved — is
    a no-op.
    """
    log = EventLog(session)
    events = await log.list(run_id)
    opened = next(
        (e for e in events if isinstance(e, GateOpened) and e.step_key == step_key),
        None,
    )
    resolved = any(isinstance(e, GateResolved) and e.step_key == step_key for e in events)
    if opened is None or resolved:
        return  # stale timer; caller deletes the row

    if opened.on_timeout == "approve":
        logger.warning(
            "gate_timeout_auto_approved",
            run_id=str(run_id),
            gate_key=opened.gate_key,
            step_key=step_key,
            artifact_id=str(opened.artifact_id),
        )
        await _resolve_on_session(
            session,
            run_id,
            opened.gate_key,
            "approved",
            "timeout",
            f"gate {opened.gate_key!r} timed out with on_timeout='approve'",
            notify,
        )
        return

    if opened.on_timeout == "escalate":
        approvers = opened.escalate_to if opened.escalate_to else opened.approvers
        # Re-arm with the original duration; the approver swap is recorded in
        # the warning (and surfaced by notifier plugins in later waves) — no
        # new event is appended (idempotent re-open).
        fire_at = datetime.now(UTC) + timedelta(seconds=opened.timeout_s or 0.0)
        try:
            await session.execute(
                schema.timers.insert().values(
                    run_id=run_id, step_key=step_key, fire_at=fire_at, kind="gate_timeout"
                )
            )
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        logger.warning(
            "gate_timeout_escalated",
            run_id=str(run_id),
            gate_key=opened.gate_key,
            step_key=step_key,
            approvers=list(approvers),
        )
        if notify is not None:
            await notify(run_id)
        return

    # "fail" (default): the run fails terminally.
    error: dict[str, Any] = {
        "type": "GateTimeout",
        "message": (
            f"gate {opened.gate_key!r} (step {step_key!r}) timed out at "
            f"{opened.timeout_at} with on_timeout='fail'"
        ),
    }
    event = RunFailed(error=error, failed_step=step_key)
    await log.append(run_id, event)
    await apply_event_to_run_row(session, run_id, event)
    # Terminal: make sure no queue row lingers (the suspend already deleted
    # it; this is defensive against crash-retried firing).
    try:
        await session.execute(
            delete(schema.task_queue).where(schema.task_queue.c.run_id == run_id)
        )
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    # A child run failing via gate timeout must still wake its waiting parent
    # (doc 03 §9). Lazy import: engine imports context, which lazily imports
    # this module — the reverse edge must not be module-level.
    from flow_speckit.workflows.engine import settle_parent_on_child_terminal

    await settle_parent_on_child_terminal(session, run_id, notify=notify)
    logger.warning(
        "gate_timeout_failed_run",
        run_id=str(run_id),
        gate_key=opened.gate_key,
        step_key=step_key,
    )
