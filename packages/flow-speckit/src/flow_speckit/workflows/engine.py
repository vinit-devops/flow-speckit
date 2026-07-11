"""``WorkflowEngine`` — start and execute runs by replay (doc 03 §§1, 3, 4).

``start_run`` records ``run_started`` and enqueues the run; ``execute_run``
is the single replay-driven code path shared by first execution, resume and
crash recovery: it folds the recorded event log into a replay cursor, invokes
the pinned workflow body with a fresh :class:`WorkflowContext`, and lets step
memoization do the rest.

Projection maintenance: ``workflow_runs`` is kept in sync via targeted
UPDATEs (:func:`apply_event_to_run_row`) applied after every event append —
at rest the row always matches what ``project_run`` would fold, and it stays
fully rebuildable from ``workflow_events``.

Outcome contract:

- Normal return / replayed completion → ``RunOutcome(status="completed")``.
- Ordinary step/body exceptions (including exhausted retries) → ``run_failed``
  appended, queue row removed, ``RunOutcome(status="failed", error=...)``.
- :class:`NonDeterminismError` and :class:`StepKindUnavailableError` are
  operator/configuration errors and **re-raise** to the caller after being
  recorded (a mismatch against an already-completed log records nothing — the
  completed run's history is never poisoned).
- A pinned ``(name, version)`` missing from the registry appends ``run_failed``
  with bump-your-version guidance (doc 03 §8) and returns a failed outcome.
- ``_SuspendRun`` (gates ``waiting_gate``, durable sleep / retry backoff
  ``waiting_timer``): the run row status is set from the suspend signal, the
  ``task_queue`` row is DELETED (a suspended run must not be claimable) and a
  non-terminal outcome is returned. Wake paths — ``timers.fire_due_timers``
  and ``gates.resolve_gate`` — re-insert the queue row and call ``notify``,
  matching doc 03 §7's wake-via-re-enqueue + ``pg_notify``.

Retry backoff default: with ``backoff=None`` (the default), a retryable step
failure parks the run on a ``timers`` row (kind ``retry``) instead of
re-executing in-process; pass ``backoff=immediate_backoff`` (or any
``BackoffFn``) to retry within a single pass — the test seam.

Dev ergonomics: ``auto_approve=True`` resolves every gate instantly with
actor ``auto`` (doc 03 §6) — no suspension, loudly logged.

Crash injection: the optional ``fault_hook`` (see :data:`FaultHook`) is
awaited at the named checkpoint boundaries ``after_side_effect``,
``before_checkpoint_commit`` and ``after_checkpoint`` — the §10 harness seam.

Wave 5 (dispatch/cancel/children):

- ``notify`` (a :data:`~flow_speckit.workflows.timers.NotifyFn`) is awaited
  after every enqueue the engine performs; ``queue.make_notifier`` wires it
  to ``pg_notify('flow_speckit_wake', run_id)``.
- :func:`cancel_run` appends ``run_cancelled`` and cascades to children;
  ``execute_run`` treats a logged ``run_cancelled`` (event presence, not the
  folded status) as terminal, and a live body observes it at its next step
  boundary via :class:`~flow_speckit.workflows.errors.CancelledRun`.
- ``ctx.child_workflow`` parents suspend with payload ``kind="child"`` and
  projection status ``running``; child terminal transitions settle the
  parent via :func:`settle_parent_on_child_terminal`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict
from sqlalchemy import Row, delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from flow_speckit.storage import schema
from flow_speckit.workflows.context import (
    BackoffFn,
    FaultHook,
    StepHandler,
    StepInvocation,
    StepResult,
    WorkflowContext,
    apply_event_to_run_row,
    child_run_id,
)
from flow_speckit.workflows.errors import (
    CancelledRun,
    InvalidCancellation,
    NonDeterminismError,
    StepKindUnavailableError,
    UnknownRun,
    _SuspendRun,
    error_payload,
)
from flow_speckit.workflows.events import (
    EventLog,
    GateResolved,
    RunCancelled,
    RunCompleted,
    RunFailed,
    RunStarted,
    RunStatus,
    StepCompleted,
    StepFailed,
    StepStarted,
    project_run,
)
from flow_speckit.workflows.registry import UnknownWorkflow, WorkflowRegistry
from flow_speckit.workflows.timers import NotifyFn, upsert_task_queue

__all__ = [
    "RunOutcome",
    "StepHandler",
    "StepInvocation",
    "StepResult",
    "WorkflowEngine",
    "cancel_run",
    "settle_parent_on_child_terminal",
]

TERMINAL_STATUSES = ("completed", "failed", "cancelled")


class RunOutcome(BaseModel):
    """The result of one ``execute_run`` pass."""

    model_config = ConfigDict(frozen=True)

    run_id: UUID
    status: RunStatus
    output: Any = None
    error: dict[str, Any] | None = None


class WorkflowEngine:
    """Replay-driven runner over the event log; see the module docstring."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        registry: WorkflowRegistry,
        *,
        handlers: Mapping[str, StepHandler] | None = None,
        fault_hook: FaultHook | None = None,
        backoff: BackoffFn | None = None,
        config: Mapping[str, Any] | None = None,
        auto_approve: bool = False,
        notify: NotifyFn | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._registry = registry
        self._handlers: dict[str, StepHandler] = dict(handlers or {})
        self._fault_hook = fault_hook
        self._backoff = backoff  # None = durable timer-based retry backoff
        self._config = config
        self._auto_approve = auto_approve
        # Wake seam (doc 03 §7): awaited after every enqueue this engine
        # performs (start_run, child settles, suspend-race re-enqueues).
        # Wire to queue.make_notifier(...) for pg_notify; default no-op.
        self._notify = notify

    async def start_run(
        self,
        workflow_name: str,
        version: str,
        input: dict[str, Any],
        actor: str,
        *,
        parent_run_id: UUID | None = None,
        run_id: UUID | None = None,
    ) -> UUID:
        """Record ``run_started``, insert the run row (pending) and enqueue it.

        ``parent_run_id``/``run_id`` are the child-workflow seam (doc 03 §9):
        ``ctx.child_workflow`` starts children under a deterministic run id
        with their parent recorded on the run row.
        """
        # Fail fast on a definition that could never execute.
        self._registry.get(workflow_name, version)
        run_id = run_id if run_id is not None else uuid4()
        async with self._session_factory() as session:
            await EventLog(session).append(
                run_id,
                RunStarted(
                    workflow_name=workflow_name,
                    workflow_version=version,
                    input=input,
                    actor=actor,
                ),
            )
            try:
                await session.execute(
                    schema.workflow_runs.insert().values(
                        run_id=run_id,
                        workflow_name=workflow_name,
                        workflow_version=version,
                        status="pending",
                        input=input,
                        parent_run_id=parent_run_id,
                    )
                )
                await session.execute(
                    schema.task_queue.insert().values(run_id=run_id, available_at=func.now())
                )
                await session.commit()
            except Exception:
                await session.rollback()
                raise
        if self._notify is not None:
            await self._notify(run_id)
        return run_id

    async def _start_child(
        self,
        *,
        name: str,
        version: str | None,
        input: dict[str, Any],
        parent_run_id: UUID,
        run_id: UUID,
    ) -> None:
        """``ChildStarter`` implementation handed to every WorkflowContext."""
        pinned = version if version is not None else self._registry.latest(name).version
        await self.start_run(
            name,
            pinned,
            input,
            actor=f"run:{parent_run_id}",
            parent_run_id=parent_run_id,
            run_id=run_id,
        )

    async def execute_run(self, run_id: UUID) -> RunOutcome:
        """Replay ``run_id`` from its event log; execute live from the first miss."""
        async with self._session_factory() as session:
            row = await self._load_run(session, run_id)
            log = EventLog(session)
            events = await log.list(run_id)
            prior = project_run(run_id, events)
            # Cancellation is judged by EVENT PRESENCE, not the folded status:
            # a step checkpoint racing past cancel_run folds the status back
            # to "running", but run_cancelled in the log is forever.
            if any(isinstance(e, RunCancelled) for e in events):
                return RunOutcome(run_id=run_id, status="cancelled", error=prior.error)

            workflow_name: str = row.workflow_name
            workflow_version: str = row.workflow_version
            try:
                definition = self._registry.get(workflow_name, workflow_version)
            except UnknownWorkflow:
                error = {
                    "type": "UnknownWorkflow",
                    "message": (
                        f"No registered workflow {workflow_name!r} version "
                        f"{workflow_version!r}. Runs pin their version at run_started "
                        "(doc 03 §8): register a changed definition under a NEW version "
                        "and keep the pinned version registered until in-flight runs "
                        "drain."
                    ),
                }
                failed_step = prior.current_step or "<pinned-version>"
                await self._fail_run(session, log, run_id, error, failed_step=failed_step)
                return RunOutcome(run_id=run_id, status="failed", error=error)

            sealed = prior.status == "completed"
            ctx = WorkflowContext(
                run_id=run_id,
                session=session,
                event_log=log,
                handlers=self._handlers,
                memoized={
                    e.step_key: e.result for e in events if isinstance(e, StepCompleted)
                },
                sealed=sealed,
                fault_hook=self._fault_hook,
                backoff=self._backoff,
                config=self._config,
                events=events,
                auto_approve=self._auto_approve,
                child_starter=self._start_child,
            )
            run_input: dict[str, Any] = row.input
            try:
                output = await definition.fn(ctx, **run_input)
                ctx.assert_replay_consistent()
            except _SuspendRun as suspend:
                # Park the run: projection status from the signal, and the
                # queue row is deleted so no worker can claim a suspended run.
                # Wake paths (fire_due_timers / resolve_gate / child settles)
                # re-enqueue — and may have ALREADY done so concurrently, so
                # the park always ends with a reconciliation pass (see
                # _reconcile_suspend for the invariant).
                await self._set_status(session, run_id, suspend.run_status)
                await self._delete_queue_row(session, run_id)
                return await self._reconcile_suspend(session, log, run_id, suspend)
            except CancelledRun:
                # cancel_run already appended run_cancelled, deleted the queue
                # row and the timers; re-assert the row status (a checkpoint
                # committing after the cancel folds it back to "running") and
                # finish quietly — no run_failed for a cancellation.
                await self._set_status(session, run_id, "cancelled")
                await self._delete_queue_row(session, run_id)
                return RunOutcome(run_id=run_id, status="cancelled")
            except NonDeterminismError as exc:
                if not sealed:
                    await self._fail_run(
                        session, log, run_id, error_payload(exc), failed_step=exc.step_key
                    )
                raise
            except StepKindUnavailableError as exc:
                await self._fail_run(
                    session,
                    log,
                    run_id,
                    error_payload(exc),
                    failed_step=ctx.last_step_key or "<unknown>",
                )
                raise
            except Exception as exc:
                error = error_payload(exc)
                await self._fail_run(
                    session, log, run_id, error, failed_step=ctx.last_step_key or "<unknown>"
                )
                return RunOutcome(run_id=run_id, status="failed", error=error)

            if not sealed:
                event = RunCompleted(
                    output_ref=output if isinstance(output, UUID) else None
                )
                await log.append(run_id, event)
                await apply_event_to_run_row(session, run_id, event)
                await self._delete_queue_row(session, run_id)
                await settle_parent_on_child_terminal(session, run_id, notify=self._notify)
            return RunOutcome(run_id=run_id, status="completed", output=output)

    # -- helpers ---------------------------------------------------------------

    async def _reconcile_suspend(
        self, session: AsyncSession, log: EventLog, run_id: UUID, suspend: _SuspendRun
    ) -> RunOutcome:
        """Close the eaten-wake window after a suspend parked the run.

        INVARIANT (suspend vs wake): the durable wake source is committed
        INSIDE the ctx before ``_SuspendRun`` propagates (the ``timers`` row
        for sleep/retry, ``gate_opened`` for gates, the child's run/queue
        rows for children) — so a wake path (``fire_due_timers``,
        ``resolve_gate``, a child settle, ``cancel_run``, the gate-timeout
        ``fail`` policy) can legitimately run in the window between that
        commit and the engine's unconditional queue-row delete above. Its
        ``upsert_task_queue`` re-enqueue would then be eaten by the delete:
        no queue row, no (or spent) wake source, run stranded forever.

        Resolution — delete FIRST, then re-read durable state and reconcile.
        Every wake leaves a durable trace the suspend side can observe:

        - sleep fire      → the step's ``step_completed`` (append-only log)
        - child settle    → the step's ``step_completed``/``step_failed``
        - gate resolution → the step's ``gate_resolved``
        - retry fire      → appends nothing, but its ``timers`` row is gone
          (a retry suspend always commits one before raising)
        - cancel_run / gate-timeout ``fail`` → ``run_cancelled``/``run_failed``
          (these also overwrite the row status we just set — re-assert it)

        A wake that committed before this read is honored here (terminal
        status re-asserted, or ``pending`` + re-enqueue + notify); one that
        commits after it performs its own upsert AFTER our delete, so the
        queue row survives on that side. Either way nothing is lost — at
        worst the run is enqueued twice, and ``upsert_task_queue`` is
        idempotent. No interleaving strands the run.
        """
        fresh = await log.list(run_id)
        for event in reversed(fresh):  # last terminal event wins, as in the fold
            if isinstance(event, RunCancelled):
                await self._set_status(session, run_id, "cancelled")
                return RunOutcome(run_id=run_id, status="cancelled")
            if isinstance(event, RunFailed):
                await self._set_status(session, run_id, "failed")
                return RunOutcome(run_id=run_id, status="failed", error=event.error)

        step_key = suspend.step_key
        kind = suspend.payload.get("kind")
        woken: bool
        if kind == "child":
            woken = any(
                isinstance(e, (StepCompleted, StepFailed)) and e.step_key == step_key
                for e in fresh
            )
        elif kind == "sleep":
            woken = any(
                isinstance(e, StepCompleted) and e.step_key == step_key for e in fresh
            )
        elif kind == "retry":
            woken = not await self._timer_exists(session, run_id, step_key, "retry")
        else:  # gate suspends carry gate_key/artifact_id, no "kind"
            woken = any(
                isinstance(e, GateResolved) and e.step_key == step_key for e in fresh
            )
        if not woken:
            return RunOutcome(run_id=run_id, status=suspend.run_status)
        await self._set_status(session, run_id, "pending")
        await upsert_task_queue(session, run_id)
        if self._notify is not None:
            await self._notify(run_id)
        return RunOutcome(run_id=run_id, status="pending")

    @staticmethod
    async def _timer_exists(
        session: AsyncSession, run_id: UUID, step_key: str, kind: str
    ) -> bool:
        result = await session.execute(
            select(schema.timers.c.timer_id).where(
                schema.timers.c.run_id == run_id,
                schema.timers.c.step_key == step_key,
                schema.timers.c.kind == kind,
            )
        )
        exists = result.first() is not None
        await session.rollback()  # read-only: release the SELECT's transaction
        return exists

    @staticmethod
    async def _load_run(session: AsyncSession, run_id: UUID) -> Row[Any]:
        result = await session.execute(
            select(schema.workflow_runs).where(schema.workflow_runs.c.run_id == run_id)
        )
        row = result.one_or_none()
        await session.rollback()  # read-only: release the SELECT's transaction
        if row is None:
            raise UnknownRun(f"No workflow run {run_id}")
        return row

    async def _fail_run(
        self,
        session: AsyncSession,
        log: EventLog,
        run_id: UUID,
        error: dict[str, Any],
        *,
        failed_step: str,
    ) -> None:
        event = RunFailed(error=error, failed_step=failed_step)
        await log.append(run_id, event)
        await apply_event_to_run_row(session, run_id, event)
        # A failed run is terminal until an explicit resume re-enqueues it, so
        # its queue row goes too (spec only mandates this for completed runs;
        # keeping failed rows queued would make workers re-claim them forever).
        await self._delete_queue_row(session, run_id)
        await settle_parent_on_child_terminal(session, run_id, notify=self._notify)

    @staticmethod
    async def _delete_queue_row(session: AsyncSession, run_id: UUID) -> None:
        try:
            await session.execute(
                delete(schema.task_queue).where(schema.task_queue.c.run_id == run_id)
            )
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    @staticmethod
    async def _set_status(session: AsyncSession, run_id: UUID, status: RunStatus) -> None:
        try:
            await session.execute(
                update(schema.workflow_runs)
                .where(schema.workflow_runs.c.run_id == run_id)
                .values(status=status, updated_at=func.now())
            )
            await session.commit()
        except Exception:
            await session.rollback()
            raise


# ---------------------------------------------------------------------------
# Child settles and cancellation (doc 03 §9)
# ---------------------------------------------------------------------------


async def settle_parent_on_child_terminal(
    session: AsyncSession, child_id: UUID, *, notify: NotifyFn | None = None
) -> None:
    """If ``child_id`` is a terminal child run with a live waiting parent,
    settle the parent's ``child`` step and wake the parent.

    Appends the parent's ``step_completed`` (child completed; result carries
    the child's run id + ``output_ref``) or ``step_failed(will_retry=False)``
    (child failed/cancelled — surfaces as the parent's step failure, doc 03
    §9), sets the parent ``pending``, re-enqueues it and awaits ``notify``.

    Called from every terminal transition path: ``execute_run`` completion,
    ``_fail_run``, ``cancel_run`` and the gate-timeout ``fail`` policy. Safe
    to call for any run: non-children, non-terminal children, terminal
    parents and already-settled steps are all no-ops (idempotent — the
    crash-retried settle appends nothing twice). The child→step mapping is
    inverted from the deterministic :func:`child_run_id` derivation, so no
    schema linkage column is needed.
    """
    result = await session.execute(
        select(schema.workflow_runs).where(schema.workflow_runs.c.run_id == child_id)
    )
    child = result.one_or_none()
    await session.rollback()  # read-only: release the SELECT's transaction
    if child is None or child.parent_run_id is None:
        return
    if child.status not in TERMINAL_STATUSES:
        return
    parent_id: UUID = child.parent_run_id
    result = await session.execute(
        select(schema.workflow_runs.c.status).where(schema.workflow_runs.c.run_id == parent_id)
    )
    parent_status = result.scalar_one_or_none()
    await session.rollback()
    if parent_status is None or parent_status in TERMINAL_STATUSES:
        return

    log = EventLog(session)
    events = await log.list(parent_id)
    step_key = next(
        (
            e.step_key
            for e in events
            if isinstance(e, StepStarted)
            and e.step_kind == "child"
            and child_run_id(parent_id, e.step_key) == child_id
        ),
        None,
    )
    if step_key is None:
        return  # parent never recorded this child's dispatch (defensive)
    if any(
        isinstance(e, (StepCompleted, StepFailed)) and e.step_key == step_key for e in events
    ):
        return  # already settled (this settle path or the parent's replay)

    event: StepCompleted | StepFailed
    if child.status == "completed":
        event = StepCompleted(
            step_key=step_key,
            result={
                "child_run_id": str(child_id),
                "output_ref": str(child.output_ref) if child.output_ref is not None else None,
            },
            duration_ms=0,
        )
    else:
        error: dict[str, Any] = child.error or {
            "type": "ChildRunCancelled",
            "message": f"child run {child_id} was cancelled",
        }
        prior_failures = sum(
            1 for e in events if isinstance(e, StepFailed) and e.step_key == step_key
        )
        event = StepFailed(
            step_key=step_key, error=error, attempt=prior_failures + 1, will_retry=False
        )
    await log.append(parent_id, event)
    # The parent is parked awaiting a claim: "pending" (a worker must re-claim
    # it), not the fold's "running" — same queue-derived refinement as
    # gate_resolved. apply_event_to_run_row is deliberately skipped.
    try:
        await session.execute(
            update(schema.workflow_runs)
            .where(schema.workflow_runs.c.run_id == parent_id)
            .values(status="pending", updated_at=func.now())
        )
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    await upsert_task_queue(session, parent_id)
    if notify is not None:
        await notify(parent_id)


async def _cancel_on_session(
    session: AsyncSession,
    run_id: UUID,
    actor: str,
    reason: str,
    notify: NotifyFn | None,
) -> None:
    result = await session.execute(
        select(schema.workflow_runs.c.status).where(schema.workflow_runs.c.run_id == run_id)
    )
    status = result.scalar_one_or_none()
    await session.rollback()  # read-only: release the SELECT's transaction
    if status is None:
        raise UnknownRun(f"No workflow run {run_id}")
    if status in TERMINAL_STATUSES:
        raise InvalidCancellation(run_id, status)

    log = EventLog(session)
    event = RunCancelled(actor=actor, reason=reason)
    await log.append(run_id, event)
    await apply_event_to_run_row(session, run_id, event)  # -> cancelled
    try:
        await session.execute(
            delete(schema.task_queue).where(schema.task_queue.c.run_id == run_id)
        )
        await session.execute(delete(schema.timers).where(schema.timers.c.run_id == run_id))
        await session.commit()
    except Exception:
        await session.rollback()
        raise

    # Cascade parent -> children (doc 03 §9): every live descendant goes too.
    result = await session.execute(
        select(schema.workflow_runs.c.run_id).where(
            schema.workflow_runs.c.parent_run_id == run_id,
            schema.workflow_runs.c.status.not_in(TERMINAL_STATUSES),
        )
    )
    children = [row.run_id for row in result.all()]
    await session.rollback()
    for child_id in children:
        try:
            await _cancel_on_session(
                session, child_id, actor, f"parent run {run_id} cancelled: {reason}", notify
            )
        except InvalidCancellation:
            continue  # the child reached a terminal state concurrently

    # A directly-cancelled CHILD surfaces to its waiting parent as a step
    # failure. In the cascade above the parent was cancelled first (terminal),
    # so this is a no-op there.
    await settle_parent_on_child_terminal(session, run_id, notify=notify)


async def cancel_run(
    session_or_factory: AsyncSession | async_sessionmaker[AsyncSession],
    run_id: UUID,
    actor: str,
    reason: str,
    *,
    notify: NotifyFn | None = None,
) -> None:
    """Cancel ``run_id`` (doc 03 §§3, 9).

    Legal from ``pending``/``running``/``waiting_gate``/``waiting_timer``;
    terminal states raise :class:`InvalidCancellation` and an unknown run
    raises :class:`UnknownRun`. Appends ``run_cancelled`` (projection →
    ``cancelled``), deletes the run's queue row and ALL of its timers, and
    cascades to non-terminal child runs recursively. A running worker
    observes the cancellation at its next step boundary (the context raises
    ``CancelledRun``; bounded grace — the in-flight step finishes first).
    In-flight execution-backend terminate signals are doc 05 territory.
    """
    if isinstance(session_or_factory, AsyncSession):
        await _cancel_on_session(session_or_factory, run_id, actor, reason, notify)
        return
    async with session_or_factory() as session:
        await _cancel_on_session(session, run_id, actor, reason, notify)
