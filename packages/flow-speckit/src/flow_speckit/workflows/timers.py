"""Durable timers: the ``timers`` table and the scheduler's firing pass (doc 03 §§5-7).

Suspend / queue-row contract (wave 4, honored by every wake path)
-----------------------------------------------------------------
A suspended run must not be claimable: when the engine catches ``_SuspendRun``
it **deletes** the run's ``task_queue`` row (after setting the projection
status), and every wake path **re-inserts** it via :func:`upsert_task_queue`
with ``available_at = now()`` and ``claimed_by = NULL``, then calls the
injected ``notify`` callable. This matches doc 03 §7, where waking is
re-enqueue + ``pg_notify`` — wave 5 wires ``notify`` to ``pg_notify``; the
default here is a no-op.

``ctx.sleep`` wake contract
---------------------------
The live sleep path appends ``step_started(kind=sleep)``, inserts a ``timers``
row (kind ``sleep``, ``fire_at = now + duration``) and suspends with status
``waiting_timer``. **The timer-firing path appends the step's**
``step_completed`` **event itself** (result ``{"slept": true}``) before
re-enqueueing, so the woken replay finds the key memoized and returns
instantly — no special resume logic in the context. A replay that arrives
while the timer is still pending finds the live ``timers`` row and re-suspends
without appending anything (idempotent, doc 03 §4).

Firing order is crash-safe in the at-least-once sense: complete/act →
re-enqueue → delete the timer row. A crash mid-sequence leaves the timer in
place, so the next scheduler pass retries; :func:`fire_due_timers` therefore
tolerates already-completed sleeps and already-resolved gates (stale timers
are simply deleted).

Timer kinds (schema CHECK constraint):

- ``sleep``        → append the step's ``step_completed``, re-enqueue, notify.
- ``retry``        → re-enqueue, notify (replay re-reaches the failed step;
  the attempt number derives from prior ``step_failed`` events in the log).
- ``gate_timeout`` → apply the ``on_timeout`` policy recorded in the gate's
  ``gate_opened`` payload — see ``gates.handle_gate_timeout``.

Due-ness is judged by the DATABASE clock (``fire_at <= now()``), so tests
inject "past-due" timers simply by sleeping/retrying with zero or negative
durations — never by sleeping real seconds.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import Row, delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from flow_speckit.storage import schema
from flow_speckit.workflows.context import apply_event_to_run_row
from flow_speckit.workflows.events import EventLog, StepCompleted

__all__ = [
    "NotifyFn",
    "fire_due_timers",
    "upsert_task_queue",
]

logger = structlog.get_logger(__name__)

NotifyFn = Callable[[UUID], Awaitable[None]]
"""Wake-notification seam: awaited with the woken ``run_id`` after every
re-enqueue. Wave 5 wires this to ``pg_notify``; the default is a no-op."""


async def upsert_task_queue(session: AsyncSession, run_id: UUID) -> None:
    """(Re-)enqueue ``run_id``: insert its queue row or make it claimable now."""
    stmt = pg_insert(schema.task_queue).values(run_id=run_id, available_at=func.now())
    stmt = stmt.on_conflict_do_update(
        index_elements=["run_id"],
        set_={"available_at": func.now(), "claimed_by": None, "heartbeat_at": None},
    )
    try:
        await session.execute(stmt)
        await session.commit()
    except Exception:
        await session.rollback()
        raise


async def _delete_timer(session: AsyncSession, timer_id: UUID) -> None:
    try:
        await session.execute(
            delete(schema.timers).where(schema.timers.c.timer_id == timer_id)
        )
        await session.commit()
    except Exception:
        await session.rollback()
        raise


async def _notify(notify: NotifyFn | None, run_id: UUID) -> None:
    if notify is not None:
        await notify(run_id)


async def _fire_sleep(
    session: AsyncSession, row: Row[Any], notify: NotifyFn | None
) -> None:
    log = EventLog(session)
    events = await log.list(row.run_id)
    already_completed = any(
        isinstance(e, StepCompleted) and e.step_key == row.step_key for e in events
    )
    if not already_completed:
        event = StepCompleted(step_key=row.step_key, result={"slept": True}, duration_ms=0)
        await log.append(row.run_id, event)
        await apply_event_to_run_row(session, row.run_id, event)
    await upsert_task_queue(session, row.run_id)
    await _delete_timer(session, row.timer_id)
    await _notify(notify, row.run_id)


async def fire_due_timers(
    session: AsyncSession, *, notify: NotifyFn | None = None
) -> int:
    """Fire every ``timers`` row whose ``fire_at`` has passed; return the count.

    Doc 03 §7's scheduler loop calls this repeatedly (wave 5); it is safe to
    call from anywhere — actions are idempotent per the module docstring.
    """
    # Imported here, not at module level: gates.py imports this module for
    # NotifyFn/upsert_task_queue, so the reverse edge must stay lazy.
    from flow_speckit.workflows.gates import handle_gate_timeout

    result = await session.execute(
        select(schema.timers)
        .where(schema.timers.c.fire_at <= func.now())
        .order_by(schema.timers.c.fire_at.asc())
    )
    due = result.all()
    await session.rollback()  # read-only: release the SELECT's transaction

    for row in due:
        if row.kind == "sleep":
            await _fire_sleep(session, row, notify)
        elif row.kind == "retry":
            await upsert_task_queue(session, row.run_id)
            await _delete_timer(session, row.timer_id)
            await _notify(notify, row.run_id)
        else:  # gate_timeout
            await handle_gate_timeout(
                session, run_id=row.run_id, step_key=row.step_key, notify=notify
            )
            await _delete_timer(session, row.timer_id)
        logger.info(
            "timer_fired",
            timer_id=str(row.timer_id),
            run_id=str(row.run_id),
            step_key=row.step_key,
            kind=row.kind,
        )
    return len(due)
