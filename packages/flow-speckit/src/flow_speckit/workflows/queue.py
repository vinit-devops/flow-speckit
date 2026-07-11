"""Postgres-only dispatch queue primitives (doc 03 §7).

The ``task_queue`` table is the only broker: a row means "this run wants a
worker". Rows are inserted at ``start_run``, re-inserted by every wake path
(``timers.upsert_task_queue`` — which clears ``claimed_by``/``heartbeat_at``
and doubles as the crash re-enqueue primitive), and DELETED when a run
suspends or reaches a terminal state.

- **Claim** (:func:`claim_one`): the doc's ``UPDATE ... WHERE run_id IN
  (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING run_id`` verbatim — safe
  under arbitrary worker concurrency; ``attempts`` counts claims.
- **Liveness**: :func:`heartbeat` refreshes ``heartbeat_at`` while a worker
  executes; :func:`reap_stale` clears claims whose heartbeat went stale
  (worker died mid-run) so any worker can re-claim — replay makes the
  re-execution safe (at-least-once, doc 03 §4).
- **Wakeup**: ``pg_notify`` on the :data:`WAKE_CHANNEL` channel, emitted on
  enqueue, gate-resolve and timer-fire. :func:`make_notifier` builds the
  concrete :data:`~flow_speckit.workflows.timers.NotifyFn` to inject into
  ``WorkflowEngine(notify=...)``, ``fire_due_timers``/``Scheduler`` and
  ``resolve_gate``; workers ``LISTEN`` (see ``worker.py``) with a poll
  fallback for missed notifications.

Session discipline matches the rest of the package: every function here owns
its transaction (commit on success, rollback on failure or after reads).
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from flow_speckit.storage import schema
from flow_speckit.workflows.timers import NotifyFn

__all__ = [
    "WAKE_CHANNEL",
    "claim_one",
    "heartbeat",
    "make_notifier",
    "notify_wake",
    "reap_stale",
    "release",
]

WAKE_CHANNEL = "flow_speckit_wake"
"""LISTEN/NOTIFY channel; the payload is the woken run_id as text."""

_CLAIM_SQL = text(
    """
    UPDATE task_queue
       SET claimed_by = :worker_id, heartbeat_at = now(), attempts = attempts + 1
     WHERE run_id IN (
            SELECT run_id
              FROM task_queue
             WHERE claimed_by IS NULL AND available_at <= now()
             ORDER BY available_at
             LIMIT 1
               FOR UPDATE SKIP LOCKED)
    RETURNING run_id
    """
)

_HEARTBEAT_SQL = text(
    """
    UPDATE task_queue
       SET heartbeat_at = now()
     WHERE run_id = :run_id AND claimed_by = :worker_id
    RETURNING run_id
    """
)

_REAP_SQL = text(
    """
    UPDATE task_queue
       SET claimed_by = NULL, heartbeat_at = NULL, available_at = now()
     WHERE claimed_by IS NOT NULL
       AND (heartbeat_at IS NULL
            OR heartbeat_at < now() - make_interval(secs => :older_than))
    RETURNING run_id
    """
)

_NOTIFY_SQL = text("SELECT pg_notify(:channel, :payload)")


async def claim_one(session: AsyncSession, worker_id: str) -> UUID | None:
    """Claim the single most-overdue unclaimed, due run; ``None`` if empty.

    ``FOR UPDATE SKIP LOCKED`` guarantees no two workers ever claim the same
    row, whatever the concurrency (doc 03 §10 queue-contention property).
    """
    try:
        result = await session.execute(_CLAIM_SQL, {"worker_id": worker_id})
        row = result.first()
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    if row is None:
        return None
    claimed = row[0]
    return claimed if isinstance(claimed, UUID) else UUID(str(claimed))


async def heartbeat(session: AsyncSession, run_id: UUID, worker_id: str) -> bool:
    """Refresh ``heartbeat_at`` for a claim this worker still owns.

    Returns False when the claim is gone (row deleted, or reaped and
    re-claimed elsewhere) — the executing worker keeps going regardless;
    replay converges either way.
    """
    try:
        result = await session.execute(
            _HEARTBEAT_SQL, {"run_id": str(run_id), "worker_id": worker_id}
        )
        beaten = result.first() is not None
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    return beaten


async def release(
    session: AsyncSession, run_id: UUID, *, worker_id: str | None = None
) -> bool:
    """Delete ``run_id``'s queue row; returns whether a row was deleted.

    With ``worker_id`` the delete only matches a row still claimed by that
    worker — the safe form for post-execution bookkeeping, since it can never
    eat a wake re-enqueue (``upsert_task_queue`` clears ``claimed_by``).
    Terminal and suspended runs normally have no row left (the engine deletes
    it inside ``execute_run``), so this is defensive.
    """
    base = delete(schema.task_queue).where(schema.task_queue.c.run_id == run_id)
    if worker_id is not None:
        base = base.where(schema.task_queue.c.claimed_by == worker_id)
    stmt = base.returning(schema.task_queue.c.run_id)
    try:
        result = await session.execute(stmt)
        deleted = result.first() is not None
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    return deleted


async def reap_stale(session: AsyncSession, older_than_seconds: float = 60.0) -> int:
    """Clear claims whose heartbeat is older than the threshold; return count.

    The crash-recovery path (doc 03 §7): a worker that died mid-run stops
    heartbeating; clearing ``claimed_by`` (and resetting ``available_at``)
    makes the run claimable again and replay resumes it from its last
    checkpoint. Runs in the Scheduler's tick by default (see ``worker.py``).
    """
    try:
        result = await session.execute(_REAP_SQL, {"older_than": older_than_seconds})
        reaped = len(result.all())
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    return reaped


async def notify_wake(session: AsyncSession, run_id: UUID) -> None:
    """``pg_notify(WAKE_CHANNEL, run_id)`` — delivered on commit."""
    try:
        await session.execute(
            _NOTIFY_SQL, {"channel": WAKE_CHANNEL, "payload": str(run_id)}
        )
        await session.commit()
    except Exception:
        await session.rollback()
        raise


def make_notifier(session_factory: async_sessionmaker[AsyncSession]) -> NotifyFn:
    """The concrete :data:`~flow_speckit.workflows.timers.NotifyFn`: each call
    opens a short-lived session and emits :func:`notify_wake`."""

    async def notify(run_id: UUID) -> None:
        async with session_factory() as session:
            await notify_wake(session, run_id)

    return notify
