"""Worker and scheduler loops over the Postgres queue (doc 03 §7).

``Worker``
    Claim → ``execute_run`` → next, in an asyncio task. Wakes on
    ``LISTEN flow_speckit_wake`` (a dedicated raw asyncpg connection, when a
    ``listen_dsn`` is given) with a poll fallback every ``poll_interval``
    seconds for missed notifications. While a run executes, a sidecar task
    heartbeats its claim every ``heartbeat_interval`` seconds so the reaper
    leaves it alone. Post-execution bookkeeping is deliberately thin: the
    engine itself deletes the queue row on terminal AND suspended outcomes,
    so the worker only performs a defensive, claim-scoped ``release`` (which
    can never eat a concurrent wake re-enqueue — those clear ``claimed_by``).

``Scheduler``
    The doc's singleton timer loop: every ``interval`` seconds, take
    ``pg_try_advisory_lock(WORKFLOWS_LOCK_CLASS_ID, SCHEDULER_LOCK_OBJECT_ID)``
    on a dedicated connection; if another scheduler holds it, skip silently;
    otherwise run ``fire_due_timers`` (firing actions are idempotent) and
    ``reap_stale`` — the reaper lives HERE, not in the worker, so N workers
    do not hammer it — then unlock. The lock is session-level and explicitly
    unlocked in a ``finally`` before the connection returns to the pool.

``run_inline``
    CLI-mode composition (doc 03 §7): one Worker + one Scheduler as
    in-process asyncio tasks, as an async context manager. Construct your
    ``WorkflowEngine`` with ``notify=make_notifier(...)`` so ``start_run``
    itself NOTIFYs; ``run_inline`` wires the scheduler's timer-fire
    notifications for you.

All intervals are constructor-injectable; tests run with ~50 ms.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import asyncpg  # type: ignore[import-untyped]
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from flow_speckit.storage.db import session_factory
from flow_speckit.storage.locks import SCHEDULER_LOCK_OBJECT_ID, WORKFLOWS_LOCK_CLASS_ID
from flow_speckit.workflows.engine import TERMINAL_STATUSES, WorkflowEngine
from flow_speckit.workflows.queue import (
    WAKE_CHANNEL,
    claim_one,
    heartbeat,
    make_notifier,
    reap_stale,
    release,
)
from flow_speckit.workflows.timers import NotifyFn, fire_due_timers

__all__ = [
    "Scheduler",
    "Worker",
    "run_inline",
    "to_listen_dsn",
]

logger = structlog.get_logger(__name__)

_TRY_LOCK_SQL = text("SELECT pg_try_advisory_lock(:class_id, :obj_id)")
_UNLOCK_SQL = text("SELECT pg_advisory_unlock(:class_id, :obj_id)")
_LOCK_PARAMS = {"class_id": WORKFLOWS_LOCK_CLASS_ID, "obj_id": SCHEDULER_LOCK_OBJECT_ID}


def to_listen_dsn(url: str) -> str:
    """A SQLAlchemy URL (``postgresql+asyncpg://``) as a raw asyncpg DSN."""
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


class Worker:
    """One claim-and-execute loop; see the module docstring."""

    def __init__(
        self,
        engine: WorkflowEngine,
        sessions: async_sessionmaker[AsyncSession],
        *,
        worker_id: str | None = None,
        listen_dsn: str | None = None,
        poll_interval: float = 5.0,
        heartbeat_interval: float = 15.0,
    ) -> None:
        self._engine = engine
        self._sessions = sessions
        self.worker_id = worker_id if worker_id is not None else f"worker-{uuid4().hex[:12]}"
        self._listen_dsn = listen_dsn
        self._poll_interval = poll_interval
        self._heartbeat_interval = heartbeat_interval
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._listener: Any = None

    async def start(self) -> None:
        if self._task is not None:
            return
        if self._listen_dsn is not None:
            self._listener = await asyncpg.connect(to_listen_dsn(self._listen_dsn))
            await self._listener.add_listener(WAKE_CHANNEL, self._on_notify)
        self._task = asyncio.create_task(
            self._loop(), name=f"flow-speckit-worker:{self.worker_id}"
        )

    async def stop(self) -> None:
        """Graceful shutdown: cancel the loop (and any in-flight heartbeat
        sidecar via structured cleanup) and close the LISTEN connection."""
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._listener is not None:
            with contextlib.suppress(Exception):
                await self._listener.remove_listener(WAKE_CHANNEL, self._on_notify)
                await self._listener.close()
            self._listener = None

    async def __aenter__(self) -> Worker:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    def _on_notify(self, connection: Any, pid: int, channel: str, payload: str) -> None:
        # asyncpg invokes listener callbacks on the event loop: Event.set()
        # here is loop-safe. Any payload wakes the claim loop; claim_one
        # decides what is actually due.
        self._wake.set()

    async def _loop(self) -> None:
        while True:
            self._wake.clear()
            try:
                await self._drain()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("worker_drain_failed", worker_id=self.worker_id)
            # A notify that arrived during the drain is already .set(): the
            # wait returns immediately and we re-drain — no lost wakeups.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self._poll_interval)

    async def _drain(self) -> None:
        """Claim and execute until the queue has nothing due."""
        while True:
            async with self._sessions() as session:
                run_id = await claim_one(session, self.worker_id)
            if run_id is None:
                return
            await self._execute_claimed(run_id)

    async def _execute_claimed(self, run_id: UUID) -> None:
        beat = asyncio.create_task(
            self._heartbeat_loop(run_id),
            name=f"flow-speckit-heartbeat:{self.worker_id}:{run_id}",
        )
        try:
            outcome = await self._engine.execute_run(run_id)
            logger.info(
                "worker_run_pass_finished",
                worker_id=self.worker_id,
                run_id=str(run_id),
                status=outcome.status,
            )
            release_row = outcome.status in TERMINAL_STATUSES
        except Exception:
            # execute_run recorded run_failed where appropriate (and re-raised
            # operator errors like NonDeterminismError); keep the loop alive.
            logger.exception("worker_run_failed", worker_id=self.worker_id, run_id=str(run_id))
            release_row = True
        finally:
            beat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await beat
        if release_row:
            # Defensive: the engine already deleted the row on terminal
            # outcomes. Claim-scoped so a concurrent wake re-enqueue (which
            # clears claimed_by) can never be eaten.
            async with self._sessions() as session:
                await release(session, run_id, worker_id=self.worker_id)

    async def _heartbeat_loop(self, run_id: UUID) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            try:
                async with self._sessions() as session:
                    await heartbeat(session, run_id, self.worker_id)
            except Exception:
                logger.exception(
                    "worker_heartbeat_failed", worker_id=self.worker_id, run_id=str(run_id)
                )


class Scheduler:
    """The advisory-lock-guarded timer/reaper loop; see the module docstring."""

    def __init__(
        self,
        db: AsyncEngine,
        *,
        interval: float = 1.0,
        notify: NotifyFn | None = None,
        reap_after_seconds: float = 60.0,
    ) -> None:
        self._db = db
        self._sessions = session_factory(db)
        self._interval = interval
        self._notify = notify
        self._reap_after_seconds = reap_after_seconds
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="flow-speckit-scheduler")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def __aenter__(self) -> Scheduler:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    async def _loop(self) -> None:
        while True:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("scheduler_tick_failed")
            await asyncio.sleep(self._interval)

    async def _tick(self) -> None:
        async with self._db.connect() as lock_conn:
            result = await lock_conn.execute(_TRY_LOCK_SQL, _LOCK_PARAMS)
            acquired = bool(result.scalar())
            await lock_conn.commit()  # session-level lock survives the commit
            if not acquired:
                return  # another scheduler owns this tick — skip silently
            try:
                async with self._sessions() as session:
                    fired = await fire_due_timers(session, notify=self._notify)
                    reaped = await reap_stale(
                        session, older_than_seconds=self._reap_after_seconds
                    )
                if fired or reaped:
                    logger.info("scheduler_tick", fired=fired, reaped=reaped)
            finally:
                # The lock is session-level: it would survive the pooled
                # connection's reset-on-return, so it MUST be unlocked here.
                await lock_conn.execute(_UNLOCK_SQL, _LOCK_PARAMS)
                await lock_conn.commit()


@contextlib.asynccontextmanager
async def run_inline(
    engine: WorkflowEngine,
    db: AsyncEngine,
    *,
    worker_id: str | None = None,
    listen_dsn: str | None = None,
    poll_interval: float = 5.0,
    heartbeat_interval: float = 15.0,
    scheduler_interval: float = 1.0,
    reap_after_seconds: float = 60.0,
) -> AsyncIterator[tuple[Worker, Scheduler]]:
    """One Worker + one Scheduler as in-process asyncio tasks (CLI mode).

    ``listen_dsn`` (the database URL; ``postgresql+asyncpg://`` is accepted)
    enables LISTEN-based wakeups — without it the worker polls every
    ``poll_interval``. The scheduler's timer fires notify via ``pg_notify``
    either way; construct ``engine`` with ``notify=make_notifier(...)`` so
    fresh ``start_run`` enqueues notify too.
    """
    worker = Worker(
        engine,
        session_factory(db),
        worker_id=worker_id,
        listen_dsn=listen_dsn,
        poll_interval=poll_interval,
        heartbeat_interval=heartbeat_interval,
    )
    scheduler = Scheduler(
        db,
        interval=scheduler_interval,
        notify=make_notifier(session_factory(db)),
        reap_after_seconds=reap_after_seconds,
    )
    await worker.start()
    await scheduler.start()
    try:
        yield worker, scheduler
    finally:
        await scheduler.stop()
        await worker.stop()
