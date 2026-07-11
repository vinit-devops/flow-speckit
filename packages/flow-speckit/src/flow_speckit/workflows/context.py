"""``WorkflowContext`` — the whole ``ctx`` API (doc 03 §§4, 5).

Replay model
------------
Every side effect in a workflow body goes through a ``ctx.*`` step method.
Each call site is assigned a deterministic ``step_key`` — the caller's label,
plus ``#N`` (N >= 2) for repeated labels within one replay pass. The replay
cursor is a **dict keyed by step_key** built from the run's ``step_completed``
events (not a positional cursor — this is what keeps ``ctx.parallel`` legal
when a later wave implements it): a memoized key returns its recorded result
instantly with no re-execution and no new events; a miss takes the live path.

Non-determinism detection (doc 03 §4):

- Requesting a step the log cannot contain — i.e. a non-memoized key while the
  run's log is *sealed* (already ``run_completed``) — fails immediately with
  :class:`NonDeterminismError` naming the offending key (extra/renamed step).
- Memoized steps that a replay pass never requested (removed/renamed step) are
  detected when the body returns, via :meth:`WorkflowContext.assert_replay_consistent`.
- For an in-flight (unsealed) run, a live miss is simply the first
  un-checkpointed step and executes normally.

Checkpoint protocol for live steps (at-least-once, doc 03 §4): append
``step_started`` → run the side effect → ``fault_hook("after_side_effect")``
→ ``fault_hook("before_checkpoint_commit")`` → append ``step_completed`` and
sync the ``workflow_runs`` projection → ``fault_hook("after_checkpoint")``.
A crash between the side effect and the checkpoint re-executes the step on
the next ``execute_run``.

Determinism of results: live step results are canonicalized with
``pydantic_core.to_jsonable_python`` *before* being returned to the body, so
the live pass and every replay pass see byte-identical (JSON-shaped) values.

Retries: ``RetryPolicy`` lives on the step call. The engine-default backoff
(``backoff=None``) is **durable**: after appending ``step_failed(will_retry=
True)`` the context inserts a ``timers`` row (kind ``retry``, ``fire_at = now
+ policy.delay(attempt)``) and raises ``_SuspendRun(waiting_timer)`` — the run
releases its worker and the scheduler re-enqueues it when the timer fires
(``timers.fire_due_timers``). On wake, replay re-reaches the step live and the
attempt number is derived by counting the step's prior ``step_failed`` events
in the log. Passing an explicit :data:`BackoffFn` (e.g.
:func:`immediate_backoff`) opts into in-process retries instead — the seam the
test harness uses.

Gates and sleeps: ``ctx.gate`` (doc 03 §6, implemented in ``gates.py`` +
here) and ``ctx.sleep`` (``timers.py``) are replay-aware like every step, but
suspend via ``_SuspendRun`` instead of blocking; see those modules for the
wake contracts.

``ctx.config`` is a read-only mapping supplied by the engine. It is *not* a
memoized intrinsic yet — the config-plumbing work that would snapshot it into
the log is deliberately deferred; treat it as immutable for a run's lifetime.

Step factories (wave 5): every public step method is a synchronous factory
that allocates its ``step_key`` at coroutine CONSTRUCTION time and returns an
awaitable. Plain ``await ctx.run_skill(...)`` is unchanged; the eager
allocation is what makes ``ctx.parallel([...])`` deterministic — keys are
assigned in list order while the step list is being built, before
``asyncio.gather`` can interleave anything. Branches of one run share one
AsyncSession, so all session touchpoints serialize on an internal
``asyncio.Lock`` (side effects still run concurrently).

Cancellation (doc 03 §9): ``cancel_run`` appends ``run_cancelled``; every
LIVE step checks the log for it at its step boundary and raises
:class:`~flow_speckit.workflows.errors.CancelledRun` (BaseException — bodies
cannot swallow it), which the engine converts to a ``cancelled`` outcome.
In-flight execution-backend terminate signals are doc 05 territory.
"""

from __future__ import annotations

import asyncio
import random as _stdlib_random
import time
from collections.abc import Awaitable, Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Protocol
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import structlog
from pydantic import BaseModel, ConfigDict
from pydantic_core import to_jsonable_python
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from flow_speckit.storage import schema
from flow_speckit.workflows.errors import (
    CancelledRun,
    ChildWorkflowFailed,
    NonDeterminismError,
    StepKindUnavailableError,
    _SuspendRun,
    error_payload,
)
from flow_speckit.workflows.events import (
    EventLog,
    GateOpened,
    GateResolved,
    OnTimeout,
    RunCompleted,
    RunFailed,
    RunStarted,
    StepCompleted,
    StepCost,
    StepFailed,
    StepKind,
    StepStarted,
    WorkflowEvent,
)

if TYPE_CHECKING:
    from flow_speckit.workflows.gates import GateDecision

logger = structlog.get_logger(__name__)

__all__ = [
    "BackoffFn",
    "ChildStarter",
    "FaultHook",
    "RetryPolicy",
    "StepHandler",
    "StepInvocation",
    "StepResult",
    "WorkflowContext",
    "apply_event_to_run_row",
    "child_run_id",
    "immediate_backoff",
]


# ---------------------------------------------------------------------------
# Step-handler seam (consumed by engine.py, implemented by Phases 4/5)
# ---------------------------------------------------------------------------


class StepInvocation(BaseModel):
    """One live execution request handed to a :class:`StepHandler`.

    ``payload`` is kind-specific and already JSON-canonicalized, so a handler
    may echo any part of it into its result.
    """

    model_config = ConfigDict(frozen=True)

    run_id: UUID
    step_key: str
    step_kind: str
    label: str
    attempt: int
    payload: dict[str, Any]


class StepResult(BaseModel):
    """A handler's result. ``result`` must be JSON-serializable — it is stored
    inline in the ``step_completed`` event (artifact-ref indirection arrives
    with the Skill Engine in Phase 4)."""

    model_config = ConfigDict(frozen=True)

    result: Any = None
    cost: StepCost | None = None


class StepHandler(Protocol):
    """Executes the side effect for one step kind ("skill" | "execute" | "open_pr")."""

    async def __call__(self, step: StepInvocation) -> StepResult: ...


class ChildStarter(Protocol):
    """Engine-provided seam that starts a child run (doc 03 §§5, 9).

    Implemented by ``WorkflowEngine._start_child``: resolves ``version``
    (``None`` = the registry's latest for ``name``), records ``run_started``
    with ``parent_run_id`` set, inserts the run/queue rows under the GIVEN
    ``run_id`` and notifies. The context derives ``run_id`` deterministically
    (:func:`child_run_id`) so a crash-retried dispatch finds the existing
    child instead of starting a duplicate.
    """

    async def __call__(
        self,
        *,
        name: str,
        version: str | None,
        input: dict[str, Any],
        parent_run_id: UUID,
        run_id: UUID,
    ) -> None: ...


_CHILD_RUN_NAMESPACE = uuid5(NAMESPACE_URL, "flow-speckit:child-run")


def child_run_id(parent_run_id: UUID, step_key: str) -> UUID:
    """Deterministic child run id for a parent's ``child`` step.

    ``uuid5(namespace, f"{parent}:{step_key}")`` — identical across replays
    and crash retries, which is what makes ``ctx.child_workflow`` idempotent
    without a schema column linking child runs to parent step keys: a
    re-executed dispatch recomputes the same id and finds the existing child,
    and the wake path inverts the mapping by testing each recorded ``child``
    step key against the terminal child's run id.
    """
    return uuid5(_CHILD_RUN_NAMESPACE, f"{parent_run_id}:{step_key}")


# (handler key) -> (ctx method name, subsystem that ships the real handler)
_HANDLER_SUBSYSTEMS: dict[str, tuple[str, str]] = {
    "skill": ("run_skill", "Skill Engine (Phase 4)"),
    "execute": ("execute", "Execution Engine (Phase 5)"),
    "open_pr": ("open_pr", "Git provider integration (Phase 5)"),
}


# ---------------------------------------------------------------------------
# Retry policy + backoff seam
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RetryPolicy:
    """Per-step-call retry policy (doc 03 §5). Never lives inside skills."""

    max_attempts: int = 3
    backoff_base: timedelta = timedelta(seconds=10)
    backoff_factor: float = 2.0
    retry_on: tuple[type[BaseException], ...] = (Exception,)

    def delay(self, attempt: int) -> timedelta:
        """Exponential backoff before re-running after failed ``attempt`` (1-based)."""
        return self.backoff_base * (self.backoff_factor ** (attempt - 1))


BackoffFn = Callable[[int, "RetryPolicy"], Awaitable[None]]
"""Injectable in-process backoff seam. When a :class:`WorkflowContext` is
built WITHOUT one (``backoff=None``, the engine default), retry backoff is
durable instead: a ``timers`` row parks the run and the retry re-executes on
the next ``execute_run`` after the timer fires. Provide a ``BackoffFn`` (e.g.
:func:`immediate_backoff`) to retry in-process within one pass — the test
seam."""

FaultHook = Callable[[str, str], Awaitable[None]]
"""Crash-injection seam for the doc 03 §10 harness: awaited as
``fault_hook(checkpoint_name, step_key)`` at ``"after_side_effect"``,
``"before_checkpoint_commit"`` and ``"after_checkpoint"``. To simulate a hard
crash, raise a ``BaseException`` subclass — the engine only converts
``Exception`` into ``run_failed``, so a BaseException propagates untouched,
exactly like a killed worker (nothing else gets appended)."""


async def immediate_backoff(attempt: int, policy: RetryPolicy) -> None:
    """In-process backoff: none. Retries re-execute immediately in one pass.

    No longer the default — pass it explicitly to opt out of the durable
    timer-based backoff (see :data:`BackoffFn`)."""


_NO_RETRY = RetryPolicy(max_attempts=1)

_MISS: Any = object()


def _artifact_id(artifact: Any) -> UUID:
    """Accept a bare UUID or anything with a UUID ``.id`` (e.g. ArtifactRef)."""
    if isinstance(artifact, UUID):
        return artifact
    candidate = getattr(artifact, "id", None)
    if isinstance(candidate, UUID):
        return candidate
    raise TypeError(
        "ctx.gate(artifact=...) requires a UUID or an object with a UUID .id "
        f"(e.g. ArtifactRef); got {type(artifact).__name__}"
    )


# ---------------------------------------------------------------------------
# Projection sync
# ---------------------------------------------------------------------------


async def apply_event_to_run_row(
    session: AsyncSession, run_id: UUID, event: WorkflowEvent
) -> None:
    """Targeted UPDATE keeping ``workflow_runs`` convergent with ``project_run``.

    Applied after every event append so the projection row never diverges (at
    rest) from what folding the log would compute; the fold rules mirrored
    here are documented in ``events.py``. The event append and this UPDATE
    are separate transactions (``EventLog`` owns its own commit), so a crash
    in between leaves a stale-but-rebuildable row — the event log stays the
    single source of truth.
    """
    values: dict[str, Any]
    if isinstance(event, RunStarted):
        values = {"status": "pending"}
    elif isinstance(event, StepStarted):
        values = {
            "status": "waiting_timer" if event.step_kind == "sleep" else "running",
            "current_step": event.step_key,
        }
    elif isinstance(event, StepCompleted):
        values = {"status": "running"}
    elif isinstance(event, StepFailed):
        values = {"status": "waiting_timer" if event.will_retry else "running"}
    elif isinstance(event, GateOpened):
        values = {"status": "waiting_gate"}
    elif isinstance(event, GateResolved):
        values = {"status": "pending"}
    elif isinstance(event, RunCompleted):
        values = {"status": "completed", "output_ref": event.output_ref}
    elif isinstance(event, RunFailed):
        values = {"status": "failed", "error": event.error, "current_step": event.failed_step}
    else:  # RunCancelled — the union is closed, so this is exhaustive.
        values = {"status": "cancelled"}
    try:
        await session.execute(
            update(schema.workflow_runs)
            .where(schema.workflow_runs.c.run_id == run_id)
            .values(updated_at=func.now(), **values)
        )
        await session.commit()
    except Exception:
        await session.rollback()
        raise


# ---------------------------------------------------------------------------
# The context
# ---------------------------------------------------------------------------


class WorkflowContext:
    """The ``ctx`` handed to a workflow body — see the module docstring."""

    def __init__(
        self,
        *,
        run_id: UUID,
        session: AsyncSession,
        event_log: EventLog,
        handlers: Mapping[str, StepHandler],
        memoized: Mapping[str, Any],
        sealed: bool = False,
        fault_hook: FaultHook | None = None,
        backoff: BackoffFn | None = None,
        config: Mapping[str, Any] | None = None,
        events: Sequence[WorkflowEvent] = (),
        auto_approve: bool = False,
        child_starter: ChildStarter | None = None,
    ) -> None:
        self.run_id = run_id
        self._session = session
        self._log = event_log
        self._handlers = dict(handlers)
        self._memoized: dict[str, Any] = dict(memoized)
        self._sealed = sealed
        self._fault_hook = fault_hook
        # None = durable timer-based backoff (engine default); see BackoffFn.
        self._backoff: BackoffFn | None = backoff
        self.config: Mapping[str, Any] = MappingProxyType(dict(config or {}))
        self._auto_approve = auto_approve
        self._child_starter = child_starter
        # ctx.parallel runs branches concurrently but they all share ONE
        # AsyncSession, which is not concurrency-safe: every session touchpoint
        # below serializes on this lock. Handlers (the actual side effects)
        # still run concurrently — only checkpoint plumbing serializes.
        self._db_lock = asyncio.Lock()
        self._ordinals: dict[str, int] = {}
        self._requested: set[str] = set()
        self._last_step_key: str | None = None
        # Replay state derived from the recorded log: per-step failure counts
        # (attempt derivation across suspend/wake) and gate open/resolve
        # events keyed by step_key (gate memoization — gates never append
        # step_completed, so the `memoized` map cannot serve them).
        self._failed_counts: dict[str, int] = {}
        self._gates_opened: dict[str, GateOpened] = {}
        self._gates_resolved: dict[str, GateResolved] = {}
        for event in events:
            if isinstance(event, StepFailed):
                self._failed_counts[event.step_key] = (
                    self._failed_counts.get(event.step_key, 0) + 1
                )
            elif isinstance(event, GateOpened):
                self._gates_opened[event.step_key] = event
            elif isinstance(event, GateResolved):
                self._gates_resolved[event.step_key] = event

    # -- step-key bookkeeping -----------------------------------------------

    @property
    def last_step_key(self) -> str | None:
        """The most recently allocated step key (for failure attribution)."""
        return self._last_step_key

    def _allocate(self, label: str) -> str:
        ordinal = self._ordinals.get(label, 0) + 1
        self._ordinals[label] = ordinal
        step_key = label if ordinal == 1 else f"{label}#{ordinal}"
        if step_key in self._requested:  # unreachable via ordinals; defensive
            raise NonDeterminismError(step_key, "step key requested twice in one replay pass")
        self._requested.add(step_key)
        self._last_step_key = step_key
        return step_key

    def _require_live(self, step_key: str) -> None:
        if self._sealed:
            raise NonDeterminismError(
                step_key,
                "the run's event log is complete but replay requested a step it "
                "does not contain (extra or renamed step)",
            )

    def assert_replay_consistent(self) -> None:
        """Fail if memoized steps were never requested (removed/renamed step).

        The engine calls this when the body returns; leftover keys mean the
        replayed call sequence no longer matches the recorded log.
        """
        leftover = sorted(set(self._memoized) - self._requested)
        if leftover:
            raise NonDeterminismError(
                leftover[0],
                f"memoized step(s) {leftover!r} were never requested by the "
                "replayed body (removed or renamed step)",
            )

    # -- checkpoint protocol --------------------------------------------------

    async def _append(self, event: WorkflowEvent) -> None:
        async with self._db_lock:
            await self._log.append(self.run_id, event)
            await apply_event_to_run_row(self._session, self.run_id, event)

    async def _fault(self, checkpoint: str, step_key: str) -> None:
        if self._fault_hook is not None:
            await self._fault_hook(checkpoint, step_key)

    async def _raise_if_cancelled(self, step_key: str) -> None:
        """Step-boundary cancellation check (doc 03 §9, bounded grace).

        Consults the append-only LOG for ``run_cancelled`` rather than the
        projection row: a checkpoint committing after ``cancel_run`` sets the
        row status back to ``running``, but an appended ``run_cancelled``
        event can never be un-appended. Only live steps pay the roundtrip —
        memoized replay never reaches this check.
        """
        async with self._db_lock:
            result = await self._session.execute(
                select(schema.workflow_events.c.seq)
                .where(
                    schema.workflow_events.c.run_id == self.run_id,
                    schema.workflow_events.c.event_type == "run_cancelled",
                )
                .limit(1)
            )
            cancelled = result.first() is not None
            await self._session.rollback()  # read-only: release the transaction
        if cancelled:
            raise CancelledRun(step_key)

    async def _checkpointed(
        self,
        step_key: str,
        event_kind: StepKind,
        side_effect: Callable[[int], Awaitable[StepResult]],
        policy: RetryPolicy,
    ) -> Any:
        # Attempt numbering survives suspend/wake: prior step_failed events in
        # the log count as spent attempts (doc 03 §5 durable backoff).
        attempt = self._failed_counts.get(step_key, 0) + 1
        while True:
            await self._raise_if_cancelled(step_key)
            await self._append(StepStarted(step_key=step_key, step_kind=event_kind))
            started = time.monotonic()
            try:
                step_result = await side_effect(attempt)
            except Exception as exc:  # _SuspendRun is BaseException: never caught here
                will_retry = attempt < policy.max_attempts and isinstance(exc, policy.retry_on)
                await self._append(
                    StepFailed(
                        step_key=step_key,
                        error=error_payload(exc),
                        attempt=attempt,
                        will_retry=will_retry,
                    )
                )
                if not will_retry:
                    raise
                if self._backoff is None:
                    # Durable backoff: park the run on a retry timer; the
                    # scheduler re-enqueues it and replay re-reaches this
                    # step with the attempt count derived from the log.
                    await self._insert_timer(
                        "retry", step_key, datetime.now(UTC) + policy.delay(attempt)
                    )
                    raise _SuspendRun(
                        step_key,
                        f"retry backoff after attempt {attempt}",
                        "waiting_timer",
                        payload={"kind": "retry", "attempt": attempt},
                    ) from exc
                await self._backoff(attempt, policy)
                attempt += 1
                continue
            result = to_jsonable_python(step_result.result)
            await self._fault("after_side_effect", step_key)
            duration_ms = max(int((time.monotonic() - started) * 1000), 0)
            await self._fault("before_checkpoint_commit", step_key)
            await self._append(
                StepCompleted(
                    step_key=step_key,
                    result=result,
                    cost=step_result.cost,
                    duration_ms=duration_ms,
                )
            )
            await self._fault("after_checkpoint", step_key)
            self._memoized[step_key] = result
            return result

    def _handler_step(
        self,
        label: str,
        *,
        kind: str,
        event_kind: StepKind,
        payload: dict[str, Any],
        retry: RetryPolicy | None,
    ) -> Coroutine[Any, Any, Any]:
        # Factory pattern: the step_key is allocated SYNCHRONOUSLY, at
        # coroutine construction time, so building a ctx.parallel step list
        # assigns keys in list order before any interleaving can begin.
        step_key = self._allocate(label)
        return self._run_handler_step(
            step_key, label, kind=kind, event_kind=event_kind, payload=payload, retry=retry
        )

    async def _run_handler_step(
        self,
        step_key: str,
        label: str,
        *,
        kind: str,
        event_kind: StepKind,
        payload: dict[str, Any],
        retry: RetryPolicy | None,
    ) -> Any:
        hit = self._memoized.get(step_key, _MISS)
        if hit is not _MISS:
            return hit
        self._require_live(step_key)
        handler = self._handlers.get(kind)
        if handler is None:
            method, subsystem = _HANDLER_SUBSYSTEMS[kind]
            raise StepKindUnavailableError(kind=kind, method=method, subsystem=subsystem)
        policy = retry if retry is not None else RetryPolicy()

        async def side_effect(attempt: int) -> StepResult:
            return await handler(
                StepInvocation(
                    run_id=self.run_id,
                    step_key=step_key,
                    step_kind=kind,
                    label=label,
                    attempt=attempt,
                    payload=payload,
                )
            )

        return await self._checkpointed(step_key, event_kind, side_effect, policy)

    # -- handler-dispatched steps (doc 03 §5) ---------------------------------
    #
    # Every public step method is a SYNCHRONOUS factory returning an
    # awaitable: the step_key is allocated when the coroutine is CREATED, not
    # when it is first awaited. `await ctx.run_skill(...)` behaves exactly as
    # before; the difference only matters for ctx.parallel, where the step
    # list is built (keys assigned in list order) before asyncio.gather can
    # interleave anything.

    def run_skill(
        self,
        label: str,
        *,
        input: Any = None,
        retry: RetryPolicy | None = None,
        timeout: timedelta | None = None,
    ) -> Awaitable[Any]:
        """Run a registered skill. Handler kind ``"skill"``; event kind ``skill``."""
        payload = {
            "input": to_jsonable_python(input),
            "timeout_s": timeout.total_seconds() if timeout is not None else None,
        }
        return self._handler_step(
            label, kind="skill", event_kind="skill", payload=payload, retry=retry
        )

    def execute(
        self,
        label: str,
        *,
        plan: Any,
        backend: str,
        constraints: Any = None,
        retry: RetryPolicy | None = None,
    ) -> Awaitable[Any]:
        """Dispatch an execution backend (doc 05). Handler kind ``"execute"``."""
        payload = {
            "plan": to_jsonable_python(plan),
            "backend": backend,
            "constraints": to_jsonable_python(constraints),
        }
        return self._handler_step(
            label, kind="execute", event_kind="execute", payload=payload, retry=retry
        )

    def open_pr(
        self,
        label: str,
        *,
        change: Any,
        review: Any = None,
        retry: RetryPolicy | None = None,
    ) -> Awaitable[Any]:
        """Open a PR through the GitProvider port. Handler kind ``"open_pr"``;
        event kind ``intrinsic`` (doc 03 §5 table)."""
        payload = {
            "change": to_jsonable_python(change),
            "review": to_jsonable_python(review),
        }
        return self._handler_step(
            label, kind="open_pr", event_kind="intrinsic", payload=payload, retry=retry
        )

    # -- timers table plumbing (shared by sleep, gate timeouts, retry backoff) --

    async def _insert_timer(self, kind: str, step_key: str, fire_at: datetime) -> None:
        async with self._db_lock:
            try:
                await self._session.execute(
                    schema.timers.insert().values(
                        run_id=self.run_id, step_key=step_key, fire_at=fire_at, kind=kind
                    )
                )
                await self._session.commit()
            except Exception:
                await self._session.rollback()
                raise

    async def _timer_exists(self, step_key: str, kind: str) -> bool:
        async with self._db_lock:
            result = await self._session.execute(
                select(schema.timers.c.timer_id).where(
                    schema.timers.c.run_id == self.run_id,
                    schema.timers.c.step_key == step_key,
                    schema.timers.c.kind == kind,
                )
            )
            exists = result.first() is not None
            await self._session.rollback()  # read-only: release the transaction
        return exists

    # -- gates and durable sleep (doc 03 §§5, 6) --------------------------------

    def gate(
        self,
        label: str,
        *,
        artifact: Any = None,
        approvers: Sequence[str] = (),
        timeout: timedelta | None = None,
        on_timeout: OnTimeout = "fail",
        escalate_to: Sequence[str] | None = None,
    ) -> Awaitable[GateDecision]:
        """Human approval gate (doc 03 §6). See ``gates.py`` for the full
        lifecycle contract (resolution, timeout policies, actor identity).

        Replay-aware: a recorded ``gate_resolved`` for this step key returns
        its :class:`~flow_speckit.workflows.gates.GateDecision` instantly
        (rejection is a first-class, branchable outcome — re-gating after a
        rejection allocates a NEW step key, ``label#2``). A recorded
        ``gate_opened`` without a resolution re-suspends WITHOUT appending
        anything (idempotent re-open, doc 03 §4). The live path appends
        ``step_started`` + ``gate_opened``, arms a ``gate_timeout`` timer if
        ``timeout`` is set, and suspends with status ``waiting_gate`` — unless
        the engine runs with ``auto_approve``, which appends an immediate
        ``gate_resolved(actor="auto")`` and continues without suspending.

        ``artifact`` accepts an ``ArtifactRef`` (anything with a UUID ``.id``)
        or a bare ``UUID``.
        """
        step_key = self._allocate(label)
        return self._run_gate(
            step_key,
            label,
            artifact=artifact,
            approvers=approvers,
            timeout=timeout,
            on_timeout=on_timeout,
            escalate_to=escalate_to,
        )

    async def _run_gate(
        self,
        step_key: str,
        label: str,
        *,
        artifact: Any,
        approvers: Sequence[str],
        timeout: timedelta | None,
        on_timeout: OnTimeout,
        escalate_to: Sequence[str] | None,
    ) -> GateDecision:
        from flow_speckit.workflows.gates import decision_from_event

        resolved = self._gates_resolved.get(step_key)
        if resolved is not None:
            return decision_from_event(resolved)
        self._require_live(step_key)
        await self._raise_if_cancelled(step_key)
        opened = self._gates_opened.get(step_key)
        if opened is None:
            artifact_id = _artifact_id(artifact)
            timeout_at = datetime.now(UTC) + timeout if timeout is not None else None
            await self._append(StepStarted(step_key=step_key, step_kind="gate"))
            opened = GateOpened(
                step_key=step_key,
                gate_key=label,
                artifact_id=artifact_id,
                approvers=list(approvers),
                timeout_at=timeout_at,
                timeout_s=timeout.total_seconds() if timeout is not None else None,
                on_timeout=on_timeout,
                escalate_to=list(escalate_to) if escalate_to is not None else None,
            )
            await self._append(opened)
            self._gates_opened[step_key] = opened
            # auto_approve resolves before anyone could wait on the timer, so
            # never arm one (avoids a stale gate_timeout row).
            if timeout_at is not None and not self._auto_approve:
                await self._insert_timer("gate_timeout", step_key, timeout_at)
        if self._auto_approve:
            auto = GateResolved(
                step_key=step_key,
                gate_key=opened.gate_key,
                decision="approved",
                actor="auto",
                comment="auto-approved",
                resolved_at=datetime.now(UTC),
            )
            await self._append(auto)
            self._gates_resolved[step_key] = auto
            logger.warning(
                "gate_auto_approved",
                run_id=str(self.run_id),
                gate_key=opened.gate_key,
                step_key=step_key,
                artifact_id=str(opened.artifact_id),
            )
            return decision_from_event(auto)
        raise _SuspendRun(
            step_key,
            f"waiting on gate {opened.gate_key!r}",
            "waiting_gate",
            payload={"gate_key": opened.gate_key, "artifact_id": str(opened.artifact_id)},
        )

    def sleep(self, label: str, duration: timedelta) -> Awaitable[None]:
        """Durable timer; the run releases the worker (doc 03 §5).

        Wake contract (documented in full in ``timers.py``): the live path
        appends ``step_started(kind=sleep)``, inserts a ``timers`` row
        (kind ``sleep``) and suspends with status ``waiting_timer``. The
        timer-firing path (``fire_due_timers``) appends the step's
        ``step_completed`` itself, so on wake this call is simply memoized. A
        replay that arrives while the timer is still pending finds the live
        ``timers`` row and re-suspends without appending anything.
        """
        step_key = self._allocate(label)
        return self._run_sleep(step_key, duration)

    async def _run_sleep(self, step_key: str, duration: timedelta) -> None:
        if self._memoized.get(step_key, _MISS) is not _MISS:
            return  # timer fired; fire_due_timers appended step_completed
        self._require_live(step_key)
        await self._raise_if_cancelled(step_key)
        if not await self._timer_exists(step_key, "sleep"):
            await self._append(StepStarted(step_key=step_key, step_kind="sleep"))
            await self._insert_timer("sleep", step_key, datetime.now(UTC) + duration)
        raise _SuspendRun(
            step_key,
            f"durable sleep for {duration}",
            "waiting_timer",
            payload={"kind": "sleep"},
        )

    # -- parallel & child workflows (doc 03 §§5, 9) ------------------------------

    async def parallel(self, steps: Sequence[Awaitable[Any]]) -> list[Any]:
        """Gather over step awaitables; each memoizes independently (doc 03 §5).

        Semantics:

        - Step keys for the listed steps were allocated when the awaitables
          were CONSTRUCTED (ctx step methods are synchronous factories), i.e.
          in list order, before any interleaving — so replay assigns the same
          keys regardless of completion order. Custom branch coroutines that
          make FURTHER ctx calls allocate those keys at completion-interleaved
          runtime: keep labels unique across concurrent branches (repeated
          labels would race for ``#N`` ordinals between replays).
        - Results are returned in list order (``asyncio.gather``).
        - Each branch checkpoints independently: a completed branch's
          ``step_completed`` is committed even if a sibling later suspends,
          crashes or fails, and replays from memo after wake/recovery.
        - A suspend (gate/sleep/child) in ANY branch suspends the WHOLE run:
          the first ``_SuspendRun`` propagates, siblings are cancelled and
          awaited. A sibling cancelled between its side effect and its
          checkpoint re-executes on wake — the standard at-least-once
          contract.
        """
        tasks = [asyncio.ensure_future(step) for step in steps]
        try:
            return list(await asyncio.gather(*tasks))
        except BaseException:
            # First failure/suspend wins; make sure no sibling coroutine is
            # left running against a session/engine the caller is about to
            # unwind. Completed tasks are unaffected — their checkpoints are
            # already committed.
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    def child_workflow(
        self, label: str, *, name: str, input: dict[str, Any], version: str | None = None
    ) -> Awaitable[Any]:
        """Start a child run and await its result (doc 03 §§5, 9).

        Live path: appends ``step_started(kind=child)``, starts a child run
        whose id is the deterministic :func:`child_run_id` (with
        ``parent_run_id`` set and ``version`` pinned — ``None`` resolves the
        registry's latest), then suspends the parent. The parent stays
        ``running`` in projection (the child is doing the work) and holds no
        queue row. When the child reaches a terminal state, the engine's
        settle path appends this step's ``step_completed`` (result
        ``{"child_run_id", "output_ref"}`` — the child's output travels by
        ref, doc 03 §5) or ``step_failed(will_retry=False)`` (child
        failure/cancellation surfaces as the parent's step failure; no retry
        in v1), re-enqueues the parent and notifies. Cancellation cascades
        parent → children via ``cancel_run``.

        Because the child id is deterministic, a crash-retried dispatch finds
        the existing child instead of starting a duplicate, and a parent
        replay that finds the child already terminal settles inline.
        """
        step_key = self._allocate(label)
        return self._run_child(step_key, name=name, version=version, input=dict(input))

    async def _run_child(
        self, step_key: str, *, name: str, version: str | None, input: dict[str, Any]
    ) -> Any:
        hit = self._memoized.get(step_key, _MISS)
        if hit is not _MISS:
            return hit
        self._require_live(step_key)
        await self._raise_if_cancelled(step_key)
        if self._child_starter is None:
            raise RuntimeError(
                "ctx.child_workflow requires an engine-provided child starter; "
                "this WorkflowContext was built without one"
            )
        child_id = child_run_id(self.run_id, step_key)
        async with self._db_lock:
            result = await self._session.execute(
                select(
                    schema.workflow_runs.c.status,
                    schema.workflow_runs.c.output_ref,
                    schema.workflow_runs.c.error,
                ).where(schema.workflow_runs.c.run_id == child_id)
            )
            child = result.one_or_none()
            await self._session.rollback()  # read-only: release the transaction
        if child is None:
            await self._append(StepStarted(step_key=step_key, step_kind="child"))
            await self._child_starter(
                name=name,
                version=version,
                input=input,
                parent_run_id=self.run_id,
                run_id=child_id,
            )
        elif child.status in ("completed", "failed", "cancelled"):
            return await self._settle_child(
                step_key, child_id, child.status, child.output_ref, child.error
            )
        raise _SuspendRun(
            step_key,
            f"waiting on child workflow {name!r} run {child_id}",
            "running",
            payload={"kind": "child", "child_run_id": str(child_id)},
        )

    async def _settle_child(
        self,
        step_key: str,
        child_id: UUID,
        status: str,
        output_ref: UUID | None,
        error: dict[str, Any] | None,
    ) -> Any:
        """Parent-side settle for a child found ALREADY terminal at replay.

        The normal wake path (engine ``settle_parent_on_child_terminal``)
        appends this step's completion/failure from the child's side; this
        covers a parent replaying after that append raced past the memo
        snapshot, and the crash window where the child went terminal but the
        settle never committed (a manual resume then converges here).
        """
        async with self._db_lock:
            events = await self._log.list(self.run_id)
        completed = next(
            (e for e in events if isinstance(e, StepCompleted) and e.step_key == step_key),
            None,
        )
        if completed is not None:
            self._memoized[step_key] = completed.result
            return completed.result
        if status == "completed":
            result = {
                "child_run_id": str(child_id),
                "output_ref": str(output_ref) if output_ref is not None else None,
            }
            await self._append(StepCompleted(step_key=step_key, result=result, duration_ms=0))
            self._memoized[step_key] = result
            return result
        err: dict[str, Any] = error or {
            "type": "ChildRunCancelled",
            "message": f"child run {child_id} was cancelled",
        }
        already_failed = any(
            isinstance(e, StepFailed) and e.step_key == step_key for e in events
        )
        if not already_failed:
            await self._append(
                StepFailed(
                    step_key=step_key,
                    error=err,
                    attempt=self._failed_counts.get(step_key, 0) + 1,
                    will_retry=False,
                )
            )
        raise ChildWorkflowFailed(child_id, status, err)

    # -- memoized intrinsics ----------------------------------------------------

    def _intrinsic(self, label: str, produce: Callable[[], Any]) -> Coroutine[Any, Any, Any]:
        step_key = self._allocate(label)  # eager, like every step factory
        return self._run_intrinsic(step_key, produce)

    async def _run_intrinsic(self, step_key: str, produce: Callable[[], Any]) -> Any:
        hit = self._memoized.get(step_key, _MISS)
        if hit is not _MISS:
            return hit
        self._require_live(step_key)

        async def side_effect(attempt: int) -> StepResult:
            return StepResult(result=produce())

        return await self._checkpointed(step_key, "intrinsic", side_effect, _NO_RETRY)

    def now(self) -> Awaitable[datetime]:
        """Memoized ``datetime.now(UTC)`` — identical on every replay."""
        inner = self._intrinsic("now", lambda: datetime.now(UTC).isoformat())

        async def convert() -> datetime:
            return datetime.fromisoformat(str(await inner))

        return convert()

    def random(self) -> Awaitable[float]:
        """Memoized ``random.random()`` — identical on every replay."""
        inner = self._intrinsic("random", _stdlib_random.random)

        async def convert() -> float:
            return float(await inner)

        return convert()

    def uuid(self) -> Awaitable[UUID]:
        """Memoized ``uuid4()`` — identical on every replay."""
        inner = self._intrinsic("uuid", lambda: str(uuid4()))

        async def convert() -> UUID:
            return UUID(str(await inner))

        return convert()
