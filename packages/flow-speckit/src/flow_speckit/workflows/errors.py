"""Workflow-engine error types and internal control-flow signals (doc 03 §§4, 8).

Version-pinning failures (a run whose pinned ``(name, version)`` is no longer
registered) reuse :class:`flow_speckit.workflows.registry.UnknownWorkflow`
rather than defining a parallel error type; the engine catches it at replay
time and records a ``run_failed`` event with bump-your-version guidance.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from flow_speckit.workflows.events import RunStatus

__all__ = [
    "CancelledRun",
    "ChildWorkflowFailed",
    "InvalidCancellation",
    "NonDeterminismError",
    "StepKindUnavailableError",
    "UnknownRun",
    "error_payload",
]


class NonDeterminismError(RuntimeError):
    """Replay produced a step sequence inconsistent with the recorded event log.

    Raised loudly instead of silently corrupting state (doc 03 §4). The most
    common cause is a workflow body that changed under a pinned version.
    """

    def __init__(self, step_key: str, detail: str) -> None:
        self.step_key = step_key
        super().__init__(
            f"Non-deterministic replay at step {step_key!r}: {detail}. "
            "A workflow body must never change under a pinned version (doc 03 §8): "
            "register the changed body under a NEW workflow version; in-flight runs "
            "keep executing the version pinned at run_started."
        )


class StepKindUnavailableError(NotImplementedError):
    """A ``ctx`` step kind has no registered handler in this engine build.

    The step-handler seam is real from Phase 3 onward, but the subsystems that
    provide production handlers (Skill Engine, execution backends, git
    provider) arrive in later phases.
    """

    def __init__(self, *, kind: str, method: str, subsystem: str) -> None:
        self.kind = kind
        self.method = method
        super().__init__(
            f"ctx.{method} requires the {subsystem}; no {kind!r} step handler is "
            "registered with this WorkflowEngine"
        )


class UnknownRun(LookupError):
    """Raised when a run_id has no ``workflow_runs`` row."""


class InvalidCancellation(RuntimeError):
    """``cancel_run`` was asked to cancel a run already in a terminal state.

    Cancellation is legal from ``pending``/``running``/``waiting_gate``/
    ``waiting_timer`` only (doc 03 §3); ``completed``/``failed``/``cancelled``
    runs are immutable history.
    """

    def __init__(self, run_id: UUID, status: str) -> None:
        self.run_id = run_id
        self.status = status
        super().__init__(
            f"Run {run_id} cannot be cancelled from terminal state {status!r}; "
            "cancellation is only legal from pending/running/waiting_gate/waiting_timer"
        )


class ChildWorkflowFailed(RuntimeError):
    """A child workflow reached ``failed``/``cancelled``; surfaces as the
    parent's step failure (doc 03 §9). Ordinary ``Exception`` so the engine
    records ``run_failed`` on the parent — compensation is explicit workflow
    logic, not an automatic saga."""

    def __init__(self, child_run_id: UUID, status: str, error: dict[str, Any]) -> None:
        self.child_run_id = child_run_id
        self.status = status
        self.error = error
        super().__init__(
            f"child workflow run {child_run_id} ended {status!r}: "
            f"{error.get('type', 'Error')}: {error.get('message', '')}"
        )


class CancelledRun(BaseException):
    """Internal control-flow signal: the run was cancelled mid-execution.

    Raised by the context's step-boundary cancellation check when the event
    log already contains ``run_cancelled`` (appended by ``cancel_run``).
    ``BaseException``-derived so a workflow body's ``except Exception`` can
    never swallow it; the engine converts it into a ``cancelled`` outcome
    WITHOUT appending ``run_failed`` — ``run_cancelled`` is already in the log.
    """

    def __init__(self, step_key: str) -> None:
        self.step_key = step_key
        super().__init__(f"run cancelled; observed at step boundary {step_key!r}")


class _SuspendRun(BaseException):
    """Internal control-flow signal: the run must release its worker.

    ``BaseException``-derived so a workflow body's ``except Exception`` can
    never swallow it (only the engine may catch it). Carries why the run
    suspended; later Phase-3 waves (gates, durable timers) populate
    ``payload`` with gate/timer details and the engine parks the run
    accordingly.
    """

    def __init__(
        self,
        step_key: str,
        reason: str,
        run_status: RunStatus,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.step_key = step_key
        self.reason = reason
        self.run_status = run_status
        self.payload: dict[str, Any] = payload if payload is not None else {}
        super().__init__(f"run suspended at step {step_key!r}: {reason}")


def error_payload(exc: BaseException) -> dict[str, Any]:
    """JSON-serializable error shape stored in step_failed/run_failed events."""
    return {"type": type(exc).__name__, "message": str(exc)}
