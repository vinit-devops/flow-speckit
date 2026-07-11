"""``flow-speckit run`` / ``flow-speckit resume`` — start and drive runs (doc 07 §§1-2).

This module also hosts the CLI plumbing shared by ``runs_cmd``/``gates_cmd``:
:func:`open_workflow_env` (engine + session lifecycle, mirroring
``artifacts_cmd._open_store``), run-row/event helpers, and the one-line event
summaries used by both the attached stream and ``runs show --events``.

Exit codes (doc 07 §2 UX rules — the doc wins where earlier notes differed):

- ``0``   success (run completed)
- ``3``   run is waiting/parked under ``--detach`` (open gate, future timer,
          child run needing a worker)
- ``4``   run failed
- ``5``   run cancelled
- ``1``   not-found / configuration errors (doc 07 reserves 5 for
          *cancelled*, so unknown runs/workflows use the generic CLI error
          code, matching ``artifacts_cmd``)
- ``2``   usage errors (typer/click, e.g. ``gates reject`` without --comment)
- ``130`` Ctrl-C while attached (the run stays resumable)

Input parsing: each ``--input k=v`` value is first tried as JSON (numbers,
booleans, objects, arrays); anything that does not parse falls back to the
raw string.

``--detach`` drives the run in the calling process with no worker/scheduler:
repeated ``execute_run`` passes, firing due timers inline between passes, and
exits as soon as the run terminates or parks on something that needs the
outside world (an open gate → exit 3 with the exact resolve commands, a
future timer, or a child run). Without ``--detach`` the command stays
attached: ``run_inline`` (worker + scheduler) executes the run while the
command streams event lines by polling the event log, prints the resolve
commands whenever the run parks at a gate, and keeps waiting; Ctrl-C
detaches and the run stays resumable.

v0.1 demoable surface: workflows composed of gates, durable sleeps,
intrinsics (``ctx.now/random/uuid``), ``ctx.parallel`` and child workflows
run fully. ``ctx.run_skill`` / ``ctx.execute`` / ``ctx.open_pr`` need the
Phase 4/5 engines — no handlers are registered here, so such steps fail the
run with a clear ``StepKindUnavailableError`` message.
"""

from __future__ import annotations

import asyncio
import contextlib
import getpass
import json
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import typer
from rich.console import Console
from sqlalchemy import Row, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from flow_speckit.cli.skills_cmd import build_skill_registry
from flow_speckit.config import FlowSpeckitSettings, resolve_database_url
from flow_speckit.storage import schema
from flow_speckit.storage.db import create_engine, session_factory
from flow_speckit.workflows.builder import build_handlers
from flow_speckit.workflows.engine import WorkflowEngine
from flow_speckit.workflows.errors import (
    NonDeterminismError,
    StepKindUnavailableError,
)
from flow_speckit.workflows.events import (
    EventLog,
    GateOpened,
    GateResolved,
    RunCompleted,
    RunFailed,
    RunStarted,
    StepCompleted,
    StepFailed,
    StepStarted,
    WorkflowEvent,
)
from flow_speckit.workflows.queue import make_notifier
from flow_speckit.workflows.registry import UnknownWorkflow
from flow_speckit.workflows.registry import registry as workflow_registry
from flow_speckit.workflows.timers import fire_due_timers, upsert_task_queue
from flow_speckit.workflows.worker import run_inline

console = Console()
err_console = Console(stderr=True)

TERMINAL = ("completed", "failed", "cancelled")


# ---------------------------------------------------------------------------
# Shared CLI plumbing (also imported by runs_cmd / gates_cmd)
# ---------------------------------------------------------------------------


@dataclass
class WorkflowEnv:
    """Everything a workflow CLI command needs, with owned lifecycles."""

    db: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    engine: WorkflowEngine
    url: str


@contextlib.asynccontextmanager
async def open_workflow_env(
    *, auto_approve: bool = False, load_workflows: bool = True
) -> AsyncIterator[WorkflowEnv]:
    """Yield a :class:`WorkflowEnv` built from settings + the module registry.

    Mirrors ``artifacts_cmd._open_store``: config/registry failures surface as
    clean errors BEFORE the engine exists, and the engine is disposed no
    matter what the caller's body does. ``load_workflows=False`` skips
    entry-point + ``./workflows`` discovery for commands that never invoke a
    body (runs/gates inspection and resolution).
    """
    root = Path.cwd()
    settings = FlowSpeckitSettings.load(root)
    try:
        url = resolve_database_url(settings, root)
    except RuntimeError as exc:
        err_console.print(f"error: {exc}")
        raise typer.Exit(1) from exc
    if load_workflows:
        try:
            workflow_registry.load_entry_points()
            workflow_registry.discover_local(root)
        except Exception as exc:
            err_console.print(f"error: failed to load workflows: {exc}")
            raise typer.Exit(1) from exc
    # Build the Phase 5 handler map: this is what wires skill/execute/open_pr
    # steps to their respective subsystems (doc 03 §5, Phase 5).
    handlers = build_handlers(settings, skill_registry=build_skill_registry(root))
    db = create_engine(url)
    try:
        sessions = session_factory(db)
        engine = WorkflowEngine(
            sessions,
            workflow_registry,
            handlers=handlers,
            notify=make_notifier(sessions),
            auto_approve=auto_approve,
        )
        yield WorkflowEnv(db=db, sessions=sessions, engine=engine, url=url)
    finally:
        await db.dispose()


def default_actor() -> str:
    """Best-effort local identity; recorded, never enforced (doc 03 §6)."""
    try:
        return getpass.getuser()
    except Exception:
        return "unknown"


def parse_run_id(raw: str) -> UUID:
    try:
        return UUID(raw)
    except ValueError as exc:
        err_console.print(f"error: invalid run id: {raw!r} (expected a UUID)")
        raise typer.Exit(1) from exc


def parse_inputs(pairs: Sequence[str]) -> dict[str, Any]:
    """``k=v`` pairs → dict; values are JSON when they parse, else raw strings."""
    inputs: dict[str, Any] = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            err_console.print(f"error: --input expects k=v, got {pair!r}")
            raise typer.Exit(2)
        try:
            inputs[key] = json.loads(value)
        except json.JSONDecodeError:
            inputs[key] = value
    return inputs


async def load_run_row(session: AsyncSession, run_id: UUID) -> Row[Any] | None:
    result = await session.execute(
        select(schema.workflow_runs).where(schema.workflow_runs.c.run_id == run_id)
    )
    row = result.one_or_none()
    await session.rollback()  # read-only: release the SELECT's transaction
    return row


async def has_queue_row(session: AsyncSession, run_id: UUID) -> bool:
    result = await session.execute(
        select(schema.task_queue.c.run_id).where(schema.task_queue.c.run_id == run_id)
    )
    queued = result.first() is not None
    await session.rollback()  # read-only: release the SELECT's transaction
    return queued


def fold_open_gates(events: Sequence[WorkflowEvent]) -> list[GateOpened]:
    """``gate_opened`` events without a matching resolution, in open order."""
    open_by_step: dict[str, GateOpened] = {}
    for event in events:
        if isinstance(event, GateOpened):
            open_by_step[event.step_key] = event
        elif isinstance(event, GateResolved):
            open_by_step.pop(event.step_key, None)
    return list(open_by_step.values())


def _fmt_error(error: dict[str, Any] | None) -> str:
    if not error:
        return "unknown error"
    return f"{error.get('type', 'Error')}: {error.get('message', '')}"


def event_summary(event: WorkflowEvent) -> str:
    """One-line, human-scannable payload highlights per event type."""
    if isinstance(event, RunStarted):
        return f"{event.workflow_name}@{event.workflow_version} by {event.actor}"
    if isinstance(event, StepStarted):
        return f"{event.step_key} ({event.step_kind})"
    if isinstance(event, StepCompleted):
        cost = f"  ${event.cost.usd:.2f}" if event.cost is not None else ""
        return f"{event.step_key}{cost}  {event.duration_ms}ms"
    if isinstance(event, StepFailed):
        return (
            f"{event.step_key} attempt={event.attempt} will_retry={event.will_retry}"
            f"  {_fmt_error(event.error)}"
        )
    if isinstance(event, GateOpened):
        return (
            f"{event.gate_key} approvers={list(event.approvers)} "
            f"artifact={event.artifact_id}"
        )
    if isinstance(event, GateResolved):
        comment = f" {event.comment!r}" if event.comment else ""
        return f"{event.gate_key} {event.decision} by {event.actor}{comment}"
    if isinstance(event, RunCompleted):
        return f"output_ref={event.output_ref}"
    if isinstance(event, RunFailed):
        return f"{event.failed_step}  {_fmt_error(event.error)}"
    return f"by {event.actor}: {event.reason}"  # RunCancelled — exhaustive


def print_gate_commands(run_id: UUID, opened: GateOpened) -> None:
    """Every pause prints the exact command to continue (doc 07 §2)."""
    console.print(
        f"gate {opened.gate_key!r} waiting for {list(opened.approvers)}",
        soft_wrap=True,
        markup=False,
    )
    console.print(
        f"  review:  flow-speckit artifacts show {opened.artifact_id}",
        soft_wrap=True,
        markup=False,
    )
    console.print(
        f"  approve: flow-speckit gates approve {run_id} {opened.gate_key}",
        soft_wrap=True,
        markup=False,
    )
    console.print(
        f'  reject:  flow-speckit gates reject {run_id} {opened.gate_key} --comment "..."',
        soft_wrap=True,
        markup=False,
    )


async def _print_open_gates(env: WorkflowEnv, run_id: UUID) -> None:
    async with env.sessions() as session:
        events = await EventLog(session).list(run_id)
    for opened in fold_open_gates(events):
        print_gate_commands(run_id, opened)


# ---------------------------------------------------------------------------
# Drivers
# ---------------------------------------------------------------------------


async def drive_detached(env: WorkflowEnv, run_id: UUID) -> int:
    """Repeated ``execute_run`` passes, firing due timers inline; exit code.

    Deviation from a literal "single execute_run" reading of the plan: a
    zero/past-due durable sleep would otherwise park a detached run at exit 3
    even though nothing external is needed. Between passes, due timers are
    fired and the run is re-driven while it holds a claimable queue row; the
    command exits 3 only when the run parks on the outside world (open gate,
    future timer, child run needing a worker).
    """
    notify = make_notifier(env.sessions)
    while True:
        try:
            outcome = await env.engine.execute_run(run_id)
        except (NonDeterminismError, StepKindUnavailableError) as exc:
            err_console.print(f"error: run {run_id} failed: {exc}", soft_wrap=True)
            return 4
        if outcome.status == "completed":
            console.print(f"run {run_id} completed", soft_wrap=True, markup=False)
            return 0
        if outcome.status == "failed":
            err_console.print(
                f"error: run {run_id} failed: {_fmt_error(outcome.error)}", soft_wrap=True
            )
            return 4
        if outcome.status == "cancelled":
            console.print(f"run {run_id} was cancelled", soft_wrap=True, markup=False)
            return 5
        if outcome.status == "waiting_gate":
            console.print(
                f"run {run_id} is waiting on a gate", soft_wrap=True, markup=False
            )
            await _print_open_gates(env, run_id)
            return 3
        # waiting_timer, a child park ("running"), or an engine-signaled
        # immediate re-enqueue ("pending"): fire due timers, then keep driving
        # while the run holds a claimable queue row.
        async with env.sessions() as session:
            await fire_due_timers(session, notify=notify)
            queued = await has_queue_row(session, run_id)
        if queued:
            continue
        console.print(
            f"run {run_id} is parked ({outcome.status}); it needs a worker or a "
            f"future timer — resume with: flow-speckit resume {run_id}",
            soft_wrap=True,
            markup=False,
        )
        return 3


async def drive_attached(env: WorkflowEnv, run_id: UUID, *, poll: float = 0.5) -> int:
    """Inline worker+scheduler; stream event lines until the run terminates.

    A run parked at a gate prints the resolve commands once per open gate and
    keeps waiting (Ctrl-C detaches; the run stays resumable).
    """
    printed_gates: set[str] = set()
    seen = 0
    async with run_inline(
        env.engine,
        env.db,
        listen_dsn=env.url,
        poll_interval=2.0,
        scheduler_interval=0.5,
    ):
        while True:
            async with env.sessions() as session:
                events = await EventLog(session).list(run_id)
                row = await load_run_row(session, run_id)
            for seq, event in enumerate(events[seen:], start=seen + 1):
                console.print(
                    f"{seq:>4}  {event.event_type:<15}  {event_summary(event)}",
                    soft_wrap=True,
                    markup=False,
                )
            seen = len(events)
            status = row.status if row is not None else None
            if status == "completed":
                console.print(f"run {run_id} completed", soft_wrap=True, markup=False)
                return 0
            if status == "failed":
                err_console.print(
                    f"error: run {run_id} failed: "
                    f"{_fmt_error(row.error if row is not None else None)}",
                    soft_wrap=True,
                )
                return 4
            if status == "cancelled":
                console.print(f"run {run_id} was cancelled", soft_wrap=True, markup=False)
                return 5
            if status == "waiting_gate":
                for opened in fold_open_gates(events):
                    if opened.step_key in printed_gates:
                        continue
                    printed_gates.add(opened.step_key)
                    print_gate_commands(run_id, opened)
            await asyncio.sleep(poll)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def run(
    workflow: str = typer.Argument(
        ..., help="Workflow name (entry points + project-local ./workflows)."
    ),
    input: list[str] = typer.Option(  # noqa: B008 — typer option factory
        [],
        "--input",
        "-i",
        metavar="K=V",
        help="Workflow input; value parsed as JSON, falling back to a raw string.",
    ),
    version: str | None = typer.Option(
        None, "--version", "-V", help="Workflow version (default: latest registered)."
    ),
    auto_approve: bool = typer.Option(
        False,
        "--auto-approve",
        help="Resolve every gate instantly with actor 'auto' (dangerous; loudly logged).",
    ),
    detach: bool = typer.Option(
        False,
        "--detach",
        help="Drive inline without a worker; exit 3 as soon as the run parks.",
    ),
    actor: str | None = typer.Option(
        None, "--actor", help="Actor recorded on run_started (default: current user)."
    ),
) -> None:
    """Start a workflow run and drive it (attached by default; --detach to park).

    v0.1: gates/sleep/intrinsics/parallel/child workflows run fully;
    skill/execute/open_pr steps need the Phase 4/5 engines and fail the run
    with a clear message.
    """
    inputs = parse_inputs(input)
    resolved_actor = actor if actor is not None else default_actor()
    try:
        code = asyncio.run(
            _run(workflow, inputs, version, auto_approve, detach, resolved_actor)
        )
    except KeyboardInterrupt:
        err_console.print("interrupted — the run keeps its state; resume it anytime")
        raise typer.Exit(130) from None
    raise typer.Exit(code)


async def _run(
    name: str,
    inputs: dict[str, Any],
    version: str | None,
    auto_approve: bool,
    detach: bool,
    actor: str,
) -> int:
    async with open_workflow_env(auto_approve=auto_approve) as env:
        if auto_approve:
            err_console.print(
                "warning: --auto-approve resolves every gate instantly with actor "
                "'auto' — do not use where human review matters",
                soft_wrap=True,
            )
        try:
            definition = (
                workflow_registry.get(name, version)
                if version is not None
                else workflow_registry.latest(name)
            )
        except UnknownWorkflow:
            known = sorted({d.name for d in workflow_registry.all()})
            err_console.print(
                f"error: unknown workflow {name!r}"
                + (f" (version {version!r})" if version is not None else "")
                + f"; known workflows: {', '.join(known) if known else '(none)'}",
                soft_wrap=True,
            )
            return 1
        run_id = await env.engine.start_run(
            definition.name, definition.version, inputs, actor
        )
        console.print(
            f"run {run_id} started — workflow {definition.name}@{definition.version}",
            soft_wrap=True,
            markup=False,
        )
        if detach:
            return await drive_detached(env, run_id)
        console.print(
            f"attached; Ctrl-C detaches — resume with: flow-speckit resume {run_id}",
            soft_wrap=True,
            markup=False,
        )
        return await drive_attached(env, run_id)


def resume(
    run_id: str = typer.Argument(..., help="Run id (UUID) of a non-terminal run."),
    detach: bool = typer.Option(
        False,
        "--detach",
        help="Drive inline without a worker; exit 3 as soon as the run parks.",
    ),
) -> None:
    """Re-enqueue a non-terminal run and drive it; replay does the rest (doc 03 §3)."""
    rid = parse_run_id(run_id)
    try:
        code = asyncio.run(_resume(rid, detach))
    except KeyboardInterrupt:
        err_console.print("interrupted — the run keeps its state; resume it anytime")
        raise typer.Exit(130) from None
    raise typer.Exit(code)


async def _resume(run_id: UUID, detach: bool) -> int:
    async with open_workflow_env() as env:
        async with env.sessions() as session:
            row = await load_run_row(session, run_id)
            if row is None:
                err_console.print(f"error: no workflow run {run_id}", soft_wrap=True)
                return 1
            if row.status in TERMINAL:
                err_console.print(
                    f"error: run {run_id} is already {row.status} — terminal runs "
                    "cannot be resumed",
                    soft_wrap=True,
                )
                return 1
            # Resume IS re-enqueue (doc 03 §3): make the run claimable and
            # wake any listening worker; replay does the rest.
            await upsert_task_queue(session, run_id)
        await make_notifier(env.sessions)(run_id)
        console.print(
            f"run {run_id} re-enqueued — workflow "
            f"{row.workflow_name}@{row.workflow_version}",
            soft_wrap=True,
            markup=False,
        )
        if detach:
            return await drive_detached(env, run_id)
        console.print(
            f"attached; Ctrl-C detaches — resume with: flow-speckit resume {run_id}",
            soft_wrap=True,
            markup=False,
        )
        return await drive_attached(env, run_id)
