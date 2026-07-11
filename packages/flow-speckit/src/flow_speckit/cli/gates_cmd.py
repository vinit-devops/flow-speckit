"""``flow-speckit gates`` — the pending-approvals inbox and resolution channel
(doc 03 §6, doc 07 §1).

``gates list`` derives open gates from the event logs of ``waiting_gate``
runs (a ``gate_opened`` without a matching ``gate_resolved``). ``approve`` /
``reject`` call :func:`flow_speckit.workflows.gates.resolve_gate`, which also
mirrors the decision onto the referenced artifact's status; ``reject``
REQUIRES ``--comment`` because the rejection comment becomes skill feedback
input (the core doc 03 §6 collaboration loop).

Exit codes follow ``run_cmd``'s table: 0 success, 1 no-open-gate/not-found,
2 usage errors (e.g. missing --comment on reject).
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any
from uuid import UUID

import typer
from rich.console import Console
from rich.table import Table
from sqlalchemy import Row, select
from sqlalchemy.ext.asyncio import AsyncSession

from flow_speckit.artifacts.store import InvalidStatusTransition
from flow_speckit.cli.run_cmd import (
    default_actor,
    fold_open_gates,
    open_workflow_env,
    parse_run_id,
)
from flow_speckit.storage import schema
from flow_speckit.workflows.events import EventLog, GateOpened, GateResolved, parse_event
from flow_speckit.workflows.gates import GateNotOpenError, resolve_gate
from flow_speckit.workflows.queue import make_notifier

gates_app = typer.Typer(
    name="gates",
    help=(
        "List and resolve human approval gates. Approving/rejecting also sets "
        "the gated artifact's status and re-enqueues the run."
    ),
)
console = Console()
err_console = Console(stderr=True)


async def _open_gates_with_opened_at(
    session: AsyncSession, run_id: UUID
) -> list[tuple[GateOpened, datetime]]:
    """Open gates for ``run_id`` with the ``gate_opened`` row's created_at.

    Reads the event table directly (rather than ``EventLog.list``) because the
    typed events do not carry ``created_at``.
    """
    result = await session.execute(
        select(
            schema.workflow_events.c.event_type,
            schema.workflow_events.c.payload,
            schema.workflow_events.c.created_at,
        )
        .where(
            schema.workflow_events.c.run_id == run_id,
            schema.workflow_events.c.event_type.in_(["gate_opened", "gate_resolved"]),
        )
        .order_by(schema.workflow_events.c.seq.asc())
    )
    rows = result.all()
    await session.rollback()  # read-only: release the SELECT's transaction
    open_by_step: dict[str, tuple[GateOpened, datetime]] = {}
    for row in rows:
        event = parse_event(row.event_type, row.payload)
        if isinstance(event, GateOpened):
            open_by_step[event.step_key] = (event, row.created_at)
        elif isinstance(event, GateResolved):
            open_by_step.pop(event.step_key, None)
    return list(open_by_step.values())


@gates_app.command("list")
def list_gates() -> None:
    """List every open gate (runs in waiting_gate) — the approvals inbox."""
    asyncio.run(_list_gates())


async def _list_gates() -> None:
    async with open_workflow_env(load_workflows=False) as env:
        async with env.sessions() as session:
            result = await session.execute(
                select(schema.workflow_runs)
                .where(schema.workflow_runs.c.status == "waiting_gate")
                .order_by(schema.workflow_runs.c.created_at.asc())
            )
            waiting: list[Row[Any]] = list(result.all())
            await session.rollback()  # read-only: release the transaction
            table = Table()
            table.add_column("RUN")
            table.add_column("WORKFLOW")
            table.add_column("GATE")
            table.add_column("ARTIFACT")
            table.add_column("APPROVERS")
            table.add_column("OPENED")
            for run in waiting:
                for opened, opened_at in await _open_gates_with_opened_at(
                    session, run.run_id
                ):
                    table.add_row(
                        str(run.run_id),
                        f"{run.workflow_name}@{run.workflow_version}",
                        opened.gate_key,
                        str(opened.artifact_id),
                        ", ".join(opened.approvers) or "-",
                        opened_at.isoformat(timespec="seconds"),
                    )
        console.print(table)


@gates_app.command("approve")
def approve(
    run_id: str = typer.Argument(..., help="Run id (UUID) waiting on the gate."),
    gate: str = typer.Argument(..., help="Gate key (the ctx.gate label)."),
    comment: str | None = typer.Option(None, "--comment", help="Optional approval note."),
    actor: str | None = typer.Option(
        None, "--actor", help="Actor recorded on gate_resolved (default: current user)."
    ),
) -> None:
    """Approve an open gate: artifact -> approved, run re-enqueued."""
    rid = parse_run_id(run_id)
    resolved_actor = actor if actor is not None else default_actor()
    asyncio.run(_resolve(rid, gate, "approved", resolved_actor, comment))


@gates_app.command("reject")
def reject(
    run_id: str = typer.Argument(..., help="Run id (UUID) waiting on the gate."),
    gate: str = typer.Argument(..., help="Gate key (the ctx.gate label)."),
    comment: str = typer.Option(
        ...,
        "--comment",
        help="REQUIRED: the rejection comment becomes skill feedback input.",
    ),
    actor: str | None = typer.Option(
        None, "--actor", help="Actor recorded on gate_resolved (default: current user)."
    ),
) -> None:
    """Reject an open gate: artifact -> rejected, run re-enqueued to branch on it."""
    rid = parse_run_id(run_id)
    resolved_actor = actor if actor is not None else default_actor()
    asyncio.run(_resolve(rid, gate, "rejected", resolved_actor, comment))


async def _resolve(
    run_id: UUID,
    gate_key: str,
    decision: str,
    actor: str,
    comment: str | None,
) -> None:
    async with open_workflow_env(load_workflows=False) as env:
        # Read the open gate first so the artifact side effect can be reported.
        async with env.sessions() as session:
            events = await EventLog(session).list(run_id)
        opened = next(
            (g for g in fold_open_gates(events) if g.gate_key == gate_key), None
        )
        try:
            result = await resolve_gate(
                env.sessions,
                run_id,
                gate_key,
                "approved" if decision == "approved" else "rejected",
                actor,
                comment,
                notify=make_notifier(env.sessions),
            )
        except GateNotOpenError as exc:
            err_console.print(f"error: {exc}", soft_wrap=True)
            raise typer.Exit(1) from exc
        except InvalidStatusTransition as exc:
            err_console.print(
                f"error: cannot resolve gate {gate_key!r} as {decision}: {exc}",
                soft_wrap=True,
            )
            raise typer.Exit(1) from exc
        console.print(
            f"gate {gate_key!r} {result.decision} by {result.actor} — "
            f"run {run_id} re-enqueued",
            soft_wrap=True,
            markup=False,
        )
        if opened is not None:
            console.print(
                f"artifact {opened.artifact_id} status -> {result.decision}",
                soft_wrap=True,
                markup=False,
            )
