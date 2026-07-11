"""``flow-speckit runs`` — list / show / cancel workflow runs (doc 07 §1).

Exit codes follow ``run_cmd``'s table: 0 success, 1 not-found or invalid
input (doc 07 reserves 5 for a *cancelled* run outcome, not lookups).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import UUID

import typer
from rich.console import Console
from rich.table import Table
from sqlalchemy import Row, select

from flow_speckit.cli.run_cmd import (
    default_actor,
    event_summary,
    load_run_row,
    open_workflow_env,
    parse_run_id,
)
from flow_speckit.storage import schema
from flow_speckit.workflows.engine import cancel_run
from flow_speckit.workflows.errors import InvalidCancellation, UnknownRun
from flow_speckit.workflows.events import EventLog
from flow_speckit.workflows.queue import make_notifier

runs_app = typer.Typer(
    name="runs",
    help=(
        "Inspect and manage workflow runs. v0.1 runs gates/sleep/intrinsics/"
        "parallel/child workflows fully; skill/execute/open_pr steps arrive "
        "with the Phase 4/5 engines."
    ),
)
console = Console()
err_console = Console(stderr=True)


def _runs_table(rows: list[Row[Any]]) -> Table:
    table = Table()
    table.add_column("RUN")
    table.add_column("WORKFLOW")
    table.add_column("STATUS")
    table.add_column("STEP")
    table.add_column("CREATED")
    table.add_column("UPDATED")
    for row in rows:
        table.add_row(
            str(row.run_id),
            f"{row.workflow_name}@{row.workflow_version}",
            row.status,
            row.current_step or "-",
            row.created_at.isoformat(timespec="seconds"),
            row.updated_at.isoformat(timespec="seconds"),
        )
    return table


@runs_app.command("list")
def list_runs() -> None:
    """List workflow runs, newest first."""
    asyncio.run(_list_runs())


async def _list_runs() -> None:
    async with open_workflow_env(load_workflows=False) as env:
        async with env.sessions() as session:
            result = await session.execute(
                select(schema.workflow_runs).order_by(
                    schema.workflow_runs.c.created_at.desc()
                )
            )
            rows = list(result.all())
            await session.rollback()  # read-only: release the transaction
        console.print(_runs_table(rows))


@runs_app.command("show")
def show_run(
    run_id: str = typer.Argument(..., help="Run id (UUID)."),
    events: bool = typer.Option(
        False, "--events", help="Append the full event log, one line per event."
    ),
) -> None:
    """Show a run's details (status, input, output/error), optionally its events."""
    rid = parse_run_id(run_id)
    asyncio.run(_show_run(rid, events))


async def _show_run(run_id: UUID, with_events: bool) -> None:
    async with open_workflow_env(load_workflows=False) as env:
        async with env.sessions() as session:
            row = await load_run_row(session, run_id)
            if row is None:
                err_console.print(f"error: no workflow run {run_id}", soft_wrap=True)
                raise typer.Exit(1)
            log = await EventLog(session).list(run_id) if with_events else []
        console.print(f"run {row.run_id}", soft_wrap=True, markup=False)
        console.print(
            f"  workflow:     {row.workflow_name}@{row.workflow_version}",
            markup=False,
        )
        console.print(f"  status:       {row.status}", markup=False)
        console.print(f"  current step: {row.current_step or '-'}", markup=False)
        if row.parent_run_id is not None:
            console.print(f"  parent run:   {row.parent_run_id}", markup=False)
        console.print(
            f"  created:      {row.created_at.isoformat(timespec='seconds')}",
            markup=False,
        )
        console.print(
            f"  updated:      {row.updated_at.isoformat(timespec='seconds')}",
            markup=False,
        )
        console.print(
            f"  input:        {json.dumps(row.input, sort_keys=True)}",
            soft_wrap=True,
            markup=False,
        )
        if row.output_ref is not None:
            console.print(f"  output_ref:   {row.output_ref}", markup=False)
        if row.error is not None:
            console.print(
                f"  error:        {json.dumps(row.error, sort_keys=True)}",
                soft_wrap=True,
                markup=False,
            )
        if with_events:
            console.print("events:", markup=False)
            for seq, event in enumerate(log, start=1):
                console.print(
                    f"{seq:>4}  {event.event_type:<15}  {event_summary(event)}",
                    soft_wrap=True,
                    markup=False,
                )


@runs_app.command("cancel")
def cancel(
    run_id: str = typer.Argument(..., help="Run id (UUID) of a non-terminal run."),
    reason: str = typer.Option(
        "cancelled via CLI", "--reason", help="Reason recorded on run_cancelled."
    ),
    actor: str | None = typer.Option(
        None, "--actor", help="Actor recorded on run_cancelled (default: current user)."
    ),
) -> None:
    """Cancel a run; cancellation cascades to its non-terminal child runs."""
    rid = parse_run_id(run_id)
    resolved_actor = actor if actor is not None else default_actor()
    asyncio.run(_cancel(rid, resolved_actor, reason))


async def _cancel(run_id: UUID, actor: str, reason: str) -> None:
    async with open_workflow_env(load_workflows=False) as env:
        try:
            await cancel_run(
                env.sessions, run_id, actor, reason, notify=make_notifier(env.sessions)
            )
        except UnknownRun as exc:
            err_console.print(f"error: no workflow run {run_id}", soft_wrap=True)
            raise typer.Exit(1) from exc
        except InvalidCancellation as exc:
            err_console.print(f"error: {exc}", soft_wrap=True)
            raise typer.Exit(1) from exc
        console.print(
            f"run {run_id} cancelled by {actor} — non-terminal child runs were "
            "cancelled with it",
            soft_wrap=True,
            markup=False,
        )
