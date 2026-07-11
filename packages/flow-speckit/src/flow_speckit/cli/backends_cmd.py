"""``flow-speckit backends list`` — list available execution backends (doc 05 §3, doc 07 §1)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from flow_speckit.execution.base import BackendHealth, ExecutionBackend
from flow_speckit.execution.local_shell import LocalShellBackend

backends_app = typer.Typer(
    name="backends",
    help="List and check available execution backends.",
)
console = Console()


@backends_app.command("list")
def list_backends(
    root: Path = typer.Option(
        Path.cwd(),
        "--root",
        "-r",
        help="Repository root.",
    ),
) -> None:
    """Show available execution backends and their health."""
    async def _check() -> None:
        backends: list[ExecutionBackend] = [LocalShellBackend()]
        table = Table(title="Execution Backends")
        table.add_column("NAME")
        table.add_column("AVAILABLE")
        table.add_column("VERSION")
        table.add_column("MESSAGE")

        for backend in backends:
            health = await backend.check_available()
            status = "✓" if health.available else "✗"
            table.add_row(
                backend.name,
                status,
                health.version or "-",
                health.message or "-",
            )

        console.print(table)

    asyncio.get_event_loop().run_until_complete(_check())