"""``flow-speckit backends list`` — list available execution backends (doc 05 §3, doc 07 §1)."""

from __future__ import annotations

import asyncio

import typer
from rich.console import Console
from rich.table import Table

from flow_speckit.execution.backend_registry import BackendRegistry

backends_app = typer.Typer(
    name="backends",
    help="List and check available execution backends.",
)
console = Console()


@backends_app.command("list")
def list_backends() -> None:
    """Show available execution backends and their health."""

    async def _check() -> None:
        registry = BackendRegistry()
        registry.discover()
        backends = registry.list_all()
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

    asyncio.run(_check())
