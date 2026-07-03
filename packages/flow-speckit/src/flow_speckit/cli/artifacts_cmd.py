from __future__ import annotations

import asyncio
from pathlib import Path

import typer
from rich.console import Console
from rich.markdown import Markdown
from rich.table import Table
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from flow_speckit.artifacts.refs import ArtifactRef
from flow_speckit.artifacts.registry import registry
from flow_speckit.artifacts.store import ArtifactNotFound, ArtifactStore
from flow_speckit.config import FlowSpeckitSettings, resolve_database_url
from flow_speckit.storage.db import create_engine, session_factory

artifacts_app = typer.Typer(name="artifacts", help="Inspect and manage artifacts.")
console = Console()
err_console = Console(stderr=True)


async def _open_store() -> tuple[AsyncEngine, AsyncSession, ArtifactStore]:
    """Build an (engine, session, store) triple from settings + the module registry.

    Callers own the returned engine/session and must dispose/close them
    (typically in a `finally` block) once done.
    """
    root = Path.cwd()
    settings = FlowSpeckitSettings.load(root)
    try:
        url = resolve_database_url(settings, root)
    except RuntimeError as exc:
        err_console.print(f"error: {exc}")
        raise typer.Exit(1) from exc
    # Load entry points BEFORE creating the engine: a failure here (e.g. a
    # RegistryCollisionError from a broken installed package) must not leak
    # an engine that no caller finally-block has been given yet.
    try:
        registry.load_entry_points()
    except Exception as exc:
        err_console.print(f"error: failed to load artifact types: {exc}")
        raise typer.Exit(1) from exc
    engine = create_engine(url)
    session = session_factory(engine)()
    return engine, session, ArtifactStore(session, registry)


def _refs_table(refs: list[ArtifactRef]) -> Table:
    table = Table()
    table.add_column("ADDRESS")
    table.add_column("TYPE")
    table.add_column("STATUS")
    table.add_column("CREATED")
    for ref in refs:
        table.add_row(ref.address, ref.type, ref.status, ref.created_at.isoformat())
    return table


@artifacts_app.command("list")
def list_artifacts(
    type: str | None = typer.Option(None, "--type", help="Filter by artifact type."),
) -> None:
    """List artifacts, newest first."""
    asyncio.run(_list_artifacts(type))


async def _list_artifacts(type_: str | None) -> None:
    engine, session, store = await _open_store()
    try:
        refs = await store.list(type=type_)
        console.print(_refs_table(refs))
    finally:
        await session.close()
        await engine.dispose()


@artifacts_app.command("show")
def show_artifact(ref: str) -> None:
    """Show an artifact's header (address/type/status/hash) and rendered body."""
    asyncio.run(_show_artifact(ref))


async def _show_artifact(ref: str) -> None:
    engine, session, store = await _open_store()
    try:
        try:
            resolved = await store.resolve(ref)
            # Pin to the resolved row's id so both reads see the same version.
            body_md = await store.get_body_md(resolved.id)
        except ArtifactNotFound as exc:
            err_console.print(f"error: artifact not found: {exc}")
            raise typer.Exit(1) from exc
        console.print(
            f"[bold]{resolved.address}[/bold] type={resolved.type} "
            f"status={resolved.status} hash={resolved.content_hash}"
        )
        # Print the STORED body_md (canonical, written at create time) — never
        # a fresh render_md(), which may have changed since the write.
        console.print(Markdown(body_md or ""))
    finally:
        await session.close()
        await engine.dispose()


@artifacts_app.command("versions")
def versions_cmd(key: str) -> None:
    """List all versions of an artifact key, oldest first."""
    asyncio.run(_versions(key))


async def _versions(key: str) -> None:
    engine, session, store = await _open_store()
    try:
        refs = await store.versions(key)
        if not refs:
            err_console.print(f"error: no versions found for key: {key}")
            raise typer.Exit(1)
        console.print(_refs_table(refs))
    finally:
        await session.close()
        await engine.dispose()


@artifacts_app.command("diff")
def diff_cmd(ref_a: str, ref_b: str) -> None:
    """Diff two artifact versions: unified text diff, then structured changes."""
    asyncio.run(_diff(ref_a, ref_b))


async def _diff(ref_a: str, ref_b: str) -> None:
    engine, session, store = await _open_store()
    try:
        try:
            result = await store.diff(ref_a, ref_b)
        except ArtifactNotFound as exc:
            err_console.print(f"error: artifact not found: {exc}")
            raise typer.Exit(1) from exc
        if result.text:
            console.print(result.text, end="")
        else:
            console.print("[dim](no textual differences)[/dim]")
        console.print()
        console.print("[bold]structured changes:[/bold]")
        if result.structured:
            for change_type, details in result.structured.items():
                console.print(f"  {change_type}: {details}")
        else:
            console.print("  (none)")
    finally:
        await session.close()
        await engine.dispose()
