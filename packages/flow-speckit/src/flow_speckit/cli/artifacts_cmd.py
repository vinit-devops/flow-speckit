from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from pathlib import Path

import typer
from rich.console import Console
from rich.markdown import Markdown
from rich.table import Table

from flow_speckit.artifacts.refs import ArtifactRef
from flow_speckit.artifacts.registry import registry
from flow_speckit.artifacts.store import ArtifactNotFound, ArtifactStore
from flow_speckit.config import FlowSpeckitSettings, resolve_database_url
from flow_speckit.storage.db import create_engine, session_factory

artifacts_app = typer.Typer(name="artifacts", help="Inspect and manage artifacts.")
console = Console()
err_console = Console(stderr=True)


@contextlib.asynccontextmanager
async def _open_store() -> AsyncIterator[ArtifactStore]:
    """Yield an ArtifactStore built from settings + the module registry.

    Owns the engine and session lifecycles via nested try/finally: the engine
    is disposed even if `session.close()` raises, and both cleanups run no
    matter what the caller's body does.
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
    # an engine that no cleanup block has been given yet.
    try:
        registry.load_entry_points()
    except Exception as exc:
        err_console.print(f"error: failed to load artifact types: {exc}")
        raise typer.Exit(1) from exc
    engine = create_engine(url)
    try:
        session = session_factory(engine)()
        try:
            yield ArtifactStore(session, registry)
        finally:
            await session.close()
    finally:
        await engine.dispose()


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
    async with _open_store() as store:
        refs = await store.list(type=type_)
        console.print(_refs_table(refs))


@artifacts_app.command("show")
def show_artifact(ref: str) -> None:
    """Show an artifact's header (address/type/status/hash) and rendered body."""
    asyncio.run(_show_artifact(ref))


async def _show_artifact(ref: str) -> None:
    async with _open_store() as store:
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


@artifacts_app.command("versions")
def versions_cmd(key: str) -> None:
    """List all versions of an artifact key, oldest first."""
    asyncio.run(_versions(key))


async def _versions(key: str) -> None:
    async with _open_store() as store:
        refs = await store.versions(key)
        if not refs:
            err_console.print(f"error: no versions found for key: {key}")
            raise typer.Exit(1)
        console.print(_refs_table(refs))


@artifacts_app.command("diff")
def diff_cmd(ref_a: str, ref_b: str) -> None:
    """Diff two artifact versions: unified text diff, then structured changes."""
    asyncio.run(_diff(ref_a, ref_b))


async def _diff(ref_a: str, ref_b: str) -> None:
    async with _open_store() as store:
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
