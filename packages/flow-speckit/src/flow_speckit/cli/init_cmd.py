from __future__ import annotations

import asyncio
from pathlib import Path

import typer
from rich.console import Console

from flow_speckit.artifacts.registry import registry
from flow_speckit.config import FlowSpeckitSettings, resolve_database_url
from flow_speckit.storage.db import create_engine, session_factory
from flow_speckit.storage.migrate import run_migrations

console = Console()
err_console = Console(stderr=True)

_TOML_TEMPLATE = """\
# Flow SpecKit configuration.
#
# Uncomment and set a database URL to point at an external Postgres
# instance. If left unset, `flow-speckit` falls back to an embedded Postgres
# server rooted at ./.flow-speckit/pg (requires the `embedded-pg` extra).
[database]
# url = "postgresql://user:password@localhost:5432/flow_speckit"
"""


def init() -> None:
    """Initialize a Flow SpecKit project: config, database, and artifact registry."""
    root = Path.cwd()
    config_path = root / "flow-speckit.toml"
    if config_path.exists():
        config_status = "exists"
    else:
        config_path.write_text(_TOML_TEMPLATE)
        config_status = "created"

    settings = FlowSpeckitSettings.load(root)
    try:
        url = resolve_database_url(settings, root)
    except RuntimeError as exc:
        err_console.print(f"error: {exc}")
        raise typer.Exit(1) from exc

    # run_migrations() drives its own event loop internally (Alembic's env.py
    # calls asyncio.run()), so it must run here, not inside _sync_registry's
    # already-running loop.
    run_migrations(url)

    # Load entry points BEFORE creating the engine: a failure here (e.g. a
    # RegistryCollisionError from a broken installed package) must not leak
    # an engine that no caller finally-block has been given yet.
    try:
        registry.load_entry_points()
    except Exception as exc:
        err_console.print(f"error: failed to load artifact types: {exc}")
        raise typer.Exit(1) from exc

    asyncio.run(_sync_registry(url))

    type_count = len(registry.all())
    console.print(f"[green]✓[/green] flow-speckit.toml {config_status}")
    console.print("[green]✓[/green] database ready")
    console.print(f"[green]✓[/green] {type_count} artifact type(s) registered")


async def _sync_registry(url: str) -> None:
    engine = create_engine(url)
    try:
        async with session_factory(engine)() as session:
            await registry.sync_to_db(session)
    finally:
        await engine.dispose()
