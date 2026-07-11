"""``flow-speckit`` CLI entrypoint and doctor command (doc 07 §1)."""

import asyncio
import platform
from pathlib import Path

import typer

import flow_speckit
from flow_speckit.cli import (
    artifacts_cmd,
    backends_cmd,
    gates_cmd,
    init_cmd,
    run_cmd,
    runs_cmd,
    skills_cmd,
)
from flow_speckit.config import FlowSpeckitSettings, resolve_database_url

app = typer.Typer(name="flow-speckit", no_args_is_help=True, add_completion=False)
app.command("init")(init_cmd.init)
app.command("run")(run_cmd.run)
app.command("resume")(run_cmd.resume)
app.add_typer(artifacts_cmd.artifacts_app, name="artifacts")
app.add_typer(runs_cmd.runs_app, name="runs")
app.add_typer(gates_cmd.gates_app, name="gates")
app.add_typer(skills_cmd.skills_app, name="skills")
app.add_typer(backends_cmd.backends_app, name="backends")


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"flow-speckit {flow_speckit.__version__}")
        raise typer.Exit()


@app.callback()
def root(
    version: bool = typer.Option(
        False, "--version", callback=_version_callback, is_eager=True
    ),
) -> None:
    """Durable, artifact-driven AI-SDLC workflow orchestration."""


@app.command("doctor")
def doctor() -> None:
    """Check database reachable, backends available (doc 07 §1)."""

    async def _run() -> None:
        root = Path.cwd()
        typer.echo(f"flow-speckit {flow_speckit.__version__}")
        typer.echo(f"Python    {platform.python_version()}")
        typer.echo(f"Repo root {root}")
        typer.echo("")

        # Config
        try:
            settings = FlowSpeckitSettings.load(root=root)
        except Exception as exc:
            typer.echo(f"✗ Config: {exc}")
            raise typer.Exit(1) from None

        config_path = root / "flow-speckit.toml"
        if config_path.exists():
            typer.echo(f"✓ Config: {config_path}")
        else:
            typer.echo("✓ Config: defaults (no flow-speckit.toml)")

        try:
            db_url = resolve_database_url(settings, root)
        except Exception as exc:
            typer.echo(f"✗ No database URL configured: {exc}")
            raise typer.Exit(1) from None

        # Database
        try:
            from sqlalchemy import text

            from flow_speckit.storage.db import create_engine

            engine = create_engine(db_url)
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            await engine.dispose()
            typer.echo("✓ Database reachable")
        except Exception as exc:
            typer.echo(f"✗ Database unreachable: {exc}")
            raise typer.Exit(1) from None

        # Backends
        from flow_speckit.execution.backend_registry import BackendRegistry

        registry = BackendRegistry()
        registry.discover()
        for backend in registry.list_all():
            try:
                health = await backend.check_available()
                if health.available:
                    typer.echo(
                        f"✓ Backend: {backend.name} ({health.version or 'unknown'})"
                    )
                else:
                    typer.echo(f"✗ Backend: {backend.name} — {health.message}")
            except Exception as exc:
                typer.echo(f"✗ Backend: {backend.name} — check failed: {exc}")

        typer.echo("")
        typer.echo("Run `flow-speckit doctor` anytime to re-check.")

    asyncio.run(_run())


def main() -> None:
    app()
