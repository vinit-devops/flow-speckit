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
    version: bool = typer.Option(False, "--version", callback=_version_callback, is_eager=True),
) -> None:
    """Durable, artifact-driven AI-SDLC workflow orchestration."""


@app.command("doctor")
def doctor() -> None:
    """Check database reachable, backends available (doc 07 §1)."""
    from flow_speckit.cli.init_cmd import (
        get_config_path,
        load_settings,
        setup_database,
    )

    config_path = get_config_path()
    console = typer.colors
    try:
        settings = load_settings()
    except Exception as exc:
        typer.echo(f"✗ Config error: {exc}")
        raise typer.Exit(1)

    typer.echo(f"✓ Config: {config_path}")

    # Database check
    from flow_speckit.storage.db import create_engine, session_factory

    try:
        engine = create_engine(settings.database_url)
        typer.echo(f"✓ Database: {settings.database_url}")
    except Exception as exc:
        typer.echo(f"✗ Database unreachable: {exc}")
        raise typer.Exit(1)

    # Backend check
    from flow_speckit.execution.local_shell import LocalShellBackend

    try:
        import asyncio

        backend = LocalShellBackend()
        health = asyncio.get_event_loop().run_until_complete(backend.check_available())
        if health.available:
            typer.echo(f"✓ Backend available: {backend.name} ({health.version})")
        else:
            typer.echo(f"✗ Backend not available: {backend.name} — {health.message}")
    except Exception as exc:
        typer.echo(f"✗ Backend check failed: {exc}")

    typer.echo("\nRun `flow-speckit doctor` anytime to re-check.")


def main() -> None:
    app()
