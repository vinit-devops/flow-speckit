import typer

import flow_speckit
from flow_speckit.cli import artifacts_cmd, gates_cmd, init_cmd, run_cmd, runs_cmd

app = typer.Typer(name="flow-speckit", no_args_is_help=True, add_completion=False)
app.command("init")(init_cmd.init)
app.command("run")(run_cmd.run)
app.command("resume")(run_cmd.resume)
app.add_typer(artifacts_cmd.artifacts_app, name="artifacts")
app.add_typer(runs_cmd.runs_app, name="runs")
app.add_typer(gates_cmd.gates_app, name="gates")


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"flow-speckit {flow_speckit.__version__}")
        raise typer.Exit()


@app.callback()
def root(
    version: bool = typer.Option(False, "--version", callback=_version_callback, is_eager=True),
) -> None:
    """Durable, artifact-driven AI-SDLC workflow orchestration."""


def main() -> None:
    app()
