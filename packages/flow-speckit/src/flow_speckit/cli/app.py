import typer

import flow_speckit
from flow_speckit.cli import artifacts_cmd, init_cmd

app = typer.Typer(name="flow-speckit", no_args_is_help=True, add_completion=False)
app.command("init")(init_cmd.init)
app.add_typer(artifacts_cmd.artifacts_app, name="artifacts")


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
