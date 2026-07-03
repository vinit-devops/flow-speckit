from typer.testing import CliRunner

import flow_speckit
from flow_speckit.cli.app import app


def test_version_dunder() -> None:
    assert flow_speckit.__version__


def test_cli_version() -> None:
    result = CliRunner().invoke(app, ["--version"])
    assert result.exit_code == 0
    assert flow_speckit.__version__ in result.output
