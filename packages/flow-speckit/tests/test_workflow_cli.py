"""CLI tests for `run` / `resume` / `runs` / `gates` (doc 07 §§1-2).

Every test is non-interactive: only the --detach driving path is exercised
(single-process execute_run passes with inline timer firing). The ATTACHED
streaming loop (run_inline worker + scheduler + event polling) is deliberately
NOT tested here — it wants real wall-clock scheduler ticks and a LISTEN
connection, which would make the suite slow/flaky; its building blocks
(worker, scheduler, queue) are covered by test_queue/test_timers/test_gates.

The demo workflow uses only intrinsics + a gate + a zero-duration durable
sleep — the v0.1 demoable surface (no skill/execute/open_pr handlers exist
yet). The gate references a pre-created artifact whose UUID is passed via
--input (ctx.gate accepts a bare UUID), which keeps the fixture honest
without wiring artifact creation through a workflow step.
"""

from __future__ import annotations

import re
import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from typer.testing import CliRunner

from flow_speckit.artifacts.models import GenericArtifact
from flow_speckit.artifacts.refs import ArtifactRef
from flow_speckit.artifacts.registry import ArtifactRegistry
from flow_speckit.artifacts.store import ArtifactStore
from flow_speckit.cli.app import app

DEMO_WORKFLOW = '''
from __future__ import annotations

from datetime import timedelta
from uuid import UUID

from flow_speckit.workflows import workflow


@workflow(name="demo", version="1")
async def demo(ctx, artifact_id: str):  # noqa: ANN001
    await ctx.now()
    await ctx.gate("demo_gate", artifact=UUID(artifact_id), approvers=["user:tester"])
    await ctx.sleep("nap", timedelta(seconds=0))
    return None
'''

RUN_ID_RE = re.compile(
    r"run ([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) started"
)


@pytest.fixture()
def workflow_project(tmp_path, monkeypatch, migrated_url):  # type: ignore[no-untyped-def]
    """A cwd with ./workflows/demo.py, DB env set, and wide console output.

    COLUMNS keeps Rich from truncating/wrapping UUIDs inside tables and
    command hints, so substring assertions stay honest.
    """
    monkeypatch.setenv("FLOW_SPECKIT_DATABASE_URL", migrated_url)
    monkeypatch.setenv("COLUMNS", "300")
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    (workflows_dir / "demo.py").write_text(DEMO_WORKFLOW)
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture()
async def gated_artifact(session: AsyncSession) -> ArtifactRef:
    """A 'proposed' artifact whose UUID the demo workflow gates on."""
    reg = ArtifactRegistry()
    reg.register(GenericArtifact, source_package="flow-speckit")
    store = ArtifactStore(session, reg)
    return await store.create(
        GenericArtifact(title="Gated", body="please review me"),
        key="cli/gated",
    )


def _start_detached(artifact_id: str) -> tuple[str, object]:
    """Start a demo run with --detach; return (run_id, result)."""
    result = CliRunner().invoke(
        app, ["run", "demo", "--input", f"artifact_id={artifact_id}", "--detach"]
    )
    match = RUN_ID_RE.search(result.output)
    assert match is not None, result.output
    return match.group(1), result


def test_run_detach_parks_at_gate_and_prints_approve_command(  # type: ignore[no-untyped-def]
    workflow_project, gated_artifact
) -> None:
    run_id, result = _start_detached(str(gated_artifact.id))
    assert result.exit_code == 3, result.output
    assert "waiting on a gate" in result.output
    assert f"flow-speckit gates approve {run_id} demo_gate" in result.output
    assert f"flow-speckit gates reject {run_id} demo_gate" in result.output


def test_gates_list_shows_waiting_gate(workflow_project, gated_artifact) -> None:  # type: ignore[no-untyped-def]
    run_id, _ = _start_detached(str(gated_artifact.id))
    listed = CliRunner().invoke(app, ["gates", "list"])
    assert listed.exit_code == 0, listed.output
    assert run_id in listed.output
    assert "demo_gate" in listed.output
    assert "demo@1" in listed.output
    assert "user:tester" in listed.output


def test_approve_then_resume_detach_completes(  # type: ignore[no-untyped-def]
    workflow_project, gated_artifact
) -> None:
    run_id, _ = _start_detached(str(gated_artifact.id))

    approved = CliRunner().invoke(
        app, ["gates", "approve", run_id, "demo_gate", "--comment", "LGTM"]
    )
    assert approved.exit_code == 0, approved.output
    assert "approved" in approved.output
    # The artifact side effect is reported (gate approval mirrors the status).
    assert str(gated_artifact.id) in approved.output

    resumed = CliRunner().invoke(app, ["resume", run_id, "--detach"])
    assert resumed.exit_code == 0, resumed.output
    assert "completed" in resumed.output

    shown = CliRunner().invoke(app, ["runs", "show", run_id])
    assert shown.exit_code == 0, shown.output
    assert "completed" in shown.output
    assert "demo@1" in shown.output

    events = CliRunner().invoke(app, ["runs", "show", run_id, "--events"])
    assert events.exit_code == 0, events.output
    for expected in (
        "run_started",
        "gate_opened",
        "gate_resolved",
        "run_completed",
        "demo_gate",
        "'LGTM'",
    ):
        assert expected in events.output, events.output


def test_auto_approve_detach_completes_without_stopping(  # type: ignore[no-untyped-def]
    workflow_project, gated_artifact
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "run",
            "demo",
            "--input",
            f"artifact_id={gated_artifact.id}",
            "--auto-approve",
            "--detach",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "completed" in result.output
    assert "auto-approve" in result.output  # loud warning


def test_runs_list_renders_run(workflow_project, gated_artifact) -> None:  # type: ignore[no-untyped-def]
    run_id, _ = _start_detached(str(gated_artifact.id))
    listed = CliRunner().invoke(app, ["runs", "list"])
    assert listed.exit_code == 0, listed.output
    assert run_id in listed.output
    assert "demo@1" in listed.output
    assert "waiting_gate" in listed.output


def test_cancel_then_second_cancel_errors(workflow_project, gated_artifact) -> None:  # type: ignore[no-untyped-def]
    run_id, _ = _start_detached(str(gated_artifact.id))
    first = CliRunner().invoke(app, ["runs", "cancel", run_id, "--reason", "nope"])
    assert first.exit_code == 0, first.output
    assert "cancelled" in first.output
    second = CliRunner().invoke(app, ["runs", "cancel", run_id])
    assert second.exit_code == 1, second.output
    assert "Traceback" not in second.output


def test_reject_requires_comment(workflow_project, gated_artifact) -> None:  # type: ignore[no-untyped-def]
    run_id, _ = _start_detached(str(gated_artifact.id))
    missing = CliRunner().invoke(app, ["gates", "reject", run_id, "demo_gate"])
    assert missing.exit_code != 0
    rejected = CliRunner().invoke(
        app, ["gates", "reject", run_id, "demo_gate", "--comment", "needs work"]
    )
    assert rejected.exit_code == 0, rejected.output
    assert "rejected" in rejected.output


def test_approve_without_open_gate_errors_cleanly(  # type: ignore[no-untyped-def]
    workflow_project,
) -> None:
    bogus = str(uuid.uuid4())
    result = CliRunner().invoke(
        app, ["gates", "approve", bogus, "demo_gate", "--comment", "x"]
    )
    assert result.exit_code == 1, result.output
    assert "no open gate" in result.output
    assert "Traceback" not in result.output


def test_unknown_run_id_errors_not_traceback(workflow_project) -> None:  # type: ignore[no-untyped-def]
    bogus = str(uuid.uuid4())
    shown = CliRunner().invoke(app, ["runs", "show", bogus])
    assert shown.exit_code == 1, shown.output
    assert "no workflow run" in shown.output
    assert "Traceback" not in shown.output
    resumed = CliRunner().invoke(app, ["resume", bogus, "--detach"])
    assert resumed.exit_code == 1, resumed.output
    assert "no workflow run" in resumed.output
    invalid = CliRunner().invoke(app, ["runs", "show", "not-a-uuid"])
    assert invalid.exit_code == 1, invalid.output
    assert "invalid run id" in invalid.output


def test_resume_terminal_run_errors(workflow_project, gated_artifact) -> None:  # type: ignore[no-untyped-def]
    run_id, _ = _start_detached(str(gated_artifact.id))
    CliRunner().invoke(app, ["gates", "approve", run_id, "demo_gate"])
    done = CliRunner().invoke(app, ["resume", run_id, "--detach"])
    assert done.exit_code == 0, done.output
    again = CliRunner().invoke(app, ["resume", run_id, "--detach"])
    assert again.exit_code == 1, again.output
    assert "terminal" in again.output


def test_unknown_workflow_lists_known(workflow_project) -> None:  # type: ignore[no-untyped-def]
    result = CliRunner().invoke(app, ["run", "does-not-exist", "--detach"])
    assert result.exit_code == 1, result.output
    assert "unknown workflow" in result.output
    assert "demo" in result.output  # known workflows are listed
    assert "Traceback" not in result.output
