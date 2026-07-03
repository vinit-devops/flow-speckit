from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from typer.testing import CliRunner

from flow_speckit.artifacts.models import GenericArtifact
from flow_speckit.artifacts.refs import ArtifactRef
from flow_speckit.artifacts.registry import ArtifactRegistry, registry
from flow_speckit.artifacts.store import ArtifactStore
from flow_speckit.cli.app import app


@pytest.fixture()
async def seeded_artifact(session: AsyncSession) -> ArtifactRef:
    reg = ArtifactRegistry()
    reg.register(GenericArtifact, source_package="flow-speckit")
    store = ArtifactStore(session, reg)
    return await store.create(
        GenericArtifact(title="Seed", body="hello from the CLI test suite"),
        key="cli/seed",
    )


def test_init_idempotent(tmp_path, monkeypatch, migrated_url) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("FLOW_SPECKIT_DATABASE_URL", migrated_url)
    monkeypatch.chdir(tmp_path)
    r1 = CliRunner().invoke(app, ["init"])
    assert r1.exit_code == 0 and (tmp_path / "flow-speckit.toml").exists()
    r2 = CliRunner().invoke(app, ["init"])
    assert r2.exit_code == 0


def test_artifacts_roundtrip(tmp_path, monkeypatch, migrated_url, seeded_artifact) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("FLOW_SPECKIT_DATABASE_URL", migrated_url)
    monkeypatch.chdir(tmp_path)
    out = CliRunner().invoke(app, ["artifacts", "list"])
    assert seeded_artifact.address.split("@")[0] in out.output
    show = CliRunner().invoke(app, ["artifacts", "show", seeded_artifact.address])
    assert show.exit_code == 0


def test_artifacts_show_missing_exits_nonzero(tmp_path, monkeypatch, migrated_url) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("FLOW_SPECKIT_DATABASE_URL", migrated_url)
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(app, ["artifacts", "show", "does/not-exist"])
    assert result.exit_code == 1


def test_artifacts_versions_and_diff(tmp_path, monkeypatch, migrated_url, seeded_artifact) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("FLOW_SPECKIT_DATABASE_URL", migrated_url)
    monkeypatch.chdir(tmp_path)
    versions = CliRunner().invoke(app, ["artifacts", "versions", seeded_artifact.key])
    assert versions.exit_code == 0
    assert seeded_artifact.address in versions.output
    diff = CliRunner().invoke(
        app, ["artifacts", "diff", seeded_artifact.address, seeded_artifact.address]
    )
    assert diff.exit_code == 0


def test_show_prints_stored_body_md_not_rerender(  # type: ignore[no-untyped-def]
    tmp_path, monkeypatch, migrated_url, seeded_artifact
) -> None:
    # If render_md() changes AFTER the artifact was written, `show` must still
    # print the body_md captured at write time (the stored column is canonical).
    monkeypatch.setenv("FLOW_SPECKIT_DATABASE_URL", migrated_url)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        GenericArtifact, "render_md", lambda self: "MONKEYPATCHED-RENDER"
    )
    result = CliRunner().invoke(app, ["artifacts", "show", seeded_artifact.address])
    assert result.exit_code == 0
    assert "hello from the CLI test suite" in result.output
    assert "MONKEYPATCHED-RENDER" not in result.output


def test_list_entry_point_failure_exits_cleanly(  # type: ignore[no-untyped-def]
    tmp_path, monkeypatch, migrated_url
) -> None:
    monkeypatch.setenv("FLOW_SPECKIT_DATABASE_URL", migrated_url)
    monkeypatch.chdir(tmp_path)

    def boom() -> None:
        raise RuntimeError("entry point exploded")

    monkeypatch.setattr(registry, "load_entry_points", boom)
    result = CliRunner().invoke(app, ["artifacts", "list"])
    assert result.exit_code == 1
    assert "Traceback" not in result.output
    assert result.exception is None or isinstance(result.exception, SystemExit)


def test_init_entry_point_failure_exits_cleanly(  # type: ignore[no-untyped-def]
    tmp_path, monkeypatch, migrated_url
) -> None:
    monkeypatch.setenv("FLOW_SPECKIT_DATABASE_URL", migrated_url)
    monkeypatch.chdir(tmp_path)

    def boom() -> None:
        raise RuntimeError("entry point exploded")

    monkeypatch.setattr(registry, "load_entry_points", boom)
    result = CliRunner().invoke(app, ["init"])
    assert result.exit_code == 1
    assert "Traceback" not in result.output
    assert result.exception is None or isinstance(result.exception, SystemExit)
