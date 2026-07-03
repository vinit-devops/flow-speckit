from pathlib import Path

from flow_speckit.config import FlowSpeckitSettings, resolve_database_url


def test_load_from_toml(tmp_path: Path) -> None:
    (tmp_path / "flow-speckit.toml").write_text('[database]\nurl = "postgresql://x/db"\n')
    settings = FlowSpeckitSettings.load(tmp_path)
    assert settings.database_url == "postgresql://x/db"


def test_env_overrides_toml(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    (tmp_path / "flow-speckit.toml").write_text('[database]\nurl = "postgresql://toml/db"\n')
    monkeypatch.setenv("FLOW_SPECKIT_DATABASE_URL", "postgresql://env/db")
    assert FlowSpeckitSettings.load(tmp_path).database_url == "postgresql://env/db"


def test_missing_toml_is_fine(tmp_path: Path) -> None:
    assert FlowSpeckitSettings.load(tmp_path).database_url is None


def test_resolve_explicit_url(tmp_path: Path) -> None:
    settings = FlowSpeckitSettings(database_url="postgresql://x/db")
    assert resolve_database_url(settings, tmp_path) == "postgresql://x/db"
