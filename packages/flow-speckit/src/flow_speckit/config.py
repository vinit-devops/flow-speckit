from __future__ import annotations

import tomllib
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class FlowSpeckitSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FLOW_SPECKIT_")

    database_url: str | None = None

    @classmethod
    def load(cls, root: Path | None = None) -> FlowSpeckitSettings:
        root = root or Path.cwd()
        toml_values: dict[str, str] = {}
        config_path = root / "flow-speckit.toml"
        if config_path.exists():
            data = tomllib.loads(config_path.read_text())
            url = data.get("database", {}).get("url")
            if url:
                toml_values["database_url"] = url
        env_only = cls()
        if env_only.database_url is not None:
            return env_only
        return cls(**toml_values)


def resolve_database_url(settings: FlowSpeckitSettings, root: Path) -> str:
    if settings.database_url:
        return settings.database_url
    try:
        import pgserver
    except ImportError as exc:
        raise RuntimeError(
            "No database configured. Set FLOW_SPECKIT_DATABASE_URL / flow-speckit.toml "
            "[database].url, or install the embedded server: "
            "pip install 'flow-speckit[embedded-pg]'"
        ) from exc
    datadir = root / ".flow-speckit" / "pg"
    datadir.parent.mkdir(parents=True, exist_ok=True)
    return str(pgserver.get_server(datadir).get_uri())  # type: ignore[attr-defined]
