from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict


class FlowSpeckitSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FLOW_SPECKIT_")

    database_url: str | None = None
    llm_tiers: dict[str, str] = {}
    llm_tier_overrides: dict[str, str] = {}
    llm_budget_max_usd_per_run: float = 25.0
    execution_backend: str = "local_shell"
    execution_command: str | None = None

    @classmethod
    def load(cls, root: Path | None = None) -> FlowSpeckitSettings:
        root = root or Path.cwd()
        toml_values: dict[str, Any] = {}
        config_path = root / "flow-speckit.toml"
        if config_path.exists():
            try:
                data = tomllib.loads(config_path.read_text())
            except tomllib.TOMLDecodeError as exc:
                raise ValueError(
                    f"invalid TOML in config file {config_path}: {exc}"
                ) from exc
            # database
            url = data.get("database", {}).get("url")
            if url:
                toml_values["database_url"] = url
            # llm (doc 06 §2)
            llm_cfg = data.get("llm", {})
            if "tiers" in llm_cfg:
                toml_values["llm_tiers"] = dict(llm_cfg["tiers"])
            if "tiers" in llm_cfg and "overrides" in llm_cfg.get("tiers", {}):
                toml_values["llm_tier_overrides"] = dict(llm_cfg["tiers"]["overrides"])
            budget = llm_cfg.get("budget", {})
            if "default_max_usd_per_run" in budget:
                toml_values["llm_budget_max_usd_per_run"] = float(
                    budget["default_max_usd_per_run"]
                )
            # execution
            exec_cfg = data.get("execution", {})
            if "backend" in exec_cfg:
                toml_values["execution_backend"] = exec_cfg["backend"]
            if "local_shell" in exec_cfg and "command" in exec_cfg["local_shell"]:
                toml_values["execution_command"] = exec_cfg["local_shell"]["command"]
        # Per-field precedence: env wins for any field it sets, toml fills the
        # rest. Init kwargs outrank env vars in pydantic-settings, so only
        # pass toml values for fields the environment did NOT set.
        env_set_fields = cls().model_fields_set
        return cls(
            **{k: v for k, v in toml_values.items() if k not in env_set_fields}
        )


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
