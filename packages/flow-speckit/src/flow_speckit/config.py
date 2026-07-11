from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LLMConfig(BaseModel):
    """Nested ``[llm]`` section (doc 06 §2)."""

    tiers: dict[str, str] = Field(default_factory=dict)
    overrides: dict[str, str] = Field(default_factory=dict)
    default_max_usd_per_run: float = 25.0


class ExecutionConfig(BaseModel):
    """Nested ``[execution]`` section (doc 05)."""

    backend: str = "local_shell"
    command: str | None = None


class FlowSpeckitSettings(BaseSettings):
    # Nested fields are env-settable via a double underscore, e.g.
    # FLOW_SPECKIT_LLM__TIERS='{"fast": "..."}'.
    model_config = SettingsConfigDict(
        env_prefix="FLOW_SPECKIT_", env_nested_delimiter="__"
    )

    database_url: str | None = None
    llm: LLMConfig = Field(default_factory=LLMConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)

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
            db_url = data.get("database", {}).get("url")
            if db_url:
                toml_values["database_url"] = db_url

            # llm (doc 06 §2) — parsed as a nested model so [llm.tiers.overrides]
            # stays structurally separate from the tier map.
            llm_raw = data.get("llm", {})
            if llm_raw:
                tiers = dict(llm_raw.get("tiers", {}))
                overrides = dict(llm_raw.get("overrides", {}))
                budget = llm_raw.get("budget", {})
                default_max = float(budget.get("default_max_usd_per_run", 25.0))
                llm_inline: dict[str, Any] = {}
                llm_inline["tiers"] = tiers
                llm_inline["overrides"] = overrides
                llm_inline["default_max_usd_per_run"] = default_max
                toml_values["llm"] = llm_inline

            # execution
            exec_raw = data.get("execution", {})
            if exec_raw:
                exec_inline: dict[str, Any] = {}
                if "backend" in exec_raw:
                    exec_inline["backend"] = exec_raw["backend"]
                if "local_shell" in exec_raw and "command" in exec_raw["local_shell"]:
                    exec_inline["command"] = exec_raw["local_shell"]["command"]
                if exec_inline:
                    toml_values["execution"] = exec_inline

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
