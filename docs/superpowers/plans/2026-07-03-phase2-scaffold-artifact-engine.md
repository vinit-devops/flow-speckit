# Foundry Phase 2 — Workspace Scaffold + Artifact Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A working `foundry` Python package (uv workspace) whose Artifact Engine stores immutable, versioned, schema-validated artifacts with lineage edges, diff, FTS search, and a CLI (`foundry init`, `foundry artifacts …`) — per `docs/design/01-architecture-overview.md` and `docs/design/02-artifact-engine.md`.

**Architecture:** Monorepo uv workspace; kernel package `packages/foundry` (src layout). Postgres-only storage (ADR-0003): SQLAlchemy 2 async + asyncpg + Alembic; embedded `pgserver` for quickstart and tests. Artifact types are frozen Pydantic v2 models registered in an `ArtifactRegistry` (entry points + explicit). `ArtifactStore` owns all writes; versioning is insert-only with automatic `supersedes`/`derived_from` edges.

**Tech Stack:** Python 3.11+, Pydantic v2, pydantic-settings, SQLAlchemy 2 (async), asyncpg, Alembic, pgserver, Typer, Rich, structlog, DeepDiff, pytest + pytest-asyncio, ruff, mypy, uv + hatchling.

## Global Constraints

- Python floor: `>=3.11`. Kernel is `mypy --strict` clean; `py.typed` shipped.
- All kernel I/O is `async def`; tests use `pytest-asyncio` in `asyncio_mode = "auto"`.
- Postgres is the only datastore (ADR-0003). Tests run against embedded `pgserver` — no Docker, no mocks of the DB.
- Artifacts are immutable: no `UPDATE` of `content` ever; new content ⇒ new version row (doc 02 §2).
- Statuses: `draft|proposed|approved|rejected|superseded`. Edge relations: `derived_from|supersedes|informs|implements|reviews`. Event vocabulary reserved for Phase 3 — do not invent tables beyond `artifacts`, `artifact_edges`, `artifact_types` here.
- Artifact addressing: `<key>@<version>`; bare `<key>` = latest non-rejected version (doc 02 §2).
- No placeholder code, no `pass` stubs, no commented-out code. Every commit passes `ruff check`, `mypy`, `pytest`.
- Work on branch `feature/phase2-artifact-engine` in an isolated worktree (superpowers:using-git-worktrees). Never commit to `main`.
- Commit messages: conventional (`feat:`, `test:`, `chore:`); NO Co-Authored-By trailers (user rule).

## File Structure (locked by this plan)

```
pyproject.toml                                  # uv workspace root + shared tool config
.github/workflows/ci.yaml
packages/foundry/
├── pyproject.toml                              # deps, extras, scripts, entry points
├── src/foundry/
│   ├── __init__.py                             # __version__
│   ├── py.typed
│   ├── config.py                               # FoundrySettings, resolve_database_url
│   ├── artifacts/
│   │   ├── __init__.py                         # public re-exports
│   │   ├── models.py                           # ArtifactModel, GenericArtifact, Status, Relation
│   │   ├── hashing.py                          # canonical_hash
│   │   ├── registry.py                         # ArtifactRegistry, entry-point scan, DB sync
│   │   ├── refs.py                             # ArtifactRef, parse_ref
│   │   ├── store.py                            # ArtifactStore (create/get/versions/status/lineage/diff/search)
│   │   ├── graph.py                            # lineage recursive CTE → LineageGraph
│   │   └── diff.py                             # ArtifactDiff (structured + text)
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── db.py                               # engine/session factories, to_async_url
│   │   ├── schema.py                           # MetaData + 3 tables
│   │   ├── migrate.py                          # run_migrations(url)
│   │   └── migrations/                         # alembic env + versions/0001, 0002
│   └── cli/
│       ├── __init__.py
│       ├── app.py                              # typer root: --version, init; mounts artifacts
│       ├── init_cmd.py
│       └── artifacts_cmd.py                    # list/show/versions/diff
└── tests/
    ├── conftest.py                             # pgserver session fixture, migrated engine, clean session
    ├── test_scaffold.py
    ├── test_config.py
    ├── test_schema_migrations.py
    ├── test_models_registry.py
    ├── test_store_versioning.py
    ├── test_lineage.py
    ├── test_status.py
    ├── test_diff.py
    ├── test_search.py
    └── test_cli.py
```

---

### Task 1: uv workspace scaffold + CLI `--version` + CI

**Files:**
- Create: `pyproject.toml` (root), `packages/foundry/pyproject.toml`, `packages/foundry/src/foundry/__init__.py`, `packages/foundry/src/foundry/py.typed`, `packages/foundry/src/foundry/cli/__init__.py`, `packages/foundry/src/foundry/cli/app.py`, `packages/foundry/tests/test_scaffold.py`, `.github/workflows/ci.yaml`

**Interfaces:**
- Produces: `foundry.__version__: str`; console script `foundry` → `foundry.cli.app:main`; dev commands `uv sync --all-extras`, `uv run pytest`, `uv run ruff check .`, `uv run mypy packages/foundry/src`.

- [ ] **Step 1: Root `pyproject.toml`**

```toml
[tool.uv.workspace]
members = ["packages/*"]

[tool.ruff]
target-version = "py311"
line-length = 100
src = ["packages/foundry/src"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "RUF"]

[tool.mypy]
strict = true
python_version = "3.11"
mypy_path = "packages/foundry/src"

[[tool.mypy.overrides]]
module = ["pgserver", "deepdiff", "deepdiff.*"]
ignore_missing_imports = true

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["packages/foundry/tests"]
```

- [ ] **Step 2: `packages/foundry/pyproject.toml`**

```toml
[project]
name = "foundry"
version = "0.1.0.dev0"
description = "Durable, artifact-driven AI-SDLC workflow orchestration"
requires-python = ">=3.11"
license = "Apache-2.0"
dependencies = [
  "pydantic>=2.7",
  "pydantic-settings>=2.2",
  "sqlalchemy[asyncio]>=2.0.30",
  "asyncpg>=0.29",
  "alembic>=1.13",
  "typer>=0.12",
  "rich>=13.7",
  "structlog>=24.1",
  "deepdiff>=7.0",
]

[project.optional-dependencies]
embedded-pg = ["pgserver>=0.1.4"]

[project.scripts]
foundry = "foundry.cli.app:main"

[project.entry-points."foundry.artifacts"]
generic = "foundry.artifacts.models:GenericArtifact"

[dependency-groups]
dev = ["pytest>=8.2", "pytest-asyncio>=0.23", "ruff>=0.4", "mypy>=1.10"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/foundry"]
```

- [ ] **Step 3: Package init + CLI**

`src/foundry/__init__.py`:
```python
__version__ = "0.1.0.dev0"
```

`src/foundry/cli/app.py`:
```python
import typer

import foundry

app = typer.Typer(name="foundry", no_args_is_help=True, add_completion=False)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"foundry {foundry.__version__}")
        raise typer.Exit()


@app.callback()
def root(
    version: bool = typer.Option(False, "--version", callback=_version_callback, is_eager=True),
) -> None:
    """Durable, artifact-driven AI-SDLC workflow orchestration."""


def main() -> None:
    app()
```

Create empty `src/foundry/py.typed` and `src/foundry/cli/__init__.py`.

- [ ] **Step 4: Failing test**

`tests/test_scaffold.py`:
```python
from typer.testing import CliRunner

import foundry
from foundry.cli.app import app


def test_version_dunder() -> None:
    assert foundry.__version__


def test_cli_version() -> None:
    result = CliRunner().invoke(app, ["--version"])
    assert result.exit_code == 0
    assert foundry.__version__ in result.output
```

- [ ] **Step 5: Run `uv sync --all-extras && uv run pytest packages/foundry/tests/test_scaffold.py -v`** — expect PASS (implementation is in step 3; if imports fail, fix before proceeding). Then `uv run ruff check . && uv run mypy packages/foundry/src` — expect clean.

- [ ] **Step 6: CI workflow** `.github/workflows/ci.yaml`:

```yaml
name: ci
on:
  push: { branches: [main] }
  pull_request:
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
        with: { python-version: "3.11" }
      - run: uv sync --all-extras
      - run: uv run ruff check .
      - run: uv run mypy packages/foundry/src
      - run: uv run pytest -q
```

- [ ] **Step 7: Commit** — `git add -A && git commit -m "feat: scaffold uv workspace, foundry package, CLI --version, CI"`

---

### Task 2: Configuration (`config.py`)

**Files:**
- Create: `packages/foundry/src/foundry/config.py`
- Test: `packages/foundry/tests/test_config.py`

**Interfaces:**
- Produces: `FoundrySettings(BaseSettings)` with `database_url: str | None`; `FoundrySettings.load(root: Path) -> FoundrySettings` (merges `foundry.toml`, env `FOUNDRY_*` wins); `resolve_database_url(settings: FoundrySettings, root: Path) -> str` (explicit URL, else embedded pgserver at `root/.foundry/pg`, else `RuntimeError` with install hint).

- [ ] **Step 1: Failing tests**

`tests/test_config.py`:
```python
from pathlib import Path

from foundry.config import FoundrySettings, resolve_database_url


def test_load_from_toml(tmp_path: Path) -> None:
    (tmp_path / "foundry.toml").write_text('[database]\nurl = "postgresql://x/db"\n')
    settings = FoundrySettings.load(tmp_path)
    assert settings.database_url == "postgresql://x/db"


def test_env_overrides_toml(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    (tmp_path / "foundry.toml").write_text('[database]\nurl = "postgresql://toml/db"\n')
    monkeypatch.setenv("FOUNDRY_DATABASE_URL", "postgresql://env/db")
    assert FoundrySettings.load(tmp_path).database_url == "postgresql://env/db"


def test_missing_toml_is_fine(tmp_path: Path) -> None:
    assert FoundrySettings.load(tmp_path).database_url is None


def test_resolve_explicit_url(tmp_path: Path) -> None:
    settings = FoundrySettings(database_url="postgresql://x/db")
    assert resolve_database_url(settings, tmp_path) == "postgresql://x/db"
```

- [ ] **Step 2: Run** `uv run pytest packages/foundry/tests/test_config.py -v` — expect FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement** `src/foundry/config.py`:

```python
from __future__ import annotations

import tomllib
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class FoundrySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FOUNDRY_")

    database_url: str | None = None

    @classmethod
    def load(cls, root: Path | None = None) -> FoundrySettings:
        root = root or Path.cwd()
        toml_values: dict[str, str] = {}
        config_path = root / "foundry.toml"
        if config_path.exists():
            data = tomllib.loads(config_path.read_text())
            url = data.get("database", {}).get("url")
            if url:
                toml_values["database_url"] = url
        env_only = cls()
        if env_only.database_url is not None:
            return env_only
        return cls(**toml_values)


def resolve_database_url(settings: FoundrySettings, root: Path) -> str:
    if settings.database_url:
        return settings.database_url
    try:
        import pgserver
    except ImportError as exc:
        raise RuntimeError(
            "No database configured. Set FOUNDRY_DATABASE_URL / foundry.toml [database].url, "
            "or install the embedded server: pip install 'foundry[embedded-pg]'"
        ) from exc
    datadir = root / ".foundry" / "pg"
    datadir.parent.mkdir(parents=True, exist_ok=True)
    return str(pgserver.get_server(datadir).get_uri())
```

- [ ] **Step 4: Run tests** — expect PASS. Run `ruff` + `mypy` — clean.
- [ ] **Step 5: Commit** — `git commit -m "feat: FoundrySettings config loading (toml + env) and database URL resolution"`

---

### Task 3: Storage schema, Alembic migration 0001, embedded-pg test fixtures

**Files:**
- Create: `src/foundry/storage/__init__.py`, `src/foundry/storage/db.py`, `src/foundry/storage/schema.py`, `src/foundry/storage/migrate.py`, `src/foundry/storage/migrations/env.py`, `src/foundry/storage/migrations/script.py.mako`, `src/foundry/storage/migrations/versions/0001_artifact_tables.py`
- Create: `packages/foundry/tests/conftest.py`
- Test: `packages/foundry/tests/test_schema_migrations.py`

**Interfaces:**
- Produces: `storage.schema.metadata`, tables `artifacts`, `artifact_edges`, `artifact_types` (columns exactly per doc 02 §2); `storage.db.to_async_url(url) -> str`, `storage.db.create_engine(url) -> AsyncEngine`, `storage.db.session_factory(engine) -> async_sessionmaker[AsyncSession]`; `storage.migrate.run_migrations(database_url: str) -> None`.
- Test fixtures for all later tasks: `pg_url` (session), `engine` (session, migrated), `session` (function-scoped `AsyncSession` on a truncated schema).

- [ ] **Step 1: `schema.py`**

```python
from __future__ import annotations

import uuid

from sqlalchemy import (
    CheckConstraint, Column, DateTime, ForeignKey, Integer, MetaData,
    PrimaryKeyConstraint, Table, Text, UniqueConstraint, func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = MetaData()

RELATIONS = ("derived_from", "supersedes", "informs", "implements", "reviews")
STATUSES = ("draft", "proposed", "approved", "rejected", "superseded")

artifacts = Table(
    "artifacts", metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("type", Text, nullable=False),
    Column("key", Text, nullable=False),
    Column("version", Integer, nullable=False),
    Column("content", JSONB, nullable=False),
    Column("content_hash", Text, nullable=False, index=True),
    Column("body_md", Text),
    Column("status", Text, nullable=False, server_default="proposed"),
    Column("schema_version", Integer, nullable=False),
    Column("created_by_run", UUID(as_uuid=True)),
    Column("created_by_step", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("key", "version", name="uq_artifacts_key_version"),
    CheckConstraint(f"status IN {STATUSES!r}".replace("[", "(").replace("]", ")"), name="ck_artifacts_status"),
)

artifact_edges = Table(
    "artifact_edges", metadata,
    Column("from_id", UUID(as_uuid=True), ForeignKey("artifacts.id"), nullable=False),
    Column("to_id", UUID(as_uuid=True), ForeignKey("artifacts.id"), nullable=False),
    Column("relation", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("from_id", "to_id", "relation", name="pk_artifact_edges"),
    CheckConstraint(f"relation IN {RELATIONS!r}".replace("[", "(").replace("]", ")"), name="ck_edges_relation"),
)

artifact_types = Table(
    "artifact_types", metadata,
    Column("name", Text, nullable=False),
    Column("schema_version", Integer, nullable=False),
    Column("json_schema", JSONB, nullable=False),
    Column("source_package", Text, nullable=False),
    Column("registered_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("name", "schema_version", name="pk_artifact_types"),
)
```

(Note: build the CHECK constraints with explicit SQL strings, e.g. `CheckConstraint("status IN ('draft','proposed','approved','rejected','superseded')")` — the `.replace` trick above is illustrative shorthand; write the literal string in code.)

- [ ] **Step 2: `db.py`**

```python
from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine,
)


def to_async_url(url: str) -> str:
    if url.startswith("postgresql+asyncpg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    raise ValueError(f"Unsupported database URL: {url!r}")


def create_engine(url: str) -> AsyncEngine:
    return create_async_engine(to_async_url(url), pool_pre_ping=True)


def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
```

- [ ] **Step 3: Alembic.** `migrate.py` runs programmatically (no alembic.ini):

```python
from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def alembic_config(database_url: str) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    cfg.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    return cfg


def run_migrations(database_url: str) -> None:
    command.upgrade(alembic_config(database_url), "head")
```

`migrations/env.py` (async template, offline mode unsupported):

```python
import asyncio

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

from foundry.storage.db import to_async_url
from foundry.storage.schema import metadata


def do_run_migrations(connection) -> None:  # type: ignore[no-untyped-def]
    context.configure(connection=connection, target_metadata=metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    engine = create_async_engine(to_async_url(context.config.get_main_option("sqlalchemy.url")))
    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)
        await connection.commit()
    await engine.dispose()


asyncio.run(run_async_migrations())
```

`migrations/versions/0001_artifact_tables.py`: `revision = "0001"`, `down_revision = None`; `upgrade()` = `op.create_table(...)` for the three tables exactly matching `schema.py` (copy column definitions; include the unique/check/PK constraints and the `content_hash` index); `downgrade()` drops them in reverse order. Copy `script.py.mako` from alembic's async template.

- [ ] **Step 4: Test fixtures.** `tests/conftest.py`:

```python
from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pgserver
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from foundry.storage.db import create_engine, session_factory
from foundry.storage.migrate import run_migrations


@pytest.fixture(scope="session")
def pg_url(tmp_path_factory: pytest.TempPathFactory) -> str:
    datadir = tmp_path_factory.mktemp("pg") / "data"
    server = pgserver.get_server(datadir)
    return str(server.get_uri())


@pytest.fixture(scope="session")
def migrated_url(pg_url: str) -> str:
    run_migrations(pg_url)
    return pg_url


@pytest.fixture()
async def engine(migrated_url: str) -> AsyncIterator[AsyncEngine]:
    eng = create_engine(migrated_url)
    yield eng
    await eng.dispose()


@pytest.fixture()
async def session(engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    async with engine.connect() as conn:
        await conn.execute(text("TRUNCATE artifact_edges, artifacts, artifact_types CASCADE"))
        await conn.commit()
    async with session_factory(engine)() as s:
        yield s
```

- [ ] **Step 5: Failing test** `tests/test_schema_migrations.py`:

```python
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def test_tables_exist(session: AsyncSession) -> None:
    rows = await session.execute(text(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    ))
    names = {r[0] for r in rows}
    assert {"artifacts", "artifact_edges", "artifact_types", "alembic_version"} <= names


async def test_key_version_unique(session: AsyncSession) -> None:
    import pytest
    from sqlalchemy.exc import IntegrityError

    insert = text(
        "INSERT INTO artifacts (id, type, key, version, content, content_hash, schema_version) "
        "VALUES (gen_random_uuid(), 'generic', 'k', 1, '{}', 'h', 1)"
    )
    await session.execute(insert)
    with pytest.raises(IntegrityError):
        await session.execute(insert)
```

- [ ] **Step 6: Run** `uv run pytest packages/foundry/tests/test_schema_migrations.py -v` — FAIL first (missing modules), then implement steps 1–4 fully and re-run — PASS. `ruff` + `mypy` clean. (pgserver is a test-time requirement: add it to the `dev` dependency group as well.)
- [ ] **Step 7: Commit** — `git commit -m "feat: postgres schema, alembic migration 0001, embedded-pg test fixtures"`

---

### Task 4: `ArtifactModel`, `GenericArtifact`, hashing, `ArtifactRegistry`

**Files:**
- Create: `src/foundry/artifacts/__init__.py`, `models.py`, `hashing.py`, `registry.py`
- Test: `tests/test_models_registry.py`

**Interfaces:**
- Produces:
  - `ArtifactModel(BaseModel)` — frozen; subclass kwargs `artifact_type: str`, `schema_version: int = 1` set `ClassVar`s `artifact_type` / `artifact_schema_version`; `render_md() -> str` default renderer.
  - `GenericArtifact(ArtifactModel, artifact_type="generic")` with `title: str`, `body: str = ""`, `metadata: dict[str, Any] = {}`.
  - `Status = Literal["draft","proposed","approved","rejected","superseded"]`, `Relation = Literal["derived_from","supersedes","informs","implements","reviews"]`.
  - `canonical_hash(content: Mapping[str, Any]) -> str` — sha256 of `json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False)`.
  - `ArtifactRegistry`: `.register(cls, source_package="local")` (local overrides installed with structlog warning; installed-vs-installed collision raises `RegistryCollisionError`), `.get(name, schema_version=None) -> type[ArtifactModel]` (raises `UnknownArtifactType`), `.load_entry_points()` (group `foundry.artifacts`), `.sync_to_db(session)` (upsert `artifact_types` rows with exported JSON Schema), `.all() -> list[RegisteredType]`. Module-level `registry = ArtifactRegistry()`.

- [ ] **Step 1: Failing tests** `tests/test_models_registry.py`:

```python
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from foundry.artifacts.hashing import canonical_hash
from foundry.artifacts.models import ArtifactModel, GenericArtifact
from foundry.artifacts.registry import (
    ArtifactRegistry, RegistryCollisionError, UnknownArtifactType,
)


class Memo(ArtifactModel, artifact_type="memo", schema_version=2):
    title: str
    points: list[str] = []


def test_classvars_set() -> None:
    assert Memo.artifact_type == "memo"
    assert Memo.artifact_schema_version == 2
    assert GenericArtifact.artifact_type == "generic"


def test_frozen() -> None:
    m = Memo(title="t")
    with pytest.raises(Exception):
        m.title = "changed"  # type: ignore[misc]


def test_canonical_hash_order_independent() -> None:
    assert canonical_hash({"a": 1, "b": 2}) == canonical_hash({"b": 2, "a": 1})
    assert canonical_hash({"a": 1}) != canonical_hash({"a": 2})


def test_generic_render_md() -> None:
    art = GenericArtifact(title="Hello", body="World")
    md = art.render_md()
    assert "# Hello" in md and "World" in md


def test_registry_register_get() -> None:
    reg = ArtifactRegistry()
    reg.register(Memo, source_package="pkg-a")
    assert reg.get("memo") is Memo
    with pytest.raises(UnknownArtifactType):
        reg.get("nope")


def test_registry_collision_rules() -> None:
    reg = ArtifactRegistry()

    class MemoA(ArtifactModel, artifact_type="memo"):
        title: str

    class MemoB(ArtifactModel, artifact_type="memo"):
        title: str

    reg.register(MemoA, source_package="pkg-a")
    with pytest.raises(RegistryCollisionError):
        reg.register(MemoB, source_package="pkg-b")
    reg.register(MemoB, source_package="local")  # local override allowed
    assert reg.get("memo") is MemoB


def test_entry_points_load_generic() -> None:
    reg = ArtifactRegistry()
    reg.load_entry_points()
    assert reg.get("generic") is GenericArtifact


async def test_sync_to_db(session: AsyncSession) -> None:
    reg = ArtifactRegistry()
    reg.register(Memo, source_package="pkg-a")
    await reg.sync_to_db(session)
    await reg.sync_to_db(session)  # idempotent upsert
    rows = (await session.execute(text("SELECT name, schema_version FROM artifact_types"))).all()
    assert ("memo", 2) in [tuple(r) for r in rows]
```

- [ ] **Step 2: Run — FAIL.** Then implement:

`hashing.py`:
```python
import hashlib
import json
from collections.abc import Mapping
from typing import Any


def canonical_hash(content: Mapping[str, Any]) -> str:
    payload = json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode()).hexdigest()
```

`models.py`:
```python
from __future__ import annotations

from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field

Status = Literal["draft", "proposed", "approved", "rejected", "superseded"]
Relation = Literal["derived_from", "supersedes", "informs", "implements", "reviews"]


class ArtifactModel(BaseModel):
    model_config = ConfigDict(frozen=True)

    artifact_type: ClassVar[str]
    artifact_schema_version: ClassVar[int] = 1

    def __init_subclass__(cls, *, artifact_type: str | None = None,
                          schema_version: int = 1, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if artifact_type is not None:
            cls.artifact_type = artifact_type
            cls.artifact_schema_version = schema_version

    def render_md(self) -> str:
        lines = [f"# {self.artifact_type}"]
        for name, value in self.model_dump(mode="json").items():
            lines.append(f"**{name}:** {value}")
        return "\n\n".join(lines)


class GenericArtifact(ArtifactModel, artifact_type="generic"):
    title: str
    body: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)

    def render_md(self) -> str:
        return f"# {self.title}\n\n{self.body}"
```

`registry.py` (essentials — `RegisteredType` dataclass `{cls, source_package}`; dict keyed by `artifact_type`; collision logic per test; `load_entry_points()` uses `importlib.metadata.entry_points(group="foundry.artifacts")`; `sync_to_db` uses `sqlalchemy.dialects.postgresql.insert(...).on_conflict_do_update` on `(name, schema_version)` writing `cls.model_json_schema()`).

`__init__.py` re-exports: `ArtifactModel`, `GenericArtifact`, `Status`, `Relation`, `canonical_hash`, `ArtifactRegistry`, `registry`.

- [ ] **Step 3: Run tests — PASS; `ruff` + `mypy` clean.**
- [ ] **Step 4: Commit** — `git commit -m "feat: ArtifactModel base, GenericArtifact, canonical hashing, ArtifactRegistry"`

---

### Task 5: `ArtifactStore` — create / get / versions (versioning, dedup, supersedes)

**Files:**
- Create: `src/foundry/artifacts/refs.py`, `src/foundry/artifacts/store.py`
- Test: `tests/test_store_versioning.py`

**Interfaces:**
- Produces:
  - `ArtifactRef(BaseModel)`: `id: UUID`, `type: str`, `key: str`, `version: int`, `status: Status`, `content_hash: str`, `created_at: datetime`; property `address -> str` (`f"{key}@{version}"`).
  - `parse_ref(ref: str) -> UUID | tuple[str, int | None]` — UUID string → UUID; `"key@3"` → `("key", 3)`; `"key"` → `("key", None)`.
  - `ArtifactStore(session: AsyncSession, registry: ArtifactRegistry)`:
    - `async create(model, *, key, run_id=None, step_key=None, derived_from: Sequence[UUID] = (), status: Status = "proposed") -> ArtifactRef` — validates type registered; **dedup**: if latest version of `key` has identical `content_hash`, return its ref unchanged; else insert `version = latest + 1`, add `supersedes` edge new→old, and set old row's status to `superseded` (status is the one mutable column — content never mutates).
    - `async get(ref: str | UUID) -> ArtifactModel` — bare key = latest non-rejected version; rehydrates via registry class. Raises `ArtifactNotFound`.
    - `async resolve(ref: str | UUID) -> ArtifactRef`.
    - `async versions(key: str) -> list[ArtifactRef]` (ascending).
- Consumes: Task 3 `session` fixture; Task 4 registry/models/hash.

- [ ] **Step 1: Failing tests** `tests/test_store_versioning.py`:

```python
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from foundry.artifacts.models import GenericArtifact
from foundry.artifacts.registry import ArtifactRegistry
from foundry.artifacts.store import ArtifactNotFound, ArtifactStore


@pytest.fixture()
def store(session: AsyncSession) -> ArtifactStore:
    reg = ArtifactRegistry()
    reg.register(GenericArtifact, source_package="foundry")
    return ArtifactStore(session, reg)


async def test_create_first_version(store: ArtifactStore) -> None:
    ref = await store.create(GenericArtifact(title="A"), key="notes/a")
    assert (ref.version, ref.status, ref.address) == (1, "proposed", "notes/a@1")


async def test_new_content_bumps_version_and_supersedes(store: ArtifactStore) -> None:
    r1 = await store.create(GenericArtifact(title="A"), key="notes/a")
    r2 = await store.create(GenericArtifact(title="A2"), key="notes/a")
    assert r2.version == 2
    assert (await store.resolve(r1.id)).status == "superseded"
    latest = await store.get("notes/a")
    assert isinstance(latest, GenericArtifact) and latest.title == "A2"


async def test_identical_content_dedups(store: ArtifactStore) -> None:
    r1 = await store.create(GenericArtifact(title="A"), key="notes/a")
    r2 = await store.create(GenericArtifact(title="A"), key="notes/a")
    assert (r2.id, r2.version) == (r1.id, 1)


async def test_get_by_address_and_uuid(store: ArtifactStore) -> None:
    r1 = await store.create(GenericArtifact(title="A"), key="notes/a")
    await store.create(GenericArtifact(title="B"), key="notes/a")
    assert isinstance(await store.get("notes/a@1"), GenericArtifact)
    assert (await store.get(r1.id)).title == "A"  # type: ignore[attr-defined]


async def test_versions_ascending(store: ArtifactStore) -> None:
    await store.create(GenericArtifact(title="A"), key="notes/a")
    await store.create(GenericArtifact(title="B"), key="notes/a")
    assert [r.version for r in await store.versions("notes/a")] == [1, 2]


async def test_get_missing_raises(store: ArtifactStore) -> None:
    with pytest.raises(ArtifactNotFound):
        await store.get("nope")
```

- [ ] **Step 2: Run — FAIL.** Implement `refs.py` + `store.py`. Core of `create`:

```python
async def create(self, model: ArtifactModel, *, key: str, run_id: UUID | None = None,
                 step_key: str | None = None, derived_from: Sequence[UUID] = (),
                 status: Status = "proposed") -> ArtifactRef:
    self._registry.get(model.artifact_type)  # must be registered
    content = model.model_dump(mode="json")
    digest = canonical_hash(content)
    latest = await self._latest_row(key, lock=True)   # SELECT ... FOR UPDATE
    if latest is not None and latest.content_hash == digest:
        return _row_to_ref(latest)
    version = 1 if latest is None else latest.version + 1
    row_id = uuid4()
    await self._session.execute(schema.artifacts.insert().values(
        id=row_id, type=model.artifact_type, key=key, version=version,
        content=content, content_hash=digest, body_md=model.render_md(),
        status=status, schema_version=model.artifact_schema_version,
        created_by_run=run_id, created_by_step=step_key,
    ))
    if latest is not None:
        await self._add_edge(row_id, latest.id, "supersedes")
        await self._set_status_raw(latest.id, "superseded")
    for parent in derived_from:
        await self._add_edge(row_id, parent, "derived_from")
    await self._session.commit()
    return await self.resolve(row_id)
```

`get` for bare key: `WHERE key = :key AND status != 'rejected' ORDER BY version DESC LIMIT 1`. `_add_edge` inserts with `on_conflict_do_nothing` (idempotent re-runs, doc 03 §4).

- [ ] **Step 3: Run tests — PASS; lint/type clean.**
- [ ] **Step 4: Commit** — `git commit -m "feat: ArtifactStore create/get/versions with dedup and supersedes edges"`

---

### Task 6: Lineage — `derived_from` wiring + recursive-CTE graph queries

**Files:**
- Create: `src/foundry/artifacts/graph.py`
- Modify: `src/foundry/artifacts/store.py` (add `lineage`)
- Test: `tests/test_lineage.py`

**Interfaces:**
- Produces: `LineageEdge(BaseModel)` `{from_id: UUID, to_id: UUID, relation: Relation}`; `LineageGraph(BaseModel)` `{root: UUID, nodes: list[ArtifactRef], edges: list[LineageEdge]}`; `ArtifactStore.lineage(ref, *, direction: Literal["up","down"] = "up", max_depth: int = 32) -> LineageGraph`. "up" follows `from_id → to_id` (ancestors/provenance); "down" the reverse (impact).

- [ ] **Step 1: Failing tests** `tests/test_lineage.py` (fixture `store` same as Task 5):

```python
async def test_lineage_up_walks_derived_from(store: ArtifactStore) -> None:
    brief = await store.create(GenericArtifact(title="brief"), key="brief")
    design = await store.create(GenericArtifact(title="design"), key="design",
                                derived_from=[brief.id])
    plan = await store.create(GenericArtifact(title="plan"), key="plan",
                              derived_from=[design.id])
    graph = await store.lineage(plan.id, direction="up")
    ids = {r.id for r in graph.nodes}
    assert {brief.id, design.id, plan.id} <= ids
    relations = {e.relation for e in graph.edges}
    assert relations == {"derived_from"}


async def test_lineage_down_finds_descendants(store: ArtifactStore) -> None:
    brief = await store.create(GenericArtifact(title="brief"), key="brief")
    design = await store.create(GenericArtifact(title="design"), key="design",
                                derived_from=[brief.id])
    graph = await store.lineage(brief.id, direction="down")
    assert design.id in {r.id for r in graph.nodes}


async def test_lineage_includes_supersedes(store: ArtifactStore) -> None:
    await store.create(GenericArtifact(title="v1"), key="doc")
    v2 = await store.create(GenericArtifact(title="v2"), key="doc")
    graph = await store.lineage(v2.id, direction="up")
    assert "supersedes" in {e.relation for e in graph.edges}


async def test_lineage_max_depth(store: ArtifactStore) -> None:
    prev = await store.create(GenericArtifact(title="0"), key="n/0")
    for i in range(1, 5):
        prev = await store.create(GenericArtifact(title=str(i)), key=f"n/{i}",
                                  derived_from=[prev.id])
    graph = await store.lineage(prev.id, direction="up", max_depth=2)
    assert len(graph.nodes) == 3  # root + 2 levels
```

- [ ] **Step 2: Run — FAIL.** Implement `graph.py` with the recursive CTE from doc 02 §5 (parameterized direction: `up` joins `e.from_id = l.to_id` starting at `from_id = :root`; `down` mirrors), then join `artifacts` for node refs; dedupe nodes.

- [ ] **Step 3: Run tests — PASS; lint/type clean.**
- [ ] **Step 4: Commit** — `git commit -m "feat: lineage graph queries via recursive CTE"`

---

### Task 7: Status transitions

**Files:**
- Modify: `src/foundry/artifacts/store.py` (add `set_status`, `InvalidStatusTransition`)
- Test: `tests/test_status.py`

**Interfaces:**
- Produces: `ArtifactStore.set_status(ref, status: Status, *, actor: str) -> ArtifactRef`. Allowed: `draft→proposed`, `proposed→approved`, `proposed→rejected`, and `{draft,proposed,approved}→superseded` (store-internal). All else raises `InvalidStatusTransition`. Actor is bound into a structlog event `artifact_status_changed` (run-event integration lands in Phase 3).

- [ ] **Step 1: Failing tests** `tests/test_status.py`:

```python
async def test_approve_flow(store: ArtifactStore) -> None:
    ref = await store.create(GenericArtifact(title="A"), key="a")
    out = await store.set_status(ref.id, "approved", actor="vinit")
    assert out.status == "approved"


async def test_reject_flow(store: ArtifactStore) -> None:
    ref = await store.create(GenericArtifact(title="A"), key="a")
    assert (await store.set_status(ref.id, "rejected", actor="vinit")).status == "rejected"


async def test_illegal_transitions_raise(store: ArtifactStore) -> None:
    ref = await store.create(GenericArtifact(title="A"), key="a")
    await store.set_status(ref.id, "approved", actor="vinit")
    with pytest.raises(InvalidStatusTransition):
        await store.set_status(ref.id, "proposed", actor="vinit")
    with pytest.raises(InvalidStatusTransition):
        await store.set_status(ref.id, "rejected", actor="vinit")


async def test_rejected_excluded_from_bare_key_get(store: ArtifactStore) -> None:
    r1 = await store.create(GenericArtifact(title="A"), key="a")
    await store.set_status(r1.id, "rejected", actor="vinit")
    with pytest.raises(ArtifactNotFound):
        await store.get("a")
```

- [ ] **Step 2: Run — FAIL. Implement** `set_status` with transition table
  `_ALLOWED = {("draft","proposed"), ("proposed","approved"), ("proposed","rejected"), ("draft","superseded"), ("proposed","superseded"), ("approved","superseded")}`; single `UPDATE artifacts SET status=...` guarded by current status check; structlog `artifact_status_changed` with `actor`, `artifact_id`, `from`, `to`.
- [ ] **Step 3: Run tests — PASS; lint/type clean.**
- [ ] **Step 4: Commit** — `git commit -m "feat: guarded artifact status transitions"`

---

### Task 8: Diff (structured + text)

**Files:**
- Create: `src/foundry/artifacts/diff.py`
- Modify: `src/foundry/artifacts/store.py` (add `diff`)
- Test: `tests/test_diff.py`

**Interfaces:**
- Produces: `ArtifactDiff(BaseModel)` `{a: str, b: str, structured: dict[str, Any], text: str}` (`a`/`b` are addresses); `ArtifactStore.diff(ref_a, ref_b) -> ArtifactDiff` — `structured` from `DeepDiff(content_a, content_b, ignore_order=False).to_dict()` made JSON-safe via its `to_json()` round-trip; `text` = `difflib.unified_diff` over `body_md` lines with addresses as file labels.

- [ ] **Step 1: Failing tests** `tests/test_diff.py`:

```python
async def test_diff_detects_field_change(store: ArtifactStore) -> None:
    await store.create(GenericArtifact(title="Old title"), key="doc")
    await store.create(GenericArtifact(title="New title"), key="doc")
    d = await store.diff("doc@1", "doc@2")
    assert d.a == "doc@1" and d.b == "doc@2"
    assert "values_changed" in d.structured
    assert "-# Old title" in d.text and "+# New title" in d.text


async def test_diff_identical_is_empty(store: ArtifactStore) -> None:
    ref = await store.create(GenericArtifact(title="Same"), key="doc")
    d = await store.diff(ref.id, ref.id)
    assert d.structured == {} and d.text == ""
```

- [ ] **Step 2: Run — FAIL. Implement** (`json.loads(DeepDiff(...).to_json())` for JSON-safe structured output).
- [ ] **Step 3: Run tests — PASS; lint/type clean.**
- [ ] **Step 4: Commit** — `git commit -m "feat: artifact diff (structured DeepDiff + unified text)"`

---

### Task 9: Full-text search

**Files:**
- Create: `src/foundry/storage/migrations/versions/0002_artifact_search.py`
- Modify: `src/foundry/artifacts/store.py` (add `search`)
- Test: `tests/test_search.py`

**Interfaces:**
- Produces: migration 0002 (`down_revision = "0001"`) adding generated column + GIN index:

```python
def upgrade() -> None:
    op.execute(
        "ALTER TABLE artifacts ADD COLUMN search_tsv tsvector "
        "GENERATED ALWAYS AS (to_tsvector('english', coalesce(body_md, '') || ' ' || (content::text))) STORED"
    )
    op.execute("CREATE INDEX ix_artifacts_search ON artifacts USING GIN (search_tsv)")
```

- `ArtifactStore.search(query: str, *, type: str | None = None, limit: int = 50) -> list[ArtifactRef]` — `WHERE search_tsv @@ plainto_tsquery('english', :q)` (+ optional `AND type = :type`), `ORDER BY ts_rank(search_tsv, plainto_tsquery('english', :q)) DESC`.

- [ ] **Step 1: Failing tests** `tests/test_search.py`:

```python
async def test_search_finds_body_text(store: ArtifactStore) -> None:
    await store.create(GenericArtifact(title="CSV export design", body="streaming exporter"), key="d/csv")
    await store.create(GenericArtifact(title="Auth refactor", body="oauth tokens"), key="d/auth")
    hits = await store.search("exporter")
    assert [h.key for h in hits] == ["d/csv"]


async def test_search_type_filter(store: ArtifactStore) -> None:
    await store.create(GenericArtifact(title="CSV export"), key="d/csv")
    assert await store.search("csv", type="nonexistent") == []
```

- [ ] **Step 2: Run — FAIL (column missing). Add migration 0002 + `search`; re-run — PASS.** Confirm fresh-DB migration chain works: fixtures already run `upgrade head`.
- [ ] **Step 3: Commit** — `git commit -m "feat: artifact full-text search via generated tsvector + GIN"`

---

### Task 10: CLI — `foundry init` + `foundry artifacts list|show|versions|diff`

**Files:**
- Create: `src/foundry/cli/init_cmd.py`, `src/foundry/cli/artifacts_cmd.py`
- Modify: `src/foundry/cli/app.py` (mount subcommands)
- Test: `tests/test_cli.py`

**Interfaces:**
- Produces:
  - `foundry init` — writes minimal `foundry.toml` if absent; resolves DB URL (`resolve_database_url`); runs `run_migrations`; syncs registry (`load_entry_points` + `sync_to_db`); prints the doc-07-style checklist (config created/exists, database ready, N artifact types registered). Idempotent.
  - `foundry artifacts list [--type T]` — Rich table: ADDRESS, TYPE, STATUS, CREATED.
  - `foundry artifacts show <ref>` — prints `body_md` (Rich Markdown) + a header line with address/status/hash.
  - `foundry artifacts versions <key>` — table of versions.
  - `foundry artifacts diff <ref-a> <ref-b>` — prints `text` diff, then structured changes.
  - CLI is sync (Typer); bridge with `asyncio.run(...)` per command; a small `_open_store()` helper builds engine/session/store from `FoundrySettings.load()`.
  - Store needs a list API — add `ArtifactStore.list(type: str | None = None, limit: int = 100) -> list[ArtifactRef]` (latest versions first) with a unit test in `tests/test_cli.py` or `test_store_versioning.py`.
- Consumes: everything from Tasks 2–9.

- [ ] **Step 1: Failing tests** `tests/test_cli.py` (CliRunner; point CLI at the test DB via `monkeypatch.setenv("FOUNDRY_DATABASE_URL", migrated_url)` and `tmp_path` cwd):

```python
def test_init_idempotent(tmp_path, monkeypatch, migrated_url) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("FOUNDRY_DATABASE_URL", migrated_url)
    monkeypatch.chdir(tmp_path)
    r1 = CliRunner().invoke(app, ["init"])
    assert r1.exit_code == 0 and (tmp_path / "foundry.toml").exists()
    r2 = CliRunner().invoke(app, ["init"])
    assert r2.exit_code == 0


def test_artifacts_roundtrip(tmp_path, monkeypatch, migrated_url, seeded_artifact) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("FOUNDRY_DATABASE_URL", migrated_url)
    monkeypatch.chdir(tmp_path)
    out = CliRunner().invoke(app, ["artifacts", "list"])
    assert seeded_artifact.address.split("@")[0] in out.output
    show = CliRunner().invoke(app, ["artifacts", "show", seeded_artifact.address])
    assert show.exit_code == 0
```

(`seeded_artifact` = small conftest fixture creating one GenericArtifact through the store against `migrated_url`.)

- [ ] **Step 2: Run — FAIL. Implement commands; re-run — PASS; lint/type clean.**
- [ ] **Step 3: Full suite + quality gates:** `uv run pytest -q && uv run ruff check . && uv run mypy packages/foundry/src` — all green.
- [ ] **Step 4: Commit** — `git commit -m "feat: foundry init and artifacts CLI (list/show/versions/diff)"`

---

## Final verification (whole phase)

- [ ] `uv run pytest -q` — full suite green.
- [ ] `uv run ruff check .` and `uv run mypy packages/foundry/src` — clean.
- [ ] Manual smoke in a scratch dir: `uv run foundry init && uv run foundry --version && uv run foundry artifacts list` against embedded pg.
- [ ] Use superpowers:finishing-a-development-branch — present merge/PR options for `feature/phase2-artifact-engine`.
