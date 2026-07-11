from __future__ import annotations

from collections.abc import AsyncIterator

import pgserver
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from flow_speckit.storage.db import create_engine, session_factory
from flow_speckit.storage.migrate import run_migrations


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
async def db_session_factory(engine: AsyncEngine):
    """Yield an ``async_sessionmaker`` for the migrated database with clean tables."""
    async with engine.connect() as conn:
        await conn.execute(
            text(
                "TRUNCATE artifact_edges, artifacts, artifact_types, "
                "workflow_events, workflow_runs, task_queue, timers CASCADE"
            )
        )
        await conn.commit()
    return session_factory(engine)


@pytest.fixture()
async def session(engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    async with engine.connect() as conn:
        await conn.execute(
            text(
                "TRUNCATE artifact_edges, artifacts, artifact_types, "
                "workflow_events, workflow_runs, task_queue, timers CASCADE"
            )
        )
        await conn.commit()
    async with session_factory(engine)() as s:
        yield s
