from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession


async def test_tables_exist(session: AsyncSession) -> None:
    rows = await session.execute(
        text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    )
    names = {r[0] for r in rows}
    assert {"artifacts", "artifact_edges", "artifact_types", "alembic_version"} <= names


async def test_key_version_unique(session: AsyncSession) -> None:
    insert = text(
        "INSERT INTO artifacts (id, type, key, version, content, content_hash, schema_version) "
        "VALUES (gen_random_uuid(), 'generic', 'k', 1, '{}', 'h', 1)"
    )
    await session.execute(insert)
    with pytest.raises(IntegrityError):
        await session.execute(insert)
