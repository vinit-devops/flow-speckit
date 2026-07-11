from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession


async def test_workflow_tables_exist(session: AsyncSession) -> None:
    rows = await session.execute(
        text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    )
    names = {r[0] for r in rows}
    assert {"workflow_events", "workflow_runs", "task_queue", "timers"} <= names


async def test_workflow_events_pk_rejects_duplicate_seq(session: AsyncSession) -> None:
    insert = text(
        "INSERT INTO workflow_events (run_id, seq, event_type, payload) "
        "VALUES ('00000000-0000-0000-0000-000000000001', 1, 'run_started', '{}')"
    )
    await session.execute(insert)
    with pytest.raises(IntegrityError):
        await session.execute(insert)


async def test_workflow_runs_status_check(session: AsyncSession) -> None:
    insert = text(
        "INSERT INTO workflow_runs (run_id, workflow_name, workflow_version, status, input) "
        "VALUES (gen_random_uuid(), 'feature', '1', 'bogus', '{}')"
    )
    with pytest.raises(IntegrityError):
        await session.execute(insert)


async def test_timers_kind_check(session: AsyncSession) -> None:
    insert = text(
        "INSERT INTO timers (timer_id, run_id, step_key, fire_at, kind) "
        "VALUES (gen_random_uuid(), gen_random_uuid(), 'gate1', now(), 'bogus')"
    )
    with pytest.raises(IntegrityError):
        await session.execute(insert)
