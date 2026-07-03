from __future__ import annotations

import asyncio

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from flow_speckit.storage.db import to_async_url
from flow_speckit.storage.schema import metadata


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    url = context.config.get_main_option("sqlalchemy.url")
    if url is None:
        raise RuntimeError("alembic config has no sqlalchemy.url set")
    engine = create_async_engine(to_async_url(url))
    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)
        await connection.commit()
    await engine.dispose()


asyncio.run(run_async_migrations())
