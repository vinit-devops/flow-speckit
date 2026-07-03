import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from flow_speckit.artifacts.models import GenericArtifact
from flow_speckit.artifacts.registry import ArtifactRegistry
from flow_speckit.artifacts.store import ArtifactNotFound, ArtifactStore
from flow_speckit.storage import schema
from flow_speckit.storage.db import session_factory


@pytest.fixture()
def store(session: AsyncSession) -> ArtifactStore:
    reg = ArtifactRegistry()
    reg.register(GenericArtifact, source_package="flow-speckit")
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


async def test_list_newest_first_and_type_filter(store: ArtifactStore) -> None:
    await store.create(GenericArtifact(title="A"), key="notes/a")
    await store.create(GenericArtifact(title="B"), key="notes/b")
    refs = await store.list()
    assert [r.key for r in refs] == ["notes/b", "notes/a"]
    assert await store.list(type="nonexistent") == []


async def test_list_respects_limit(store: ArtifactStore) -> None:
    await store.create(GenericArtifact(title="A"), key="notes/a")
    await store.create(GenericArtifact(title="B"), key="notes/b")
    assert len(await store.list(limit=1)) == 1


async def test_get_body_md_returns_stored_column(store: ArtifactStore) -> None:
    ref = await store.create(GenericArtifact(title="A", body="stored body"), key="notes/a")
    assert await store.get_body_md(ref.address) == "# A\n\nstored body"
    with pytest.raises(ArtifactNotFound):
        await store.get_body_md("nope")


async def test_concurrent_creates_on_new_key(
    engine: AsyncEngine, session: AsyncSession
) -> None:
    # Two independent sessions racing to create the FIRST version of the same
    # fresh key must serialize: one gets version 1, the other version 2 — no
    # IntegrityError from a duplicate (key, version) insert may escape.
    reg = ArtifactRegistry()
    reg.register(GenericArtifact, source_package="flow-speckit")
    factory = session_factory(engine)
    async with factory() as s1, factory() as s2:
        store1 = ArtifactStore(s1, reg)
        store2 = ArtifactStore(s2, reg)
        r1, r2 = await asyncio.gather(
            store1.create(GenericArtifact(title="one"), key="notes/race"),
            store2.create(GenericArtifact(title="two"), key="notes/race"),
        )
    assert {r1.version, r2.version} == {1, 2}
    assert {r.version for r in await ArtifactStore(session, reg).versions("notes/race")} == {1, 2}


async def _edge_exists(
    session: AsyncSession, *, from_id: object, to_id: object, relation: str
) -> bool:
    result = await session.execute(
        select(schema.artifact_edges).where(
            schema.artifact_edges.c.from_id == from_id,
            schema.artifact_edges.c.to_id == to_id,
            schema.artifact_edges.c.relation == relation,
        )
    )
    exists = result.first() is not None
    await session.rollback()
    return exists


async def test_create_after_rejected_version_mints_new_version(
    store: ArtifactStore, session: AsyncSession
) -> None:
    r1 = await store.create(GenericArtifact(title="A"), key="notes/a")
    await store.set_status(r1.id, "rejected", actor="vinit")

    r2 = await store.create(GenericArtifact(title="A2"), key="notes/a")

    assert r2.version == 2
    assert (await store.resolve(r1.id)).status == "rejected"
    assert not await _edge_exists(
        session, from_id=r2.id, to_id=r1.id, relation="supersedes"
    )


async def test_create_after_rejected_version_does_not_dedup_against_it(
    store: ArtifactStore,
) -> None:
    # Re-proposing content identical to a REJECTED version must mint a new
    # version rather than dedup against the rejected row.
    r1 = await store.create(GenericArtifact(title="A"), key="notes/a")
    await store.set_status(r1.id, "rejected", actor="vinit")

    r2 = await store.create(GenericArtifact(title="A"), key="notes/a")

    assert r2.id != r1.id
    assert r2.version == 2
    assert (await store.get("notes/a")).title == "A"  # type: ignore[attr-defined]


async def test_create_after_rejected_multi_version(
    store: ArtifactStore, session: AsyncSession
) -> None:
    v1 = await store.create(GenericArtifact(title="v1"), key="notes/multi")
    v2 = await store.create(GenericArtifact(title="v2"), key="notes/multi")
    await store.set_status(v2.id, "rejected", actor="vinit")

    v3 = await store.create(GenericArtifact(title="v3"), key="notes/multi")

    assert v3.version == 3
    assert (await store.resolve(v1.id)).status == "superseded"
    assert (await store.resolve(v2.id)).status == "rejected"
    assert (await store.resolve(v3.id)).status == "proposed"
    assert not await _edge_exists(
        session, from_id=v3.id, to_id=v2.id, relation="supersedes"
    )


async def test_create_failure_leaves_no_open_transaction(
    store: ArtifactStore, session: AsyncSession
) -> None:
    with pytest.raises(IntegrityError):
        await store.create(
            GenericArtifact(title="A"),
            key="notes/bad-parent",
            derived_from=[uuid4()],
        )
    assert not session.in_transaction()
    # Session remains usable after the failed create.
    ref = await store.create(GenericArtifact(title="B"), key="notes/bad-parent")
    assert ref.version == 1
