from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Row
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from flow_speckit.artifacts.models import GenericArtifact
from flow_speckit.artifacts.registry import ArtifactRegistry
from flow_speckit.artifacts.store import ArtifactNotFound, ArtifactStore, InvalidStatusTransition
from flow_speckit.storage import schema
from flow_speckit.storage.db import session_factory


@pytest.fixture()
def store(session: AsyncSession) -> ArtifactStore:
    reg = ArtifactRegistry()
    reg.register(GenericArtifact, source_package="flow-speckit")
    return ArtifactStore(session, reg)


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


async def test_concurrent_status_change_raises_with_fresh_status(
    engine: AsyncEngine,
    session: AsyncSession,
    store: ArtifactStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Exercise the rowcount-0 branch of the guarded UPDATE: after set_status
    # reads the row as "proposed" but before it issues the UPDATE, a second
    # session flips the status to "rejected" and commits. The guarded UPDATE
    # then matches zero rows and set_status must raise InvalidStatusTransition
    # naming the FRESH status, leaving the session clean and usable.
    ref = await store.create(GenericArtifact(title="A"), key="a")

    real_get_row = ArtifactStore._get_row
    flipped = False

    async def flipping_get_row(self: ArtifactStore, r: str | UUID) -> Row[Any] | None:
        nonlocal flipped
        row = await real_get_row(self, r)
        if not flipped:
            flipped = True
            async with session_factory(engine)() as other:
                await other.execute(
                    schema.artifacts.update()
                    .where(schema.artifacts.c.id == ref.id)
                    .values(status="rejected")
                )
                await other.commit()
        return row

    monkeypatch.setattr(ArtifactStore, "_get_row", flipping_get_row)
    with pytest.raises(InvalidStatusTransition, match=r"from 'rejected'"):
        await store.set_status(ref.id, "approved", actor="vinit")
    monkeypatch.undo()

    # No dangling transaction and the session is still usable.
    assert not session.in_transaction()
    got = await store.get(ref.id)
    assert isinstance(got, GenericArtifact) and got.title == "A"


async def test_raise_paths_leave_no_open_transaction(
    session: AsyncSession, store: ArtifactStore
) -> None:
    ref = await store.create(GenericArtifact(title="A"), key="a")
    await store.set_status(ref.id, "approved", actor="vinit")

    with pytest.raises(InvalidStatusTransition):
        await store.set_status(ref.id, "proposed", actor="vinit")
    assert not session.in_transaction()

    with pytest.raises(ArtifactNotFound):
        await store.set_status(uuid4(), "approved", actor="vinit")
    assert not session.in_transaction()

    # Session remains usable after the raise paths.
    assert (await store.resolve(ref.id)).status == "approved"


async def test_get_leaves_no_open_transaction(
    session: AsyncSession, store: ArtifactStore
) -> None:
    ref = await store.create(GenericArtifact(title="A"), key="a")
    assert not session.in_transaction()

    await store.get(ref.id)
    assert not session.in_transaction()

    with pytest.raises(ArtifactNotFound):
        await store.get(uuid4())
    assert not session.in_transaction()


async def test_resolve_leaves_no_open_transaction(
    session: AsyncSession, store: ArtifactStore
) -> None:
    ref = await store.create(GenericArtifact(title="A"), key="a")
    assert not session.in_transaction()

    await store.resolve(ref.id)
    assert not session.in_transaction()

    with pytest.raises(ArtifactNotFound):
        await store.resolve(uuid4())
    assert not session.in_transaction()


async def test_versions_leaves_no_open_transaction(
    session: AsyncSession, store: ArtifactStore
) -> None:
    await store.create(GenericArtifact(title="A"), key="a")
    assert not session.in_transaction()

    await store.versions("a")
    assert not session.in_transaction()

    # A key with no rows is a success path (empty list), not a raise path.
    assert await store.versions("nope") == []
    assert not session.in_transaction()


async def test_lineage_leaves_no_open_transaction(
    session: AsyncSession, store: ArtifactStore
) -> None:
    ref = await store.create(GenericArtifact(title="A"), key="a")
    assert not session.in_transaction()

    await store.lineage(ref.id)
    assert not session.in_transaction()

    with pytest.raises(ArtifactNotFound):
        await store.lineage(uuid4())
    assert not session.in_transaction()
