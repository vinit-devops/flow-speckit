from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from flow_speckit.artifacts.models import GenericArtifact
from flow_speckit.artifacts.registry import ArtifactRegistry
from flow_speckit.artifacts.store import ArtifactStore


@pytest.fixture()
def store(session: AsyncSession) -> ArtifactStore:
    reg = ArtifactRegistry()
    reg.register(GenericArtifact, source_package="flow-speckit")
    return ArtifactStore(session, reg)


async def test_search_finds_body_text(store: ArtifactStore) -> None:
    await store.create(
        GenericArtifact(title="CSV export design", body="streaming exporter"), key="d/csv"
    )
    await store.create(GenericArtifact(title="Auth refactor", body="oauth tokens"), key="d/auth")
    hits = await store.search("exporter")
    assert [h.key for h in hits] == ["d/csv"]


async def test_search_type_filter(store: ArtifactStore) -> None:
    await store.create(GenericArtifact(title="CSV export"), key="d/csv")
    assert await store.search("csv", type="nonexistent") == []


async def test_search_equal_rank_orders_by_id(store: ArtifactStore) -> None:
    # Identical text under two keys ranks identically; the trailing `id`
    # ORDER BY key must yield a stable id-ascending order on the tie.
    r1 = await store.create(
        GenericArtifact(title="Widget spec", body="rotary gadget"), key="d/one"
    )
    r2 = await store.create(
        GenericArtifact(title="Widget spec", body="rotary gadget"), key="d/two"
    )
    hits = await store.search("widget")
    assert [h.id for h in hits] == sorted([r1.id, r2.id])
