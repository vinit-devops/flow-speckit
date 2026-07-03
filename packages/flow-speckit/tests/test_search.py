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
