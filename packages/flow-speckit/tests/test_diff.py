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
