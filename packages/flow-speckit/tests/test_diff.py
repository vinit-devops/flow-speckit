import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from flow_speckit.artifacts.models import GenericArtifact
from flow_speckit.artifacts.registry import ArtifactRegistry
from flow_speckit.artifacts.store import ArtifactNotFound, ArtifactStore
from flow_speckit.storage import schema


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


async def _null_body_md(session: AsyncSession, *keys_at: tuple[str, int]) -> None:
    for key, version in keys_at:
        await session.execute(
            schema.artifacts.update()
            .where(
                schema.artifacts.c.key == key,
                schema.artifacts.c.version == version,
            )
            .values(body_md=None)
        )
    await session.commit()


async def test_diff_one_body_md_none(
    store: ArtifactStore, session: AsyncSession
) -> None:
    await store.create(GenericArtifact(title="First", body="alpha"), key="doc")
    await store.create(GenericArtifact(title="Second", body="beta"), key="doc")
    await _null_body_md(session, ("doc", 1))
    d = await store.diff("doc@1", "doc@2")
    # A NULL body diffs as empty text: side a contributes only headers, so
    # every hunk line is an addition from side b.
    assert d.text.startswith("--- doc@1")
    assert "+# Second" in d.text
    assert "-#" not in d.text
    assert "values_changed" in d.structured


async def test_diff_both_body_md_none(
    store: ArtifactStore, session: AsyncSession
) -> None:
    await store.create(GenericArtifact(title="First"), key="doc")
    await store.create(GenericArtifact(title="Second"), key="doc")
    await _null_body_md(session, ("doc", 1), ("doc", 2))
    d = await store.diff("doc@1", "doc@2")
    # Two NULL bodies produce no text diff, but structured content still
    # reflects the field change.
    assert d.text == ""
    assert "values_changed" in d.structured


async def test_diff_cross_key(store: ArtifactStore) -> None:
    await store.create(GenericArtifact(title="Alpha", body="one"), key="keyA")
    await store.create(GenericArtifact(title="Beta", body="two"), key="keyB")
    d = await store.diff("keyA@1", "keyB@1")
    assert (d.a, d.b) == ("keyA@1", "keyB@1")
    assert "values_changed" in d.structured
    assert "-# Alpha" in d.text and "+# Beta" in d.text


async def test_diff_missing_first_ref_exits_early(
    store: ArtifactStore, session: AsyncSession
) -> None:
    await store.create(GenericArtifact(title="A"), key="doc")
    with pytest.raises(ArtifactNotFound, match="missing@1"):
        await store.diff("missing@1", "doc@1")
    # The early exit must release the read transaction before raising.
    assert not session.in_transaction()
