import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from flow_speckit.artifacts.hashing import canonical_hash
from flow_speckit.artifacts.models import ArtifactModel, GenericArtifact
from flow_speckit.artifacts.registry import (
    ArtifactRegistry,
    RegistryCollisionError,
    UnknownArtifactType,
)


class Memo(ArtifactModel, artifact_type="memo", schema_version=2):
    title: str
    points: list[str] = []  # noqa: RUF012 (pydantic field default, not a mutable class attr)


def test_classvars_set() -> None:
    assert Memo.artifact_type == "memo"
    assert Memo.artifact_schema_version == 2
    assert GenericArtifact.artifact_type == "generic"


def test_frozen() -> None:
    m = Memo(title="t")
    with pytest.raises(Exception):  # noqa: B017 (pydantic raises ValidationError, not asserted here)
        m.title = "changed"  # type: ignore[misc]


def test_canonical_hash_order_independent() -> None:
    assert canonical_hash({"a": 1, "b": 2}) == canonical_hash({"b": 2, "a": 1})
    assert canonical_hash({"a": 1}) != canonical_hash({"a": 2})


def test_generic_render_md() -> None:
    art = GenericArtifact(title="Hello", body="World")
    md = art.render_md()
    assert "# Hello" in md and "World" in md


def test_registry_register_get() -> None:
    reg = ArtifactRegistry()
    reg.register(Memo, source_package="pkg-a")
    assert reg.get("memo") is Memo
    with pytest.raises(UnknownArtifactType):
        reg.get("nope")


def test_registry_collision_rules() -> None:
    reg = ArtifactRegistry()

    class MemoA(ArtifactModel, artifact_type="memo"):
        title: str

    class MemoB(ArtifactModel, artifact_type="memo"):
        title: str

    reg.register(MemoA, source_package="pkg-a")
    with pytest.raises(RegistryCollisionError):
        reg.register(MemoB, source_package="pkg-b")
    reg.register(MemoB, source_package="local")  # local override allowed
    assert reg.get("memo") is MemoB


def test_entry_points_load_generic() -> None:
    reg = ArtifactRegistry()
    reg.load_entry_points()
    assert reg.get("generic") is GenericArtifact


async def test_sync_to_db(session: AsyncSession) -> None:
    reg = ArtifactRegistry()
    reg.register(Memo, source_package="pkg-a")
    await reg.sync_to_db(session)
    await reg.sync_to_db(session)  # idempotent upsert
    rows = (await session.execute(text("SELECT name, schema_version FROM artifact_types"))).all()
    assert ("memo", 2) in [tuple(r) for r in rows]
