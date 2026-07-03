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


async def test_lineage_up_walks_derived_from(store: ArtifactStore) -> None:
    brief = await store.create(GenericArtifact(title="brief"), key="brief")
    design = await store.create(GenericArtifact(title="design"), key="design",
                                derived_from=[brief.id])
    plan = await store.create(GenericArtifact(title="plan"), key="plan",
                              derived_from=[design.id])
    graph = await store.lineage(plan.id, direction="up")
    ids = {r.id for r in graph.nodes}
    assert {brief.id, design.id, plan.id} <= ids
    relations = {e.relation for e in graph.edges}
    assert relations == {"derived_from"}


async def test_lineage_down_finds_descendants(store: ArtifactStore) -> None:
    brief = await store.create(GenericArtifact(title="brief"), key="brief")
    design = await store.create(GenericArtifact(title="design"), key="design",
                                derived_from=[brief.id])
    graph = await store.lineage(brief.id, direction="down")
    assert design.id in {r.id for r in graph.nodes}


async def test_lineage_includes_supersedes(store: ArtifactStore) -> None:
    await store.create(GenericArtifact(title="v1"), key="doc")
    v2 = await store.create(GenericArtifact(title="v2"), key="doc")
    graph = await store.lineage(v2.id, direction="up")
    assert "supersedes" in {e.relation for e in graph.edges}


async def test_lineage_max_depth(store: ArtifactStore) -> None:
    prev = await store.create(GenericArtifact(title="0"), key="n/0")
    for i in range(1, 5):
        prev = await store.create(GenericArtifact(title=str(i)), key=f"n/{i}",
                                  derived_from=[prev.id])
    graph = await store.lineage(prev.id, direction="up", max_depth=2)
    assert len(graph.nodes) == 3  # root + 2 levels


async def test_lineage_dedupes_edges_in_converging_dag(store: ArtifactStore) -> None:
    # Diamond that converges at `a` and continues past it to `x`: the
    # recursive CTE's UNION ALL reaches the (a, x) edge once per path
    # into `a` (via `b` and via `c`), so it comes back twice unless the
    # store dedupes on (from_id, to_id, relation).
    x = await store.create(GenericArtifact(title="x"), key="x")
    a = await store.create(GenericArtifact(title="a"), key="a", derived_from=[x.id])
    b = await store.create(GenericArtifact(title="b"), key="b", derived_from=[a.id])
    c = await store.create(GenericArtifact(title="c"), key="c", derived_from=[a.id])
    d = await store.create(
        GenericArtifact(title="d"), key="d", derived_from=[b.id, c.id]
    )

    graph = await store.lineage(d.id, direction="up")

    edge_keys = [(e.from_id, e.to_id, e.relation) for e in graph.edges]
    assert len(edge_keys) == len(set(edge_keys))
    assert edge_keys.count((a.id, x.id, "derived_from")) == 1
