from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from flow_speckit.artifacts.models import Relation
from flow_speckit.artifacts.refs import ArtifactRef, row_to_ref
from flow_speckit.storage import schema

# Recursive CTE per docs/design/02-artifact-engine.md §5, parameterized on
# direction and max depth. "up" follows from_id -> to_id (provenance,
# ancestors); "down" mirrors it (impact, descendants).
_UP_SQL = """
WITH RECURSIVE lineage AS (
    SELECT e.from_id, e.to_id, e.relation, 1 AS depth
    FROM artifact_edges e WHERE e.from_id = :root
    UNION ALL
    SELECT e.from_id, e.to_id, e.relation, l.depth + 1
    FROM artifact_edges e JOIN lineage l ON e.from_id = l.to_id
    WHERE l.depth < :max_depth
)
SELECT from_id, to_id, relation FROM lineage
"""

_DOWN_SQL = """
WITH RECURSIVE lineage AS (
    SELECT e.from_id, e.to_id, e.relation, 1 AS depth
    FROM artifact_edges e WHERE e.to_id = :root
    UNION ALL
    SELECT e.from_id, e.to_id, e.relation, l.depth + 1
    FROM artifact_edges e JOIN lineage l ON e.to_id = l.from_id
    WHERE l.depth < :max_depth
)
SELECT from_id, to_id, relation FROM lineage
"""


class LineageEdge(BaseModel):
    from_id: UUID
    to_id: UUID
    relation: Relation


class LineageGraph(BaseModel):
    root: UUID
    nodes: list[ArtifactRef]
    edges: list[LineageEdge]


async def query_lineage(
    session: AsyncSession,
    root: UUID,
    *,
    direction: Literal["up", "down"] = "up",
    max_depth: int = 32,
) -> LineageGraph:
    """Walk the artifact_edges DAG from `root` via a recursive CTE.

    "up" walks provenance (ancestors an artifact was derived from / supersedes);
    "down" walks impact (descendants derived from / superseding the root).
    """
    sql = _UP_SQL if direction == "up" else _DOWN_SQL
    result = await session.execute(text(sql), {"root": root, "max_depth": max_depth})
    # Converging DAGs can be reached via multiple paths, and the recursive
    # CTE's UNION ALL yields one row per path — dedupe on the edge identity
    # (from_id, to_id, relation), preserving first-seen order.
    seen: set[tuple[UUID, UUID, Relation]] = set()
    edges: list[LineageEdge] = []
    for row in result.all():
        identity = (row.from_id, row.to_id, row.relation)
        if identity in seen:
            continue
        seen.add(identity)
        edges.append(
            LineageEdge(from_id=row.from_id, to_id=row.to_id, relation=row.relation)
        )
    node_ids = {root}
    for edge in edges:
        node_ids.add(edge.from_id)
        node_ids.add(edge.to_id)
    rows = await session.execute(
        select(schema.artifacts).where(schema.artifacts.c.id.in_(node_ids))
    )
    nodes = [row_to_ref(row) for row in rows.all()]
    return LineageGraph(root=root, nodes=nodes, edges=edges)
