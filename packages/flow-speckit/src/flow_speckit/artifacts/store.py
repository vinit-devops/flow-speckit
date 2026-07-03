from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, cast
from uuid import UUID, uuid4

import structlog
from sqlalchemy import CursorResult, Row, func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from flow_speckit.artifacts.diff import ArtifactDiff, compute_diff
from flow_speckit.artifacts.graph import LineageGraph, query_lineage
from flow_speckit.artifacts.hashing import canonical_hash
from flow_speckit.artifacts.models import ArtifactModel, Relation, Status
from flow_speckit.artifacts.refs import ArtifactRef, parse_ref, row_to_ref
from flow_speckit.artifacts.registry import ArtifactRegistry
from flow_speckit.storage import schema

logger = structlog.get_logger(__name__)

_ALLOWED_TRANSITIONS: set[tuple[Status, Status]] = {
    ("draft", "proposed"),
    ("proposed", "approved"),
    ("proposed", "rejected"),
    ("draft", "superseded"),
    ("proposed", "superseded"),
    ("approved", "superseded"),
}


class ArtifactNotFound(LookupError):
    """Raised when a requested artifact ref/key cannot be resolved."""


class InvalidStatusTransition(ValueError):
    """Raised when a requested status transition is not in the allowed table."""

    def __init__(self, from_status: str, to_status: str) -> None:
        super().__init__(
            f"Cannot transition artifact status from {from_status!r} to {to_status!r}"
        )
        self.from_status = from_status
        self.to_status = to_status


class ArtifactStore:
    """Async CRUD/query façade over the `artifacts` and `artifact_edges` tables.

    ArtifactStore owns the session transaction lifecycle: writes commit,
    reads roll back; do not share the session with an outer transaction.
    """

    def __init__(self, session: AsyncSession, registry: ArtifactRegistry) -> None:
        self._session = session
        self._registry = registry

    async def create(
        self,
        model: ArtifactModel,
        *,
        key: str,
        run_id: UUID | None = None,
        step_key: str | None = None,
        derived_from: Sequence[UUID] = (),
        status: Status = "proposed",
    ) -> ArtifactRef:
        self._registry.get(model.artifact_type)  # must be registered
        content = model.model_dump(mode="json")
        digest = canonical_hash(content)
        try:
            # Serialize all creates for this key (including the first-version
            # case, where there is no row to lock) with a transaction-scoped
            # advisory lock; it auto-releases on the commit/rollback below.
            await self._session.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": key}
            )
            # Dedup only compares against the latest NON-rejected row: content
            # identical to a rejected version must still mint a new version.
            latest = await self._latest_row(key)
            if latest is not None and latest.content_hash == digest:
                # Dedup made no writes: release the transaction (and the
                # advisory lock) instead of leaving it open on the session.
                ref = row_to_ref(latest)
                await self._session.rollback()
                return ref
            # Version numbering considers ALL rows for this key (including
            # rejected ones) so a rejected latest version never collides with
            # the next inserted version.
            next_version = await self._next_version(key)
            row_id = uuid4()
            await self._session.execute(
                schema.artifacts.insert().values(
                    id=row_id,
                    type=model.artifact_type,
                    key=key,
                    version=next_version,
                    content=content,
                    content_hash=digest,
                    body_md=model.render_md(),
                    status=status,
                    schema_version=model.artifact_schema_version,
                    created_by_run=run_id,
                    created_by_step=step_key,
                )
            )
            if latest is not None:
                # Only the previous non-rejected latest row gets superseded;
                # rejected rows are terminal and get no supersedes edge.
                await self._add_edge(row_id, latest.id, "supersedes")
                await self._set_status_raw(latest.id, "superseded")
            for parent in derived_from:
                await self._add_edge(row_id, parent, "derived_from")
            await self._session.commit()
        except Exception:
            await self._session.rollback()
            raise
        return await self.resolve(row_id)

    async def _next_version(self, key: str) -> int:
        result = await self._session.execute(
            select(func.max(schema.artifacts.c.version)).where(
                schema.artifacts.c.key == key
            )
        )
        current_max = result.scalar_one_or_none()
        return 1 if current_max is None else current_max + 1

    async def get(self, ref: str | UUID) -> ArtifactModel:
        row = await self._get_row(ref)
        if row is None:
            await self._end_read()
            raise ArtifactNotFound(str(ref))
        cls = self._registry.get(row.type)
        model = cls.model_validate(row.content)
        # Read-only: release the transaction opened by the SELECT above.
        await self._end_read()
        return model

    async def resolve(self, ref: str | UUID) -> ArtifactRef:
        row = await self._get_row(ref)
        if row is None:
            await self._end_read()
            raise ArtifactNotFound(str(ref))
        result = row_to_ref(row)
        # Read-only: release the transaction opened by the SELECT above.
        await self._end_read()
        return result

    async def get_body_md(self, ref: str | UUID) -> str | None:
        """Return the STORED body_md column for a ref.

        The stored column is the canonical rendering captured at write time
        (``diff()`` also treats it as authoritative); it must be preferred
        over re-rendering the model, whose ``render_md()`` may have changed
        since the artifact was written.
        """
        row = await self._get_row(ref)
        if row is None:
            # The SELECT above opened a transaction; release it before raising.
            await self._end_read()
            raise ArtifactNotFound(str(ref))
        body: str | None = row.body_md
        # Read-only: release the transaction opened by the SELECT above.
        await self._end_read()
        return body

    async def lineage(
        self,
        ref: str | UUID,
        *,
        direction: Literal["up", "down"] = "up",
        max_depth: int = 32,
    ) -> LineageGraph:
        row = await self._get_row(ref)
        if row is None:
            await self._end_read()
            raise ArtifactNotFound(str(ref))
        graph = await query_lineage(
            self._session, row.id, direction=direction, max_depth=max_depth
        )
        # Read-only: release the transaction opened by the SELECTs above.
        await self._end_read()
        return graph

    async def versions(self, key: str) -> list[ArtifactRef]:
        result = await self._session.execute(
            select(schema.artifacts)
            .where(schema.artifacts.c.key == key)
            .order_by(schema.artifacts.c.version.asc())
        )
        refs = [row_to_ref(row) for row in result.all()]
        # Read-only: release the transaction opened by the SELECT above.
        await self._end_read()
        return refs

    async def diff(self, ref_a: str | UUID, ref_b: str | UUID) -> ArtifactDiff:
        row_a = await self._get_row(ref_a)
        row_b = await self._get_row(ref_b)
        if row_a is None or row_b is None:
            await self._end_read()
            raise ArtifactNotFound(str(ref_a) if row_a is None else str(ref_b))
        # Read-only: release the transaction opened by the SELECTs above.
        await self._end_read()
        return compute_diff(
            row_to_ref(row_a).address,
            row_a.content,
            row_a.body_md,
            row_to_ref(row_b).address,
            row_b.content,
            row_b.body_md,
        )

    async def search(
        self, query: str, *, type: str | None = None, limit: int = 50
    ) -> list[ArtifactRef]:
        sql = (
            "SELECT id, type, key, version, status, content_hash, created_at "
            "FROM artifacts "
            "WHERE search_tsv @@ plainto_tsquery('english', :q)"
        )
        params: dict[str, Any] = {"q": query, "limit": limit}
        if type is not None:
            sql += " AND type = :type"
            params["type"] = type
        sql += (
            " ORDER BY ts_rank(search_tsv, plainto_tsquery('english', :q)) DESC LIMIT :limit"
        )
        result = await self._session.execute(text(sql), params)
        rows = result.all()
        # Read-only: release the transaction opened by the SELECT above.
        await self._end_read()
        return [row_to_ref(row) for row in rows]

    async def set_status(
        self, ref: str | UUID, status: Status, *, actor: str
    ) -> ArtifactRef:
        row = await self._get_row(ref)
        if row is None:
            # The SELECT above opened a transaction; release it before raising.
            await self._end_read()
            raise ArtifactNotFound(str(ref))
        current: Status = row.status
        if (current, status) not in _ALLOWED_TRANSITIONS:
            await self._end_read()
            raise InvalidStatusTransition(current, status)
        result = cast(
            CursorResult[Any],
            await self._session.execute(
                schema.artifacts.update()
                .where(
                    schema.artifacts.c.id == row.id,
                    schema.artifacts.c.status == current,
                )
                .values(status=status)
            ),
        )
        if result.rowcount == 0:
            # Status changed concurrently since our read; report the fresh
            # status and discard the no-op transaction before raising.
            fresh = await self._get_row(row.id)
            fresh_status = fresh.status if fresh is not None else current
            await self._end_read()
            raise InvalidStatusTransition(fresh_status, status)
        logger.info(
            "artifact_status_changed",
            actor=actor,
            artifact_id=str(row.id),
            **{"from": current, "to": status},
        )
        await self._session.commit()
        return await self.resolve(row.id)

    # NOTE: defined after every other method whose annotations use the bare
    # `list[...]` builtin generic — naming this method `list` shadows that
    # name in the class namespace for any code appearing after it.
    async def list(
        self, type: str | None = None, limit: int = 100
    ) -> list[ArtifactRef]:
        stmt = (
            select(schema.artifacts)
            # Secondary key `id` makes ordering deterministic when rows share
            # a created_at timestamp.
            .order_by(schema.artifacts.c.created_at.desc(), schema.artifacts.c.id)
            .limit(limit)
        )
        if type is not None:
            stmt = stmt.where(schema.artifacts.c.type == type)
        result = await self._session.execute(stmt)
        rows = result.all()
        # Read-only: release the transaction opened by the SELECT above.
        await self._end_read()
        return [row_to_ref(row) for row in rows]

    async def _end_read(self) -> None:
        """Roll back the implicit transaction opened by a preceding SELECT.

        Every read-only method (and every raise-before-mutation path) must
        call this immediately before returning or raising, so the session
        never carries a dangling open transaction back to the caller.
        """
        await self._session.rollback()

    async def _get_row(self, ref: str | UUID) -> Row[Any] | None:
        parsed = ref if isinstance(ref, UUID) else parse_ref(ref)
        if isinstance(parsed, UUID):
            result = await self._session.execute(
                select(schema.artifacts).where(schema.artifacts.c.id == parsed)
            )
            return result.first()
        key, version = parsed
        if version is not None:
            result = await self._session.execute(
                select(schema.artifacts).where(
                    schema.artifacts.c.key == key,
                    schema.artifacts.c.version == version,
                )
            )
            return result.first()
        return await self._latest_row(key)

    async def _latest_row(self, key: str) -> Row[Any] | None:
        stmt = (
            select(schema.artifacts)
            .where(
                schema.artifacts.c.key == key,
                schema.artifacts.c.status != "rejected",
            )
            .order_by(schema.artifacts.c.version.desc())
            .limit(1)
        )
        result = await self._session.execute(stmt)
        return result.first()

    async def _add_edge(self, from_id: UUID, to_id: UUID, relation: Relation) -> None:
        stmt = insert(schema.artifact_edges).values(
            from_id=from_id, to_id=to_id, relation=relation
        )
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["from_id", "to_id", "relation"]
        )
        await self._session.execute(stmt)

    async def _set_status_raw(self, artifact_id: UUID, status: Status) -> None:
        await self._session.execute(
            schema.artifacts.update()
            .where(schema.artifacts.c.id == artifact_id)
            .values(status=status)
        )
