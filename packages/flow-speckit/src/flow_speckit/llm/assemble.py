"""ContextAssembler — deterministic artifact-context packing (doc 06 §5).

Walks lineage upward from primary inputs, orders deterministically, budgets
by token count with graceful degradation: full body → summary → one-line
title+ref.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from flow_speckit.artifacts.models import ArtifactModel
from flow_speckit.artifacts.store import ArtifactNotFound, ArtifactStore

if TYPE_CHECKING:
    from flow_speckit.artifacts.graph import LineageGraph
    from flow_speckit.artifacts.refs import ArtifactRef


@dataclass
class ContextChunk:
    """One artifact included in assembled context, with its fidelity level."""

    ref_address: str
    fidelity: Literal["full", "summary", "title"]
    content: str


@dataclass
class AssembledContext:
    """The packed context handed to an LLM prompt (doc 06 §5); persisted in
    the step event payload so "what did the model actually see?" is always
    answerable."""

    primary_refs: list[str] = field(default_factory=list)
    chunks: list[ContextChunk] = field(default_factory=list)
    total_tokens: int = 0

    def render(self) -> str:
        """Render all chunks as a single system prompt context block."""
        lines: list[str] = []
        for chunk in self.chunks:
            header = (
                f"## Artifact: {chunk.ref_address} (fidelity={chunk.fidelity})"
            )
            lines.append(header)
            lines.append("")
            lines.append(chunk.content)
            lines.append("")
        return "\n".join(lines)


@dataclass
class _Ancestor:
    """One lineage ancestor: its stored ref, distance from the primary, and
    the canonical stored body."""

    ref: ArtifactRef
    depth: int
    body: str


class ContextAssembler:
    """Deterministic artifact-context packer (doc 06 §5).

    Algorithm:
    1. Walk lineage upward from primaries via the store's edge graph.
    2. Order deterministically: depth descending, then created_at.
    3. Budget by token count; degrade ancestors gracefully.
    4. Every included artifact carries its ref so skills can cite provenance.
    """

    _CHARS_PER_TOKEN: float = 4.0

    # How many characters of full body to surface as a "summary" before cutting
    _SUMMARY_MAX_CHARS: int = 2000
    # Max lineage depth to walk
    _MAX_DEPTH: int = 8

    def __init__(self, store: ArtifactStore) -> None:
        self._store = store

    async def assemble(
        self,
        *primary: ArtifactModel,
        primary_refs: Sequence[str] | None = None,
        budget_tokens: int = 24_000,
        include: Literal["lineage", "primary-only"] = "lineage",
    ) -> AssembledContext:
        """Assemble context from *primary* artifacts and optionally their lineage.

        ``primary_refs`` are the store addresses (``"key@N"`` or bare key) of
        the primaries, aligned by position. Models carry no identity of their
        own, so lineage can only be walked for primaries whose ref is given;
        without refs the result degrades to primary-only content.
        """
        result = AssembledContext()
        refs = list(primary_refs or [])

        # 1. Primary artifacts always at full fidelity
        primary_chunks: list[ContextChunk] = []
        for i, model in enumerate(primary):
            ref_key = (
                refs[i]
                if i < len(refs)
                else f"{model.artifact_type}/{_title_slug(model)}"
            )
            primary_chunks.append(
                ContextChunk(
                    ref_address=ref_key,
                    fidelity="full",
                    content=model.render_md(),
                )
            )
            result.primary_refs.append(ref_key)

        result.chunks = list(primary_chunks)
        result.total_tokens = self._estimate_tokens(primary_chunks)
        remaining = budget_tokens - result.total_tokens

        # 2. Walk lineage upward and add ancestors with degrading fidelity
        if include == "lineage" and remaining > 0 and self._store is not None and refs:
            ancestors = await self._collect_ancestors(
                refs, max_depth=self._MAX_DEPTH
            )
            # Deterministic order: farthest ancestors first, then created_at
            ancestors.sort(key=lambda a: (-a.depth, a.ref.created_at))

            for ancestor in ancestors:
                if remaining <= 0:
                    break
                chunk = self._degrade(ancestor, remaining)
                chunk_tokens = self._estimate_chars(len(chunk.content))
                result.chunks.append(chunk)
                result.total_tokens += chunk_tokens
                remaining -= chunk_tokens

        return result

    def _degrade(self, ancestor: _Ancestor, remaining: int) -> ContextChunk:
        """Pick the highest fidelity that fits: full → summary → title."""
        body = ancestor.body
        address = ancestor.ref.address
        if self._estimate_chars(len(body)) <= remaining:
            return ContextChunk(
                ref_address=address, fidelity="full", content=body
            )
        summary = body[: self._SUMMARY_MAX_CHARS]
        if self._estimate_chars(len(summary)) <= remaining:
            return ContextChunk(
                ref_address=address,
                fidelity="summary",
                content=summary + "\n... (truncated)",
            )
        return ContextChunk(
            ref_address=address,
            fidelity="title",
            content=f"# {ancestor.ref.key} (ref: {address})",
        )

    async def _collect_ancestors(
        self, refs: Sequence[str], *, max_depth: int
    ) -> list[_Ancestor]:
        """Walk the store's lineage graph upward from every primary ref,
        deduplicating shared ancestors across primaries."""
        seen: set[UUID] = set()
        ancestors: list[_Ancestor] = []
        for ref in refs:
            try:
                graph = await self._store.lineage(
                    ref, direction="up", max_depth=max_depth
                )
            except ArtifactNotFound:
                # A primary mid-creation has no stored row (and thus no
                # lineage) yet — skip it rather than fail assembly.
                continue
            depths = _depths_from_root(graph)
            seen.add(graph.root)
            for node in graph.nodes:
                if node.id in seen:
                    continue
                seen.add(node.id)
                body = await self._store.get_body_md(node.id) or ""
                ancestors.append(
                    _Ancestor(
                        ref=node, depth=depths.get(node.id, 1), body=body
                    )
                )
        return ancestors

    def _estimate_tokens(self, chunks: list[ContextChunk]) -> int:
        return self._estimate_chars(sum(len(chunk.content) for chunk in chunks))

    def _estimate_chars(self, chars: int) -> int:
        return max(1, int(chars / self._CHARS_PER_TOKEN))


# -- internal helpers -----------------------------------------------------------


def _title_slug(model: ArtifactModel) -> str:
    title = getattr(model, "title", None)
    if title and isinstance(title, str):
        return title.lower().replace(" ", "-")[:60]
    return "unnamed"


def _depths_from_root(graph: LineageGraph) -> dict[UUID, int]:
    """BFS distance of every lineage node from the graph root."""
    adjacency: dict[UUID, list[UUID]] = {}
    for edge in graph.edges:
        adjacency.setdefault(edge.from_id, []).append(edge.to_id)
        adjacency.setdefault(edge.to_id, []).append(edge.from_id)
    depths = {graph.root: 0}
    queue: deque[UUID] = deque([graph.root])
    while queue:
        current = queue.popleft()
        for neighbor in adjacency.get(current, ()):
            if neighbor not in depths:
                depths[neighbor] = depths[current] + 1
                queue.append(neighbor)
    return depths
