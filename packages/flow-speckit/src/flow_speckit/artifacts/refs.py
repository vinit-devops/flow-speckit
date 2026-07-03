from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import Row

from flow_speckit.artifacts.models import Status


class ArtifactRef(BaseModel):
    id: UUID
    type: str
    key: str
    version: int
    status: Status
    content_hash: str
    created_at: datetime

    @property
    def address(self) -> str:
        return f"{self.key}@{self.version}"


def row_to_ref(row: Row[Any]) -> ArtifactRef:
    return ArtifactRef(
        id=row.id,
        type=row.type,
        key=row.key,
        version=row.version,
        status=row.status,
        content_hash=row.content_hash,
        created_at=row.created_at,
    )


def parse_ref(ref: str) -> UUID | tuple[str, int | None]:
    try:
        return UUID(ref)
    except ValueError:
        pass
    key, sep, version_str = ref.rpartition("@")
    if sep and version_str.isdigit():
        return key, int(version_str)
    return ref, None
