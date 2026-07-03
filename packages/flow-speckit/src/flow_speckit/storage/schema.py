from __future__ import annotations

import uuid

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = MetaData()

RELATIONS = ("derived_from", "supersedes", "informs", "implements", "reviews")
STATUSES = ("draft", "proposed", "approved", "rejected", "superseded")

artifacts = Table(
    "artifacts",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("type", Text, nullable=False),
    Column("key", Text, nullable=False),
    Column("version", Integer, nullable=False),
    Column("content", JSONB, nullable=False),
    Column("content_hash", Text, nullable=False, index=True),
    Column("body_md", Text),
    Column("status", Text, nullable=False, server_default="proposed"),
    Column("schema_version", Integer, nullable=False),
    Column("created_by_run", UUID(as_uuid=True)),
    Column("created_by_step", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("key", "version", name="uq_artifacts_key_version"),
    # Keep this literal in sync with the STATUSES tuple above and with the
    # matching CHECK constraint in migrations/versions/0001_artifact_tables.py.
    CheckConstraint(
        "status IN ('draft','proposed','approved','rejected','superseded')",
        name="ck_artifacts_status",
    ),
)
# NOTE: `search_tsv` (a generated tsvector column added by migration 0002)
# is deliberately omitted from this metadata. Keeping it out means Core
# INSERT/UPDATE statements built from `artifacts` never try to write to a
# GENERATED ALWAYS AS column. Autogenerate users beware: `alembic revision
# --autogenerate` will propose dropping `search_tsv` since it has no
# matching Column here — do not accept that diff.

artifact_edges = Table(
    "artifact_edges",
    metadata,
    Column("from_id", UUID(as_uuid=True), ForeignKey("artifacts.id"), nullable=False),
    Column("to_id", UUID(as_uuid=True), ForeignKey("artifacts.id"), nullable=False),
    Column("relation", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("from_id", "to_id", "relation", name="pk_artifact_edges"),
    # Keep this literal in sync with the RELATIONS tuple above and with the
    # matching CHECK constraint in migrations/versions/0001_artifact_tables.py.
    CheckConstraint(
        "relation IN ('derived_from','supersedes','informs','implements','reviews')",
        name="ck_edges_relation",
    ),
)

artifact_types = Table(
    "artifact_types",
    metadata,
    Column("name", Text, nullable=False),
    Column("schema_version", Integer, nullable=False),
    Column("json_schema", JSONB, nullable=False),
    Column("source_package", Text, nullable=False),
    Column("registered_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("name", "schema_version", name="pk_artifact_types"),
)
