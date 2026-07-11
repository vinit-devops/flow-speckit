from __future__ import annotations

import uuid

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = MetaData()

RELATIONS = ("derived_from", "supersedes", "informs", "implements", "reviews")
STATUSES = ("draft", "proposed", "approved", "rejected", "superseded")

WORKFLOW_EVENT_TYPES = (
    "run_started",
    "step_started",
    "step_completed",
    "step_failed",
    "gate_opened",
    "gate_resolved",
    "run_completed",
    "run_failed",
    "run_cancelled",
)
WORKFLOW_STATUSES = (
    "pending",
    "running",
    "waiting_gate",
    "waiting_timer",
    "completed",
    "failed",
    "cancelled",
)
TIMER_KINDS = ("gate_timeout", "sleep", "retry")

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

workflow_events = Table(
    "workflow_events",
    metadata,
    Column("run_id", UUID(as_uuid=True), nullable=False),
    Column("seq", BigInteger, nullable=False),
    Column("event_type", Text, nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("run_id", "seq", name="pk_workflow_events"),
    # Keep this literal in sync with the WORKFLOW_EVENT_TYPES tuple above and with the
    # matching CHECK constraint in migrations/versions/0003_workflow_tables.py.
    CheckConstraint(
        "event_type IN ('run_started','step_started','step_completed','step_failed',"
        "'gate_opened','gate_resolved','run_completed','run_failed','run_cancelled')",
        name="ck_workflow_events_event_type",
    ),
)

workflow_runs = Table(
    "workflow_runs",
    metadata,
    Column("run_id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("workflow_name", Text, nullable=False),
    Column("workflow_version", Text, nullable=False),
    Column("status", Text, nullable=False, server_default="pending", index=True),
    Column("current_step", Text),
    Column("input", JSONB, nullable=False),
    Column("output_ref", UUID(as_uuid=True)),
    Column("error", JSONB),
    Column("parent_run_id", UUID(as_uuid=True)),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    # Keep this literal in sync with the WORKFLOW_STATUSES tuple above and with the
    # matching CHECK constraint in migrations/versions/0003_workflow_tables.py.
    CheckConstraint(
        "status IN ('pending','running','waiting_gate','waiting_timer',"
        "'completed','failed','cancelled')",
        name="ck_workflow_runs_status",
    ),
)

task_queue = Table(
    "task_queue",
    metadata,
    Column("run_id", UUID(as_uuid=True), primary_key=True),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("claimed_by", Text),
    Column("heartbeat_at", DateTime(timezone=True)),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Index(
        "ix_task_queue_available_unclaimed",
        "available_at",
        postgresql_where=text("claimed_by IS NULL"),
    ),
)

timers = Table(
    "timers",
    metadata,
    Column("timer_id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("run_id", UUID(as_uuid=True), nullable=False),
    Column("step_key", Text, nullable=False),
    Column("fire_at", DateTime(timezone=True), nullable=False, index=True),
    Column("kind", Text, nullable=False),
    # Keep this literal in sync with the TIMER_KINDS tuple above and with the
    # matching CHECK constraint in migrations/versions/0003_workflow_tables.py.
    CheckConstraint(
        "kind IN ('gate_timeout','sleep','retry')",
        name="ck_timers_kind",
    ),
)
