"""workflow tables

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-11

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: str | Sequence[str] | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "workflow_events",
        sa.Column("run_id", UUID(as_uuid=True), nullable=False),
        sa.Column("seq", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("run_id", "seq", name="pk_workflow_events"),
        # Keep this literal in sync with the WORKFLOW_EVENT_TYPES tuple in
        # storage/schema.py and with the matching CHECK constraint there.
        sa.CheckConstraint(
            "event_type IN ('run_started','step_started','step_completed','step_failed',"
            "'gate_opened','gate_resolved','run_completed','run_failed','run_cancelled')",
            name="ck_workflow_events_event_type",
        ),
    )

    op.create_table(
        "workflow_runs",
        sa.Column("run_id", UUID(as_uuid=True), primary_key=True),
        sa.Column("workflow_name", sa.Text(), nullable=False),
        sa.Column("workflow_version", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("current_step", sa.Text()),
        sa.Column("input", JSONB(), nullable=False),
        sa.Column("output_ref", UUID(as_uuid=True)),
        sa.Column("error", JSONB()),
        sa.Column("parent_run_id", UUID(as_uuid=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        # Keep this literal in sync with the WORKFLOW_STATUSES tuple in
        # storage/schema.py and with the matching CHECK constraint there.
        sa.CheckConstraint(
            "status IN ('pending','running','waiting_gate','waiting_timer',"
            "'completed','failed','cancelled')",
            name="ck_workflow_runs_status",
        ),
    )
    op.create_index("ix_workflow_runs_status", "workflow_runs", ["status"], unique=False)

    op.create_table(
        "task_queue",
        sa.Column("run_id", UUID(as_uuid=True), primary_key=True),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("claimed_by", sa.Text()),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True)),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index(
        "ix_task_queue_available_unclaimed",
        "task_queue",
        ["available_at"],
        unique=False,
        postgresql_where=sa.text("claimed_by IS NULL"),
    )

    op.create_table(
        "timers",
        sa.Column("timer_id", UUID(as_uuid=True), primary_key=True),
        sa.Column("run_id", UUID(as_uuid=True), nullable=False),
        sa.Column("step_key", sa.Text(), nullable=False),
        sa.Column("fire_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        # Keep this literal in sync with the TIMER_KINDS tuple in
        # storage/schema.py and with the matching CHECK constraint there.
        sa.CheckConstraint(
            "kind IN ('gate_timeout','sleep','retry')",
            name="ck_timers_kind",
        ),
    )
    op.create_index("ix_timers_fire_at", "timers", ["fire_at"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_timers_fire_at", table_name="timers")
    op.drop_table("timers")
    op.drop_index("ix_task_queue_available_unclaimed", table_name="task_queue")
    op.drop_table("task_queue")
    op.drop_index("ix_workflow_runs_status", table_name="workflow_runs")
    op.drop_table("workflow_runs")
    op.drop_table("workflow_events")
