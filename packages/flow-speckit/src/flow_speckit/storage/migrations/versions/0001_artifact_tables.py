"""artifact tables

Revision ID: 0001
Revises:
Create Date: 2026-07-03

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "artifacts",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("content", JSONB(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("body_md", sa.Text()),
        sa.Column("status", sa.Text(), nullable=False, server_default="proposed"),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("created_by_run", UUID(as_uuid=True)),
        sa.Column("created_by_step", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("key", "version", name="uq_artifacts_key_version"),
        # Keep this literal in sync with the STATUSES tuple in storage/schema.py
        # and with the matching CHECK constraint there.
        sa.CheckConstraint(
            "status IN ('draft','proposed','approved','rejected','superseded')",
            name="ck_artifacts_status",
        ),
    )
    op.create_index(
        "ix_artifacts_content_hash", "artifacts", ["content_hash"], unique=False
    )

    op.create_table(
        "artifact_edges",
        sa.Column("from_id", UUID(as_uuid=True), sa.ForeignKey("artifacts.id"), nullable=False),
        sa.Column("to_id", UUID(as_uuid=True), sa.ForeignKey("artifacts.id"), nullable=False),
        sa.Column("relation", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("from_id", "to_id", "relation", name="pk_artifact_edges"),
        # Keep this literal in sync with the RELATIONS tuple in
        # storage/schema.py and with the matching CHECK constraint there.
        sa.CheckConstraint(
            "relation IN ('derived_from','supersedes','informs','implements','reviews')",
            name="ck_edges_relation",
        ),
    )

    op.create_table(
        "artifact_types",
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("json_schema", JSONB(), nullable=False),
        sa.Column("source_package", sa.Text(), nullable=False),
        sa.Column(
            "registered_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("name", "schema_version", name="pk_artifact_types"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("artifact_types")
    op.drop_table("artifact_edges")
    op.drop_index("ix_artifacts_content_hash", table_name="artifacts")
    op.drop_table("artifacts")
