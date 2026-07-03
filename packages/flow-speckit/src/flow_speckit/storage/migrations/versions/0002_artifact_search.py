"""artifact full-text search

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-03

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        "ALTER TABLE artifacts ADD COLUMN search_tsv tsvector "
        "GENERATED ALWAYS AS (to_tsvector('english', "
        "coalesce(body_md, '') || ' ' || (content::text))) STORED"
    )
    op.execute("CREATE INDEX ix_artifacts_search ON artifacts USING GIN (search_tsv)")


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_artifacts_search", table_name="artifacts")
    op.execute("ALTER TABLE artifacts DROP COLUMN search_tsv")
