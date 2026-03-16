"""Add features JSONB column to users table

Revision ID: 005
Revises: 004
Create Date: 2026-03-17
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None

DEFAULT_FEATURES = '["calculators", "research"]'
ALL_FEATURES = '["calculators", "research", "analysis", "adscore"]'


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("features", JSONB, server_default=DEFAULT_FEATURES, nullable=True),
    )

    # Backfill: owner/admin get all features, others get defaults
    op.execute(
        f"UPDATE users SET features = '{ALL_FEATURES}' WHERE role IN ('owner', 'admin')"
    )
    op.execute(
        f"UPDATE users SET features = '{DEFAULT_FEATURES}' WHERE role NOT IN ('owner', 'admin') OR features IS NULL"
    )


def downgrade() -> None:
    op.drop_column("users", "features")
