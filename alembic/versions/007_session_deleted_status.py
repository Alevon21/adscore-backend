"""Add 'deleted' value to sessionstatus enum

Revision ID: 007
Revises: 006
Create Date: 2026-03-19
"""
from alembic import op

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE sessionstatus ADD VALUE IF NOT EXISTS 'deleted'")


def downgrade() -> None:
    # PostgreSQL doesn't support removing enum values directly.
    # The 'deleted' value will remain but be unused after downgrade.
    pass
