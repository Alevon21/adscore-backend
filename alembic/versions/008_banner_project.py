"""Add project field to banners for grouping creatives.

Revision ID: 008
Revises: 007
"""

from alembic import op
import sqlalchemy as sa

revision = "008"
down_revision = "007b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("banners", sa.Column("project", sa.String(200), nullable=True))
    op.create_index("ix_banners_tenant_project", "banners", ["tenant_id", "project"])


def downgrade() -> None:
    op.drop_index("ix_banners_tenant_project", table_name="banners")
    op.drop_column("banners", "project")
