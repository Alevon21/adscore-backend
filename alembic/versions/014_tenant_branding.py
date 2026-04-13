"""Add logo_url and brand_color to tenants for white-label exports.

Revision ID: 014
"""
from alembic import op
import sqlalchemy as sa

revision = "014"
down_revision = "013"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("tenants", sa.Column("logo_url", sa.String(500), nullable=True))
    op.add_column("tenants", sa.Column("brand_color", sa.String(7), nullable=True))


def downgrade():
    op.drop_column("tenants", "brand_color")
    op.drop_column("tenants", "logo_url")
