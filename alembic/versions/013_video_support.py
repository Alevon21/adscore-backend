"""Add video support fields to banners table.

Revision ID: 013
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "013"
down_revision = "012"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("banners", sa.Column("media_type", sa.String(10), server_default="image", nullable=True))
    op.add_column("banners", sa.Column("video_meta", JSONB, nullable=True))
    op.add_column("banners", sa.Column("keyframes", JSONB, nullable=True))


def downgrade():
    op.drop_column("banners", "keyframes")
    op.drop_column("banners", "video_meta")
    op.drop_column("banners", "media_type")
