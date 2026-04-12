"""Add shared_links table for share-by-link feature.

Revision ID: 011
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "011"
down_revision = "010"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "shared_links",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("token", UUID(as_uuid=True), unique=True, nullable=False),
        sa.Column("report_type", sa.String(50), nullable=False),
        sa.Column("filters", JSONB(), nullable=True),
        sa.Column("tenant_id", UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("created_by", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_shared_links_token", "shared_links", ["token"])
    op.create_index("ix_shared_links_tenant_created", "shared_links", ["tenant_id", "created_at"])


def downgrade():
    op.drop_index("ix_shared_links_tenant_created", table_name="shared_links")
    op.drop_index("ix_shared_links_token", table_name="shared_links")
    op.drop_table("shared_links")
