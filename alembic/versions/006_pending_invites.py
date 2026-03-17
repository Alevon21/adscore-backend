"""Add pending_invites table

Revision ID: 006
Revises: 005
Create Date: 2026-03-17
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pending_invites",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id", UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("email", sa.String(320), nullable=False),
        sa.Column("role", sa.Enum("owner", "admin", "analyst", "viewer", name="userrole", create_type=False), nullable=False, server_default="analyst"),
        sa.Column("invited_by", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_pending_invites_email", "pending_invites", ["email"])
    op.create_index("ix_pending_invites_tenant", "pending_invites", ["tenant_id"])


def downgrade() -> None:
    op.drop_index("ix_pending_invites_tenant", "pending_invites")
    op.drop_index("ix_pending_invites_email", "pending_invites")
    op.drop_table("pending_invites")
