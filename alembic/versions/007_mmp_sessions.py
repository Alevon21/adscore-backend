"""Add mmp_sessions table

Revision ID: 007
Revises: 006
Create Date: 2026-03-20
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "mmp_sessions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id", UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("created_by", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("status", sa.String(20), server_default="uploaded", nullable=False),
        sa.Column("file_names", JSONB, nullable=True),
        sa.Column("total_rows", sa.Integer, nullable=True),
        sa.Column("date_range_min", sa.DateTime(timezone=True), nullable=True),
        sa.Column("date_range_max", sa.DateTime(timezone=True), nullable=True),
        sa.Column("trackers", JSONB, nullable=True),
        sa.Column("campaigns", JSONB, nullable=True),
        sa.Column("countries", JSONB, nullable=True),
        sa.Column("platforms", JSONB, nullable=True),
        sa.Column("benchmark_trackers", JSONB, nullable=True),
        sa.Column("thresholds", JSONB, nullable=True),
        sa.Column("analysis_result", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_mmp_sessions_tenant_created", "mmp_sessions", ["tenant_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_mmp_sessions_tenant_created", "mmp_sessions")
    op.drop_table("mmp_sessions")
