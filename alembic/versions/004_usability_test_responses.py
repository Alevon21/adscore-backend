"""Add usability_test_responses table for UX trust research

Revision ID: 004
Revises: 003
Create Date: 2026-03-13
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "usability_test_responses",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id", UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("user_id", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("scenario_responses", JSONB(), nullable=True),
        sa.Column("survey_responses", JSONB(), nullable=True),
        sa.Column("agreement_rate", sa.Float(), nullable=True),
        sa.Column("total_duration_sec", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_usability_test_tenant_created", "usability_test_responses", ["tenant_id", "created_at"])


def downgrade() -> None:
    op.drop_table("usability_test_responses")
