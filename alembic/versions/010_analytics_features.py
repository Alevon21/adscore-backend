"""Add concept_group to banners, audience_segment to placements, ab_tests table.

Revision ID: 010
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None


def upgrade():
    # Banner: concept_group for versioning
    op.add_column("banners", sa.Column("concept_group", sa.String(200), nullable=True))
    op.create_index("ix_banners_tenant_concept", "banners", ["tenant_id", "concept_group"])

    # CreativePlacement: audience_segment
    op.add_column("creative_placements", sa.Column("audience_segment", sa.String(200), nullable=True))

    # AB Tests table
    op.create_table(
        "ab_tests",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("user_id", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("hypothesis_id", UUID(as_uuid=True), sa.ForeignKey("hypotheses.id", ondelete="SET NULL"), nullable=True),
        sa.Column("name", sa.String(500), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("control_banner_id", UUID(as_uuid=True), nullable=False),
        sa.Column("test_banner_id", UUID(as_uuid=True), nullable=False),
        sa.Column("metric", sa.String(50), nullable=False, server_default="ctr"),
        sa.Column("target_sample_size", sa.Integer, nullable=False, server_default="1000"),
        sa.Column("confidence_level", sa.Numeric, nullable=False, server_default="0.95"),
        sa.Column("status", sa.String(20), nullable=False, server_default="draft"),
        sa.Column("control_metrics", JSONB, nullable=True, server_default="{}"),
        sa.Column("test_metrics", JSONB, nullable=True, server_default="{}"),
        sa.Column("result", JSONB, nullable=True, server_default="{}"),
        sa.Column("project", sa.String(200), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_ab_tests_tenant", "ab_tests", ["tenant_id"])
    op.create_index("ix_ab_tests_status", "ab_tests", ["tenant_id", "status"])


def downgrade():
    op.drop_index("ix_ab_tests_status", table_name="ab_tests")
    op.drop_index("ix_ab_tests_tenant", table_name="ab_tests")
    op.drop_table("ab_tests")
    op.drop_column("creative_placements", "audience_segment")
    op.drop_index("ix_banners_tenant_concept", table_name="banners")
    op.drop_column("banners", "concept_group")
