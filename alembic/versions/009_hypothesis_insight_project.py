"""Add project column to hypotheses and creative_insights tables.

Revision ID: 009
"""
from alembic import op
import sqlalchemy as sa

revision = "009"
down_revision = "008"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("hypotheses", sa.Column("project", sa.String(200), nullable=True))
    op.add_column("creative_insights", sa.Column("project", sa.String(200), nullable=True))
    op.create_index("ix_hypotheses_project", "hypotheses", ["tenant_id", "project"])
    op.create_index("ix_insights_project", "creative_insights", ["tenant_id", "project"])


def downgrade():
    op.drop_index("ix_insights_project", table_name="creative_insights")
    op.drop_index("ix_hypotheses_project", table_name="hypotheses")
    op.drop_column("creative_insights", "project")
    op.drop_column("hypotheses", "project")
