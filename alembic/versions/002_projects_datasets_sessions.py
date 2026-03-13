"""Add projects, datasets, stored_files, scoring_sessions, scoring_results

Revision ID: 002
Revises: 001
Create Date: 2026-03-13
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Enums
    project_status = sa.Enum("active", "archived", "deleted", name="projectstatus")
    dataset_status = sa.Enum("created", "processing", "completed", "failed", name="datasetstatus")
    file_status = sa.Enum("uploading", "ready", "deleted", name="filestatus")
    session_status = sa.Enum("uploaded", "mapped", "scoring", "completed", "failed", name="sessionstatus")

    # Projects
    op.create_table(
        "projects",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id", UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", project_status, nullable=False, server_default="active"),
        sa.Column("created_by", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_projects_tenant_created", "projects", ["tenant_id", "created_at"])

    # Datasets
    op.create_table(
        "datasets",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("project_id", UUID(as_uuid=True), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("tenant_id", UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", dataset_status, nullable=False, server_default="created"),
        sa.Column("created_by", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_datasets_tenant_created", "datasets", ["tenant_id", "created_at"])

    # Stored Files
    op.create_table(
        "stored_files",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("dataset_id", UUID(as_uuid=True), sa.ForeignKey("datasets.id"), nullable=True),
        sa.Column("tenant_id", UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("original_name", sa.String(500), nullable=False),
        sa.Column("storage_key", sa.String(1000), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("mime_type", sa.String(100), nullable=False),
        sa.Column("checksum_sha256", sa.String(64), nullable=True),
        sa.Column("status", file_status, nullable=False, server_default="uploading"),
        sa.Column("uploaded_by", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_stored_files_tenant", "stored_files", ["tenant_id"])
    op.create_index("ix_stored_files_dataset", "stored_files", ["dataset_id"])

    # Scoring Sessions
    op.create_table(
        "scoring_sessions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("dataset_id", UUID(as_uuid=True), sa.ForeignKey("datasets.id"), nullable=True),
        sa.Column("tenant_id", UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("file_id", UUID(as_uuid=True), sa.ForeignKey("stored_files.id"), nullable=True),
        sa.Column("status", session_status, nullable=False, server_default="uploaded"),
        sa.Column("mode", sa.String(50), nullable=True),
        sa.Column("n_rows", sa.Integer(), nullable=True),
        sa.Column("file_name", sa.String(500), nullable=True),
        sa.Column("columns_detected", JSONB(), nullable=True),
        sa.Column("auto_mapped", JSONB(), nullable=True),
        sa.Column("mapping", JSONB(), nullable=True),
        sa.Column("events_detected", JSONB(), nullable=True),
        sa.Column("params", JSONB(), nullable=True),
        sa.Column("created_by", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_sessions_tenant_created", "scoring_sessions", ["tenant_id", "created_at"])

    # Scoring Results
    op.create_table(
        "scoring_results",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("session_id", UUID(as_uuid=True), sa.ForeignKey("scoring_sessions.id"), nullable=False),
        sa.Column("tenant_id", UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("results", JSONB(), nullable=True),
        sa.Column("stats", JSONB(), nullable=True),
        sa.Column("text_part_result", JSONB(), nullable=True),
        sa.Column("campaign_analysis", JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_results_session", "scoring_results", ["session_id"])
    op.create_index("ix_results_tenant", "scoring_results", ["tenant_id"])


def downgrade() -> None:
    op.drop_table("scoring_results")
    op.drop_table("scoring_sessions")
    op.drop_table("stored_files")
    op.drop_table("datasets")
    op.drop_table("projects")
    op.execute("DROP TYPE IF EXISTS sessionstatus")
    op.execute("DROP TYPE IF EXISTS filestatus")
    op.execute("DROP TYPE IF EXISTS datasetstatus")
    op.execute("DROP TYPE IF EXISTS projectstatus")
