import uuid
import enum
from datetime import datetime, timezone
from typing import Optional, List, Dict

from sqlalchemy import (
    String, Boolean, DateTime, Integer, BigInteger, Text, Enum, ForeignKey, Index, Float,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, deferred
from sqlalchemy.dialects.postgresql import UUID, JSONB

from database import Base


def utcnow():
    return datetime.now(timezone.utc)


def new_uuid():
    return uuid.uuid4()


class UserRole(str, enum.Enum):
    owner = "owner"
    admin = "admin"
    analyst = "analyst"
    viewer = "viewer"


class TenantPlan(str, enum.Enum):
    free = "free"
    pro = "pro"
    enterprise = "enterprise"


class ProjectStatus(str, enum.Enum):
    active = "active"
    archived = "archived"
    deleted = "deleted"


class DatasetStatus(str, enum.Enum):
    created = "created"
    processing = "processing"
    completed = "completed"
    failed = "failed"


class FileStatus(str, enum.Enum):
    uploading = "uploading"
    ready = "ready"
    deleted = "deleted"


class SessionStatus(str, enum.Enum):
    uploaded = "uploaded"
    mapped = "mapped"
    scoring = "scoring"
    completed = "completed"
    failed = "failed"
    deleted = "deleted"


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    plan: Mapped[TenantPlan] = mapped_column(Enum(TenantPlan), default=TenantPlan.free, nullable=False)
    storage_quota_mb: Mapped[int] = mapped_column(Integer, default=1024, nullable=False)  # 1GB default
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    logo_url: Mapped[Optional[str]] = deferred(mapped_column(String(500), nullable=True))
    brand_color: Mapped[Optional[str]] = deferred(mapped_column(String(7), nullable=True))  # hex e.g. #3B82F6

    users: Mapped[List["User"]] = relationship(back_populates="tenant", lazy="selectin")


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    supabase_uid: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    role: Mapped[UserRole] = mapped_column(Enum(UserRole), default=UserRole.analyst, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    last_login: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    features: Mapped[Optional[list]] = mapped_column(JSONB, default=lambda: ["calculators", "research"], nullable=True)

    tenant: Mapped[Tenant] = relationship(back_populates="users", lazy="selectin")

    __table_args__ = (
        Index("ix_users_tenant_id", "tenant_id"),
    )


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    action: Mapped[str] = mapped_column(String(100), nullable=False)  # e.g. "upload", "score", "delete"
    resource_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)  # e.g. "file", "dataset"
    resource_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    ip: Mapped[Optional[str]] = mapped_column(String(45), nullable=True)
    user_agent: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    details: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (
        Index("ix_audit_tenant_created", "tenant_id", "created_at"),
    )


# ── Sprint 2 models ──────────────────────────────────────────────


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[ProjectStatus] = mapped_column(Enum(ProjectStatus), default=ProjectStatus.active, nullable=False)
    created_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, onupdate=utcnow)

    tenant: Mapped[Tenant] = relationship(lazy="selectin")
    datasets: Mapped[List["Dataset"]] = relationship(back_populates="project", lazy="selectin")

    __table_args__ = (
        Index("ix_projects_tenant_created", "tenant_id", "created_at"),
    )


class Dataset(Base):
    __tablename__ = "datasets"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    project_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("projects.id"), nullable=False)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[DatasetStatus] = mapped_column(Enum(DatasetStatus), default=DatasetStatus.created, nullable=False)
    created_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, onupdate=utcnow)

    project: Mapped[Project] = relationship(back_populates="datasets", lazy="selectin")

    __table_args__ = (
        Index("ix_datasets_tenant_created", "tenant_id", "created_at"),
    )


class StoredFile(Base):
    __tablename__ = "stored_files"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    dataset_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    original_name: Mapped[str] = mapped_column(String(500), nullable=False)
    storage_key: Mapped[str] = mapped_column(String(1000), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mime_type: Mapped[str] = mapped_column(String(100), nullable=False)
    checksum_sha256: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    status: Mapped[FileStatus] = mapped_column(Enum(FileStatus), default=FileStatus.uploading, nullable=False)
    uploaded_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (
        Index("ix_stored_files_tenant", "tenant_id"),
        Index("ix_stored_files_dataset", "dataset_id"),
    )


class ScoringSession(Base):
    __tablename__ = "scoring_sessions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    dataset_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    file_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("stored_files.id"), nullable=True)
    status: Mapped[SessionStatus] = mapped_column(Enum(SessionStatus), default=SessionStatus.uploaded, nullable=False)
    mode: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    n_rows: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    file_name: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    columns_detected: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    auto_mapped: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    mapping: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    events_detected: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    params: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, onupdate=utcnow)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_sessions_tenant_created", "tenant_id", "created_at"),
    )


class ScoringResult(Base):
    __tablename__ = "scoring_results"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    session_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("scoring_sessions.id"), nullable=False)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    results: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    stats: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    text_part_result: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    campaign_analysis: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (
        Index("ix_results_session", "session_id"),
        Index("ix_results_tenant", "tenant_id"),
    )


class Banner(Base):
    __tablename__ = "banners"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    created_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    original_filename: Mapped[str] = mapped_column(String(500), nullable=False)
    storage_key: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    file_size_bytes: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    width: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    height: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    mime_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    metrics: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    tags: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    tags_status: Mapped[str] = mapped_column(String(20), default="pending", nullable=False)
    tags_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tagged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    explanation: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    explained_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    project: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    concept_group: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    media_type: Mapped[Optional[str]] = mapped_column(String(10), default="image", nullable=True)  # 'image' | 'video'
    video_meta: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)  # duration, fps, codec, etc.
    keyframes: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)   # [{index, timestamp, frame_type, image_url, tags, cqs_score}]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, onupdate=utcnow)

    __table_args__ = (
        Index("ix_banners_tenant_created", "tenant_id", "created_at"),
        Index("ix_banners_tenant_project", "tenant_id", "project"),
        Index("ix_banners_tenant_concept", "tenant_id", "concept_group"),
    )


class PendingInvite(Base):
    __tablename__ = "pending_invites"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    role: Mapped[UserRole] = mapped_column(Enum(UserRole), default=UserRole.analyst, nullable=False)
    invited_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    tenant: Mapped[Tenant] = relationship(lazy="selectin")

    __table_args__ = (
        Index("ix_pending_invites_email", "email"),
        Index("ix_pending_invites_tenant", "tenant_id"),
    )


class UsabilityTestResponse(Base):
    __tablename__ = "usability_test_responses"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    scenario_responses: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    survey_responses: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    agreement_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    total_duration_sec: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (
        Index("ix_usability_test_tenant_created", "tenant_id", "created_at"),
    )


class MmpSession(Base):
    __tablename__ = "mmp_sessions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    created_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="uploaded", nullable=False)
    mmp_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # "adjust" | "appsflyer"

    file_names: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)
    total_rows: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    date_range_min: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    date_range_max: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    trackers: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)
    campaigns: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)
    countries: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)
    platforms: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)

    benchmark_trackers: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)
    thresholds: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    analysis_result: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_mmp_sessions_tenant_created", "tenant_id", "created_at"),
    )


class SharedLink(Base):
    __tablename__ = "shared_links"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    token: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), unique=True, nullable=False, index=True)
    report_type: Mapped[str] = mapped_column(String(50), nullable=False)
    filters: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    created_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (
        Index("ix_shared_links_token", "token"),
        Index("ix_shared_links_tenant_created", "tenant_id", "created_at"),
    )


class Comment(Base):
    __tablename__ = "comments"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    # Polymorphic target: "banner" or "hypothesis"
    target_type: Mapped[str] = mapped_column(String(50), nullable=False)
    target_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, onupdate=utcnow)

    user = relationship("User", lazy="joined")

    __table_args__ = (
        Index("ix_comments_target", "tenant_id", "target_type", "target_id"),
        Index("ix_comments_user", "tenant_id", "user_id"),
    )
