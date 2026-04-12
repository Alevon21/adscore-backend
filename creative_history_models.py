"""Database models and Pydantic schemas for creative history system."""

import uuid as uuid_mod
from datetime import datetime, date
from typing import Optional, List

from pydantic import BaseModel
from sqlalchemy import (
    Column, String, Text, Date, DateTime, Numeric, Boolean, Integer,
    ForeignKey, Index, ARRAY,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from db_models import Base, new_uuid, utcnow


# ---------------------------------------------------------------------------
# SQLAlchemy ORM Models
# ---------------------------------------------------------------------------

class CreativePlacement(Base):
    __tablename__ = "creative_placements"

    id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    tenant_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    creative_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    platform: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    campaign: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    ad_group: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    geo: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    metrics: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)
    verdict: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    decision_score: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    fatigue_score: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    source: Mapped[str] = mapped_column(String(50), default="manual", nullable=False)
    audience_segment: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (
        Index("ix_placements_tenant_creative", "tenant_id", "creative_id"),
        Index("ix_placements_period", "period_start", "period_end"),
    )


class Hypothesis(Base):
    __tablename__ = "hypotheses"

    id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    tenant_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    user_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    hypothesis_type: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="proposed", nullable=False)
    confidence: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    impact_score: Mapped[Optional[float]] = mapped_column(Numeric, nullable=True)
    supporting_data: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict, nullable=True)
    validation_result: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict, nullable=True)
    tags: Mapped[Optional[list]] = mapped_column(ARRAY(String), default=list, nullable=True)
    source: Mapped[str] = mapped_column(String(50), default="manual", nullable=False)
    project: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, onupdate=utcnow)

    __table_args__ = (
        Index("ix_hypotheses_tenant", "tenant_id"),
        Index("ix_hypotheses_status", "tenant_id", "status"),
        Index("ix_hypotheses_project", "tenant_id", "project"),
    )


class CreativeInsight(Base):
    __tablename__ = "creative_insights"

    id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    tenant_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    insight_type: Mapped[str] = mapped_column(String(50), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), default="info", nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    action_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    supporting_data: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict, nullable=True)
    creative_ids: Mapped[Optional[list]] = mapped_column(ARRAY(UUID(as_uuid=True)), default=list, nullable=True)
    hypothesis_id: Mapped[Optional[uuid_mod.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("hypotheses.id", ondelete="SET NULL"), nullable=True)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_dismissed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    project: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (
        Index("ix_insights_tenant", "tenant_id"),
        Index("ix_insights_project", "tenant_id", "project"),
    )


class CreativeGeneration(Base):
    __tablename__ = "creative_generations"

    id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    tenant_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    user_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    platform: Mapped[str] = mapped_column(String(50), nullable=False)
    format: Mapped[str] = mapped_column(String(50), nullable=False)
    goal: Mapped[str] = mapped_column(String(200), nullable=False)
    project: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    result: Mapped[dict] = mapped_column(JSONB, nullable=False)
    input_summary: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (
        Index("ix_generations_tenant", "tenant_id"),
        Index("ix_generations_created", "tenant_id", "created_at"),
    )


class ABTest(Base):
    __tablename__ = "ab_tests"

    id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    tenant_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    user_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    hypothesis_id: Mapped[Optional[uuid_mod.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("hypotheses.id", ondelete="SET NULL"), nullable=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    control_banner_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    test_banner_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    metric: Mapped[str] = mapped_column(String(50), default="ctr", nullable=False)
    target_sample_size: Mapped[int] = mapped_column(Integer, default=1000, nullable=False)
    confidence_level: Mapped[float] = mapped_column(Numeric, default=0.95, nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="draft", nullable=False)  # draft, running, completed, cancelled
    control_metrics: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict, nullable=True)
    test_metrics: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict, nullable=True)
    result: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict, nullable=True)
    project: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (
        Index("ix_ab_tests_tenant", "tenant_id"),
        Index("ix_ab_tests_status", "tenant_id", "status"),
    )


# ---------------------------------------------------------------------------
# Pydantic Schemas
# ---------------------------------------------------------------------------

class PlacementCreate(BaseModel):
    platform: Optional[str] = None
    campaign: Optional[str] = None
    ad_group: Optional[str] = None
    geo: Optional[str] = None
    period_start: str
    period_end: str
    metrics: dict = {}
    verdict: Optional[str] = None
    decision_score: Optional[float] = None
    fatigue_score: Optional[float] = None
    source: str = "manual"
    audience_segment: Optional[str] = None

class PlacementRecord(BaseModel):
    id: str
    creative_id: str
    platform: Optional[str] = None
    campaign: Optional[str] = None
    ad_group: Optional[str] = None
    geo: Optional[str] = None
    period_start: str
    period_end: str
    metrics: dict
    verdict: Optional[str] = None
    decision_score: Optional[float] = None
    fatigue_score: Optional[float] = None
    source: str
    audience_segment: Optional[str] = None
    created_at: str

class PlacementListResponse(BaseModel):
    placements: List[PlacementRecord]
    total: int

class HypothesisCreate(BaseModel):
    title: str
    description: Optional[str] = None
    hypothesis_type: str
    confidence: Optional[float] = None
    impact_score: Optional[float] = None
    supporting_data: dict = {}
    tags: List[str] = []
    source: str = "manual"
    project: Optional[str] = None

class HypothesisUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    hypothesis_type: Optional[str] = None
    status: Optional[str] = None
    confidence: Optional[float] = None
    impact_score: Optional[float] = None
    supporting_data: Optional[dict] = None
    validation_result: Optional[dict] = None
    tags: Optional[List[str]] = None

class HypothesisRecord(BaseModel):
    id: str
    title: str
    description: Optional[str] = None
    hypothesis_type: str
    status: str
    confidence: Optional[float] = None
    impact_score: Optional[float] = None
    supporting_data: dict
    validation_result: dict
    tags: List[str]
    source: str
    project: Optional[str] = None
    created_at: str
    updated_at: Optional[str] = None

class HypothesisListResponse(BaseModel):
    hypotheses: List[HypothesisRecord]
    total: int

class InsightRecord(BaseModel):
    id: str
    insight_type: str
    severity: str
    title: str
    description: str
    action_text: Optional[str] = None
    supporting_data: dict
    creative_ids: List[str]
    hypothesis_id: Optional[str] = None
    is_read: bool
    is_dismissed: bool
    project: Optional[str] = None
    created_at: str

class InsightListResponse(BaseModel):
    insights: List[InsightRecord]
    total: int


class ABTestCreate(BaseModel):
    name: str
    description: Optional[str] = None
    hypothesis_id: Optional[str] = None
    control_banner_id: str
    test_banner_id: str
    metric: str = "ctr"
    target_sample_size: int = 1000
    confidence_level: float = 0.95
    project: Optional[str] = None


class ABTestRecord(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    hypothesis_id: Optional[str] = None
    control_banner_id: str
    test_banner_id: str
    metric: str
    target_sample_size: int
    confidence_level: float
    status: str
    control_metrics: dict
    test_metrics: dict
    result: dict
    project: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    created_at: str


class ABTestListResponse(BaseModel):
    tests: List[ABTestRecord]
    total: int
