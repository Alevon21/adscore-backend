"""Pydantic models for AdScore creative analytics."""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class BannerMetrics(BaseModel):
    # Raw metrics from CSV
    impressions: Optional[int] = None
    clicks: Optional[int] = None
    spend: Optional[float] = None
    installs: Optional[int] = None
    revenue: Optional[float] = None
    event_1: Optional[int] = None
    event_2: Optional[int] = None
    event_3: Optional[int] = None
    event_4: Optional[int] = None
    # Computed rates
    ctr: Optional[float] = None
    cr_install: Optional[float] = None
    cr_event: Optional[float] = None
    # Metadata
    platform: Optional[str] = None
    campaign: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    # Event labels (custom names from mapping)
    event_labels: Optional[dict] = None


class BannerVisualTags(BaseModel):
    has_faces: bool = False
    n_people: int = 0
    background_type: Optional[str] = None
    background_color: Optional[str] = None
    objects: List[str] = Field(default_factory=list)
    color_scheme: Optional[str] = None
    dominant_colors: List[str] = Field(default_factory=list)


class BannerTextTags(BaseModel):
    headline: Optional[str] = None
    subtitle: Optional[str] = None
    offer: Optional[str] = None
    cta_text: Optional[str] = None
    has_urgency_words: bool = False
    urgency_words: List[str] = Field(default_factory=list)


class BannerStructuralTags(BaseModel):
    has_cta_button: bool = False
    cta_button_color: Optional[str] = None
    cta_position: Optional[str] = None
    has_logo: bool = False
    logo_position: Optional[str] = None
    text_image_ratio: Optional[float] = None


class BannerEmotionalTags(BaseModel):
    tonality: Optional[str] = None
    has_smiling_face: bool = False
    energy_level: Optional[str] = None


class BannerTags(BaseModel):
    visual: BannerVisualTags = Field(default_factory=BannerVisualTags)
    text_elements: BannerTextTags = Field(default_factory=BannerTextTags)
    structural: BannerStructuralTags = Field(default_factory=BannerStructuralTags)
    emotional: BannerEmotionalTags = Field(default_factory=BannerEmotionalTags)


class BannerRecord(BaseModel):
    id: str
    filename: str = ""
    original_filename: str = ""
    upload_date: str = ""
    file_size_bytes: int = 0
    width: int = 0
    height: int = 0
    metrics: BannerMetrics = Field(default_factory=BannerMetrics)
    tags: Optional[BannerTags] = None
    tags_status: str = "pending"  # pending | processing | done | error | no_image
    tags_error: Optional[str] = None
    tagged_at: Optional[str] = None
    explanation: Optional[str] = None
    explained_at: Optional[str] = None
    image_url: Optional[str] = None


class BannerUploadResponse(BaseModel):
    id: str
    filename: str
    metrics: BannerMetrics
    tags_status: str
    image_url: Optional[str] = None


class BannerListResponse(BaseModel):
    banners: List[BannerRecord]
    total: int


class TagResponse(BaseModel):
    banner_id: str
    tags: Optional[BannerTags] = None
    tags_status: str
    tags_error: Optional[str] = None


class ElementMetricStats(BaseModel):
    avg_with: float = 0.0
    avg_without: float = 0.0
    delta: float = 0.0
    delta_pct: float = 0.0
    correlation: float = 0.0
    p_value: float = 1.0


class ElementPerformance(BaseModel):
    element_name: str
    element_category: str
    n_with: int = 0
    n_without: int = 0
    metrics: Dict[str, ElementMetricStats] = Field(default_factory=dict)


class InsightsResponse(BaseModel):
    elements: List[ElementPerformance]
    n_banners: int
    generated_at: str
    platform_slices: Dict[str, List[ElementPerformance]] = Field(default_factory=dict)


class ExplainResponse(BaseModel):
    banner_id: str
    explanation: str
    explained_at: str
    cached: bool = False


class CSVUploadResponse(BaseModel):
    imported: int
    errors: List[str]
    banners: List[BannerUploadResponse]
