"""Pydantic models for AdScore creative analytics."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, model_validator


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
    # Computed rates (stored as fractions: 0.032 = 3.2%)
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

    @model_validator(mode="after")
    def _normalise_rates(self) -> BannerMetrics:
        """Compute rate fields from raw counts when possible; normalise % → fraction."""
        impr = self.impressions
        # Always recompute from raw counts if available — most reliable
        if impr and impr > 0:
            if self.clicks is not None:
                self.ctr = round(self.clicks / impr, 6)
            if self.installs is not None:
                self.cr_install = round(self.installs / impr, 6)
            # cr_event: sum of all event columns / impressions
            events_total = sum(
                getattr(self, f"event_{i}") or 0 for i in range(1, 5)
            )
            if events_total > 0:
                self.cr_event = round(events_total / impr, 6)
        else:
            # No impressions — normalise any value > 1 as percentage
            for field in ("ctr", "cr_install", "cr_event"):
                val = getattr(self, field)
                if val is not None and val > 1:
                    setattr(self, field, val / 100.0)
        return self


class BannerVisualTags(BaseModel):
    has_faces: bool = False
    n_people: int = 0
    background_type: Optional[str] = None
    background_color: Optional[str] = None
    objects: List[str] = Field(default_factory=list)
    color_scheme: Optional[str] = None
    dominant_colors: List[str] = Field(default_factory=list)
    # v2 fields
    visual_clutter: Optional[str] = None
    focal_point: Optional[str] = None
    whitespace_ratio: Optional[float] = None
    rule_of_thirds: Optional[bool] = None
    visual_hierarchy: Optional[str] = None
    design_quality: Optional[str] = None


class BannerTextTags(BaseModel):
    headline: Optional[str] = None
    subtitle: Optional[str] = None
    offer: Optional[str] = None
    cta_text: Optional[str] = None
    has_urgency_words: bool = False
    urgency_words: List[str] = Field(default_factory=list)
    # v2 fields
    font_count: Optional[int] = None
    font_size_hierarchy: Optional[str] = None
    text_readability: Optional[str] = None
    font_style: Optional[str] = None
    text_percentage: Optional[float] = None


class BannerStructuralTags(BaseModel):
    has_cta_button: bool = False
    cta_button_color: Optional[str] = None
    cta_position: Optional[str] = None
    has_logo: bool = False
    logo_position: Optional[str] = None
    text_image_ratio: Optional[float] = None
    # v2 fields
    product_visible: Optional[bool] = None
    product_prominence: Optional[float] = None
    price_visible: Optional[bool] = None
    price_prominence: Optional[str] = None
    before_after: Optional[bool] = None
    safe_zones_clear: Optional[bool] = None


class BannerEmotionalTags(BaseModel):
    tonality: Optional[str] = None
    has_smiling_face: bool = False
    energy_level: Optional[str] = None
    # v2 fields
    emotional_triggers: List[str] = Field(default_factory=list)
    trust_signals: List[str] = Field(default_factory=list)
    personalization_level: Optional[str] = None


class BannerAccessibilityTags(BaseModel):
    contrast_adequate: Optional[bool] = None
    min_font_readable: Optional[bool] = None
    color_blind_safe: Optional[bool] = None
    information_density: Optional[str] = None


class BannerPlatformFitTags(BaseModel):
    thumb_stop_potential: Optional[str] = None
    format_type: Optional[str] = None
    first_impression_strength: Optional[str] = None


class BannerTags(BaseModel):
    visual: BannerVisualTags = Field(default_factory=BannerVisualTags)
    text_elements: BannerTextTags = Field(default_factory=BannerTextTags)
    structural: BannerStructuralTags = Field(default_factory=BannerStructuralTags)
    emotional: BannerEmotionalTags = Field(default_factory=BannerEmotionalTags)
    accessibility: Optional[BannerAccessibilityTags] = None
    platform_fit: Optional[BannerPlatformFitTags] = None


class VideoMetaResponse(BaseModel):
    duration: Optional[float] = None
    width: Optional[int] = None
    height: Optional[int] = None
    fps: Optional[float] = None
    codec: Optional[str] = None
    scene_count: Optional[int] = None


class KeyFrameResponse(BaseModel):
    index: int = 0
    timestamp: float = 0.0
    frame_type: str = "interval"  # hook | scene_change | cta | interval
    image_url: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None
    cqs_score: Optional[int] = None


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
    project: Optional[str] = None
    concept_group: Optional[str] = None
    media_type: str = "image"  # image | video
    video_meta: Optional[VideoMetaResponse] = None
    keyframes: Optional[List[KeyFrameResponse]] = None


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
