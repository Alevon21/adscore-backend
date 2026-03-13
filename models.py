"""Pydantic models for Haraba Text Scoring API."""

from typing import Dict, List, Literal, Optional
from pydantic import BaseModel, Field, model_validator


class EventConfig(BaseModel):
    slot: str  # "event_1" .. "event_4"
    label: str  # user-facing name, e.g. "Регистрации"
    column: Optional[str] = None  # original CSV column name, e.g. "conversions_1"
    is_primary: bool = False  # main conversion for CPA


class ScoringParams(BaseModel):
    alpha: float = 0.05
    fdr_level: float = 0.01
    min_impressions: int = 100
    min_clicks: int = 10
    min_conversions: int = 5
    min_conversions_event: str = ""  # event slot to filter on (e.g. "event_1"), empty = primary
    winsorize_lower: float = 0.01
    winsorize_upper: float = 0.99
    weight_mode: Literal["manual", "auto", "goal_traffic", "goal_conversions", "goal_revenue", "goal_installs"] = "manual"
    weights: Dict[str, float] = Field(default_factory=lambda: {
        "CTR": 0.20,
        "CR": 0.25,
        "CPA": 0.25,
        "ROI": 0.05,
    })
    top_threshold: float = 0.80
    low_threshold: float = 0.20
    score_quality_floor: float = 0.55
    good_quality_floor: float = 0.45
    events: List[EventConfig] = Field(default_factory=list)
    # --- Verdict thresholds (K1: extracted from constants.py) ---
    scale_threshold: float = 0.68
    exclude_threshold: float = 0.30
    optimize_lower: float = 0.45
    strong_z: float = 0.60
    weak_z: float = 0.35
    critical_z: float = 0.25
    # --- Financial (K1: extracted from financial.py) ---
    saturation_discount: float = 0.70
    # --- Fatigue (K1: extracted from fatigue.py) ---
    cold_start_days: int = 14
    max_fatigue_penalty: float = 0.15
    fatigue_p_value_threshold: float = 0.15

    @model_validator(mode='after')
    def validate_bounds(self):
        if self.winsorize_lower >= self.winsorize_upper:
            raise ValueError(
                f"winsorize_lower ({self.winsorize_lower}) must be < winsorize_upper ({self.winsorize_upper})"
            )
        if not (0 <= self.winsorize_lower <= 1) or not (0 <= self.winsorize_upper <= 1):
            raise ValueError("winsorize bounds must be in [0, 1]")
        if self.low_threshold >= self.top_threshold:
            raise ValueError(
                f"low_threshold ({self.low_threshold}) must be < top_threshold ({self.top_threshold})"
            )
        return self


class MappingRequest(BaseModel):
    session_id: str
    mapping: Dict[str, str]
    events: List[EventConfig] = Field(default_factory=list)


class ScoreRequest(BaseModel):
    session_id: str
    params: ScoringParams = Field(default_factory=ScoringParams)


class ABTestRequest(BaseModel):
    session_id: str
    text_id_a: str
    text_id_b: str
    metric: str = "CR"


class Verdict(BaseModel):
    verdict: str          # Масштабировать / ОК / Оптимизировать / Исключить / Мало данных
    reason: str           # Human-readable explanation
    reason_type: str      # L1: объёмы / цена / конверсия / трафик / смешанная
    reason_detail: str    # L2 detail
    strengths: List[str] = Field(default_factory=list)
    weaknesses: List[str] = Field(default_factory=list)


class TextResult(BaseModel):
    text_id: str
    headline: str = ""
    composite_score: float
    # --- Score Layers (v2.1) ---
    decision_score: Optional[float] = None       # sigmoid-based, used for verdicts
    ranking_score: Optional[float] = None        # fractional rank (0..1) for UI sorting
    benchmark_score: Optional[float] = None      # distance from batch median
    relative_score: Optional[float] = None       # within-batch percentile
    reliability_score: Optional[float] = None    # min(1, clicks/200)
    decision_confidence: Optional[str] = None    # "low" / "medium" / "high"
    # --- Anomaly Detection (v2.1) ---
    anomaly_detected: bool = False
    anomaly_code: Optional[str] = None           # "attribution_anomaly" / "high_conversion_density" / "cost_outlier"
    # --- Financial Impact (v2.1) ---
    excess_cost: Optional[float] = None          # overspend vs CPA target
    missed_conversions: Optional[float] = None   # conversions left on table
    revenue_gap: Optional[float] = None          # gap from target ROAS
    real_savings: Optional[float] = None         # counterfactual savings
    real_savings_adjusted: Optional[float] = None  # real_savings * saturation_discount
    target_source: Optional[str] = None          # "batch_top_median" / "batch_median"
    # --- Problem Type (v2.1) ---
    problem_type: Optional[str] = None           # hook / traffic_quality / landing_mismatch / ...
    metric_pattern: Optional[str] = None         # "high_ctr_low_cr" / ...
    pattern_confidence: Optional[float] = None   # 0..1
    # --- Traffic Quality Proxy (v2.1 G1) ---
    click_quality_proxy_type: Optional[str] = None  # "install_cr" / "engaged_action" / "none"
    traffic_proxy_missing: bool = False              # true when no downstream signal available
    # --- Fatigue (v2.1) ---
    fatigue_score: Optional[float] = None        # 0..1 severity
    fatigue_penalty: Optional[float] = None      # 0..0.15
    declining_recently: bool = False
    # --- Core fields ---
    category: str
    alt_category: str = ""
    mode: str
    metrics: Dict[str, Optional[float]]
    z_scores: Dict[str, Optional[float]]
    warnings: List[str] = Field(default_factory=list)
    n_impressions: int = 0
    n_clicks: int = 0
    verdict: Optional[Verdict] = None
    # Segment fields
    campaign: str = ""
    platform: str = ""
    device: str = ""
    # Std deviations for proper t-tests on continuous metrics
    std_metrics: Dict[str, float] = Field(default_factory=dict)
    # --- Statistical Enrichment (v2.2) ---
    # CTR = clicks / impressions
    ctr_ci_low: Optional[float] = None              # 95 % credible interval lower bound
    ctr_ci_high: Optional[float] = None             # 95 % credible interval upper bound
    prob_ctr_better: Optional[float] = None         # Bayesian P(CTR > batch median)
    ctr_pvalue: Optional[float] = None              # Binomial p-value vs batch mean CTR
    is_significant_bh: Optional[bool] = None        # Significant after Benjamini-Hochberg correction
    # CR = conversions / clicks
    cr_ci_low: Optional[float] = None
    cr_ci_high: Optional[float] = None
    prob_cr_better: Optional[float] = None          # Bayesian P(CR > batch median)
    # CR_install = installs / clicks
    cr_install_ci_low: Optional[float] = None
    cr_install_ci_high: Optional[float] = None
    prob_cr_install_better: Optional[float] = None  # Bayesian P(CR_install > batch median)


class ExcludedText(BaseModel):
    text_id: str
    headline: str = ""
    reason: str
    n_impressions: int = 0
    n_clicks: int = 0
    n_conversions: int = 0


class Insight(BaseModel):
    type: str          # "top_performers" | "budget_waste" | "segment_pattern" | "sample_warning"
    icon: str          # "trophy" | "alert_triangle" | "chart" | "info"
    title: str
    description: str
    severity: str = "info"   # "info" | "warning" | "success"


class ScoringResult(BaseModel):
    results: List[TextResult]
    stats: Dict


# --- Text Part Analysis models ---

class TextPartRequest(BaseModel):
    session_id: str
    custom_parts: List[str] = Field(default_factory=list)
    primary_metric: str = "composite_score"
    max_combination_size: int = 3


class PartImpact(BaseModel):
    part_name: str
    n_with: int
    n_without: int
    metric_with: float
    metric_without: float
    delta: float
    delta_pct: float
    p_value: float
    significant: bool
    effect_size: float = 0.0       # Cohen's d
    confidence: str = "noise"      # "high" | "medium" | "low" | "noise"
    ci_lower: float = 0.0          # 95% CI lower bound for delta
    ci_upper: float = 0.0          # 95% CI upper bound for delta


class CombinationResult(BaseModel):
    parts: List[str]
    n_texts: int
    avg_metric: float
    rank: int
    ci_lower: float = 0.0
    ci_upper: float = 0.0


class ExcludedPartInfo(BaseModel):
    part_name: str
    reason: str      # "none_match" | "too_few" | "too_many" | "all_match"
    n_with: int
    n_total: int
    message: str


class AnalysisSummary(BaseModel):
    positive_elements: List[str] = Field(default_factory=list)
    negative_elements: List[str] = Field(default_factory=list)
    neutral_elements: List[str] = Field(default_factory=list)
    recommendation: str = ""
    sample_warning: str = ""


class TextPartAnalysisResult(BaseModel):
    parts_detected: List[str]
    part_impacts: Dict[str, List[PartImpact]]
    best_combinations: List[CombinationResult]
    part_flags: Dict[str, List[bool]]
    n_texts_analyzed: int = 0
    n_texts_total: int = 0
    excluded_parts: List[ExcludedPartInfo] = Field(default_factory=list)
    summary: Optional[AnalysisSummary] = None


# --- Word extraction models ---

class ExtractWordsRequest(BaseModel):
    session_id: str
    min_length: int = 3
    include_bigrams: bool = True


class ExtractedWord(BaseModel):
    word: str
    count: int
    frequency: float


class ExtractWordsResult(BaseModel):
    words: List[ExtractedWord]
    n_texts: int
    headlines: List[str]


# --- Campaign-level analysis models ---

class CampaignVerdict(BaseModel):
    verdict: str          # Масштабировать / ОК / Оптимизировать / Исключить / Мало данных
    reason: str
    reason_type: str      # эффективность / бюджет / качество
    reason_detail: str
    strengths: List[str] = Field(default_factory=list)
    weaknesses: List[str] = Field(default_factory=list)


class CampaignResult(BaseModel):
    campaign: str
    n_texts: int
    n_texts_scored: int = 0
    total_impressions: int = 0
    total_clicks: int = 0
    total_spend: float = 0.0
    total_revenue: float = 0.0
    total_events: Dict[str, int] = Field(default_factory=dict)
    metrics: Dict[str, Optional[float]] = Field(default_factory=dict)
    z_scores: Dict[str, Optional[float]] = Field(default_factory=dict)
    composite_score: float = 0.5
    avg_text_score: float = 0.5
    category: str = "AVERAGE"
    alt_category: str = ""
    verdict: Optional[CampaignVerdict] = None
    is_reliable: bool = True
    warnings: List[str] = Field(default_factory=list)
    text_verdict_distribution: Dict[str, int] = Field(default_factory=dict)
    budget_waste_pct: float = 0.0
    score_spread: float = 0.0
    best_text_id: str = ""
    best_text_score: float = 0.0
    worst_text_id: str = ""
    worst_text_score: float = 0.0
    # --- Financial Impact (v2.1) ---
    excess_cost: Optional[float] = None
    real_savings: Optional[float] = None
    real_savings_adjusted: Optional[float] = None


class CampaignInsight(BaseModel):
    type: str
    icon: str
    title: str
    description: str
    severity: str = "info"


class CampaignAnalysisRequest(BaseModel):
    session_id: str


class CampaignAnalysisResult(BaseModel):
    campaigns: List[CampaignResult]
    insights: List[CampaignInsight] = Field(default_factory=list)
    n_campaigns: int = 0
    overall_best_campaign: str = ""
    overall_worst_campaign: str = ""
    reliability_threshold: int = 500
