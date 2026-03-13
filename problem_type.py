"""
Problem Type Engine + Cross-Metric Recommendation Matrix.
Classifies each text into a problem type and metric pattern.
Also classifies traffic quality proxy type (v2.1 G1).

Spec: AdScore v2.1 Sections 11-12, Block G.
"""

import logging
from typing import Dict, List, Optional, Tuple

from models import EventConfig, TextResult

logger = logging.getLogger(__name__)

# Z-score thresholds (sigmoid-normalized, 0-1)
STRONG = 0.60
WEAK = 0.35
VERY_WEAK = 0.25

# Problem type labels (Russian for frontend display)
PROBLEM_LABELS = {
    "hook": "Проблема привлечения",
    "traffic_quality": "Кликбейт / разрыв контекста",
    "landing_mismatch": "Проблема онбординга / ASO",
    "auction": "Дорогой аукцион",
    "activation": "Слабая активация",
    "monetization": "Сломанная монетизация",
    "microsegment": "Микросегмент",
    "mixed": "Смешанные сигналы",
    "insufficient_data": "Мало данных",
    "anomaly": "Аномалия",
}


def classify_problem_types(
    results: List[TextResult],
    events: list,
) -> None:
    """
    Classify each text into a problem type and metric pattern.
    Mutates results in-place.
    """
    for result in results:
        # Anomaly already detected — skip classification
        if result.anomaly_detected:
            result.problem_type = "anomaly"
            result.metric_pattern = "anomaly_detected"
            result.pattern_confidence = 1.0
            continue

        z = result.z_scores or {}
        if not z or len(z) < 2:
            result.problem_type = "insufficient_data"
            result.metric_pattern = None
            result.pattern_confidence = 0.0
            continue

        problem, pattern, confidence = _classify_single(z, events)
        result.problem_type = problem
        result.metric_pattern = pattern
        result.pattern_confidence = round(confidence, 2)


def _classify_single(
    z: Dict[str, Optional[float]],
    events: list,
) -> Tuple[str, Optional[str], float]:
    """
    Classify a single text based on z-score patterns.
    Returns (problem_type, metric_pattern, confidence).
    """
    ctr_z = z.get("CTR")
    cpc_z = z.get("CPC")
    cpm_z = z.get("CPM")
    roi_z = z.get("ROI")

    # Get deepest CR z-score
    cr_z = None
    cr_install_z = z.get("CR_install")
    reg_z = None
    if events:
        for ev in reversed(events):
            val = z.get(f"CR_{ev.slot}")
            if val is not None:
                cr_z = val
                break
        # Look for registration-specific CR
        for ev in events:
            slot = ev.slot
            label = (ev.label or "").lower()
            if "регистр" in label or "registr" in label:
                reg_z = z.get(f"CR_{slot}")
                break
    if cr_z is None:
        # Fix: explicit None check — 0.0 is a valid z-score
        val = z.get("CR")
        cr_z = val if val is not None else cr_install_z

    # --- 1. Hook: weak CTR, no reliable downstream signal ---
    if ctr_z is not None and ctr_z <= WEAK:
        # Check if there's a strong downstream signal (hidden gem)
        if cr_z is not None and cr_z >= STRONG:
            # This is actually a hidden gem / microsegment
            return "microsegment", "low_ctr_high_cr", 0.75
        return "hook", "low_ctr", 0.8

    # --- 2. Traffic quality / Clickbait: High CTR + Low CR ---
    if (ctr_z is not None and ctr_z >= STRONG
            and cr_z is not None and cr_z <= WEAK):
        return "traffic_quality", "high_ctr_low_cr", 0.85

    # --- 3. Landing mismatch: Good CTR + Good install + Weak reg/purchase ---
    if (ctr_z is not None and ctr_z >= 0.5
            and cr_install_z is not None and cr_install_z >= 0.5
            and reg_z is not None and reg_z <= WEAK):
        return "landing_mismatch", "good_upper_weak_registration", 0.75

    # --- 4. Auction: high CPC/CPM (low z = expensive) ---
    cost_expensive = (
        (cpc_z is not None and cpc_z <= WEAK)
        or (cpm_z is not None and cpm_z <= WEAK)
    )
    if cost_expensive:
        other_ok = (ctr_z is None or ctr_z >= 0.45) and (cr_z is None or cr_z >= 0.45)
        if other_ok:
            return "auction", "high_cost_ok_performance", 0.7

    # --- 5. Activation: good upper funnel + weak deep funnel ---
    if (ctr_z is not None and ctr_z >= 0.5
            and cr_install_z is not None and cr_install_z >= 0.5
            and cr_z is not None and cr_z <= WEAK):
        return "activation", "good_upper_weak_activation", 0.75

    # --- 6. Monetization: strong upper funnel + weak ROI ---
    if (ctr_z is not None and ctr_z >= 0.5
            and (cr_z is None or cr_z >= 0.45)
            and roi_z is not None and roi_z <= WEAK):
        return "monetization", "good_funnel_weak_roi", 0.7

    # --- 7. Microsegment: high ROI + low volume ---
    if roi_z is not None and roi_z >= STRONG:
        if ctr_z is not None and ctr_z <= WEAK:
            return "microsegment", "high_roi_low_volume", 0.6

    # --- 8. Mixed: some strong + some weak metrics ---
    valid_z = {k: v for k, v in z.items() if v is not None}
    n_strong = sum(1 for v in valid_z.values() if v >= STRONG)
    n_weak = sum(1 for v in valid_z.values() if v <= WEAK)
    if n_strong >= 1 and n_weak >= 1:
        return "mixed", "conflicting_signals", 0.5

    # --- Default: no clear problem ---
    return "mixed", None, 0.3


# ---------------------------------------------------------------------------
# Traffic Quality Proxy Typing (v2.1 G1)
# ---------------------------------------------------------------------------

# Event labels that indicate engaged actions (case-insensitive substring match)
_ENGAGED_ACTION_PATTERNS = [
    "landing", "view", "engaged", "session", "scroll",
    "time_on_site", "page_depth", "pageview", "просмотр",
    "сессия", "глубина", "время_на_сайте",
]


def classify_traffic_proxy(
    results: List[TextResult],
    events: List[EventConfig],
) -> None:
    """
    Classify click-quality proxy type for each text (mutates in-place).

    Case A: CR_install available → install_cr
    Case B: No install, but engaged action event → engaged_action
    Case C: No downstream signal → none + traffic_proxy_missing=true
    """
    has_install = any(
        r.metrics.get("CR_install") is not None
        for r in results
    )

    engaged_event = _find_engaged_event(events)

    for r in results:
        cr_install = r.metrics.get("CR_install")

        if cr_install is not None:
            r.click_quality_proxy_type = "install_cr"
            r.traffic_proxy_missing = False
        elif engaged_event:
            cr_key = f"CR_{engaged_event.slot}"
            if r.metrics.get(cr_key) is not None:
                r.click_quality_proxy_type = "engaged_action"
                r.traffic_proxy_missing = False
            else:
                r.click_quality_proxy_type = "none"
                r.traffic_proxy_missing = True
        elif has_install:
            # Batch has install data but this text doesn't
            r.click_quality_proxy_type = "install_cr"
            r.traffic_proxy_missing = False
        else:
            # Check if any CR event exists at all
            has_any_cr = any(
                k.startswith("CR") and v is not None
                for k, v in r.metrics.items()
            )
            if has_any_cr:
                r.click_quality_proxy_type = "engaged_action"
                r.traffic_proxy_missing = False
            else:
                r.click_quality_proxy_type = "none"
                r.traffic_proxy_missing = True


def _find_engaged_event(events: List[EventConfig]) -> Optional[EventConfig]:
    """Find the first event whose label suggests an engaged action."""
    for ev in events:
        label = (ev.label or "").lower()
        if any(pat in label for pat in _ENGAGED_ACTION_PATTERNS):
            return ev
    return None
