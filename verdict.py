"""
Verdict/Recommendation engine for AdScore v2.3.

Assigns actionable verdicts to each scored text based on sigmoid-normalized z-scores:
- Масштабировать (Scale up)  — top performers, increase budget
- ОК (Hold)                  — average or above, keep running
- Оптимизировать (Optimize)  — mixed signals, needs tuning
- Исключить (Exclude)        — bottom performers, cut budget
- Мало данных (Insufficient) — can't judge reliably

v2.3 changes:
- Strategy-aware verdict logic: critical metrics per strategy can trigger EXCLUDE
  regardless of ranking position (absolute performance floor)
- Relevant-strength filtering: only metrics with significant weight (>=10%) in the
  current strategy count as "strengths" for mixed-signal decisions
  (prevents cheap CPM from saving a text with -98% ROI in revenue strategy)
- Small-batch adjustment: wider EXCLUDE zone when n < 10 texts
  (bottom 35% instead of 20%, so more than 1 text can be excluded)
"""

import math
from typing import List, Optional, Tuple

from constants import metric_display_name as _metric_name
from models import EventConfig, ScoringParams, TextResult, Verdict

# ── Percentile thresholds for ranking_score (0-1, uniform) ──
_SCALE_RANK = 0.80       # top 20%  -> Scale
_OK_RANK = 0.55          # 55-80%   -> OK
_WAIT_RANK = 0.35        # 35-55%   -> Wait
_EXCLUDE_RANK = 0.20     # bottom 20% -> Exclude

# ── Small-batch adjustment ──
_SMALL_BATCH = 10
_SMALL_BATCH_EXCLUDE_RANK = 0.35  # bottom 35% when n < 10

# ── Z-score thresholds for individual metrics (sigmoid 0-1) ──
_STRONG_Z = 0.58         # above average (sigmoid(0.32))
_WEAK_Z = 0.42           # below average (sigmoid(-0.32))
_CRITICAL_Z = 0.30       # severely below average

# ── Composite score thresholds (fallback when ranking unavailable) ──
_SCALE_THRESHOLD = 0.57
_EXCLUDE_THRESHOLD = 0.43

# ── Strategy profiles: critical metrics and relevance per goal ──
_STRATEGY_PROFILES = {
    "goal_revenue": {
        "label": "Доход",
        "critical_metrics": ["ROI", "RPC", "RPM"],
        "min_critical_for_exclude": 2,
        "relevant_weights": {
            "ROI": 0.20, "RPM": 0.15, "RPC": 0.15, "CR": 0.10,
            "CPA": 0.10, "CR_install": 0.10,
        },
    },
    "goal_traffic": {
        "label": "Трафик",
        "critical_metrics": ["CTR", "CPC"],
        "min_critical_for_exclude": 2,
        "relevant_weights": {
            "CTR": 0.25, "CPC": 0.20, "CPM": 0.10, "CPI": 0.10,
            "CR_install": 0.10,
        },
    },
    "goal_conversions": {
        "label": "Конверсии",
        "critical_metrics": ["CR", "CPA"],
        "min_critical_for_exclude": 2,
        "relevant_weights": {
            "CR": 0.20, "CPA": 0.20, "CR_install": 0.15, "CTR": 0.10,
            "CPI": 0.10,
        },
    },
    "goal_installs": {
        "label": "Установки",
        "critical_metrics": ["CR_install", "CPI"],
        "min_critical_for_exclude": 2,
        "relevant_weights": {
            "CR_install": 0.30, "CPI": 0.25, "CPC": 0.10,
        },
    },
}

_RELEVANT_WEIGHT_THRESHOLD = 0.10

_ANOMALY_LABELS = {
    "attribution_anomaly": "подозрение на фрод атрибуции",
    "high_conversion_density": "аномально высокая конверсия (>50%)",
    "cost_outlier": "экстремальный выброс стоимости",
}


# ── Helpers ──

def _get_base_metric(metric: str) -> str:
    """Map event-specific metrics to their base for weight lookup.
    CR_event_1 -> CR, CPA_event_2 -> CPA, CR_install stays as-is."""
    if metric.startswith("CR_") and metric != "CR_install":
        return "CR"
    if metric.startswith("CPA_"):
        return "CPA"
    return metric


def _filter_relevant_strengths(strengths: List[str], weight_mode: str) -> List[str]:
    """Filter strengths to only include metrics relevant for the current strategy.
    A metric is relevant if its weight in the strategy preset >= 10%."""
    profile = _STRATEGY_PROFILES.get(weight_mode)
    if not profile:
        return strengths
    rw = profile["relevant_weights"]
    return [
        m for m in strengths
        if rw.get(m, rw.get(_get_base_metric(m), 0)) >= _RELEVANT_WEIGHT_THRESHOLD
    ]


def _resolve_metric_z(metric: str, z_scores: dict, events: List[EventConfig]) -> Optional[float]:
    """Get z-score for a metric, resolving event-specific versions."""
    if metric in z_scores and z_scores[metric] is not None:
        return z_scores[metric]
    if metric == "CR":
        for ev in reversed(events):
            key = f"CR_{ev.slot}"
            if key in z_scores and z_scores[key] is not None:
                return z_scores[key]
        if "CR_install" in z_scores and z_scores["CR_install"] is not None:
            return z_scores["CR_install"]
    if metric == "CPA":
        for ev in events:
            if ev.is_primary:
                key = f"CPA_{ev.slot}"
                if key in z_scores and z_scores[key] is not None:
                    return z_scores[key]
        if events:
            key = f"CPA_{events[0].slot}"
            if key in z_scores and z_scores[key] is not None:
                return z_scores[key]
    return None


def _strengths_weaknesses(z_scores: dict, strong_z: float = _STRONG_Z, weak_z: float = _WEAK_Z) -> Tuple[List[str], List[str]]:
    """Identify strong (z >= strong_z) and weak (z <= weak_z) metrics."""
    strengths = []
    weaknesses = []
    for m, v in z_scores.items():
        if v is None:
            continue
        if v >= strong_z:
            strengths.append(m)
        elif v <= weak_z:
            weaknesses.append(m)
    strengths.sort(key=lambda m: z_scores.get(m, 0), reverse=True)
    weaknesses.sort(key=lambda m: z_scores.get(m, 1))
    return strengths, weaknesses


def _deepest_cr_z(z_scores: dict, events: List[EventConfig]) -> Optional[float]:
    """Get CR z-score for deepest (last) event in funnel."""
    if events:
        for ev in reversed(events):
            key = f"CR_{ev.slot}"
            if key in z_scores and z_scores[key] is not None:
                return z_scores[key]
    return z_scores.get("CR")


def _primary_cpa_z(z_scores: dict, events: List[EventConfig]) -> Optional[float]:
    """Get CPA z-score for primary event."""
    if events:
        for ev in events:
            if ev.is_primary:
                key = f"CPA_{ev.slot}"
                if key in z_scores:
                    return z_scores[key]
        key = f"CPA_{events[0].slot}"
        if key in z_scores:
            return z_scores[key]
    return z_scores.get("CPA")


# ── Main classification ──

def classify(
    result: TextResult,
    events: List[EventConfig],
    event_labels: dict = None,
    params: ScoringParams = None,
    n_batch: int = None,
) -> Verdict:
    """Classify a single text result into a verdict.

    v2.3: strategy-aware logic, relevant-strength filtering, small-batch adjustment.
    """
    # Resolve thresholds from params or defaults
    strong_z = params.strong_z if params else _STRONG_Z
    weak_z = params.weak_z if params else _WEAK_Z
    critical_z = params.critical_z if params else _CRITICAL_Z
    weight_mode = params.weight_mode if params else "manual"

    z = result.z_scores or {}
    score = result.decision_score if result.decision_score is not None else result.composite_score
    rank = getattr(result, "ranking_score", None)
    strengths, weaknesses = _strengths_weaknesses(z, strong_z, weak_z)
    el = event_labels or {}

    valid_z = {m: v for m, v in z.items() if v is not None}
    n_valid = len(valid_z)

    # Strategy profile (None for manual/auto)
    profile = _STRATEGY_PROFILES.get(weight_mode)
    # Relevant strengths: only metrics with >= 10% weight in the strategy
    relevant_s = _filter_relevant_strengths(strengths, weight_mode) if profile else strengths

    # Guard: NaN score
    if score is None or math.isnan(score):
        return Verdict(
            verdict="Мало данных",
            reason="Невозможно рассчитать score",
            reason_type="объёмы",
            reason_detail="score = NaN",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # --- 0. Anomaly priority ---
    if getattr(result, "anomaly_detected", False):
        code = getattr(result, "anomaly_code", None) or "подозрительные данные"
        code_label = _ANOMALY_LABELS.get(code, code)
        return Verdict(
            verdict="Проблема QA",
            reason=f"Обнаружена аномалия: {code_label}. Проверьте данные перед принятием решений",
            reason_type="аномалия",
            reason_detail=f"{code_label}",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # --- 1. Insufficient data ---
    if n_valid == 0 or "insufficient_sample" in (result.warnings or []):
        return Verdict(
            verdict="Мало данных",
            reason="Недостаточно данных для надёжной оценки",
            reason_type="объёмы",
            reason_detail=f"кликов: {result.n_clicks}",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # Extract CR / CPA z-scores
    cpa_scores = {m: v for m, v in valid_z.items() if m.startswith("CPA")}
    cr_z = _deepest_cr_z(z, events)
    cpa_z = _primary_cpa_z(z, events)
    ctr_z = z.get("CTR")
    roi_z = z.get("ROI")
    any_cpa_critical = any(v < critical_z for v in cpa_scores.values())

    # ── Ranking zone classification ──
    # Small-batch adjustment: widen EXCLUDE zone when n < 10
    exclude_rank = _SMALL_BATCH_EXCLUDE_RANK if (n_batch and n_batch < _SMALL_BATCH) else _EXCLUDE_RANK

    is_top = (rank >= _SCALE_RANK) if rank is not None else (score >= _SCALE_THRESHOLD)
    is_ok_zone = (rank is not None and _OK_RANK <= rank < _SCALE_RANK) if rank is not None else (0.52 <= score < _SCALE_THRESHOLD)
    is_bottom = (rank <= exclude_rank) if rank is not None else (score <= _EXCLUDE_THRESHOLD)
    is_low_zone = (rank is not None and exclude_rank < rank <= _WAIT_RANK) if rank is not None else (_EXCLUDE_THRESHOLD < score <= 0.47)

    # ── 1.5 Strategy-critical EXCLUDE (absolute performance floor) ──
    # If the strategy's key metrics are all weak, EXCLUDE regardless of rank.
    # This prevents texts with -98% ROI from getting "Оптимизировать" in revenue strategy
    # just because they rank above the bottom 20%.
    # Guard: only for below-average texts (score < 0.50) that are not top-ranked.
    if profile and not is_top and score < 0.50:
        critical_z_vals = []
        for cm in profile["critical_metrics"]:
            zv = _resolve_metric_z(cm, z, events)
            if zv is not None:
                critical_z_vals.append((cm, zv))

        # Use weak_z (0.42) not critical_z (0.30) — "below average" is enough
        # for strategy-critical metrics to warrant exclusion
        n_critical_low = sum(1 for _, v in critical_z_vals if v < weak_z)

        if (len(critical_z_vals) >= profile["min_critical_for_exclude"]
                and n_critical_low >= profile["min_critical_for_exclude"]):
            failed = [cm for cm, v in critical_z_vals if v < weak_z]
            failed_names = ", ".join(_metric_name(m, el) for m in failed)
            return Verdict(
                verdict="Исключить",
                reason=f"Критичные метрики стратегии «{profile['label']}» провалены",
                reason_type="стратегия",
                reason_detail=f"провалены: {failed_names}",
                strengths=strengths,
                weaknesses=weaknesses,
            )

    # --- 2. Масштабировать (top performers) ---
    cr_ok = cr_z is None or cr_z >= weak_z  # CR at least not weak
    roi_ok = roi_z is None or roi_z >= weak_z  # ROI at least not weak (blocks negative-ROMI scaling)
    if is_top and cr_ok and roi_ok and not any_cpa_critical:
        top = ", ".join(_metric_name(m, el) for m in strengths[:3]) or "все метрики"
        pct_label = f"перцентиль {rank:.0%}" if rank is not None else f"балл {score:.2f}"
        return Verdict(
            verdict="Масштабировать",
            reason=f"Высокий {pct_label}, сильные показатели",
            reason_type="конверсия",
            reason_detail=f"сильные: {top}",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # --- 3. Исключить (bottom performers) ---
    # Guard: don't exclude if ROI is acceptable
    roi_excludable = roi_z is None or roi_z < weak_z
    n_low = sum(1 for v in valid_z.values() if v < weak_z)
    majority_low = n_low >= n_valid / 2

    # v2.3: use relevant_s instead of strengths — irrelevant "strengths" don't save from EXCLUDE
    if is_bottom and roi_excludable and (majority_low or (weaknesses and not relevant_s)):
        weak = ", ".join(_metric_name(m, el) for m in weaknesses[:3]) or "большинство"
        pct_label = f"перцентиль {rank:.0%}" if rank is not None else f"балл {score:.2f}"
        return Verdict(
            verdict="Исключить",
            reason=f"Низкий {pct_label}, слабые показатели",
            reason_type="цена" if cpa_scores else "смешанная",
            reason_detail=f"слабые: {weak}",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # --- 4. Оптимизировать (mixed signals) ---

    # 4a. Good CTR but poor CR — traffic quality issue
    if (ctr_z is not None and ctr_z >= strong_z
            and cr_z is not None and cr_z <= weak_z):
        return Verdict(
            verdict="Оптимизировать",
            reason="Хорошая кликабельность, но слабая конверсия",
            reason_type="трафик",
            reason_detail=f"CTR ({ctr_z:.2f}) хороший, CR ({cr_z:.2f}) слабый",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # 4b. Mixed signals — some RELEVANT strong, some weak
    # v2.3: only relevant strengths count (prevents cheap CPM from creating "mixed" in revenue strategy)
    if relevant_s and weaknesses:
        s = ", ".join(_metric_name(m, el) for m in relevant_s[:2])
        w = ", ".join(_metric_name(m, el) for m in weaknesses[:2])
        return Verdict(
            verdict="Оптимизировать",
            reason="Смешанные показатели — есть сильные и слабые метрики",
            reason_type="смешанная",
            reason_detail=f"сильные: {s}; слабые: {w}",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # 4c. Low zone + expensive CPA
    if is_low_zone and cpa_z is not None and cpa_z < weak_z:
        return Verdict(
            verdict="Оптимизировать",
            reason="Ниже среднего, стоимость конверсии высока",
            reason_type="цена",
            reason_detail=f"CPA z-score: {cpa_z:.2f}",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # 4d. Bottom zone but has RELEVANT strengths (not pure bottom)
    # v2.3: only relevant strengths can save from EXCLUDE
    if is_bottom and relevant_s:
        s = ", ".join(_metric_name(m, el) for m in relevant_s[:2])
        return Verdict(
            verdict="Оптимизировать",
            reason="Низкий общий балл, но есть сильные метрики",
            reason_type="смешанная",
            reason_detail=f"сильные: {s}",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # 4e. Bottom zone, no relevant strengths, but doesn't meet strict EXCLUDE criteria
    # (e.g. ROI is OK, or not majority_low) — still needs attention
    if is_bottom:
        weak = ", ".join(_metric_name(m, el) for m in weaknesses[:3]) or "общий балл"
        return Verdict(
            verdict="Исключить",
            reason="Низкий общий балл без значимых сильных метрик",
            reason_type="смешанная",
            reason_detail=f"слабые: {weak}",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # --- 5. ОК (above average, no issues) ---
    if is_ok_zone or is_top:
        return Verdict(
            verdict="ОК",
            reason="Выше среднего, стабильные показатели",
            reason_type="смешанная",
            reason_detail="метрики в пределах нормы",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # --- 6. ОК (default — near average, no strong signals) ---
    return Verdict(
        verdict="ОК",
        reason="Показатели около среднего, без явных проблем",
        reason_type="смешанная",
        reason_detail="метрики в пределах нормы",
        strengths=strengths,
        weaknesses=weaknesses,
    )


def generate_verdicts(results: List[TextResult], events: List[EventConfig], params: ScoringParams = None) -> None:
    """Generate verdicts for all results (mutates in-place).

    v2.3: passes batch size to classify() for small-batch threshold adjustment.
    """
    n_batch = len(results)
    event_labels = {ev.slot: ev.label for ev in events}
    for result in results:
        result.verdict = classify(result, events, event_labels, params=params, n_batch=n_batch)
