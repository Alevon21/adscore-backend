"""
Verdict/Recommendation engine for AdScore v2.2.

Assigns actionable verdicts to each scored text based on sigmoid-normalized z-scores:
- Масштабировать (Scale up)  — top performers, increase budget
- ОК (Hold)                  — average or above, keep running
- Оптимизировать (Optimize)  — mixed signals, needs tuning
- Исключить (Exclude)        — bottom performers, cut budget
- Мало данных (Insufficient) — can't judge reliably

v2.2 changes:
- Uses ranking_score (percentile) as primary criterion instead of composite_score
  (composite is average of sigmoid z-scores, clusters around 0.50 with ~0.35-0.65 range,
   making old thresholds 0.68/0.30 nearly unreachable)
- Ranking score gives uniform distribution: top 15% = Scale, bottom 20% = Exclude
- "Подождать" added as separate verdict for genuinely uncertain cases
- Strengths/weaknesses thresholds calibrated for sigmoid z-scores
"""

import math
from typing import List, Optional, Tuple

from constants import metric_display_name as _metric_name
from models import EventConfig, ScoringParams, TextResult, Verdict

# ── Percentile thresholds for ranking_score (0–1, uniform) ──
_SCALE_RANK = 0.80       # top 20% → Масштабировать
_OK_RANK = 0.55          # 55–80% → ОК
_WAIT_RANK = 0.35        # 35–55% → Подождать
_EXCLUDE_RANK = 0.20     # bottom 20% → Исключить (20-35% → Оптимизировать)

# ── Z-score thresholds for individual metrics (sigmoid 0–1) ──
_STRONG_Z = 0.58         # above average (sigmoid(0.32) ≈ 0.58)
_WEAK_Z = 0.42           # below average (sigmoid(-0.32) ≈ 0.42)
_CRITICAL_Z = 0.30       # severely below average

# ── Composite score thresholds (fallback when ranking unavailable) ──
_SCALE_THRESHOLD = 0.57
_EXCLUDE_THRESHOLD = 0.43

_ANOMALY_LABELS = {
    "attribution_anomaly": "подозрение на фрод атрибуции",
    "high_conversion_density": "аномально высокая конверсия (>50%)",
    "cost_outlier": "экстремальный выброс стоимости",
}


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


def classify(result: TextResult, events: List[EventConfig], event_labels: dict = None, params: ScoringParams = None) -> Verdict:
    """Classify a single text result into a verdict."""
    # Resolve thresholds from params or defaults
    strong_z = params.strong_z if params else _STRONG_Z
    weak_z = params.weak_z if params else _WEAK_Z
    critical_z = params.critical_z if params else _CRITICAL_Z

    z = result.z_scores or {}
    score = result.decision_score if result.decision_score is not None else result.composite_score
    rank = getattr(result, "ranking_score", None)
    strengths, weaknesses = _strengths_weaknesses(z, strong_z, weak_z)
    el = event_labels or {}

    valid_z = {m: v for m, v in z.items() if v is not None}
    n_valid = len(valid_z)

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

    # --- 1. Мало данных ---
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
    any_cpa_critical = any(v < critical_z for v in cpa_scores.values())

    # Use ranking_score (percentile, 0–1 uniform) as primary classifier
    # Fall back to composite_score thresholds if ranking unavailable
    is_top = (rank >= _SCALE_RANK) if rank is not None else (score >= _SCALE_THRESHOLD)
    is_ok_zone = (rank is not None and _OK_RANK <= rank < _SCALE_RANK) if rank is not None else (0.52 <= score < _SCALE_THRESHOLD)
    is_bottom = (rank <= _EXCLUDE_RANK) if rank is not None else (score <= _EXCLUDE_THRESHOLD)
    is_low_zone = (rank is not None and _EXCLUDE_RANK < rank <= _WAIT_RANK) if rank is not None else (_EXCLUDE_THRESHOLD < score <= 0.47)

    # --- 2. Масштабировать (top performers) ---
    cr_ok = cr_z is None or cr_z >= weak_z  # CR at least not weak
    if is_top and cr_ok and not any_cpa_critical:
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
    n_low = sum(1 for v in valid_z.values() if v < weak_z)
    majority_low = n_low >= n_valid / 2

    if is_bottom and (majority_low or (weaknesses and not strengths)):
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

    # 4b. Mixed signals — some strong, some weak
    if strengths and weaknesses:
        s = ", ".join(_metric_name(m, el) for m in strengths[:2])
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

    # 4d. Bottom zone but has some strengths (not pure bottom)
    if is_bottom and strengths:
        s = ", ".join(_metric_name(m, el) for m in strengths[:2])
        return Verdict(
            verdict="Оптимизировать",
            reason="Низкий общий балл, но есть сильные метрики",
            reason_type="смешанная",
            reason_detail=f"сильные: {s}",
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
    """Generate verdicts for all results (mutates in-place)."""
    event_labels = {ev.slot: ev.label for ev in events}
    for result in results:
        result.verdict = classify(result, events, event_labels, params=params)
