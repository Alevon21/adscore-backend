"""
Cross-text pattern analysis and recommendations.
Generates structured insights from scored results.
"""

from typing import Dict, List

from constants import metric_display_name as _metric_display_name
from models import Insight, TextResult


def generate_insights(results: List[TextResult], stats: Dict) -> List[Insight]:
    """Analyze scored results and produce structured insights."""
    insights: List[Insight] = []

    if not results:
        return insights

    event_labels = stats.get("event_labels", {})

    insights.extend(_top_performers(results, event_labels))
    insights.extend(_budget_waste(results, event_labels))
    insights.extend(_segment_patterns(results, stats))
    insights.extend(_sample_warning(results))

    return insights


def _top_performers(results: List[TextResult], event_labels: Dict = None) -> List[Insight]:
    """Identify top 1-3 texts by composite score."""
    scored = [r for r in results if r.verdict and r.verdict.verdict != "Мало данных"]
    if len(scored) < 2:
        return []

    sorted_results = sorted(scored, key=lambda r: r.composite_score, reverse=True)
    top = sorted_results[:3]

    lines = []
    for i, r in enumerate(top, 1):
        if r.verdict and r.verdict.strengths:
            strengths = ", ".join(_metric_display_name(m, event_labels) for m in r.verdict.strengths[:3])
        else:
            strengths = "—"
        headline_short = r.headline[:50] + ("…" if len(r.headline) > 50 else "")
        lines.append(f"{i}. {r.text_id} ({r.composite_score:.2f}) «{headline_short}» — сильные: {strengths}")

    return [Insight(
        type="top_performers",
        icon="trophy",
        title="Лидеры рейтинга",
        description=" ".join(lines),
        severity="success",
    )]


def _budget_waste(results: List[TextResult], event_labels: Dict = None) -> List[Insight]:
    """Texts with verdict=Исключить that consume budget."""
    excluded = [
        r for r in results
        if r.verdict and r.verdict.verdict == "Исключить" and r.n_impressions > 0
    ]

    if not excluded:
        return []

    excluded.sort(key=lambda r: r.n_impressions, reverse=True)
    total_impressions = sum(r.n_impressions for r in excluded)
    worst = excluded[:3]

    worst_ids = ", ".join(r.text_id for r in worst)

    return [Insight(
        type="budget_waste",
        icon="alert_triangle",
        title="Бюджет на слабые тексты",
        description=(
            f"{len(excluded)} текст(ов) с вердиктом «Исключить» получили "
            f"суммарно {total_impressions:,} показов. "
            f"Наибольший расход: {worst_ids}. "
            f"Рекомендуется перераспределить бюджет на лидеров."
        ),
        severity="warning",
    )]


def _segment_patterns(results: List[TextResult], stats: Dict) -> List[Insight]:
    """Compare average scores by segment (campaign, platform, device)."""
    insights: List[Insight] = []
    segments = stats.get("segments", {})

    seg_names = {
        "campaign": "кампаниям",
        "platform": "платформам",
        "device": "устройствам",
    }

    for seg_key in ("campaign", "platform", "device"):
        seg_values = segments.get(seg_key, [])
        if len(seg_values) < 2:
            continue

        # Group scores by segment
        by_seg: Dict[str, List[float]] = {}
        for r in results:
            val = getattr(r, seg_key, "")
            if val:
                by_seg.setdefault(val, []).append(r.composite_score)

        if len(by_seg) < 2:
            continue

        # Calculate averages (need at least 2 texts per segment)
        seg_avgs = {k: sum(v) / len(v) for k, v in by_seg.items() if len(v) >= 2}
        if len(seg_avgs) < 2:
            continue

        best_seg = max(seg_avgs, key=seg_avgs.get)
        worst_seg = min(seg_avgs, key=seg_avgs.get)

        delta = seg_avgs[best_seg] - seg_avgs[worst_seg]
        if delta < 0.05:  # difference less than 5% — not significant
            continue

        insights.append(Insight(
            type="segment_pattern",
            icon="chart",
            title=f"Различие по {seg_names.get(seg_key, seg_key)}",
            description=(
                f"«{best_seg}» (средний балл {seg_avgs[best_seg]:.2f}, "
                f"n={len(by_seg[best_seg])}) значительно лучше "
                f"«{worst_seg}» ({seg_avgs[worst_seg]:.2f}, "
                f"n={len(by_seg[worst_seg])}). "
                f"Разница: {delta:.2f}."
            ),
            severity="info",
        ))

    return insights


def _sample_warning(results: List[TextResult]) -> List[Insight]:
    """Warning if majority of texts have insufficient_sample."""
    if not results:
        return []

    n_insufficient = sum(
        1 for r in results
        if "insufficient_sample" in (r.warnings or [])
    )

    pct = n_insufficient / len(results) * 100

    if pct < 30:
        return []

    return [Insight(
        type="sample_warning",
        icon="info",
        title="Низкая статистическая надёжность",
        description=(
            f"{n_insufficient} из {len(results)} текстов ({pct:.0f}%) имеют "
            f"недостаточно данных (менее 30 кликов). Результаты могут быть ненадёжными. "
            f"Рекомендуется увеличить период сбора данных."
        ),
        severity="warning",
    )]
