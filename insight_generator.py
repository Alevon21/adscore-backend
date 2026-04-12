"""
Auto-generation engine for creative insights and hypotheses.

Imported by creative_history.py to power:
  POST /adscore/insights/generate
  POST /adscore/hypotheses/generate
"""

import uuid as uuid_mod
import logging
import math
from datetime import datetime, timezone
from collections import defaultdict
from typing import Optional
import statistics

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db_models import Banner
from creative_history_models import CreativePlacement, Hypothesis, CreativeInsight

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Element extraction (mirrors adscore._extract_boolean_elements)
# ---------------------------------------------------------------------------

_BOOL_FIELDS = {
    "visual": ["has_faces", "rule_of_thirds"],
    "text_elements": ["has_urgency_words"],
    "structural": [
        "has_cta_button", "has_logo", "product_visible",
        "price_visible", "before_after", "safe_zones_clear",
    ],
    "emotional": ["has_smiling_face"],
    "accessibility": ["contrast_adequate", "min_font_readable", "color_blind_safe"],
}

_CAT_FIELDS = {
    "visual": ["color_scheme", "background_type", "visual_clutter", "focal_point", "visual_hierarchy"],
    "text_elements": ["text_readability", "font_size_hierarchy", "font_style"],
    "structural": ["price_prominence"],
    "emotional": ["tonality", "energy_level", "personalization_level"],
    "accessibility": ["information_density"],
    "platform_fit": ["thumb_stop_potential", "format_type", "first_impression_strength"],
}

# Human-readable Russian names for boolean elements
_ELEMENT_LABELS = {
    "has_faces": "лица людей",
    "rule_of_thirds": "правило третей",
    "has_urgency_words": "слова срочности",
    "has_cta_button": "CTA-кнопка",
    "has_logo": "логотип",
    "product_visible": "видимость продукта",
    "price_visible": "видимость цены",
    "before_after": "до/после",
    "safe_zones_clear": "безопасные зоны",
    "has_smiling_face": "улыбающееся лицо",
    "contrast_adequate": "достаточный контраст",
    "min_font_readable": "читаемый шрифт",
    "color_blind_safe": "доступность для дальтоников",
}

# Russian names for categorical field names
_FIELD_LABELS = {
    "color_scheme": "цветовая схема",
    "background_type": "тип фона",
    "visual_clutter": "загруженность",
    "focal_point": "фокус внимания",
    "visual_hierarchy": "визуальная иерархия",
    "text_readability": "читаемость текста",
    "font_size_hierarchy": "иерархия шрифтов",
    "font_style": "стиль шрифта",
    "price_prominence": "заметность цены",
    "tonality": "тональность",
    "energy_level": "уровень энергии",
    "personalization_level": "персонализация",
    "information_density": "плотность информации",
    "thumb_stop_potential": "потенциал остановки",
    "format_type": "формат",
    "first_impression_strength": "сила первого впечатления",
}

# Russian names for categorical values
_VALUE_LABELS = {
    # visual_clutter / information_density
    "низкая": "низкая", "средняя": "средняя", "высокая": "высокая",
    # visual_hierarchy / text_readability / font_size_hierarchy
    "сильная": "сильная", "слабая": "слабая",
    "чёткая": "чёткая", "нечёткая": "нечёткая", "отсутствует": "отсутствует",
    # focal_point
    "продукт": "продукт", "лицо": "лицо", "текст": "текст",
    "CTA": "CTA", "логотип": "логотип", "нет": "нет",
    # tonality
    "позитивная": "позитивная", "нейтральная": "нейтральная", "агрессивная": "агрессивная",
    "эмоциональная": "эмоциональная", "информационная": "информационная", "юмористическая": "юмористическая",
    # energy_level
    "низкий": "низкий", "средний": "средний", "высокий": "высокий",
    # personalization_level
    # thumb_stop_potential
    # first_impression_strength
    "сильное": "сильное", "среднее": "среднее", "слабое": "слабое",
    # color_scheme
    "тёплые": "тёплые", "холодные": "холодные", "нейтральные": "нейтральные",
    "контрастные": "контрастные", "монохромные": "монохромные", "яркие": "яркие",
    # background_type
    "фото": "фото", "градиент": "градиент", "сплошной": "сплошной",
    "абстрактный": "абстрактный", "прозрачный": "прозрачный",
    # font_style
    "sans-serif": "без засечек", "serif": "с засечками", "display": "декоративный",
    "handwritten": "рукописный", "mixed": "смешанный",
    # format_type
    "feed": "лента", "stories": "сторис", "banner": "баннер", "универсальный": "универсальный",
    # price_prominence
}


def _extract_boolean_elements(tags: dict) -> dict[str, bool]:
    """Extract all boolean tag fields as a flat dict."""
    elements: dict[str, bool] = {}
    for category, fields in _BOOL_FIELDS.items():
        cat_data = tags.get(category, {})
        for field in fields:
            if field in cat_data:
                elements[field] = bool(cat_data[field])

    for category, fields in _CAT_FIELDS.items():
        cat_data = tags.get(category, {})
        for field in fields:
            val = cat_data.get(field)
            if val:
                elements[f"{field}_{val}"] = True

    return elements


def _get_ctr(metrics: Optional[dict]) -> Optional[float]:
    """Safely extract CTR from a metrics dict, always as a fraction (0.032 = 3.2%).

    Recomputes from clicks/impressions when available (most reliable).
    Falls back to stored ctr, normalising percentage values (>1) to fractions.
    """
    if not metrics:
        return None
    # Prefer recomputing from raw counts
    try:
        impr = metrics.get("impressions")
        clicks = metrics.get("clicks")
        if impr and clicks is not None:
            impr_f = float(impr)
            if impr_f > 0:
                return float(clicks) / impr_f
    except (ValueError, TypeError):
        pass
    # Fallback to stored ctr
    ctr = metrics.get("ctr")
    if ctr is None:
        return None
    try:
        val = float(ctr)
        # Normalise: values > 1 are stored as percentages (e.g. 3.2 = 3.2%)
        if val > 1:
            val = val / 100.0
        return val
    except (ValueError, TypeError):
        return None


def _confidence_from_sample(n: int, threshold: int = 30) -> float:
    """Higher sample -> higher confidence, capped at 0.95."""
    if n <= 0:
        return 0.1
    return min(0.95, round(0.3 + 0.65 * (n / max(n, threshold)), 2))


def _confidence_with_significance(
    group_a: list[float], group_b: list[float], n_total: int,
) -> tuple[float, float]:
    """Compute confidence combining sample size and statistical significance.

    Returns (confidence 0-1, p_value).
    """
    sample_score = _confidence_from_sample(n_total)

    # Need at least 2 values in each group for t-test
    if len(group_a) < 2 or len(group_b) < 2:
        return round(sample_score * 0.5, 2), 1.0

    # Welch's t-test (unequal variances)
    try:
        from scipy.stats import ttest_ind
        _, p_value = ttest_ind(group_a, group_b, equal_var=False)
    except ImportError:
        # Fallback: manual t-test approximation
        m_a, m_b = statistics.mean(group_a), statistics.mean(group_b)
        v_a = statistics.variance(group_a) if len(group_a) > 1 else 0
        v_b = statistics.variance(group_b) if len(group_b) > 1 else 0
        se = math.sqrt(v_a / len(group_a) + v_b / len(group_b)) if (v_a + v_b) > 0 else 1
        t_stat = abs(m_a - m_b) / se if se > 0 else 0
        # Rough p-value approximation from t-statistic
        if t_stat > 3.5:
            p_value = 0.001
        elif t_stat > 2.5:
            p_value = 0.01
        elif t_stat > 2.0:
            p_value = 0.05
        elif t_stat > 1.5:
            p_value = 0.1
        else:
            p_value = 0.5
    except Exception:
        p_value = 1.0

    if p_value < 0.01:
        sig_factor = 1.0
    elif p_value < 0.05:
        sig_factor = 0.85
    elif p_value < 0.1:
        sig_factor = 0.65
    else:
        sig_factor = 0.35

    confidence = round(min(0.95, sample_score * sig_factor), 2)
    return confidence, round(p_value, 4)


def _impact_log_scale(diff_pct: float) -> float:
    """Log-scaled impact score: differentiates 50% from 200% instead of both capping at 1.0."""
    return round(min(1.0, math.log2(1 + abs(diff_pct)) / 7), 2)


def _element_label(name: str) -> str:
    """Get Russian label for element, fallback to name."""
    if name in _ELEMENT_LABELS:
        return _ELEMENT_LABELS[name]
    # Categorical elements: "field_name_value" → split into field + value
    for field_key, field_label in _FIELD_LABELS.items():
        if name.startswith(field_key + "_"):
            value = name[len(field_key) + 1:]
            value_label = _VALUE_LABELS.get(value, value)
            return f"{field_label}: {value_label}"
    return name.replace("_", " ")


# ---------------------------------------------------------------------------
# Insight generation
# ---------------------------------------------------------------------------

async def generate_insights(
    db: AsyncSession,
    tenant_id,
    project: Optional[str] = None,
) -> list[CreativeInsight]:
    """
    Analyse all banners + placements for a tenant and return a list of
    CreativeInsight objects (not yet committed to the session).
    """
    # Fetch banners for tenant (optionally filtered by project)
    banner_stmt = select(Banner).where(Banner.tenant_id == tenant_id)
    if project:
        if project == "__none__":
            banner_stmt = banner_stmt.where(Banner.project.is_(None))
        else:
            banner_stmt = banner_stmt.where(Banner.project == project)
    banners_result = await db.execute(banner_stmt)
    banners: list[Banner] = list(banners_result.scalars().all())

    # Fetch placements for the selected banners
    banner_ids = {b.id for b in banners}
    placements_result = await db.execute(
        select(CreativePlacement).where(CreativePlacement.tenant_id == tenant_id)
    )
    all_placements: list[CreativePlacement] = list(placements_result.scalars().all())
    # Filter placements to only include those for selected banners when project filter is active
    placements = [p for p in all_placements if p.creative_id in banner_ids] if project else all_placements

    # Index placements by creative_id, sorted by period_start
    placements_by_creative: dict[uuid_mod.UUID, list[CreativePlacement]] = defaultdict(list)
    for p in placements:
        placements_by_creative[p.creative_id].append(p)
    for cid in placements_by_creative:
        placements_by_creative[cid].sort(key=lambda x: x.period_start)

    # Index banners by id
    banners_by_id: dict[uuid_mod.UUID, Banner] = {b.id: b for b in banners}

    insights: list[CreativeInsight] = []

    # ---- 1. Fatigue detection ----
    insights.extend(_detect_fatigue(tenant_id, banners_by_id, placements_by_creative))

    # ---- 2. Trend detection ----
    insights.extend(_detect_trends(tenant_id, placements))

    # ---- 3. Anomaly detection ----
    insights.extend(_detect_anomalies(tenant_id, banners_by_id, placements_by_creative))

    # ---- 4. Element correlation patterns ----
    insights.extend(_detect_element_patterns(tenant_id, banners))

    # ---- 5. Opportunity detection ----
    insights.extend(_detect_opportunities(tenant_id, banners))

    logger.info(
        "Generated %d insights for tenant %s", len(insights), tenant_id,
    )
    return insights


def _detect_fatigue(
    tenant_id,
    banners_by_id: dict[uuid_mod.UUID, Banner],
    placements_by_creative: dict[uuid_mod.UUID, list[CreativePlacement]],
) -> list[CreativeInsight]:
    """For each creative with 3+ placements, check for CTR decline from peak."""
    results: list[CreativeInsight] = []

    for creative_id, pls in placements_by_creative.items():
        if len(pls) < 2:
            continue

        ctrs = []
        for p in pls:
            ctr = _get_ctr(p.metrics)
            if ctr is not None:
                ctrs.append(ctr)

        if len(ctrs) < 2:
            continue

        peak_ctr = max(ctrs)
        if peak_ctr <= 0:
            continue

        latest_ctr = ctrs[-1]
        decline_pct = (peak_ctr - latest_ctr) / peak_ctr * 100

        if decline_pct < 20:
            continue

        severity = "critical" if decline_pct > 50 else "warning"
        banner = banners_by_id.get(creative_id)
        banner_name = banner.original_filename if banner else str(creative_id)[:8]

        results.append(CreativeInsight(
            tenant_id=tenant_id,
            insight_type="fatigue_warning",
            severity=severity,
            title=f"Усталость аудитории: {banner_name}",
            description=(
                f"CTR креатива снизился на {decline_pct:.0f}% от пикового значения "
                f"({peak_ctr:.2%} → {latest_ctr:.2%}) за {len(pls)} размещений. "
                f"Рекомендуется ротация или обновление креатива."
            ),
            action_text="Заменить или обновить креатив для предотвращения дальнейшего снижения эффективности.",
            supporting_data={
                "creative_id": str(creative_id),
                "peak_ctr": round(peak_ctr, 6),
                "latest_ctr": round(latest_ctr, 6),
                "decline_pct": round(decline_pct, 1),
                "placements_count": len(pls),
                "ctr_series": [round(c, 6) for c in ctrs],
            },
            creative_ids=[creative_id],
        ))

    return results


def _detect_trends(
    tenant_id,
    placements: list[CreativePlacement],
) -> list[CreativeInsight]:
    """Look at portfolio-level CTR trend over time."""
    if len(placements) < 3:
        return []

    # Group by period_start month
    monthly: dict[str, list[float]] = defaultdict(list)
    for p in placements:
        ctr = _get_ctr(p.metrics)
        if ctr is not None:
            key = p.period_start.strftime("%Y-%m")
            monthly[key].append(ctr)

    if len(monthly) < 2:
        return []

    sorted_months = sorted(monthly.keys())
    monthly_avgs = [(m, statistics.mean(monthly[m])) for m in sorted_months]

    # Simple linear trend: compare first half avg to second half avg
    mid = len(monthly_avgs) // 2
    first_half_avg = statistics.mean([v for _, v in monthly_avgs[:mid]])
    second_half_avg = statistics.mean([v for _, v in monthly_avgs[mid:]])

    if first_half_avg <= 0:
        return []

    change_pct = (second_half_avg - first_half_avg) / first_half_avg * 100

    if abs(change_pct) < 5:
        return []

    direction = "рост" if change_pct > 0 else "снижение"
    severity = "success" if change_pct > 0 else "warning"

    total_placements = sum(len(v) for v in monthly.values())

    return [CreativeInsight(
        tenant_id=tenant_id,
        insight_type="trend",
        severity=severity,
        title=f"Тренд CTR портфеля: {direction} на {abs(change_pct):.1f}%",
        description=(
            f"Средний CTR портфеля креативов показывает {direction} "
            f"с {first_half_avg:.2%} до {second_half_avg:.2%} "
            f"за период {sorted_months[0]} — {sorted_months[-1]}."
        ),
        action_text=(
            "Продолжайте текущую стратегию." if change_pct > 0
            else "Проанализируйте причины снижения и обновите креативы."
        ),
        supporting_data={
            "monthly_series": [
                {"month": m, "avg_ctr": round(v, 6)} for m, v in monthly_avgs
            ],
            "change_pct": round(change_pct, 1),
            "total_placements": total_placements,
        },
        creative_ids=[],
    )]


def _detect_anomalies(
    tenant_id,
    banners_by_id: dict[uuid_mod.UUID, Banner],
    placements_by_creative: dict[uuid_mod.UUID, list[CreativePlacement]],
) -> list[CreativeInsight]:
    """Find creatives whose latest CTR deviates >2 stddev from their historical mean."""
    results: list[CreativeInsight] = []

    for creative_id, pls in placements_by_creative.items():
        ctrs = []
        for p in pls:
            ctr = _get_ctr(p.metrics)
            if ctr is not None:
                ctrs.append(ctr)

        if len(ctrs) < 4:
            continue

        historical = ctrs[:-1]
        latest = ctrs[-1]

        mean = statistics.mean(historical)
        try:
            stdev = statistics.stdev(historical)
        except statistics.StatisticsError:
            continue

        if stdev == 0:
            continue

        z_score = (latest - mean) / stdev

        if abs(z_score) < 2.0:
            continue

        banner = banners_by_id.get(creative_id)
        banner_name = banner.original_filename if banner else str(creative_id)[:8]
        direction = "выше" if z_score > 0 else "ниже"

        results.append(CreativeInsight(
            tenant_id=tenant_id,
            insight_type="anomaly",
            severity="warning",
            title=f"Аномалия CTR: {banner_name}",
            description=(
                f"Последний CTR ({latest:.2%}) значительно {direction} "
                f"исторического среднего ({mean:.2%}, σ={stdev:.4f}). "
                f"Z-score: {z_score:+.1f}."
            ),
            action_text=(
                "Изучите причины аномального изменения: смена аудитории, сезонность или новый контекст."
            ),
            supporting_data={
                "creative_id": str(creative_id),
                "latest_ctr": round(latest, 6),
                "mean_ctr": round(mean, 6),
                "stdev": round(stdev, 6),
                "z_score": round(z_score, 2),
                "sample_size": len(historical),
            },
            creative_ids=[creative_id],
        ))

    return results


def _detect_element_patterns(
    tenant_id,
    banners: list[Banner],
) -> list[CreativeInsight]:
    """Find tag elements that correlate with higher/lower CTR."""
    results: list[CreativeInsight] = []

    # Gather banners with both tags and CTR
    tagged_banners: list[tuple[dict, float, uuid_mod.UUID]] = []
    for b in banners:
        if b.tags_status != "done" or not b.tags:
            continue
        ctr = _get_ctr(b.metrics)
        if ctr is None:
            continue
        tagged_banners.append((b.tags, ctr, b.id))

    if len(tagged_banners) < 5:
        return []

    # Collect per-element CTR groups
    with_element: dict[str, list[float]] = defaultdict(list)
    without_element: dict[str, list[float]] = defaultdict(list)
    element_banner_ids: dict[str, list[uuid_mod.UUID]] = defaultdict(list)

    for tags, ctr, bid in tagged_banners:
        elements = _extract_boolean_elements(tags)
        for elem_name, has in elements.items():
            if has:
                with_element[elem_name].append(ctr)
                element_banner_ids[elem_name].append(bid)
            else:
                without_element[elem_name].append(ctr)

    all_ctrs_mean = statistics.mean([ctr for _, ctr, _ in tagged_banners])

    for elem_name in with_element:
        w = with_element[elem_name]
        wo = without_element.get(elem_name, [])

        if len(w) < 2 or len(wo) < 2:
            continue

        avg_w = statistics.mean(w)
        avg_wo = statistics.mean(wo)

        if avg_wo == 0:
            continue

        diff_pct = (avg_w - avg_wo) / avg_wo * 100

        if abs(diff_pct) < 10:
            continue

        label = _element_label(elem_name)
        direction = "повышают" if diff_pct > 0 else "снижают"

        # Severity based on impact magnitude
        abs_diff = abs(diff_pct)
        if abs_diff >= 100:
            severity = "critical" if diff_pct < 0 else "success"
        elif abs_diff >= 50:
            severity = "warning" if diff_pct < 0 else "success"
        else:
            severity = "info" if diff_pct < 0 else "success"

        # Confidence from t-test + sample size
        confidence, p_value = _confidence_with_significance(w, wo, len(w) + len(wo))

        # Confidence label
        if confidence >= 0.8:
            conf_label = "высокая"
        elif confidence >= 0.6:
            conf_label = "средняя"
        else:
            conf_label = "низкая"

        results.append(CreativeInsight(
            tenant_id=tenant_id,
            insight_type="pattern",
            severity=severity,
            title=f"Креативы с элементом «{label}» {direction} CTR на {abs_diff:.0f}%",
            description=(
                f"Средний CTR креативов с «{label}»: {avg_w:.2%} "
                f"(n={len(w)}) vs без: {avg_wo:.2%} (n={len(wo)}). "
                f"Разница: {diff_pct:+.1f}%. "
                f"Уверенность: {conf_label} ({confidence:.0%})."
            ),
            action_text=(
                f"{'Используйте' if diff_pct > 0 else 'Избегайте'} элемент «{label}» "
                f"в новых креативах для улучшения CTR."
            ),
            supporting_data={
                "element": elem_name,
                "avg_ctr_with": round(avg_w, 6),
                "avg_ctr_without": round(avg_wo, 6),
                "diff_pct": round(diff_pct, 1),
                "count_with": len(w),
                "count_without": len(wo),
                "confidence": confidence,
                "portfolio_avg_ctr": round(all_ctrs_mean, 6),
            },
            creative_ids=element_banner_ids.get(elem_name, [])[:10],
        ))

    return results


def _detect_opportunities(
    tenant_id,
    banners: list[Banner],
) -> list[CreativeInsight]:
    """Find untested tag combinations where similar elements performed well."""
    results: list[CreativeInsight] = []

    tagged_banners: list[tuple[dict, float, uuid_mod.UUID]] = []
    for b in banners:
        if b.tags_status != "done" or not b.tags:
            continue
        ctr = _get_ctr(b.metrics)
        if ctr is None:
            continue
        tagged_banners.append((b.tags, ctr, b.id))

    if len(tagged_banners) < 5:
        return []

    # Identify top-performing elements (boolean only for simplicity)
    element_ctrs: dict[str, list[float]] = defaultdict(list)
    element_presence: dict[str, int] = defaultdict(int)

    for tags, ctr, _ in tagged_banners:
        elements = _extract_boolean_elements(tags)
        for elem, has in elements.items():
            if has:
                element_ctrs[elem].append(ctr)
                element_presence[elem] += 1

    if not element_ctrs:
        return []

    overall_avg = statistics.mean([ctr for _, ctr, _ in tagged_banners])

    # Find top elements (>15% above average)
    top_elements: list[tuple[str, float]] = []
    for elem, ctrs in element_ctrs.items():
        if len(ctrs) < 3:
            continue
        avg = statistics.mean(ctrs)
        if overall_avg > 0 and (avg - overall_avg) / overall_avg > 0.15:
            top_elements.append((elem, avg))

    if len(top_elements) < 2:
        return []

    # Sort by CTR descending
    top_elements.sort(key=lambda x: x[1], reverse=True)

    # Check pairs of top elements that rarely appear together
    candidates = []
    for i in range(min(len(top_elements), 5)):
        for j in range(i + 1, min(len(top_elements), 5)):
            elem_a, avg_a = top_elements[i]
            elem_b, avg_b = top_elements[j]

            # Count co-occurrences
            co_count = 0
            for tags, _, _ in tagged_banners:
                elements = _extract_boolean_elements(tags)
                if elements.get(elem_a) and elements.get(elem_b):
                    co_count += 1

            if co_count >= 3:
                continue  # Already tested together sufficiently

            candidates.append((elem_a, avg_a, elem_b, avg_b, co_count))

    # Sort by combined CTR potential descending and take top 3
    candidates.sort(key=lambda x: x[1] + x[3], reverse=True)
    for elem_a, avg_a, elem_b, avg_b, co_count in candidates[:3]:
        label_a = _element_label(elem_a)
        label_b = _element_label(elem_b)

        results.append(CreativeInsight(
            tenant_id=tenant_id,
            insight_type="opportunity",
            severity="info",
            title=f"Возможность: совместить «{label_a}» и «{label_b}»",
            description=(
                f"Элементы «{label_a}» (avg CTR {avg_a:.2%}) и «{label_b}» "
                f"(avg CTR {avg_b:.2%}) оба показывают высокий CTR, но "
                f"встречаются вместе только в {co_count} креативах. "
                f"Стоит протестировать их комбинацию."
            ),
            action_text=(
                f"Создайте новый креатив, объединяющий «{label_a}» и «{label_b}»."
            ),
            supporting_data={
                "element_a": elem_a,
                "element_b": elem_b,
                "avg_ctr_a": round(avg_a, 6),
                "avg_ctr_b": round(avg_b, 6),
                "co_occurrences": co_count,
                "total_banners": len(tagged_banners),
            },
            creative_ids=[],
        ))

    return results


# ---------------------------------------------------------------------------
# Hypothesis generation
# ---------------------------------------------------------------------------

async def generate_hypotheses(
    db: AsyncSession,
    tenant_id,
    user_id,
    project: Optional[str] = None,
) -> list[Hypothesis]:
    """
    Analyse banners + placements for a tenant and return a list of
    Hypothesis objects (not yet committed).
    """
    # Fetch data (optionally filtered by project)
    banner_stmt = select(Banner).where(Banner.tenant_id == tenant_id)
    if project:
        if project == "__none__":
            banner_stmt = banner_stmt.where(Banner.project.is_(None))
        else:
            banner_stmt = banner_stmt.where(Banner.project == project)
    banners_result = await db.execute(banner_stmt)
    banners: list[Banner] = list(banners_result.scalars().all())

    banner_ids = {b.id for b in banners}
    placements_result = await db.execute(
        select(CreativePlacement).where(CreativePlacement.tenant_id == tenant_id)
    )
    all_placements: list[CreativePlacement] = list(placements_result.scalars().all())
    placements = [p for p in all_placements if p.creative_id in banner_ids] if project else all_placements

    placements_by_creative: dict[uuid_mod.UUID, list[CreativePlacement]] = defaultdict(list)
    for p in placements:
        placements_by_creative[p.creative_id].append(p)
    for cid in placements_by_creative:
        placements_by_creative[cid].sort(key=lambda x: x.period_start)

    hypotheses: list[Hypothesis] = []

    # ---- 1. Element impact ----
    hypotheses.extend(_hypothesize_element_impact(tenant_id, user_id, banners))

    # ---- 2. Fatigue pattern ----
    hypotheses.extend(_hypothesize_fatigue_pattern(tenant_id, user_id, placements_by_creative))

    # ---- 3. Format comparison ----
    hypotheses.extend(_hypothesize_format_comparison(tenant_id, user_id, banners))

    logger.info(
        "Generated %d hypotheses for tenant %s", len(hypotheses), tenant_id,
    )
    return hypotheses


def _hypothesize_element_impact(
    tenant_id,
    user_id,
    banners: list[Banner],
) -> list[Hypothesis]:
    """For boolean elements with sufficient sample, create impact hypotheses."""
    results: list[Hypothesis] = []

    with_element: dict[str, list[float]] = defaultdict(list)
    without_element: dict[str, list[float]] = defaultdict(list)

    for b in banners:
        if b.tags_status != "done" or not b.tags:
            continue
        ctr = _get_ctr(b.metrics)
        if ctr is None:
            continue
        elements = _extract_boolean_elements(b.tags)
        for elem_name, has in elements.items():
            if has:
                with_element[elem_name].append(ctr)
            else:
                without_element[elem_name].append(ctr)

    for elem_name in with_element:
        w = with_element[elem_name]
        wo = without_element.get(elem_name, [])

        if len(w) < 2 or len(wo) < 2:
            continue

        avg_w = statistics.mean(w)
        avg_wo = statistics.mean(wo)

        if avg_wo == 0:
            continue

        diff_pct = (avg_w - avg_wo) / avg_wo * 100

        if abs(diff_pct) < 10:
            continue

        label = _element_label(elem_name)
        direction = "повышает" if diff_pct > 0 else "снижает"
        sample_total = len(w) + len(wo)
        confidence, p_value = _confidence_with_significance(w, wo, sample_total)
        impact = _impact_log_scale(diff_pct)

        results.append(Hypothesis(
            tenant_id=tenant_id,
            user_id=user_id,
            title=f"Элемент «{label}» {direction} CTR на {abs(diff_pct):.0f}%",
            description=(
                f"Анализ {sample_total} креативов показывает, что наличие элемента "
                f"«{label}» {direction} средний CTR с {avg_wo:.2%} до {avg_w:.2%} "
                f"(разница {diff_pct:+.1f}%). "
                f"Выборка: {len(w)} с элементом, {len(wo)} без."
            ),
            hypothesis_type="element_impact",
            status="proposed",
            confidence=confidence,
            impact_score=impact,
            supporting_data={
                "element": elem_name,
                "avg_ctr_with": round(avg_w, 6),
                "avg_ctr_without": round(avg_wo, 6),
                "diff_pct": round(diff_pct, 1),
                "count_with": len(w),
                "count_without": len(wo),
                "p_value": p_value,
            },
            tags=[elem_name, "ctr", "element_impact"],
            source="auto",
        ))

    return results


def _hypothesize_fatigue_pattern(
    tenant_id,
    user_id,
    placements_by_creative: dict[uuid_mod.UUID, list[CreativePlacement]],
) -> list[Hypothesis]:
    """For creatives with declining placements, hypothesize optimal rotation period."""
    results: list[Hypothesis] = []

    decline_days: list[int] = []
    creative_samples: list[dict] = []

    for creative_id, pls in placements_by_creative.items():
        if len(pls) < 2:
            continue

        ctrs_with_dates: list[tuple[datetime, float]] = []
        for p in pls:
            ctr = _get_ctr(p.metrics)
            if ctr is not None:
                ctrs_with_dates.append((
                    datetime.combine(p.period_start, datetime.min.time()),
                    ctr,
                ))

        if len(ctrs_with_dates) < 2:
            continue

        peak_idx = 0
        peak_ctr = ctrs_with_dates[0][1]
        for i, (_, ctr) in enumerate(ctrs_with_dates):
            if ctr > peak_ctr:
                peak_ctr = ctr
                peak_idx = i

        # Check decline after peak
        latest_ctr = ctrs_with_dates[-1][1]
        if peak_ctr <= 0:
            continue
        decline_pct = (peak_ctr - latest_ctr) / peak_ctr * 100

        if decline_pct < 20:
            continue

        # Days from first placement to latest
        first_date = ctrs_with_dates[0][0]
        days_to_fatigue = (ctrs_with_dates[-1][0] - first_date).days

        if days_to_fatigue > 0:
            decline_days.append(days_to_fatigue)
            creative_samples.append({
                "creative_id": str(creative_id),
                "days": days_to_fatigue,
                "decline_pct": round(decline_pct, 1),
            })

    if len(decline_days) < 1:
        return []

    avg_days = statistics.mean(decline_days)
    median_days = statistics.median(decline_days)
    confidence = _confidence_from_sample(len(decline_days), threshold=10)

    results.append(Hypothesis(
        tenant_id=tenant_id,
        user_id=user_id,
        title=f"Оптимальный период ротации креативов: ~{median_days:.0f} дней",
        description=(
            f"Анализ {len(decline_days)} креативов с признаками усталости показывает, "
            f"что значительное снижение CTR (>20%) наступает в среднем через "
            f"{avg_days:.0f} дней (медиана: {median_days:.0f} дней). "
            f"Рекомендуется обновлять креативы до наступления усталости."
        ),
        hypothesis_type="fatigue_pattern",
        status="proposed",
        confidence=confidence,
        impact_score=min(1.0, round(0.5 + len(decline_days) / 20, 2)),
        supporting_data={
            "avg_days_to_fatigue": round(avg_days, 1),
            "median_days_to_fatigue": round(median_days, 1),
            "sample_size": len(decline_days),
            "samples": creative_samples[:10],
        },
        tags=["fatigue", "rotation", "ctr"],
        source="auto",
    ))

    return results


def _hypothesize_format_comparison(
    tenant_id,
    user_id,
    banners: list[Banner],
) -> list[Hypothesis]:
    """Compare CTR across different platform_fit.format_type values."""
    results: list[Hypothesis] = []

    format_ctrs: dict[str, list[float]] = defaultdict(list)

    for b in banners:
        if b.tags_status != "done" or not b.tags:
            continue
        ctr = _get_ctr(b.metrics)
        if ctr is None:
            continue

        platform_fit = b.tags.get("platform_fit", {})
        format_type = platform_fit.get("format_type")
        if not format_type:
            continue

        format_ctrs[format_type].append(ctr)

    # Need at least 2 formats with sufficient data
    valid_formats = {
        fmt: ctrs for fmt, ctrs in format_ctrs.items() if len(ctrs) >= 2
    }

    if len(valid_formats) < 2:
        return []

    format_avgs = {
        fmt: statistics.mean(ctrs) for fmt, ctrs in valid_formats.items()
    }

    best_format = max(format_avgs, key=format_avgs.get)
    worst_format = min(format_avgs, key=format_avgs.get)

    best_avg = format_avgs[best_format]
    worst_avg = format_avgs[worst_format]

    if worst_avg <= 0:
        return []

    diff_pct = (best_avg - worst_avg) / worst_avg * 100

    if diff_pct < 10:
        return []

    total_sample = sum(len(v) for v in valid_formats.values())
    confidence, p_value = _confidence_with_significance(
        valid_formats[best_format], valid_formats[worst_format], total_sample,
    )
    impact = _impact_log_scale(diff_pct)

    results.append(Hypothesis(
        tenant_id=tenant_id,
        user_id=user_id,
        title=f"Формат «{best_format}» эффективнее «{worst_format}» на {diff_pct:.0f}%",
        description=(
            f"Сравнение {len(valid_formats)} форматов креативов показывает, что "
            f"«{best_format}» (avg CTR {best_avg:.2%}, n={len(valid_formats[best_format])}) "
            f"превосходит «{worst_format}» (avg CTR {worst_avg:.2%}, "
            f"n={len(valid_formats[worst_format])}) на {diff_pct:.0f}%. "
            f"Рекомендуется увеличить долю формата «{best_format}»."
        ),
        hypothesis_type="format_comparison",
        status="proposed",
        confidence=confidence,
        impact_score=impact,
        supporting_data={
            "formats": {
                fmt: {
                    "avg_ctr": round(avg, 6),
                    "count": len(valid_formats[fmt]),
                }
                for fmt, avg in format_avgs.items()
            },
            "best_format": best_format,
            "worst_format": worst_format,
            "diff_pct": round(diff_pct, 1),
            "p_value": p_value,
        },
        tags=["format", "comparison", "ctr"],
        source="auto",
    ))

    return results
