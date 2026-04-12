"""Predictive CTR scoring — predict CTR for banners based on element performance data."""

import logging
import uuid as uuid_mod
from typing import Optional

import numpy as np
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user, CurrentUser
from database import get_db
from db_models import Banner
from adscore import _extract_boolean_elements, _compute_element_performance, _load_tenant_banners_data

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/predictive-ctr", tags=["predictive-ctr"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class ElementContribution(BaseModel):
    element: str
    category: str
    present: bool
    delta: float  # absolute CTR delta
    delta_pct: float  # percentage delta
    correlation: float
    p_value: float
    direction: str  # "positive" | "negative" | "neutral"
    label: str  # human-readable Russian label


class PredictionResponse(BaseModel):
    banner_id: str
    banner_filename: str
    predicted_ctr: float  # predicted CTR as fraction (0.032 = 3.2%)
    predicted_ctr_pct: str  # formatted string "3.2%"
    confidence: str  # "высокая" | "средняя" | "низкая"
    confidence_reason: str
    baseline_ctr: float  # average CTR across all banners
    baseline_ctr_pct: str
    ctr_range_low: float
    ctr_range_high: float
    ctr_range_low_pct: str
    ctr_range_high_pct: str
    percentile_rank: int  # estimated percentile (0-100)
    category: str  # "Лидер" | "Выше среднего" | "Средний" | "Ниже среднего" | "Аутсайдер"
    positive_elements: list[ElementContribution]
    negative_elements: list[ElementContribution]
    neutral_elements: list[ElementContribution]
    recommendations: list[str]
    n_banners_in_model: int
    platform: Optional[str] = None


# ---------------------------------------------------------------------------
# Element name translations
# ---------------------------------------------------------------------------

ELEMENT_LABELS = {
    "has_faces": "Лица людей",
    "rule_of_thirds": "Правило третей",
    "has_urgency_words": "Слова срочности",
    "has_cta_button": "CTA-кнопка",
    "has_logo": "Логотип",
    "product_visible": "Видимость продукта",
    "price_visible": "Видимость цены",
    "before_after": "До/После",
    "safe_zones_clear": "Чистые safe-зоны",
    "has_smiling_face": "Улыбающееся лицо",
    "contrast_adequate": "Достаточный контраст",
    "min_font_readable": "Читаемый шрифт",
    "color_blind_safe": "Доступность для дальтоников",
    # Categorical elements
    "color_scheme_тёплая": "Тёплая цветовая схема",
    "color_scheme_холодная": "Холодная цветовая схема",
    "color_scheme_яркая": "Яркая цветовая схема",
    "color_scheme_нейтральная": "Нейтральная цветовая схема",
    "color_scheme_тёмная": "Тёмная цветовая схема",
    "color_scheme_пастельная": "Пастельная цветовая схема",
    "visual_clutter_низкая": "Низкая визуальная загруженность",
    "visual_clutter_средняя": "Средняя визуальная загруженность",
    "visual_clutter_высокая": "Высокая визуальная загруженность",
    "focal_point_продукт": "Фокус на продукте",
    "focal_point_лицо": "Фокус на лице",
    "focal_point_текст": "Фокус на тексте",
    "focal_point_CTA": "Фокус на CTA",
    "visual_hierarchy_сильная": "Сильная визуальная иерархия",
    "visual_hierarchy_средняя": "Средняя визуальная иерархия",
    "visual_hierarchy_слабая": "Слабая визуальная иерархия",
    "text_readability_высокая": "Высокая читаемость текста",
    "text_readability_средняя": "Средняя читаемость текста",
    "text_readability_низкая": "Низкая читаемость текста",
    "thumb_stop_potential_высокий": "Высокий thumb-stop",
    "thumb_stop_potential_средний": "Средний thumb-stop",
    "thumb_stop_potential_низкий": "Низкий thumb-stop",
    "first_impression_strength_сильное": "Сильное первое впечатление",
    "first_impression_strength_среднее": "Среднее первое впечатление",
    "first_impression_strength_слабое": "Слабое первое впечатление",
    "tonality_позитивная": "Позитивная тональность",
    "tonality_нейтральная": "Нейтральная тональность",
    "tonality_профессиональная": "Профессиональная тональность",
    "tonality_игривая": "Игривая тональность",
    "tonality_премиальная": "Премиальная тональность",
    "tonality_тревожная": "Тревожная тональность",
    "energy_level_высокая": "Высокая энергетика",
    "energy_level_средняя": "Средняя энергетика",
    "energy_level_низкая": "Низкая энергетика",
    "personalization_level_высокая": "Высокая персонализация",
    "personalization_level_средняя": "Средняя персонализация",
    "personalization_level_низкая": "Низкая персонализация",
    "font_style_sans-serif": "Шрифт sans-serif",
    "font_style_serif": "Шрифт serif",
    "font_style_display": "Декоративный шрифт",
    "font_size_hierarchy_чёткая": "Чёткая иерархия шрифтов",
    "font_size_hierarchy_нечёткая": "Нечёткая иерархия шрифтов",
    "background_type_фото": "Фото-фон",
    "background_type_сплошной цвет": "Сплошной цвет фона",
    "background_type_градиент": "Градиентный фон",
    "format_type_feed": "Формат: лента",
    "format_type_stories": "Формат: сторис",
    "format_type_banner": "Формат: баннер",
    "format_type_универсальный": "Универсальный формат",
    "price_prominence_высокая": "Высокая заметность цены",
    "price_prominence_средняя": "Средняя заметность цены",
    "price_prominence_низкая": "Низкая заметность цены",
    "information_density_низкая": "Низкая информационная плотность",
    "information_density_средняя": "Средняя информационная плотность",
    "information_density_высокая": "Высокая информационная плотность",
}

# Recommendation templates based on missing positive elements
RECO_TEMPLATES = {
    "has_cta_button": "Добавьте CTA-кнопку — баннеры с кнопкой показывают CTR на {delta_pct}% выше",
    "has_faces": "Добавьте лица людей — это повышает CTR на {delta_pct}%",
    "has_smiling_face": "Используйте улыбающееся лицо — это повышает CTR на {delta_pct}%",
    "product_visible": "Сделайте продукт видимым — это повышает CTR на {delta_pct}%",
    "has_urgency_words": "Добавьте слова срочности (Скидка, Только сегодня) — это повышает CTR на {delta_pct}%",
    "has_logo": "Добавьте логотип бренда — это повышает CTR на {delta_pct}%",
    "contrast_adequate": "Улучшите контрастность — читаемые баннеры показывают CTR на {delta_pct}% выше",
    "rule_of_thirds": "Примените правило третей для композиции — это повышает CTR на {delta_pct}%",
    "safe_zones_clear": "Убедитесь, что safe-зоны чистые — это повышает CTR на {delta_pct}%",
}


def _get_ctr_value(metrics: dict) -> Optional[float]:
    """Extract normalised CTR from metrics dict."""
    impr = metrics.get("impressions")
    clicks = metrics.get("clicks")
    if impr and float(impr) > 0 and clicks is not None:
        return float(clicks) / float(impr)
    ctr = metrics.get("ctr")
    if ctr is not None:
        fval = float(ctr)
        return fval / 100.0 if fval > 1 else fval
    return None


def _predict_ctr(banner_tags: dict, element_perf: list, all_ctrs: list[float]) -> dict:
    """
    Predict CTR based on element performance model.

    Algorithm:
    1. Compute baseline CTR (median of all banners — more robust than mean)
    2. For each element present in the banner:
       - If element has significant correlation (p < 0.1), add its weighted delta
       - Weight = |correlation| * delta (stronger correlations contribute more)
    3. Clamp result to [0, max_observed_ctr * 1.2]
    4. Compute confidence based on # of significant elements and model size
    """
    if not all_ctrs:
        return None

    baseline = float(np.median(all_ctrs))
    mean_ctr = float(np.mean(all_ctrs))
    std_ctr = float(np.std(all_ctrs)) if len(all_ctrs) > 1 else baseline * 0.3

    banner_elements = _extract_boolean_elements(banner_tags)

    # Build element contribution list
    positive = []
    negative = []
    neutral = []
    adjustment = 0.0
    significant_count = 0

    elem_perf_map = {ep.element_name: ep for ep in element_perf}

    for elem_name, ep in elem_perf_map.items():
        ctr_stats = ep.metrics.get("ctr")
        if not ctr_stats:
            continue

        present = banner_elements.get(elem_name, False)
        label = ELEMENT_LABELS.get(elem_name, elem_name.replace("_", " ").title())

        contrib = ElementContribution(
            element=elem_name,
            category=ep.element_category,
            present=present,
            delta=ctr_stats.delta,
            delta_pct=ctr_stats.delta_pct,
            correlation=ctr_stats.correlation,
            p_value=ctr_stats.p_value,
            direction="neutral",
            label=label,
        )

        is_significant = ctr_stats.p_value < 0.15 and (ep.n_with >= 2 and ep.n_without >= 2)

        if is_significant:
            significant_count += 1
            if present:
                # Element is present — add its delta contribution
                weight = min(abs(ctr_stats.correlation), 1.0)
                adjustment += ctr_stats.delta * weight
            else:
                # Element is absent — consider the inverse
                weight = min(abs(ctr_stats.correlation), 1.0)
                adjustment -= ctr_stats.delta * weight * 0.3  # partial penalty for absence

        if ctr_stats.delta > 0.0005:
            contrib.direction = "positive"
            if present:
                positive.append(contrib)
            elif is_significant:
                negative.append(contrib)  # missing a positive element
        elif ctr_stats.delta < -0.0005:
            contrib.direction = "negative"
            if present:
                negative.append(contrib)
            else:
                positive.append(contrib)  # avoiding a negative element
        else:
            neutral.append(contrib)

    # Predicted CTR
    predicted = baseline + adjustment
    predicted = max(0.0001, min(predicted, max(all_ctrs) * 1.3))

    # Confidence interval
    uncertainty = std_ctr * (1.5 if significant_count < 3 else 1.0 if significant_count < 6 else 0.7)
    ctr_low = max(0.0, predicted - uncertainty)
    ctr_high = min(1.0, predicted + uncertainty)

    # Confidence level
    n_banners = len(all_ctrs)
    if n_banners >= 20 and significant_count >= 5:
        confidence = "высокая"
        confidence_reason = f"Модель построена на {n_banners} баннерах, {significant_count} значимых элементов"
    elif n_banners >= 8 and significant_count >= 2:
        confidence = "средняя"
        confidence_reason = f"Модель на {n_banners} баннерах, {significant_count} значимых элементов — загрузите больше данных для точности"
    else:
        confidence = "низкая"
        confidence_reason = f"Мало данных ({n_banners} баннеров, {significant_count} значимых элементов) — прогноз ориентировочный"

    # Percentile rank
    percentile = int(np.searchsorted(np.sort(all_ctrs), predicted) / len(all_ctrs) * 100)
    percentile = min(99, max(1, percentile))

    # Category
    if percentile >= 80:
        category = "Лидер"
    elif percentile >= 60:
        category = "Выше среднего"
    elif percentile >= 40:
        category = "Средний"
    elif percentile >= 20:
        category = "Ниже среднего"
    else:
        category = "Аутсайдер"

    # Sort contributions by absolute delta (most impactful first)
    positive.sort(key=lambda x: abs(x.delta), reverse=True)
    negative.sort(key=lambda x: abs(x.delta), reverse=True)

    # Recommendations
    recommendations = []
    for neg in negative[:5]:
        if neg.direction == "positive" and not neg.present:
            # Missing a positive element
            tpl = RECO_TEMPLATES.get(neg.element)
            if tpl:
                recommendations.append(tpl.format(delta_pct=abs(round(neg.delta_pct, 1))))
            else:
                recommendations.append(f"Добавьте «{neg.label}» — это может повысить CTR на {abs(round(neg.delta_pct, 1))}%")
        elif neg.direction == "negative" and neg.present:
            # Has a negative element
            recommendations.append(f"Уберите/измените «{neg.label}» — этот элемент снижает CTR на {abs(round(neg.delta_pct, 1))}%")

    # Add general recommendations if few specific ones
    if len(recommendations) < 2:
        if not banner_elements.get("has_cta_button"):
            recommendations.append("Добавьте CTA-кнопку — один из самых важных элементов для кликабельности")
        if banner_elements.get("visual_clutter_высокая", False):
            recommendations.append("Снизьте визуальную загруженность — чистые баннеры привлекают больше внимания")

    def fmt(val: float) -> str:
        return f"{val * 100:.2f}%"

    return {
        "predicted_ctr": round(predicted, 6),
        "predicted_ctr_pct": fmt(predicted),
        "confidence": confidence,
        "confidence_reason": confidence_reason,
        "baseline_ctr": round(mean_ctr, 6),
        "baseline_ctr_pct": fmt(mean_ctr),
        "ctr_range_low": round(ctr_low, 6),
        "ctr_range_high": round(ctr_high, 6),
        "ctr_range_low_pct": fmt(ctr_low),
        "ctr_range_high_pct": fmt(ctr_high),
        "percentile_rank": percentile,
        "category": category,
        "positive_elements": positive[:10],
        "negative_elements": negative[:10],
        "neutral_elements": neutral[:5],
        "recommendations": recommendations[:6],
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/predict/{banner_id}")
async def predict_ctr(
    banner_id: str,
    platform: Optional[str] = Query(None, description="Filter by platform"),
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    """Predict CTR for a specific banner based on historical element performance."""
    tid = current_user.tenant.id

    # Load target banner
    result = await db.execute(
        select(Banner).where(Banner.id == banner_id, Banner.tenant_id == tid)
    )
    banner = result.scalar_one_or_none()
    if not banner:
        raise HTTPException(status_code=404, detail="Banner not found")

    if banner.tags_status != "done" or not banner.tags:
        raise HTTPException(
            status_code=400,
            detail="Banner must be tagged first. Tags status: " + (banner.tags_status or "unknown"),
        )

    # Load all tenant banners for the model
    banners_data = await _load_tenant_banners_data(db, tid)

    # Compute element performance
    element_perf = _compute_element_performance(banners_data, platform_filter=platform)

    # Collect all CTR values for baseline
    all_ctrs = []
    for b in banners_data:
        ctr = _get_ctr_value(b.get("metrics", {}))
        if ctr is not None and ctr > 0:
            if platform and b.get("metrics", {}).get("platform") != platform:
                continue
            all_ctrs.append(ctr)

    if len(all_ctrs) < 3:
        raise HTTPException(
            status_code=400,
            detail=f"Недостаточно данных для прогноза. Нужно минимум 3 баннера с метриками CTR, найдено: {len(all_ctrs)}",
        )

    prediction = _predict_ctr(banner.tags, element_perf, all_ctrs)
    if prediction is None:
        raise HTTPException(status_code=400, detail="Не удалось построить прогноз")

    return PredictionResponse(
        banner_id=banner_id,
        banner_filename=banner.original_filename or "",
        n_banners_in_model=len(all_ctrs),
        platform=platform,
        **prediction,
    )


@router.get("/banners")
async def list_predictable_banners(
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
):
    """List banners available for prediction (tagged, with or without metrics)."""
    tid = current_user.tenant.id

    result = await db.execute(
        select(Banner)
        .where(Banner.tenant_id == tid, Banner.tags_status == "done")
        .order_by(Banner.created_at.desc())
        .limit(200)
    )
    banners = result.scalars().all()

    items = []
    for b in banners:
        metrics = b.metrics or {}
        ctr = _get_ctr_value(metrics)
        items.append({
            "id": str(b.id),
            "filename": b.original_filename or "",
            "has_metrics": ctr is not None,
            "actual_ctr": round(ctr * 100, 2) if ctr else None,
            "impressions": metrics.get("impressions"),
            "platform": metrics.get("platform"),
            "project": b.project,
        })

    # Count banners with CTR for model info
    n_with_ctr = sum(1 for item in items if item["has_metrics"])

    return {
        "banners": items,
        "n_with_ctr": n_with_ctr,
        "min_required": 3,
        "can_predict": n_with_ctr >= 3,
    }
