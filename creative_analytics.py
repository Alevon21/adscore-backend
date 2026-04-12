from __future__ import annotations

"""
Creative analytics endpoints:
 - Auto-brief generation
 - Budget / scaling recommendations
 - A/B test designer CRUD
 - Creative versioning (concept groups)
 - Audience × Creative matrix
 - Stakeholder report
"""

import uuid as uuid_mod
import math
import logging
import statistics
from datetime import datetime, timezone
from typing import Optional
from collections import defaultdict

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from auth import get_current_user, CurrentUser
from db_models import Banner
from creative_history_models import (
    CreativePlacement, Hypothesis, CreativeInsight,
    ABTest, ABTestCreate, ABTestRecord, ABTestListResponse,
)
import storage as file_storage
from platform_ad_specs import (
    PLATFORMS as _ALL_PLATFORMS,
    UNIVERSAL_FORMATS as _UNIVERSAL_FORMATS,
    TECH_CONSTANTS as _TECH_CONSTANTS,
    get_platforms_by_region,
)

logger = logging.getLogger(__name__)

analytics_router = APIRouter(prefix="/adscore", tags=["creative_analytics"])


# ===========================================================================
# Helpers
# ===========================================================================

def _get_ctr(metrics: Optional[dict]) -> Optional[float]:
    """Safely extract CTR as a fraction (0.032 = 3.2%).

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


def _banner_project_filter(stmt, project: Optional[str]):
    if project == "__none__":
        return stmt.where(Banner.project.is_(None))
    elif project:
        return stmt.where(Banner.project == project)
    return stmt


_ELEMENT_LABELS = {
    "has_faces": "Лица людей",
    "rule_of_thirds": "Правило третей",
    "has_urgency_words": "Слова срочности",
    "has_cta_button": "CTA-кнопка",
    "has_logo": "Логотип",
    "product_visible": "Видимость продукта",
    "price_visible": "Видимость цены",
    "before_after": "До/После",
    "safe_zones_clear": "Безопасные зоны",
    "has_smiling_face": "Улыбающееся лицо",
    "contrast_adequate": "Достаточный контраст",
    "min_font_readable": "Читаемый шрифт",
    "color_blind_safe": "Доступность для дальтоников",
    # Categorical element labels (value-specific)
    "tonality:позитивная": "Позитивная тональность",
    "tonality:нейтральная": "Нейтральная тональность",
    "tonality:провокационная": "Провокационная тональность",
    "energy_level:высокий": "Высокая энергия",
    "energy_level:средний": "Средняя энергия",
    "energy_level:низкий": "Низкая энергия",
    "color_scheme:яркие": "Яркие цвета",
    "color_scheme:приглушённые": "Приглушённые цвета",
    "color_scheme:монохром": "Монохромная палитра",
    "color_scheme:контрастные": "Контрастные цвета",
    "background_type:фото": "Фото-фон",
    "background_type:градиент": "Градиентный фон",
    "background_type:однотонный": "Однотонный фон",
    "background_type:абстрактный": "Абстрактный фон",
    "cta_position:центр": "CTA по центру",
    "cta_position:справа внизу": "CTA справа внизу",
    "cta_position:слева внизу": "CTA слева внизу",
    "visual_hierarchy:сильная": "Сильная визуальная иерархия",
    "visual_hierarchy:слабая": "Слабая визуальная иерархия",
    "visual_clutter:низкая": "Минимум визуального шума",
    "visual_clutter:высокая": "Перегруженный макет",
    "text_readability:высокая": "Высокая читаемость",
    "text_readability:низкая": "Низкая читаемость",
    "thumb_stop_potential:высокий": "Высокий thumb-stop",
    "thumb_stop_potential:низкий": "Низкий thumb-stop",
    "personalization_level:высокая": "Высокая персонализация",
    "focal_point:продукт": "Фокус на продукте",
    "focal_point:лицо": "Фокус на лице",
    "focal_point:текст": "Фокус на тексте",
    "focal_point:CTA": "Фокус на CTA",
}

# Fallback field/value translation for composite element keys not in _ELEMENT_LABELS
_BRIEF_FIELD_LABELS = {
    "color_scheme": "Цветовая схема",
    "background_type": "Тип фона",
    "visual_clutter": "Загруженность",
    "focal_point": "Фокус внимания",
    "visual_hierarchy": "Визуальная иерархия",
    "text_readability": "Читаемость текста",
    "font_size_hierarchy": "Иерархия шрифтов",
    "font_style": "Стиль шрифта",
    "cta_position": "Позиция CTA",
    "tonality": "Тональность",
    "energy_level": "Уровень энергии",
    "personalization_level": "Персонализация",
    "thumb_stop_potential": "Стоп-скролл потенциал",
    "format_type": "Формат",
    "first_impression_strength": "Сила первого впечатления",
}

# Emotional trigger translations
_TRIGGER_LABELS = {
    "FOMO": "FOMO (страх упустить)",
    "social_proof": "социальное доказательство",
    "scarcity": "дефицит",
    "exclusivity": "эксклюзивность",
    "curiosity": "любопытство",
    "urgency": "срочность",
    "fear": "страх",
    "joy": "радость",
    "trust": "доверие",
    "nostalgia": "ностальгия",
}


def _element_label_ru(name: str) -> str:
    """Translate element key to Russian label."""
    if name in _ELEMENT_LABELS:
        return _ELEMENT_LABELS[name]
    # Try field:value format
    if ":" in name:
        field, value = name.split(":", 1)
        field_label = _BRIEF_FIELD_LABELS.get(field, field.replace("_", " ").title())
        return f"{field_label}: {value.strip()}"
    # Try field_value format (underscore-separated)
    for field_key, field_label in _BRIEF_FIELD_LABELS.items():
        if name.startswith(field_key + "_"):
            value = name[len(field_key) + 1:]
            return f"{field_label}: {value}"
    return name.replace("_", " ").title()


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

# Categorical fields to extract as "field:value" → True/False per banner
_CATEGORICAL_FIELDS = {
    "visual": ["background_type", "color_scheme", "visual_clutter", "focal_point",
               "visual_hierarchy"],
    "text_elements": ["text_readability", "font_style"],
    "structural": ["cta_position"],
    "emotional": ["tonality", "energy_level", "personalization_level"],
    "platform_fit": ["thumb_stop_potential", "format_type"],
}


def _extract_elements(tags: dict) -> dict[str, bool]:
    out: dict[str, bool] = {}
    # Boolean fields
    for cat, fields in _BOOL_FIELDS.items():
        cat_data = tags.get(cat, {})
        for f in fields:
            if f in cat_data:
                out[f] = bool(cat_data[f])
    # Categorical fields → "field:value" keys
    for cat, fields in _CATEGORICAL_FIELDS.items():
        cat_data = tags.get(cat, {})
        for f in fields:
            val = cat_data.get(f)
            if val and isinstance(val, str):
                out[f"{f}:{val}"] = True
    return out


def _aggregate_tag_field(banner_list, category: str, fields: list[str]) -> dict:
    """Aggregate tag fields across banners: for string values return most common,
    for numeric return avg."""
    result: dict = {}
    for field in fields:
        values = []
        for b in banner_list:
            cat_data = (b.tags or {}).get(category, {})
            v = cat_data.get(field)
            if v is not None:
                values.append(v)
        if not values:
            continue
        if isinstance(values[0], (int, float)):
            result[field] = round(statistics.mean(values), 2)
        else:
            # Most common string value
            counts: dict = defaultdict(int)
            for v in values:
                counts[str(v)] += 1
            best = max(counts.items(), key=lambda x: x[1])
            result[field] = {"value": best[0], "count": best[1], "total": len(values)}
    return result


# ---------------------------------------------------------------------------
# Platform ad specs — static knowledge base
# ---------------------------------------------------------------------------

_PLATFORM_AD_SPECS = _ALL_PLATFORMS  # Full 53-platform reference from platform_ad_specs.py


def _generate_creative_variants(source_banners, must_have: list[dict] | None = None) -> list[dict]:
    """Generate creative variants from top banners — framed as creative directions."""
    if not source_banners:
        return []

    # Compute average presence rate for boolean/categorical elements across all source banners
    element_presence: dict[str, int] = defaultdict(int)
    total_banners = len(source_banners)
    for b in source_banners:
        elems = _extract_elements(b.tags or {})
        for k, v in elems.items():
            if v:
                element_presence[k] += 1
    avg_rates = {k: cnt / total_banners for k, cnt in element_presence.items()}

    # Build set of positively-correlated elements from must_have
    positive_elements = set()
    if must_have:
        for entry in must_have:
            positive_elements.add(entry.get("element", ""))

    variants = []
    for i, b in enumerate(source_banners[:3]):
        tags = b.tags or {}
        vis = tags.get("visual", {})
        te = tags.get("text_elements", {})
        em = tags.get("emotional", {})
        st = tags.get("structural", {})
        pf = tags.get("platform_fit", {})
        acc = tags.get("accessibility", {})

        # Build variant name from headline or tonality
        headline = te.get("headline", "")
        tonality_val = em.get("tonality", "")
        energy = em.get("energy_level", "")

        # Derive a variant name
        if headline:
            name = headline[:50]
        elif tonality_val:
            name = f"Вариант «{tonality_val}»"
        else:
            name = f"Вариант {i + 1}"

        # --- Direction: combine tonality + focal_point + strongest element ---
        fp = vis.get("focal_point", "")
        banner_elements = _extract_elements(tags)
        # Find the element in this banner most above average
        strongest_elem = ""
        best_delta = -1.0
        for elem_key, has_it in banner_elements.items():
            if not has_it:
                continue
            avg_rate = avg_rates.get(elem_key, 0)
            # Elements unique to this banner (low avg presence) are most distinctive
            delta = 1.0 - avg_rate  # higher = rarer across corpus
            if delta > best_delta:
                best_delta = delta
                strongest_elem = elem_key

        direction_parts = []
        if tonality_val:
            direction_parts.append(tonality_val.capitalize())
        if fp:
            direction_parts.append(f"с фокусом на {fp}" if not fp.startswith("на") else f"с фокусом {fp}")
        if strongest_elem:
            direction_parts.append(_element_label_ru(strongest_elem).lower())
        if direction_parts:
            direction = " ".join(direction_parts[:3]).rstrip(".")
        else:
            direction = f"Креативное направление {i + 1}"

        # --- Key elements: 2-3 tags that distinguish this banner from others ---
        banner_active = {k for k, v in banner_elements.items() if v}
        distinctiveness = []
        for elem in banner_active:
            avg_rate = avg_rates.get(elem, 0)
            # Lower average presence = more distinctive to this banner
            distinctiveness.append((elem, avg_rate))
        distinctiveness.sort(key=lambda x: x[1])  # least common first
        key_elements = [_element_label_ru(e) for e, _ in distinctiveness[:3]]

        # --- What to keep: elements this banner has that positively correlate with CTR ---
        what_to_keep = []
        if positive_elements:
            for elem in banner_active:
                if elem in positive_elements:
                    what_to_keep.append(_element_label_ru(elem))
            what_to_keep = what_to_keep[:3]
        if not what_to_keep:
            # Fallback: use the most common elements in this banner
            common = [(e, avg_rates.get(e, 0)) for e in banner_active]
            common.sort(key=lambda x: -x[1])
            what_to_keep = [_element_label_ru(e) for e, _ in common[:3]]

        # Build visual description
        visual_desc_parts = []
        bg = vis.get("background_type", "")
        if bg:
            visual_desc_parts.append(f"фон — {bg}")
        n_people = vis.get("n_people", 0)
        has_faces = vis.get("has_faces", False)
        if has_faces and n_people:
            visual_desc_parts.append(f"{n_people} чел. в кадре" if n_people > 1 else "человек в кадре")
        elif has_faces:
            visual_desc_parts.append("люди в кадре")
        if fp:
            visual_desc_parts.append(f"фокус — {fp}")
        product = st.get("product_visible", False)
        if product:
            visual_desc_parts.append("продукт виден")
        visual_description = "; ".join(visual_desc_parts) if visual_desc_parts else "—"

        # Atmosphere
        atmo_parts = []
        if tonality_val:
            atmo_parts.append(tonality_val)
        if energy:
            atmo_parts.append(f"энергия — {energy}")
        triggers = em.get("emotional_triggers", [])
        if isinstance(triggers, list) and triggers:
            clean = [_TRIGGER_LABELS.get(t, t) for t in triggers if t and t != "нет"]
            if clean:
                atmo_parts.append(", ".join(clean[:3]))
        atmosphere = "; ".join(atmo_parts) if atmo_parts else "—"

        # Details
        details = []
        colors = vis.get("dominant_colors", [])
        scheme = vis.get("color_scheme", "")
        if scheme:
            details.append(f"Цветовая гамма: {scheme}")
        if isinstance(colors, list) and colors:
            details.append(f"Доминирующие цвета: {', '.join(colors[:4])}")
        has_logo = st.get("has_logo", False)
        logo_pos = st.get("logo_position", "")
        if has_logo:
            details.append(f"Логотип: {'в позиции ' + logo_pos if logo_pos else 'присутствует'}")
        cta_text = te.get("cta_text", "")
        if cta_text:
            details.append(f"CTA: {cta_text}")
        offer = te.get("offer", "")
        if offer:
            details.append(f"Оффер: {offer}")
        has_urgency = te.get("has_urgency_words", False)
        if has_urgency:
            uw = te.get("urgency_words", [])
            if uw:
                details.append(f"Слова срочности: {', '.join(uw[:3])}")

        ctr = _get_ctr(b.metrics)

        variants.append({
            "name": name,
            "direction": direction,
            "key_elements": key_elements,
            "what_to_keep": what_to_keep,
            "headline": headline or None,
            "subtitle": te.get("subtitle") or None,
            "offer": offer or None,
            "cta": cta_text or None,
            "visual_description": visual_description,
            "atmosphere": atmosphere,
            "details": details,
            "ctr": round(ctr, 6) if ctr is not None else None,
            "banner_id": str(b.id),
            "filename": b.original_filename,
        })

    return variants


def _build_tech_requirements(source_banners) -> list[dict]:
    """Derive technical requirements from banner analysis.

    Returns a list of structured requirement items:
      {"type": "rule", "text": "..."}
    """
    reqs: list[dict] = []
    reqs.append({"type": "subheader", "text": "Требования на основе анализа"})

    # Text percentage rule
    text_pcts = []
    for b in source_banners:
        tp = (b.tags or {}).get("text_elements", {}).get("text_percentage")
        if tp is not None:
            text_pcts.append(float(tp))
    if text_pcts:
        avg_pct = statistics.mean(text_pcts)
        limit = max(20, round(avg_pct * 100 / 5) * 5 + 5)  # round up to nearest 5
        reqs.append({"type": "rule", "text": f"Текст на баннере не должен занимать более {limit}% площади."})
    else:
        reqs.append({"type": "rule", "text": "Текст на баннере не должен занимать более 20% площади."})

    # Readability check
    low_readability = 0
    for b in source_banners:
        rd = (b.tags or {}).get("text_elements", {}).get("text_readability")
        if rd and str(rd).lower() in ("низкая", "low"):
            low_readability += 1
    if low_readability > 0:
        reqs.append({"type": "rule", "text": "Обеспечить высокую читаемость текста на любом фоне (контраст ≥ 4.5:1)."})

    # Contrast / accessibility
    low_contrast = 0
    for b in source_banners:
        ca = (b.tags or {}).get("accessibility", {}).get("contrast_adequate")
        if ca is False:
            low_contrast += 1
    if low_contrast > 0:
        reqs.append({"type": "rule", "text": "Проверить контрастность текста — часть текущих креативов не проходит WCAG."})

    reqs.append({"type": "rule", "text": "Все тексты должны быть читаемы на мобильных устройствах (мин. 14px для основного текста)."})

    # Data-driven: logo presence in top banners
    total = len(source_banners)
    if total > 0:
        logo_count = sum(
            1 for b in source_banners
            if (b.tags or {}).get("structural", {}).get("has_logo")
        )
        if logo_count > total / 2:
            reqs.append({"type": "rule", "text": f"Логотип обязателен — присутствует в {logo_count}/{total} лучших креативах."})

        cta_count = sum(
            1 for b in source_banners
            if (b.tags or {}).get("structural", {}).get("has_cta_button")
        )
        if cta_count > total / 2:
            reqs.append({"type": "rule", "text": f"CTA-кнопка рекомендуется — присутствует в {cta_count}/{total} лучших креативов."})

    return reqs


def _build_brand_compliance(source_banners, color_palette: list[dict]) -> dict:
    """Build brand compliance section from banner data."""
    # Logo
    logo_positions: list[str] = []
    has_logo_count = 0
    for b in source_banners:
        st = (b.tags or {}).get("structural", {})
        if st.get("has_logo"):
            has_logo_count += 1
            pos = st.get("logo_position")
            if pos:
                logo_positions.append(pos)

    logo_info: dict = {}
    if has_logo_count > 0:
        logo_info["present_in"] = f"{has_logo_count}/{len(source_banners)} креативов"
        if logo_positions:
            pos_counts: dict[str, int] = defaultdict(int)
            for p in logo_positions:
                pos_counts[p] += 1
            best_pos = max(pos_counts.items(), key=lambda x: x[1])
            logo_info["recommended_position"] = best_pos[0]
        logo_info["rules"] = [
            "Должен быть чётко виден.",
            "Не перекрывает основные визуальные элементы.",
            "Не доминирует, но заметен.",
        ]

    # Brand colors from palette
    brand_colors = [c["hex"] for c in color_palette[:4]] if color_palette else []

    return {
        "logo": logo_info,
        "brand_colors": brand_colors,
        "rules": [
            "Фирменные цвета и шрифты обязательны к использованию.",
            "Все элементы должны соответствовать tone of voice бренда.",
        ],
    }


def _build_platform_specific_requirements(platform_data: dict) -> list[dict]:
    """Build platform-specific tech requirements for targeted briefs.

    Returns a list of structured requirement items:
      {"type": "header", "text": "..."}
      {"type": "rule", "text": "..."}
      {"type": "subheader", "text": "..."}
      {"type": "format", "text": "..."}
    """
    if not platform_data:
        return []

    pid = platform_data.get("id", "")
    name = platform_data.get("platform", "")
    reqs: list[dict] = [{"type": "header", "text": f"Требования площадки: {name}"}]

    # Platform-specific rules
    _PLATFORM_RULES: dict[str, list[str]] = {
        "vk_ads": [
            "Текст на изображении не более 20% площади.",
            "Безопасная зона изображений: 10% сверху.",
            "Безопасная зона видео: 10% сверху, 20% снизу.",
            "Загружать все пропорции: 1:1, 4:5, 16:9, 9:16 в одно универсальное объявление.",
            "Минимальная ширина видео: 600 px. Рекомендуемое: 1280×720 или 1920×1080.",
            "Видеокодеки: H.264/VP9/HEVC + MP3/AAC.",
            "Карусель: 3–6 слайдов, 600×600 px каждый.",
            "Rewarded Video: до 30 сек, MP4, макс. 10 МБ, 400–450 кбит/с видео, до 25 fps.",
            "Playable Ads: ZIP (HTML5), макс. 2 МБ, 15–60 сек.",
            "Дневной бюджет минимум 5× CPA.",
            "Исключить десктопные плейсменты для мобильного UA.",
            "Для игр: Rewarded + Playable обязательны.",
        ],
        "yandex_direct": [
            "Изображения: 450–5000 px по минимальной стороне, до 10 МБ.",
            "Загружать все пропорции: 1:1, 4:3, 16:9, 9:16 в Retina-разрешениях.",
            "До 5 заголовков + 3 текста + 5 изображений + 2 видео в одном объявлении.",
            "Графические баннеры: JPG/PNG/GIF, макс. 512 КБ.",
            "Видео: MP4/WebM/MOV, 5–60 сек, мин. 360p, рек. 1080p, ≥20 fps.",
            "Видеокодеки: H.264/VP8 + AAC/MP3.",
            "Стоковые видео запрещены с мая 2024 — только уникальный контент.",
            "Playable Ads: ZIP, макс. 3 МБ, index.html ≤150 КБ, ≤20 файлов.",
            "Для игр: Playable Ads + Rewarded Video в приоритете.",
        ],
        "meta_ads": [
            "Текст на изображении не более 20% площади.",
            "Feed: 1080×1080 (1:1) или 1080×1350 (4:5).",
            "Stories/Reels: 1080×1920 (9:16).",
            "Рекомендуемая длина видео: 15 сек.",
            "Видео макс. 4 ГБ, форматы: MP4, MOV, GIF.",
            "Карусель: 2–10 слайдов, 1080×1080 каждый.",
            "Advantage+ автоматически тестирует комбинации.",
            "SKAN 4.0 / ATT совместимость.",
            "Минимальный бюджет на сплит-тест ~$100.",
        ],
        "tiktok": [
            "Только вертикальное видео 9:16 (1080×1920) — единственный реальный формат.",
            "Длительность: 5–60 сек (рекомендуется 9–15 сек).",
            "Аудио обязательно, 16-bit.",
            "Первые 3 секунды критичны для удержания.",
            "Субтитры повышают completion rate.",
            "Мин. разрешение: 540×960, рекомендуется 1080×1920.",
            "24–60 fps.",
            "Макс. вес видео: 500 МБ.",
        ],
        "google_uac": [
            "Система сама комбинирует ассеты — загружайте максимум ресурсов.",
            "Изображения: 1:1 (300–1200px), 16:9 (600–1200px), 9:16 (314–628px).",
            "Текстовые строки: до 4 шт., до 25 символов каждая.",
            "Видео: загружать на YouTube, рекомендуется 15–30 сек.",
            "HTML5: макс. 1 МБ, отдельная верификация.",
            "Автоставки: tCPA или tROAS.",
        ],
        "telegram": [
            "Sponsored Messages: только Telegram-ссылки (t.me/), до 160 символов.",
            "CTA-кнопка: до 30 символов.",
            "Прямой UA невозможен — для инсталлов используйте Mini Apps через партнёрские сети.",
            "Premium-пользователи не видят Sponsored Messages.",
            "Медиа Sponsored: только через партнёрские аккаунты, CPM на 30–100% выше.",
        ],
        "snapchat": [
            "Обязательный формат: 9:16 (1080×1920).",
            "Рекомендуемая длина видео: 3–10 секунд.",
            "Безопасная зона: 120 px сверху, 250 px снизу.",
            "Логотип размещать в верхней зоне безопасности.",
            "Gen-Z аудитория 13–34 лет.",
            "SKAN совместимость.",
        ],
        "unity_ads": [
            "Rewarded Video: 15–30 сек, Landscape 1280×720 и Portrait 720×1280.",
            "Endcard обязателен: 1200×628 или 628×1200, JPG, ≤1 МБ.",
            "Баннер: 320×50 или 728×90, ≤150 КБ.",
            "Playable: HTML5, ≤5 МБ.",
            "Сильный инвентарь в мобильных играх.",
        ],
        "mintegral": [
            "Endcard 1200×627 обязателен для всех видеоформатов.",
            "Иконка 512×512 обязательна.",
            "Видео: рекомендуемые длительности 6/15/30/60 сек, макс. 120 сек.",
            "Playable: HTML5, ≤5 МБ + 1200×627 image + 512×512 icon.",
            "MREC готовить в 3 весовых вариантах.",
        ],
        "moloco": [
            "Загружайте ВСЕ Image-размеры для максимального охвата инвентаря.",
            "Основные размеры: 300×250, 320×480, 320×50, 728×90.",
            "Native: 1200×627 + 627×627 критичны (особенно для японских бирж).",
            "Шрифт минимум 10pt.",
            "Playable: HTML single file, data-URI, без ZIP.",
        ],
    }

    platform_reqs = _PLATFORM_RULES.get(pid, [])
    reqs.extend({"type": "rule", "text": r} for r in platform_reqs)

    # Add format summary from platform data
    formats = platform_data.get("formats", [])
    if formats:
        reqs.append({"type": "subheader", "text": "Форматы для подготовки"})
        for f in formats:
            sizes_str = ", ".join(f.get("sizes", [])[:4])
            files_str = "/".join(f.get("file_formats", []))
            max_s = f.get("max_file_size", "")
            reqs.append({"type": "format", "text": f"{f['name']}: {sizes_str} | {files_str} | макс. {max_s}"})

    return reqs


# ===========================================================================
# 1. Creative Brief
# ===========================================================================

@analytics_router.get("/creative-brief")
async def generate_creative_brief(
    project: Optional[str] = None,
    target_platform: Optional[str] = None,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Auto-generate a creative brief based on element correlation data.
    target_platform: platform id (e.g. 'vk_ads', 'yandex_direct') to tailor the brief."""
    tid = current_user.tenant.id

    stmt = select(Banner).where(Banner.tenant_id == tid, Banner.tags_status == "done")
    stmt = _banner_project_filter(stmt, project)
    result = await db.execute(stmt)
    banners = list(result.scalars().all())

    if len(banners) < 3:
        return {"brief": None, "error": "Недостаточно данных. Загрузите минимум 3 протегированных креатива с метриками."}

    # Compute element impact — two passes:
    # 1) collect all elements each banner has
    # 2) for each known element, banners that don't have it go to without_elem
    banner_data: list[tuple[float, dict[str, bool]]] = []
    all_element_names: set[str] = set()

    for b in banners:
        ctr = _get_ctr(b.metrics)
        if ctr is None or not b.tags:
            continue
        elements = _extract_elements(b.tags)
        banner_data.append((ctr, elements))
        all_element_names.update(k for k, v in elements.items() if v)

    with_elem: dict[str, list[float]] = defaultdict(list)
    without_elem: dict[str, list[float]] = defaultdict(list)

    for ctr, elements in banner_data:
        for name in all_element_names:
            if elements.get(name):
                with_elem[name].append(ctr)
            else:
                without_elem[name].append(ctr)

    positive = []
    negative = []

    min_samples = 2 if len(banners) >= 10 else 1
    min_diff = 5.0 if len(banners) >= 10 else 3.0

    for name in with_elem:
        w = with_elem[name]
        wo = without_elem.get(name, [])
        if len(w) < min_samples or len(wo) < min_samples:
            continue
        avg_w = statistics.mean(w)
        avg_wo = statistics.mean(wo)
        if avg_wo == 0:
            continue
        diff = (avg_w - avg_wo) / avg_wo * 100
        if abs(diff) < min_diff:
            continue
        label = _element_label_ru(name)
        entry = {"element": name, "label": label, "diff_pct": round(diff, 1), "avg_ctr_with": round(avg_w, 6), "count": len(w)}
        if diff > 0:
            positive.append(entry)
        else:
            negative.append(entry)

    positive.sort(key=lambda x: x["diff_pct"], reverse=True)
    negative.sort(key=lambda x: x["diff_pct"])

    # Fallback: if no correlation data, extract common traits from top banners
    if not positive and not negative and banner_data:
        sorted_bd = sorted(banner_data, key=lambda x: x[0], reverse=True)
        top_n = max(1, len(sorted_bd) // 3)  # top third
        top_elements: dict[str, int] = defaultdict(int)
        for ctr, elems in sorted_bd[:top_n]:
            for name, has in elems.items():
                if has:
                    top_elements[name] += 1
        for name, cnt in sorted(top_elements.items(), key=lambda x: -x[1]):
            if cnt >= top_n:  # present in all top banners
                label = _element_label_ru(name)
                avg_ctr = statistics.mean([c for c, e in sorted_bd[:top_n] if e.get(name)])
                positive.append({
                    "element": name, "label": label,
                    "diff_pct": None,  # no comparison data
                    "avg_ctr_with": round(avg_ctr, 6), "count": cnt,
                    "note": "Присутствует у всех лучших креативов",
                })
            if len(positive) >= 5:
                break

    # --- Deduplication of must_have / avoid ---
    # Vague categorical values that are NOT actionable for a designer
    _VAGUE_VALUES = {"средняя", "средний", "нормальная", "нормальный", "умеренная", "умеренный"}

    def _dedup_element_list(items: list[dict]) -> list[dict]:
        """Remove near-duplicate and non-actionable elements."""
        # 1. Filter out vague categorical values
        filtered = []
        for entry in items:
            elem = entry.get("element", "")
            if ":" in elem:
                value_part = elem.split(":", 1)[1].strip()
                if value_part.lower() in _VAGUE_VALUES:
                    continue
            filtered.append(entry)

        # 2. Deduplicate: if two elements have similar diff_pct (within 5%)
        #    AND their labels share a common word, keep the shorter label
        result = []
        used = set()
        for i, a in enumerate(filtered):
            if i in used:
                continue
            best = a
            for j, c in enumerate(filtered):
                if j <= i or j in used:
                    continue
                diff_a = a.get("diff_pct")
                diff_b = c.get("diff_pct")
                if diff_a is None or diff_b is None:
                    continue
                if abs(diff_a - diff_b) > 5.0:
                    continue
                words_a = set(a.get("label", "").lower().split())
                words_b = set(c.get("label", "").lower().split())
                if words_a & words_b:
                    # Keep the one with shorter label
                    used.add(j)
                    if len(c.get("label", "")) < len(best.get("label", "")):
                        best = c
            result.append(best)
        return result

    positive = _dedup_element_list(positive)
    negative = _dedup_element_list(negative)

    # Best format
    format_ctrs: dict[str, list[float]] = defaultdict(list)
    for b in banners:
        ctr = _get_ctr(b.metrics)
        if ctr is None or not b.tags:
            continue
        ft = (b.tags.get("platform_fit") or {}).get("format_type")
        if ft:
            format_ctrs[ft].append(ctr)

    best_format = None
    if format_ctrs:
        best_format = max(format_ctrs.items(), key=lambda x: statistics.mean(x[1]) if len(x[1]) >= 2 else 0)
        best_format = {"format": best_format[0], "avg_ctr": round(statistics.mean(best_format[1]), 6), "count": len(best_format[1])}

    # Top banners as references (sorted by CTR)
    banners_with_ctr = [(b, _get_ctr(b.metrics)) for b in banners if _get_ctr(b.metrics) is not None]
    banners_with_ctr.sort(key=lambda x: x[1], reverse=True)
    references = []
    for b, ctr in banners_with_ctr[:3]:
        references.append({
            "id": str(b.id),
            "filename": b.original_filename,
            "ctr": round(ctr, 6),
            "storage_key": b.storage_key,
        })

    # ---- Extended brief sections (aggregated from top banners' tags) ----
    # Use top banners (with CTR) or all tagged banners
    top_banners = [b for b, _ in banners_with_ctr[:5]] if banners_with_ctr else []
    source_banners = top_banners if top_banners else [b for b in banners if b.tags]

    # 1. Visual style
    visual_style = _aggregate_tag_field(source_banners, "visual", [
        "background_type", "color_scheme", "visual_clutter", "visual_hierarchy",
        "focal_point", "whitespace_ratio",
    ])

    # 2. Color palette — collect dominant_colors from top banners
    all_colors: list[str] = []
    for b in source_banners:
        colors = (b.tags or {}).get("visual", {}).get("dominant_colors", [])
        if isinstance(colors, list):
            all_colors.extend(colors)
    color_counts: dict[str, int] = defaultdict(int)
    for c in all_colors:
        if isinstance(c, str) and c.startswith("#"):
            color_counts[c.upper()] += 1
    top_colors = sorted(color_counts.items(), key=lambda x: -x[1])[:5]
    color_palette = [{"hex": c, "count": n} for c, n in top_colors]

    # 3. Typography
    typography = _aggregate_tag_field(source_banners, "text_elements", [
        "font_style", "font_count", "font_size_hierarchy", "text_readability",
        "text_percentage",
    ])

    # 4. Content / messaging — collect headlines, CTAs, offers from top banners
    content_examples: dict[str, list[str]] = {"headlines": [], "ctas": [], "offers": []}
    for b in source_banners:
        te = (b.tags or {}).get("text_elements", {})
        if te.get("headline"):
            content_examples["headlines"].append(te["headline"])
        if te.get("cta_text"):
            content_examples["ctas"].append(te["cta_text"])
        if te.get("offer"):
            content_examples["offers"].append(te["offer"])
    for k in content_examples:
        content_examples[k] = list(dict.fromkeys(content_examples[k]))[:5]

    # 5. Tonality / emotion
    tonality = _aggregate_tag_field(source_banners, "emotional", [
        "tonality", "energy_level", "personalization_level",
    ])
    all_triggers: list[str] = []
    all_trust: list[str] = []
    for b in source_banners:
        em = (b.tags or {}).get("emotional", {})
        triggers = em.get("emotional_triggers", [])
        if isinstance(triggers, list):
            all_triggers.extend([_TRIGGER_LABELS.get(t, t) for t in triggers if t and t != "нет"])
        trust = em.get("trust_signals", [])
        if isinstance(trust, list):
            all_trust.extend([t for t in trust if t and t != "нет"])
    trigger_counts = defaultdict(int)
    for t in all_triggers:
        trigger_counts[t] += 1
    trust_counts = defaultdict(int)
    for t in all_trust:
        trust_counts[t] += 1
    tonality["emotional_triggers"] = sorted(trigger_counts.items(), key=lambda x: -x[1])[:5]
    tonality["trust_signals"] = sorted(trust_counts.items(), key=lambda x: -x[1])[:5]

    # 6. Structure
    structure = _aggregate_tag_field(source_banners, "structural", [
        "cta_position", "logo_position", "text_image_ratio",
    ])

    # 7. Platform fit
    platform = _aggregate_tag_field(source_banners, "platform_fit", [
        "format_type", "thumb_stop_potential", "first_impression_strength",
    ])

    # ---- Creative variants (like OTP Bank brief format) ----
    # Group top banners by tonality/theme to create distinct variants
    variants = _generate_creative_variants(source_banners, must_have=positive[:5])

    # ---- Technical requirements ----
    tech_requirements = _build_tech_requirements(source_banners)

    # ---- Brand compliance ----
    brand_compliance = _build_brand_compliance(source_banners, color_palette)

    # ---- Platform ad specs ----
    if target_platform:
        # Filter to specific platform + add platform-specific requirements
        from platform_ad_specs import get_platform_by_id
        target_p = get_platform_by_id(target_platform)
        platform_specs = [target_p] if target_p else _PLATFORM_AD_SPECS
        platform_specific_reqs = _build_platform_specific_requirements(target_p) if target_p else []
        tech_requirements = platform_specific_reqs + tech_requirements
    else:
        platform_specs = _PLATFORM_AD_SPECS

    # Sign image URLs for variants, references, and available banners
    sign_keys = []
    for b in banners:
        if b.storage_key and b.storage_key not in sign_keys:
            sign_keys.append(b.storage_key)
    signed_urls = {}
    if sign_keys:
        try:
            signed_urls = await file_storage.get_signed_urls(sign_keys)
        except Exception:
            pass
    # Attach image URLs to variants
    banner_by_id = {str(b.id): b for b in banners}
    for v in variants:
        bid = v.get("banner_id")
        b = banner_by_id.get(bid)
        if b and b.storage_key:
            v["image_url"] = signed_urls.get(b.storage_key)
    # Attach image URLs to references
    for ref in references:
        sk = ref.get("storage_key")
        if sk:
            ref["image_url"] = signed_urls.get(sk)

    # All banners for manual selection in brief UI — sorted by CTR (those with metrics first)
    available_banners = []
    seen_ids = set()
    # First: banners with CTR (sorted best first)
    for b, ctr in banners_with_ctr:
        available_banners.append({
            "id": str(b.id),
            "filename": b.original_filename,
            "ctr": round(ctr, 6),
            "image_url": signed_urls.get(b.storage_key) if b.storage_key else None,
        })
        seen_ids.add(b.id)
    # Then: remaining banners without CTR
    for b in banners:
        if b.id not in seen_ids:
            available_banners.append({
                "id": str(b.id),
                "filename": b.original_filename,
                "ctr": None,
                "image_url": signed_urls.get(b.storage_key) if b.storage_key else None,
            })
    available_banners = available_banners[:50]  # cap at 50

    return {
        "brief": {
            "project": project,
            "target_platform": target_platform,
            "total_banners_analyzed": len(banners),
            "banners_with_metrics": len(banners_with_ctr),
            "must_have": positive[:5],
            "avoid": negative[:5],
            "best_format": best_format,
            "references": references,
            # Extended sections
            "visual_style": visual_style,
            "color_palette": color_palette,
            "typography": typography,
            "content_examples": content_examples,
            "tonality": tonality,
            "structure": structure,
            "platform": platform,
            # OTP Bank style sections
            "variants": variants,
            "tech_requirements": tech_requirements,
            "brand_compliance": brand_compliance,
            # Platform specs (filtered or all)
            "platform_specs": platform_specs,
            "available_banners": available_banners,
            "universal_formats": _UNIVERSAL_FORMATS,
            "tech_constants": _TECH_CONSTANTS,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
    }


# ===========================================================================
# 2. Budget / Scaling Recommendations
# ===========================================================================

@analytics_router.get("/budget-recommendations")
async def get_budget_recommendations(
    project: Optional[str] = None,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Analyse banners + placements and produce scale/watch/stop recommendations."""
    tid = current_user.tenant.id

    # Load banners
    bstmt = select(Banner).where(Banner.tenant_id == tid)
    bstmt = _banner_project_filter(bstmt, project)
    banners = list((await db.execute(bstmt)).scalars().all())
    banner_map = {b.id: b for b in banners}
    banner_ids = set(banner_map.keys())

    # Load placements for those banners
    pstmt = select(CreativePlacement).where(CreativePlacement.tenant_id == tid)
    all_placements = list((await db.execute(pstmt)).scalars().all())
    placements = [p for p in all_placements if p.creative_id in banner_ids]

    # Group placements by creative
    by_creative: dict[uuid_mod.UUID, list[CreativePlacement]] = defaultdict(list)
    for p in placements:
        by_creative[p.creative_id].append(p)
    for cid in by_creative:
        by_creative[cid].sort(key=lambda x: x.period_start)

    # Global stats
    all_ctrs = []
    for b in banners:
        ctr = _get_ctr(b.metrics)
        if ctr is not None:
            all_ctrs.append(ctr)

    if not all_ctrs:
        return {"recommendations": [], "summary": {"total_banners": len(banners), "avg_ctr": None}}

    avg_ctr = statistics.mean(all_ctrs)
    sorted_ctrs = sorted(all_ctrs)
    p10 = sorted_ctrs[max(0, int(len(sorted_ctrs) * 0.1))]
    p90 = sorted_ctrs[min(len(sorted_ctrs) - 1, int(len(sorted_ctrs) * 0.9))]

    recommendations = []

    for b in banners:
        ctr = _get_ctr(b.metrics)
        if ctr is None:
            continue

        pls = by_creative.get(b.id, [])
        placement_count = len(pls)

        # Fatigue check
        fatigue_pct = 0
        ctr_trend = "stable"
        if len(pls) >= 2:
            ctrs_series = [_get_ctr(p.metrics) for p in pls]
            ctrs_series = [c for c in ctrs_series if c is not None]
            if len(ctrs_series) >= 2:
                peak = max(ctrs_series)
                if peak > 0:
                    fatigue_pct = round((peak - ctrs_series[-1]) / peak * 100, 1)
                if ctrs_series[-1] > ctrs_series[-2]:
                    ctr_trend = "up"
                elif ctrs_series[-1] < ctrs_series[-2]:
                    ctr_trend = "down"

        # Calculate spend share (from placement metrics)
        total_spend = 0
        banner_spend = 0
        for p in all_placements:
            s = (p.metrics or {}).get("spend", 0)
            try:
                total_spend += float(s)
            except (ValueError, TypeError):
                pass
        for p in pls:
            s = (p.metrics or {}).get("spend", 0)
            try:
                banner_spend += float(s)
            except (ValueError, TypeError):
                pass
        spend_share = round(banner_spend / total_spend * 100, 1) if total_spend > 0 else None

        # Determine action
        if ctr >= p90 and fatigue_pct < 20:
            action = "scale"
            reason = f"CTR {ctr:.2%} в топ-10%"
            if spend_share is not None and spend_share < 30:
                reason += f", но получает только {spend_share}% бюджета"
        elif fatigue_pct >= 50:
            action = "stop"
            reason = f"Усталость {fatigue_pct}% — CTR значительно снизился от пика"
        elif ctr <= p10:
            action = "stop"
            reason = f"CTR {ctr:.2%} в нижних 10%"
        elif fatigue_pct >= 20:
            action = "watch"
            reason = f"CTR снижается (усталость {fatigue_pct}%), ротация через ~7 дней"
        elif ctr_trend == "down":
            action = "watch"
            reason = f"CTR {ctr:.2%} в тренде на снижение"
        else:
            action = "keep"
            reason = f"CTR {ctr:.2%} — стабильный перформанс"

        recommendations.append({
            "banner_id": str(b.id),
            "filename": b.original_filename,
            "storage_key": b.storage_key,
            "ctr": round(ctr, 6),
            "action": action,
            "reason": reason,
            "fatigue_pct": fatigue_pct,
            "ctr_trend": ctr_trend,
            "placement_count": placement_count,
            "spend_share": spend_share,
            "project": b.project,
            "concept_group": b.concept_group,
        })

    # Sort: scale first, then watch, then keep, then stop
    action_order = {"scale": 0, "watch": 1, "keep": 2, "stop": 3}
    recommendations.sort(key=lambda x: (action_order.get(x["action"], 9), -x["ctr"]))

    # Generate signed image URLs
    storage_keys = [r["storage_key"] for r in recommendations if r.get("storage_key")]
    signed = {}
    if storage_keys:
        try:
            signed = await file_storage.get_signed_urls(storage_keys)
        except Exception:
            pass
    for r in recommendations:
        r["image_url"] = signed.get(r.get("storage_key")) if r.get("storage_key") else None

    return {
        "recommendations": recommendations,
        "summary": {
            "total_banners": len(banners),
            "avg_ctr": round(avg_ctr, 6),
            "scale_count": sum(1 for r in recommendations if r["action"] == "scale"),
            "watch_count": sum(1 for r in recommendations if r["action"] == "watch"),
            "stop_count": sum(1 for r in recommendations if r["action"] == "stop"),
            "keep_count": sum(1 for r in recommendations if r["action"] == "keep"),
        },
    }


# ===========================================================================
# 5. A/B Test Designer
# ===========================================================================

def _ab_test_to_record(t: ABTest) -> ABTestRecord:
    return ABTestRecord(
        id=str(t.id),
        name=t.name,
        description=t.description,
        hypothesis_id=str(t.hypothesis_id) if t.hypothesis_id else None,
        control_banner_id=str(t.control_banner_id),
        test_banner_id=str(t.test_banner_id),
        metric=t.metric,
        target_sample_size=t.target_sample_size,
        confidence_level=float(t.confidence_level),
        status=t.status,
        control_metrics=t.control_metrics or {},
        test_metrics=t.test_metrics or {},
        result=t.result or {},
        project=t.project,
        started_at=t.started_at.isoformat() if t.started_at else None,
        completed_at=t.completed_at.isoformat() if t.completed_at else None,
        created_at=t.created_at.isoformat() if isinstance(t.created_at, datetime) else str(t.created_at) if t.created_at else None,
    )


@analytics_router.get("/ab-tests", response_model=ABTestListResponse)
async def list_ab_tests(
    status: Optional[str] = None,
    project: Optional[str] = None,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id
    stmt = select(ABTest).where(ABTest.tenant_id == tid)
    if status:
        stmt = stmt.where(ABTest.status == status)
    if project:
        if project == "__none__":
            stmt = stmt.where(ABTest.project.is_(None))
        else:
            stmt = stmt.where(ABTest.project == project)
    stmt = stmt.order_by(ABTest.created_at.desc())
    tests = list((await db.execute(stmt)).scalars().all())
    return ABTestListResponse(tests=[_ab_test_to_record(t) for t in tests], total=len(tests))


@analytics_router.post("/ab-tests", response_model=ABTestRecord)
async def create_ab_test(
    body: ABTestCreate,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id
    test = ABTest(
        id=uuid_mod.uuid4(),
        tenant_id=tid,
        user_id=current_user.user.id,
        hypothesis_id=uuid_mod.UUID(body.hypothesis_id) if body.hypothesis_id else None,
        name=body.name,
        description=body.description,
        control_banner_id=uuid_mod.UUID(body.control_banner_id),
        test_banner_id=uuid_mod.UUID(body.test_banner_id),
        metric=body.metric,
        target_sample_size=body.target_sample_size,
        confidence_level=body.confidence_level,
        status="draft",
        project=body.project,
        created_at=datetime.now(timezone.utc),
    )
    db.add(test)
    await db.commit()
    await db.refresh(test)
    return _ab_test_to_record(test)


@analytics_router.patch("/ab-tests/{test_id}")
async def update_ab_test(
    test_id: str,
    body: dict,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id
    stmt = select(ABTest).where(ABTest.id == uuid_mod.UUID(test_id), ABTest.tenant_id == tid)
    test = (await db.execute(stmt)).scalar_one_or_none()
    if not test:
        from fastapi import HTTPException
        raise HTTPException(404, "Test not found")

    allowed = {"name", "description", "status", "control_metrics", "test_metrics", "result", "target_sample_size"}
    for k, v in body.items():
        if k in allowed:
            setattr(test, k, v)

    if body.get("status") == "running" and not test.started_at:
        test.started_at = datetime.now(timezone.utc)
    if body.get("status") in ("completed", "cancelled") and not test.completed_at:
        test.completed_at = datetime.now(timezone.utc)

    # Auto-calculate result when metrics are provided
    if body.get("control_metrics") or body.get("test_metrics"):
        cm = test.control_metrics or {}
        tm = test.test_metrics or {}
        ctrl_val = cm.get(test.metric)
        test_val = tm.get(test.metric)
        ctrl_n = cm.get("impressions", 0)
        test_n = tm.get("impressions", 0)

        if ctrl_val is not None and test_val is not None:
            try:
                ctrl_val = float(ctrl_val)
                test_val = float(test_val)
                lift = ((test_val - ctrl_val) / ctrl_val * 100) if ctrl_val > 0 else 0

                # Significance: Z-test for proportions
                total_n = int(ctrl_n) + int(test_n)
                significant = False
                p_value = None
                if total_n > 0 and int(ctrl_n) > 0 and int(test_n) > 0:
                    p_pool = (ctrl_val * int(ctrl_n) + test_val * int(test_n)) / total_n
                    if p_pool > 0 and p_pool < 1:
                        se = math.sqrt(p_pool * (1 - p_pool) * (1/int(ctrl_n) + 1/int(test_n)))
                        if se > 0:
                            z = (test_val - ctrl_val) / se
                            # Approx p-value (two-tailed)
                            p_value = round(2 * (1 - 0.5 * (1 + math.erf(abs(z) / math.sqrt(2)))), 4)
                            significant = p_value < (1 - float(test.confidence_level))

                test.result = {
                    "lift_pct": round(lift, 2),
                    "control_value": ctrl_val,
                    "test_value": test_val,
                    "significant": significant,
                    "p_value": p_value,
                    "sample_progress": round(total_n / test.target_sample_size * 100, 1) if test.target_sample_size > 0 else 0,
                }

                # Auto-complete if significance reached
                if significant and test.status == "running":
                    test.status = "completed"
                    test.completed_at = datetime.now(timezone.utc)
                    # Update linked hypothesis
                    if test.hypothesis_id:
                        hyp_status = "validated" if lift > 0 else "rejected"
                        await db.execute(
                            update(Hypothesis).where(Hypothesis.id == test.hypothesis_id).values(status=hyp_status)
                        )
            except (ValueError, TypeError, ZeroDivisionError):
                pass

    await db.commit()
    await db.refresh(test)
    return _ab_test_to_record(test)


@analytics_router.delete("/ab-tests/{test_id}")
async def delete_ab_test(
    test_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id
    stmt = select(ABTest).where(ABTest.id == uuid_mod.UUID(test_id), ABTest.tenant_id == tid)
    test = (await db.execute(stmt)).scalar_one_or_none()
    if not test:
        from fastapi import HTTPException
        raise HTTPException(404, "Test not found")
    await db.delete(test)
    await db.commit()
    return {"ok": True}


@analytics_router.get("/ab-tests/sample-size")
async def calculate_sample_size(
    baseline_rate: float = Query(..., description="e.g. 0.02 for 2% CTR"),
    mde: float = Query(..., description="Minimum detectable effect, e.g. 0.2 for 20% lift"),
    confidence: float = Query(0.95),
    power: float = Query(0.8),
):
    """Sample size calculator for A/B test planning."""
    alpha = 1 - confidence
    # Z-scores (approx)
    z_alpha = 1.96 if confidence >= 0.95 else 1.645
    z_beta = 0.84 if power >= 0.8 else 0.675

    p1 = baseline_rate
    p2 = baseline_rate * (1 + mde)
    p_avg = (p1 + p2) / 2

    if p_avg <= 0 or p_avg >= 1:
        return {"sample_size_per_variant": 0, "total_sample": 0}

    n = ((z_alpha * math.sqrt(2 * p_avg * (1 - p_avg)) + z_beta * math.sqrt(p1 * (1 - p1) + p2 * (1 - p2))) ** 2) / ((p2 - p1) ** 2)
    n = int(math.ceil(n))

    return {
        "sample_size_per_variant": n,
        "total_sample": n * 2,
        "baseline_rate": baseline_rate,
        "expected_rate": round(p2, 6),
        "mde": mde,
        "confidence": confidence,
        "power": power,
    }


# ===========================================================================
# 6. Creative Versioning (Concept Groups)
# ===========================================================================

@analytics_router.get("/concept-groups")
async def list_concept_groups(
    project: Optional[str] = None,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List concept groups with their banners and comparison."""
    tid = current_user.tenant.id

    stmt = select(Banner).where(Banner.tenant_id == tid, Banner.concept_group.isnot(None))
    stmt = _banner_project_filter(stmt, project)
    result = await db.execute(stmt)
    banners = list(result.scalars().all())

    groups: dict[str, list] = defaultdict(list)
    for b in banners:
        ctr = _get_ctr(b.metrics)
        groups[b.concept_group].append({
            "id": str(b.id),
            "filename": b.original_filename,
            "storage_key": b.storage_key,
            "ctr": round(ctr, 6) if ctr is not None else None,
            "project": b.project,
            "created_at": b.created_at.isoformat() if b.created_at else None,
        })

    # Sort versions within each group by created_at
    result_groups = []
    for name, versions in groups.items():
        versions.sort(key=lambda x: x["created_at"] or "")
        ctrs = [v["ctr"] for v in versions if v["ctr"] is not None]

        best = max(versions, key=lambda v: v["ctr"] or 0) if ctrs else None
        improvement = None
        if len(ctrs) >= 2:
            first_ctr = ctrs[0]
            last_ctr = ctrs[-1]
            if first_ctr > 0:
                improvement = round((last_ctr - first_ctr) / first_ctr * 100, 1)

        result_groups.append({
            "name": name,
            "versions": versions,
            "version_count": len(versions),
            "best_version": best,
            "improvement_pct": improvement,
            "avg_ctr": round(statistics.mean(ctrs), 6) if ctrs else None,
        })

    result_groups.sort(key=lambda g: g["version_count"], reverse=True)

    return {"groups": result_groups, "total": len(result_groups)}


@analytics_router.post("/banners/set-concept-group")
async def set_concept_group(
    body: dict,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Assign banners to a concept group."""
    tid = current_user.tenant.id
    banner_ids = body.get("banner_ids", [])
    concept_group = body.get("concept_group")  # None to remove

    if not banner_ids:
        return {"updated": 0}

    uuids = [uuid_mod.UUID(bid) for bid in banner_ids]
    await db.execute(
        update(Banner)
        .where(Banner.id.in_(uuids), Banner.tenant_id == tid)
        .values(concept_group=concept_group)
    )
    await db.commit()
    return {"updated": len(banner_ids), "concept_group": concept_group}


@analytics_router.post("/concept-groups/rename")
async def rename_concept_group(
    body: dict,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id
    old_name = body.get("old_name", "")
    new_name = body.get("new_name", "").strip()
    if not old_name or not new_name:
        from fastapi import HTTPException
        raise HTTPException(400, "old_name and new_name required")

    result = await db.execute(
        update(Banner)
        .where(Banner.tenant_id == tid, Banner.concept_group == old_name)
        .values(concept_group=new_name)
    )
    await db.commit()
    return {"renamed": old_name, "to": new_name, "updated": result.rowcount}


# ===========================================================================
# 7. Audience × Creative Matrix
# ===========================================================================

@analytics_router.get("/audience-matrix")
async def get_audience_matrix(
    project: Optional[str] = None,
    metric: str = "ctr",
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Build audience_segment × creative matrix with CTR/metric values."""
    tid = current_user.tenant.id

    # Load banners
    bstmt = select(Banner).where(Banner.tenant_id == tid)
    bstmt = _banner_project_filter(bstmt, project)
    banners = list((await db.execute(bstmt)).scalars().all())
    banner_map = {b.id: b for b in banners}
    banner_ids = set(banner_map.keys())

    # Load placements with audience_segment
    pstmt = select(CreativePlacement).where(
        CreativePlacement.tenant_id == tid,
        CreativePlacement.audience_segment.isnot(None),
    )
    all_placements = list((await db.execute(pstmt)).scalars().all())
    placements = [p for p in all_placements if p.creative_id in banner_ids]

    if not placements:
        return {"matrix": [], "segments": [], "creatives": [], "has_data": False}

    # Build matrix: segment → creative → [metric_values]
    matrix_data: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    segments_set = set()
    creatives_set = set()

    for p in placements:
        seg = p.audience_segment
        cid = p.creative_id
        val = (p.metrics or {}).get(metric)
        if val is None:
            continue
        try:
            val = float(val)
        except (ValueError, TypeError):
            continue

        segments_set.add(seg)
        creatives_set.add(cid)
        matrix_data[seg][cid].append(val)

    segments = sorted(segments_set)
    creatives = sorted(creatives_set, key=lambda c: banner_map[c].original_filename if c in banner_map else "")

    # Build output matrix
    matrix = []
    for cid in creatives:
        b = banner_map.get(cid)
        row = {
            "banner_id": str(cid),
            "filename": b.original_filename if b else str(cid)[:8],
            "storage_key": b.storage_key if b else None,
            "segments": {},
        }
        for seg in segments:
            vals = matrix_data[seg].get(cid, [])
            if vals:
                row["segments"][seg] = round(statistics.mean(vals), 6)
            else:
                row["segments"][seg] = None
        matrix.append(row)

    # Find best combinations
    best_combos = []
    for seg in segments:
        best_cid = None
        best_val = -1
        for cid in creatives:
            vals = matrix_data[seg].get(cid, [])
            if vals:
                avg = statistics.mean(vals)
                if avg > best_val:
                    best_val = avg
                    best_cid = cid
        if best_cid:
            b = banner_map.get(best_cid)
            best_combos.append({
                "segment": seg,
                "banner_id": str(best_cid),
                "filename": b.original_filename if b else "",
                "value": round(best_val, 6),
            })

    return {
        "matrix": matrix,
        "segments": segments,
        "creatives": [{"id": str(c), "filename": banner_map[c].original_filename if c in banner_map else ""} for c in creatives],
        "best_combinations": best_combos,
        "metric": metric,
        "has_data": True,
    }


# ===========================================================================
# 8. Stakeholder Report
# ===========================================================================

@analytics_router.get("/stakeholder-report")
async def get_stakeholder_report(
    project: Optional[str] = None,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Aggregated report for stakeholders."""
    tid = current_user.tenant.id

    # Banners
    bstmt = select(Banner).where(Banner.tenant_id == tid)
    bstmt = _banner_project_filter(bstmt, project)
    banners = list((await db.execute(bstmt)).scalars().all())

    # Placements
    banner_ids = {b.id for b in banners}
    pstmt = select(CreativePlacement).where(CreativePlacement.tenant_id == tid)
    all_placements = list((await db.execute(pstmt)).scalars().all())
    placements = [p for p in all_placements if p.creative_id in banner_ids] if project else all_placements

    # Hypotheses
    hstmt = select(Hypothesis).where(Hypothesis.tenant_id == tid)
    if project:
        if project == "__none__":
            hstmt = hstmt.where(Hypothesis.project.is_(None))
        else:
            hstmt = hstmt.where(Hypothesis.project == project)
    hypotheses = list((await db.execute(hstmt)).scalars().all())

    # Insights
    istmt = select(CreativeInsight).where(CreativeInsight.tenant_id == tid, CreativeInsight.is_dismissed == False)
    if project:
        if project == "__none__":
            istmt = istmt.where(CreativeInsight.project.is_(None))
        else:
            istmt = istmt.where(CreativeInsight.project == project)
    insights = list((await db.execute(istmt)).scalars().all())

    # A/B tests
    tstmt = select(ABTest).where(ABTest.tenant_id == tid)
    if project:
        if project == "__none__":
            tstmt = tstmt.where(ABTest.project.is_(None))
        else:
            tstmt = tstmt.where(ABTest.project == project)
    ab_tests = list((await db.execute(tstmt)).scalars().all())

    # Compute stats
    tagged = [b for b in banners if b.tags_status == "done"]
    ctrs = [_get_ctr(b.metrics) for b in banners if _get_ctr(b.metrics) is not None]

    top_banners = sorted(
        [(b, _get_ctr(b.metrics)) for b in banners if _get_ctr(b.metrics) is not None],
        key=lambda x: x[1], reverse=True
    )[:5]

    bottom_banners = sorted(
        [(b, _get_ctr(b.metrics)) for b in banners if _get_ctr(b.metrics) is not None],
        key=lambda x: x[1]
    )[:5]

    # Hypothesis stats
    hyp_by_status = defaultdict(int)
    for h in hypotheses:
        hyp_by_status[h.status] += 1

    # Insight stats
    insight_by_type = defaultdict(int)
    insight_by_severity = defaultdict(int)
    for i in insights:
        insight_by_type[i.insight_type] += 1
        insight_by_severity[i.severity] += 1

    # AB test stats
    ab_by_status = defaultdict(int)
    for t in ab_tests:
        ab_by_status[t.status] += 1

    # Sign image URLs for top/bottom creatives
    sign_keys = []
    for b, _ in top_banners + bottom_banners:
        if b.storage_key and b.storage_key not in sign_keys:
            sign_keys.append(b.storage_key)
    signed_urls = {}
    if sign_keys:
        try:
            signed_urls = await file_storage.get_signed_urls(sign_keys)
        except Exception:
            pass

    return {
        "report": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "project": project,
            "overview": {
                "total_creatives": len(banners),
                "tagged_creatives": len(tagged),
                "total_placements": len(placements),
                "avg_ctr": round(statistics.mean(ctrs), 6) if ctrs else None,
                "median_ctr": round(statistics.median(ctrs), 6) if ctrs else None,
                "best_ctr": round(max(ctrs), 6) if ctrs else None,
                "worst_ctr": round(min(ctrs), 6) if ctrs else None,
            },
            "top_creatives": [
                {
                    "id": str(b.id), "filename": b.original_filename,
                    "storage_key": b.storage_key, "ctr": round(ctr, 6),
                    "image_url": signed_urls.get(b.storage_key) if b.storage_key else None,
                }
                for b, ctr in top_banners
            ],
            "bottom_creatives": [
                {
                    "id": str(b.id), "filename": b.original_filename,
                    "storage_key": b.storage_key, "ctr": round(ctr, 6),
                    "image_url": signed_urls.get(b.storage_key) if b.storage_key else None,
                }
                for b, ctr in bottom_banners
            ],
            "hypotheses": {
                "total": len(hypotheses),
                "by_status": dict(hyp_by_status),
                "validated_count": hyp_by_status.get("validated", 0),
                "testing_count": hyp_by_status.get("testing", 0),
            },
            "insights": {
                "total_active": len(insights),
                "by_type": dict(insight_by_type),
                "by_severity": dict(insight_by_severity),
                "critical_count": insight_by_severity.get("critical", 0),
                "warning_count": insight_by_severity.get("warning", 0),
            },
            "ab_tests": {
                "total": len(ab_tests),
                "by_status": dict(ab_by_status),
                "running_count": ab_by_status.get("running", 0),
                "completed_count": ab_by_status.get("completed", 0),
            },
        },
    }


# ---------------------------------------------------------------------------
# Platform Ad Specs — standalone reference endpoint
# ---------------------------------------------------------------------------

@analytics_router.get("/platform-specs")
async def get_platform_specs(
    platform_id: Optional[str] = None,
    region: Optional[str] = None,
):
    """Standalone platform specs reference — no auth required for static data."""
    specs = _PLATFORM_AD_SPECS

    if platform_id:
        specs = [p for p in specs if p["id"] == platform_id]
    elif region:
        specs = [p for p in specs if p.get("region") == region]

    # Build platform-specific tech requirements if single platform requested
    platform_reqs = []
    if platform_id and specs:
        platform_reqs = _build_platform_specific_requirements(specs[0])

    return {
        "platform_specs": specs,
        "platform_requirements": platform_reqs,
        "universal_formats": _UNIVERSAL_FORMATS,
        "tech_constants": _TECH_CONSTANTS,
    }
