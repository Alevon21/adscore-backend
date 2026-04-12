"""Cross-platform creative adaptation recommendations."""

import json
import logging
import os

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from auth import get_current_user, CurrentUser
from database import get_db
from db_models import Banner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/adscore", tags=["cross_platform"])

PLATFORM_SPECS = {
    "vk": {"name": "VK Реклама", "formats": ["1080x607", "1080x1080", "1000x120"], "max_text_pct": 50, "video_max_sec": 15, "tone": "дружелюбный, неформальный", "audience": "18-45, RU/CIS"},
    "google": {"name": "Google Ads", "formats": ["1200x628", "1080x1080", "300x250", "728x90"], "max_text_pct": 20, "video_max_sec": 30, "tone": "информативный, лаконичный", "audience": "глобальная"},
    "facebook": {"name": "Meta (FB/IG)", "formats": ["1080x1080", "1200x628", "1080x1920"], "max_text_pct": 20, "video_max_sec": 15, "tone": "вовлекающий, эмоциональный", "audience": "25-55, глобальная"},
    "yandex": {"name": "Яндекс Директ", "formats": ["1080x607", "300x250", "728x90", "240x400"], "max_text_pct": 50, "video_max_sec": 15, "tone": "информативный, прямой", "audience": "25-55, RU"},
    "tiktok": {"name": "TikTok Ads", "formats": ["1080x1920", "1080x1080"], "max_text_pct": 10, "video_max_sec": 60, "tone": "динамичный, молодёжный, UGC-стиль", "audience": "16-35, глобальная"},
    "mytarget": {"name": "myTarget", "formats": ["1080x607", "600x600", "240x400"], "max_text_pct": 50, "video_max_sec": 30, "tone": "прямой, рекламный", "audience": "25-55, RU/CIS"},
    "instagram": {"name": "Instagram", "formats": ["1080x1080", "1080x1920", "1080x566"], "max_text_pct": 15, "video_max_sec": 60, "tone": "визуальный, эстетичный", "audience": "18-40, глобальная"},
}

ADAPT_PROMPT = """Ты — эксперт по кросс-платформенной рекламе. Тебе нужно адаптировать креатив с одной платформы на другую.

## Исходный креатив
**Платформа:** {source_name}
**Элементы баннера:**
{elements}

## Целевая платформа
**Платформа:** {target_name}
**Форматы:** {target_formats}
**Макс. текст:** {target_text_pct}%
**Тональность:** {target_tone}
**Аудитория:** {target_audience}

## Задача

Дай конкретные рекомендации по адаптации. Верни JSON:

```json
{{
  "summary": "Краткое резюме ключевых изменений (1-2 предложения)",
  "adaptations": [
    {{
      "category": "Формат/Текст/Визуал/Тональность/CTA",
      "change": "Что изменить",
      "reason": "Почему",
      "priority": "high/medium/low"
    }}
  ],
  "keep": ["Что оставить без изменений (список)"],
  "warnings": ["Важные ограничения платформы"],
  "recommended_format": "Рекомендуемый размер для целевой платформы"
}}
```

Верни ТОЛЬКО JSON без markdown."""


class AdaptRequest(BaseModel):
    banner_id: str
    target_platform: str
    source_platform: Optional[str] = None


@router.post("/cross-platform-adapt")
async def cross_platform_adapt(
    body: AdaptRequest,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Generate cross-platform adaptation recommendations for a banner."""
    tid = current_user.tenant.id

    # Fetch banner
    q = select(Banner).where(Banner.tenant_id == tid, Banner.id == body.banner_id)
    banner = (await db.execute(q)).scalar_one_or_none()
    if not banner:
        raise HTTPException(status_code=404, detail="Баннер не найден")

    if not banner.tags or banner.tags_status != "done":
        raise HTTPException(status_code=400, detail="Баннер должен быть протегирован")

    # Determine source platform
    source_platform = body.source_platform
    if not source_platform and banner.metrics:
        metrics = banner.metrics if isinstance(banner.metrics, dict) else {}
        source_platform = metrics.get("platform", "")
    if not source_platform:
        source_platform = "universal"

    target = body.target_platform
    if target not in PLATFORM_SPECS:
        raise HTTPException(status_code=400, detail=f"Неизвестная платформа: {target}")

    target_spec = PLATFORM_SPECS[target]
    source_spec = PLATFORM_SPECS.get(source_platform, {"name": source_platform.title()})

    # Format banner elements from tags
    tags = banner.tags if isinstance(banner.tags, dict) else {}
    element_lines = []
    for cat, data in tags.items():
        if isinstance(data, dict):
            for k, v in data.items():
                if v and v not in (False, "нет", "отсутствует"):
                    element_lines.append(f"- {k}: {v}")
        elif isinstance(data, (str, bool)):
            if data and data not in (False, "нет"):
                element_lines.append(f"- {cat}: {data}")

    prompt = ADAPT_PROMPT.format(
        source_name=source_spec.get("name", source_platform),
        elements="\n".join(element_lines[:30]) if element_lines else "Нет данных",
        target_name=target_spec["name"],
        target_formats=", ".join(target_spec["formats"]),
        target_text_pct=target_spec["max_text_pct"],
        target_tone=target_spec["tone"],
        target_audience=target_spec["audience"],
    )

    # Call Claude
    import anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY не настроен")

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text.strip()
    if response_text.startswith("```"):
        lines = response_text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        response_text = "\n".join(lines)

    try:
        result = json.loads(response_text)
    except json.JSONDecodeError:
        logger.error("Failed to parse cross-platform response: %s", response_text)
        raise HTTPException(status_code=500, detail="AI не смог сформировать ответ")

    return {
        "banner_id": body.banner_id,
        "source_platform": source_platform,
        "target_platform": target,
        "source_name": source_spec.get("name", source_platform),
        "target_name": target_spec["name"],
        **result,
    }


@router.get("/platforms-for-adapt")
async def platforms_for_adapt(
    current_user: CurrentUser = Depends(get_current_user),
):
    """Return available platforms for cross-platform adaptation."""
    return {
        "platforms": [
            {"id": k, "name": v["name"], "formats": v["formats"]}
            for k, v in PLATFORM_SPECS.items()
        ]
    }
