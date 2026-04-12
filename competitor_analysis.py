"""Competitor Creative Analysis — AI-powered comparison of competitor banners with user's own creatives."""

import base64
import json
import logging
import os
from io import BytesIO
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from typing import Optional

from auth import get_current_user, CurrentUser
from database import get_db
from db_models import Banner
from adscore_tagger import _resize_image
import storage as file_storage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/competitor", tags=["competitor"])

VALID_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".webp")
MAX_SIZE = 10 * 1024 * 1024  # 10 MB

COMPETITOR_ANALYSIS_PROMPT = """Ты — эксперт по performance-маркетингу и рекламным креативам.

Тебе даны два рекламных баннера:
1. **Баннер пользователя** — его собственный креатив
2. **Баннер конкурента** — креатив конкурента

Проведи детальный сравнительный анализ и верни JSON:

{
  "competitor_strengths": [
    "строка — что конкурент делает лучше (3-5 пунктов)"
  ],
  "competitor_weaknesses": [
    "строка — слабые стороны конкурента (2-4 пункта)"
  ],
  "user_advantages": [
    "строка — преимущества баннера пользователя (2-4 пункта)"
  ],
  "user_improvements": [
    "строка — что пользователю стоит позаимствовать/улучшить (3-5 конкретных рекомендаций)"
  ],
  "visual_comparison": {
    "color_scheme": "у кого лучше и почему (1-2 предложения)",
    "composition": "у кого лучше композиция и почему",
    "typography": "у кого лучше типографика",
    "cta_effectiveness": "у кого эффективнее CTA"
  },
  "element_comparison": {
    "shared_elements": ["элементы, которые используют оба"],
    "unique_to_competitor": ["элементы только у конкурента"],
    "unique_to_user": ["элементы только у пользователя"],
    "missing_elements": ["элементы, которые стоит добавить обоим"]
  },
  "scores": {
    "user_overall": число 1-10,
    "competitor_overall": число 1-10,
    "user_thumb_stop": число 1-10,
    "competitor_thumb_stop": число 1-10,
    "user_clarity": число 1-10,
    "competitor_clarity": число 1-10
  },
  "action_plan": [
    "строка — конкретное действие для улучшения (3-5 пунктов, от самого важного)"
  ],
  "summary": "краткое резюме в 2-3 предложения — главный вывод из сравнения"
}

Правила:
- Все строки на РУССКОМ языке
- Будь конкретным: не "улучшить дизайн", а "добавить контрастный CTA-баттон в правый нижний угол"
- Оценки от 1 до 10, где 10 = идеальный перформанс-креатив
- Если один баннер значительно лучше — скажи это прямо
- Верни ТОЛЬКО JSON, без markdown-обрамления"""

COMPETITOR_SOLO_PROMPT = """Ты — эксперт по performance-маркетингу и рекламным креативам.

Проанализируй этот рекламный баннер конкурента и верни JSON:

{
  "competitor_strengths": [
    "строка — сильные стороны креатива (3-5 пунктов)"
  ],
  "competitor_weaknesses": [
    "строка — слабые стороны креатива (2-4 пункта)"
  ],
  "user_improvements": [
    "строка — что можно позаимствовать для своих креативов (3-5 конкретных рекомендаций)"
  ],
  "visual_analysis": {
    "color_scheme": "анализ цветовой схемы (1-2 предложения)",
    "composition": "анализ композиции",
    "typography": "анализ типографики",
    "cta_effectiveness": "анализ CTA"
  },
  "detected_elements": [
    "список всех обнаруженных элементов креатива"
  ],
  "scores": {
    "overall": число 1-10,
    "thumb_stop": число 1-10,
    "clarity": число 1-10,
    "emotional_impact": число 1-10,
    "brand_presence": число 1-10
  },
  "estimated_target": "предполагаемая целевая аудитория (1 предложение)",
  "estimated_objective": "предполагаемая цель креатива: awareness | consideration | conversion",
  "action_plan": [
    "строка — конкретное действие, чтобы сделать лучше (3-5 пунктов)"
  ],
  "summary": "краткое резюме в 2-3 предложения — главный вывод"
}

Правила:
- Все строки на РУССКОМ языке
- Будь конкретным и практичным
- Оценки от 1 до 10, где 10 = идеальный перформанс-креатив
- Верни ТОЛЬКО JSON, без markdown-обрамления"""


def _analyze_with_claude(images: list[tuple[bytes, str]], prompt: str) -> dict:
    """Call Claude Vision with one or more images and a prompt."""
    import anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not configured")

    content = []
    for img_bytes, label in images:
        resized = _resize_image(img_bytes)
        b64 = base64.b64encode(resized).decode("utf-8")
        content.append({"type": "text", "text": f"--- {label} ---"})
        content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": "image/jpeg", "data": b64},
        })

    content.append({"type": "text", "text": prompt})

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        messages=[{"role": "user", "content": content}],
    )

    response_text = message.content[0].text.strip()
    if response_text.startswith("```"):
        lines = response_text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        response_text = "\n".join(lines)

    try:
        return json.loads(response_text)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse competitor analysis: %s\n%s", e, response_text)
        raise ValueError(f"AI returned invalid JSON: {e}")


@router.post("/analyze")
async def analyze_competitor(
    competitor_image: UploadFile = File(...),
    user_banner_id: str = Form(""),
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Analyze a competitor creative, optionally comparing with user's own banner."""
    ext = Path(competitor_image.filename or "image.png").suffix.lower()
    if ext not in VALID_EXTENSIONS:
        raise HTTPException(400, "Supported formats: PNG, JPG, GIF, WebP")

    competitor_bytes = await competitor_image.read()
    if len(competitor_bytes) > MAX_SIZE:
        raise HTTPException(400, "File too large (max 10 MB)")

    tid = current_user.tenant.id

    # If user provided a banner to compare
    if user_banner_id.strip():
        import uuid as uuid_mod
        try:
            bid = uuid_mod.UUID(user_banner_id)
        except ValueError:
            raise HTTPException(400, "Invalid banner ID")

        result = await db.execute(
            select(Banner).where(Banner.id == bid, Banner.tenant_id == tid)
        )
        banner = result.scalar_one_or_none()
        if not banner:
            raise HTTPException(404, "Banner not found")

        # Fetch user's banner image from storage
        import storage as file_storage
        try:
            user_bytes = await file_storage.download_file(banner.storage_key)
        except Exception:
            raise HTTPException(400, "Cannot fetch user banner image")

        images = [
            (user_bytes, "Баннер пользователя"),
            (competitor_bytes, "Баннер конкурента"),
        ]
        analysis = _analyze_with_claude(images, COMPETITOR_ANALYSIS_PROMPT)
        analysis["mode"] = "comparison"
        analysis["user_banner_id"] = str(banner.id)
        analysis["user_banner_name"] = banner.original_filename
    else:
        # Solo competitor analysis
        images = [(competitor_bytes, "Баннер конкурента")]
        analysis = _analyze_with_claude(images, COMPETITOR_SOLO_PROMPT)
        analysis["mode"] = "solo"

    return analysis


@router.get("/banners")
async def list_user_banners_for_comparison(
    project: Optional[str] = None,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List user's banners (id + name + thumbnail) for the comparison picker."""
    tid = current_user.tenant.id
    q = select(Banner).where(Banner.tenant_id == tid)
    if project == "__none__":
        q = q.where(Banner.project.is_(None))
    elif project:
        q = q.where(Banner.project == project)
    q = q.order_by(Banner.created_at.desc()).limit(50)
    result = await db.execute(q)
    rows = result.scalars().all()

    # Get signed URLs for thumbnails
    keys = [b.storage_key for b in rows if b.storage_key]
    signed = await file_storage.get_signed_urls(keys) if keys else {}

    banners = [
        {
            "id": str(b.id),
            "name": b.original_filename,
            "image_url": signed.get(b.storage_key),
            "project": b.project,
        }
        for b in rows
    ]
    return {"banners": banners}
