"""AI Creative Generator — generates creative briefs/mockups based on element performance data."""

import json
import logging
import os

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from auth import get_current_user, CurrentUser
from database import get_db
from db_models import Banner
from creative_history_models import CreativeGeneration

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/adscore", tags=["creative_generator"])

GENERATOR_PROMPT = """Ты — креативный директор в performance-маркетинге. На основе аналитики рекламных баннеров тебе нужно сгенерировать ТЗ (техническое задание) для НОВОГО креатива.

## Входные данные

**Целевая платформа:** {platform}
**Формат:** {format}
**Цель:** {goal}

**Лучшие элементы (повышают CTR):**
{must_have}

**Худшие элементы (снижают CTR):**
{avoid}

**Визуальный стиль лучших крео:**
{visual_style}

**Тональность лучших крео:**
{tonality}

**Примеры текстов с высоким CTR:**
{content_examples}

## Задача

Сгенерируй 3 варианта креатива. Для каждого варианта верни JSON-объект со следующей структурой:

```json
{{
  "variants": [
    {{
      "name": "Краткое название концепции",
      "concept": "Описание концепции в 2-3 предложениях",
      "headline": "Заголовок баннера",
      "subheadline": "Подзаголовок (если нужен)",
      "cta_text": "Текст CTA-кнопки",
      "body_text": "Основной текст (если нужен)",
      "visual_description": "Детальное описание визуала: фон, расположение элементов, цвета, стиль фото/иллюстрации",
      "color_palette": ["#hex1", "#hex2", "#hex3"],
      "layout": "Описание композиции: что вверху, что в центре, CTA, логотип",
      "mood": "Настроение/атмосфера",
      "why_it_works": "Почему этот вариант должен работать (связь с data)",
      "estimated_impact": "Ожидаемый эффект (выше/ниже среднего CTR и почему)"
    }}
  ],
  "general_recommendations": [
    "Общая рекомендация 1",
    "Общая рекомендация 2",
    "Общая рекомендация 3"
  ]
}}
```

Важно:
- Каждый вариант должен использовать ЛУЧШИЕ элементы и ИЗБЕГАТЬ худших
- Варианты должны быть РАЗНЫМИ по стилю (агрессивный vs мягкий, минималистичный vs насыщенный)
- Тексты на русском языке
- Привязывайся к реальным данным (элементы, CTR, корреляции)
- Верни ТОЛЬКО JSON без markdown-обрамления"""


class GenerateRequest(BaseModel):
    platform: str = "universal"
    format: str = "1080x1080"
    goal: str = "Максимизация CTR"
    project: Optional[str] = None


def _call_claude_text(prompt: str) -> dict:
    """Call Claude text API (no vision) for creative generation."""
    import anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not configured")

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text.strip()
    if response_text.startswith("```"):
        lines = response_text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        response_text = "\n".join(lines)

    try:
        return json.loads(response_text)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse creative generation: %s\n%s", e, response_text)
        raise HTTPException(status_code=500, detail="AI не смог сформировать ответ в JSON-формате")


def _format_elements(elements: list[dict], limit: int = 10) -> str:
    """Format element performance list into readable text."""
    if not elements:
        return "Нет данных"
    lines = []
    for el in elements[:limit]:
        name = el.get("label") or el.get("name", "")
        diff = el.get("diff_pct", 0)
        sign = "+" if diff > 0 else ""
        lines.append(f"- {name}: {sign}{diff:.1f}% к CTR")
    return "\n".join(lines)


@router.post("/generate-creative")
async def generate_creative(
    body: GenerateRequest,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Generate AI creative brief/mockup based on element performance data."""
    tid = current_user.tenant.id

    # Gather element performance data from brief endpoint logic
    from creative_analytics import _extract_elements, _get_ctr, _aggregate_tag_field

    # Fetch tagged banners
    q = select(Banner).where(
        Banner.tenant_id == tid,
        Banner.tags_status == "done",
        Banner.tags.isnot(None),
    )
    if body.project:
        q = q.where(Banner.project == body.project)

    rows = (await db.execute(q)).scalars().all()
    if len(rows) < 3:
        raise HTTPException(status_code=400, detail="Нужно минимум 3 протегированных баннера для генерации")

    # Compute element performance (simplified from brief endpoint)
    banner_elements = []
    ctrs = []
    for b in rows:
        tags = b.tags if isinstance(b.tags, dict) else {}
        ctr = _get_ctr(b.metrics if isinstance(b.metrics, dict) else {})
        if ctr is None:
            continue
        elems = _extract_elements(tags)
        banner_elements.append(elems)
        ctrs.append(ctr)

    if len(ctrs) < 3:
        raise HTTPException(status_code=400, detail="Нужно минимум 3 баннера с CTR-данными")

    # Compute element impact
    from collections import defaultdict
    with_elem = defaultdict(list)
    without_elem = defaultdict(list)

    all_keys = set()
    for elems in banner_elements:
        all_keys.update(elems.keys())

    for i, elems in enumerate(banner_elements):
        for key in all_keys:
            if elems.get(key):
                with_elem[key].append(ctrs[i])
            else:
                without_elem[key].append(ctrs[i])

    impacts = []
    for key in all_keys:
        w = with_elem[key]
        wo = without_elem[key]
        if len(w) < 1 or len(wo) < 1:
            continue
        avg_w = sum(w) / len(w)
        avg_wo = sum(wo) / len(wo)
        if avg_wo == 0:
            continue
        diff_pct = (avg_w - avg_wo) / avg_wo * 100
        if abs(diff_pct) < 2:
            continue
        # Create readable label
        label = key.replace("_", " ").replace(":", ": ").title()
        impacts.append({
            "name": key,
            "label": label,
            "diff_pct": round(diff_pct, 1),
            "n_with": len(w),
            "n_without": len(wo),
        })

    impacts.sort(key=lambda x: x["diff_pct"], reverse=True)
    must_have = [e for e in impacts if e["diff_pct"] > 0]
    avoid = [e for e in impacts if e["diff_pct"] < 0]
    avoid.sort(key=lambda x: x["diff_pct"])

    # Aggregate visual style and tonality from top banners
    sorted_banners = sorted(
        [(b, _get_ctr(b.metrics if isinstance(b.metrics, dict) else {})) for b in rows if _get_ctr(b.metrics if isinstance(b.metrics, dict) else {}) is not None],
        key=lambda x: x[1],
        reverse=True,
    )
    top_banners = [b for b, _ in sorted_banners[:5]]

    visual_parts = []
    visual_agg = _aggregate_tag_field(top_banners, "visual", ["background_type", "color_scheme", "visual_hierarchy", "visual_clutter"])
    for field, val in visual_agg.items():
        v = val["value"] if isinstance(val, dict) else str(val)
        visual_parts.append(f"- {field.replace('_', ' ').title()}: {v}")

    tonality_parts = []
    emotional_agg = _aggregate_tag_field(top_banners, "emotional", ["tonality", "energy_level", "personalization_level"])
    for field, val in emotional_agg.items():
        v = val["value"] if isinstance(val, dict) else str(val)
        tonality_parts.append(f"- {field.replace('_', ' ').title()}: {v}")

    content_parts = []
    text_agg = _aggregate_tag_field(top_banners, "text_elements", ["headline", "cta_text", "offer"])
    for field, val in text_agg.items():
        v = val["value"] if isinstance(val, dict) else str(val)
        content_parts.append(f"- {field.replace('_', ' ').title()}: {v}")

    # Build prompt
    filled_prompt = GENERATOR_PROMPT.format(
        platform=body.platform,
        format=body.format,
        goal=body.goal,
        must_have=_format_elements(must_have),
        avoid=_format_elements(avoid),
        visual_style="\n".join(visual_parts) if visual_parts else "Нет данных",
        tonality="\n".join(tonality_parts) if tonality_parts else "Нет данных",
        content_examples="\n".join(content_parts) if content_parts else "Нет данных",
    )

    # Call Claude
    result = _call_claude_text(filled_prompt)

    input_summary = {
        "total_banners": len(rows),
        "banners_with_ctr": len(ctrs),
        "top_positive_elements": must_have[:5],
        "top_negative_elements": avoid[:5],
        "platform": body.platform,
        "format": body.format,
        "goal": body.goal,
    }

    # Save generation to history
    import uuid as uuid_mod
    generation = CreativeGeneration(
        id=uuid_mod.uuid4(),
        tenant_id=tid,
        user_id=current_user.user.id,
        platform=body.platform,
        format=body.format,
        goal=body.goal,
        project=body.project,
        result=result,
        input_summary=input_summary,
    )
    db.add(generation)
    await db.commit()

    logger.info("Saved creative generation %s (tenant %s)", generation.id, tid)

    return {
        "id": str(generation.id),
        "variants": result.get("variants", []),
        "general_recommendations": result.get("general_recommendations", []),
        "input_summary": input_summary,
    }


@router.get("/generations")
async def list_generations(
    project: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List saved creative generations."""
    tid = current_user.tenant.id
    stmt = (
        select(CreativeGeneration)
        .where(CreativeGeneration.tenant_id == tid)
    )
    if project:
        stmt = stmt.where(CreativeGeneration.project == project)
    stmt = stmt.order_by(CreativeGeneration.created_at.desc()).offset(offset).limit(limit)

    rows = (await db.execute(stmt)).scalars().all()

    # Total count
    count_stmt = select(func.count(CreativeGeneration.id)).where(CreativeGeneration.tenant_id == tid)
    if project:
        count_stmt = count_stmt.where(CreativeGeneration.project == project)
    total = (await db.execute(count_stmt)).scalar() or 0

    return {
        "generations": [
            {
                "id": str(g.id),
                "platform": g.platform,
                "format": g.format,
                "goal": g.goal,
                "project": g.project,
                "variants": g.result.get("variants", []) if isinstance(g.result, dict) else [],
                "general_recommendations": g.result.get("general_recommendations", []) if isinstance(g.result, dict) else [],
                "input_summary": g.input_summary,
                "created_at": g.created_at.isoformat() if g.created_at else None,
            }
            for g in rows
        ],
        "total": total,
    }


@router.get("/generations/{generation_id}")
async def get_generation(
    generation_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a single generation by ID."""
    import uuid as uuid_mod
    tid = current_user.tenant.id
    stmt = (
        select(CreativeGeneration)
        .where(CreativeGeneration.id == uuid_mod.UUID(generation_id))
        .where(CreativeGeneration.tenant_id == tid)
    )
    g = (await db.execute(stmt)).scalar_one_or_none()
    if not g:
        raise HTTPException(404, "Generation not found")
    return {
        "id": str(g.id),
        "platform": g.platform,
        "format": g.format,
        "goal": g.goal,
        "project": g.project,
        "variants": g.result.get("variants", []) if isinstance(g.result, dict) else [],
        "general_recommendations": g.result.get("general_recommendations", []) if isinstance(g.result, dict) else [],
        "input_summary": g.input_summary,
        "created_at": g.created_at.isoformat() if g.created_at else None,
    }


@router.delete("/generations/{generation_id}")
async def delete_generation(
    generation_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a generation."""
    import uuid as uuid_mod
    tid = current_user.tenant.id
    stmt = (
        select(CreativeGeneration)
        .where(CreativeGeneration.id == uuid_mod.UUID(generation_id))
        .where(CreativeGeneration.tenant_id == tid)
    )
    g = (await db.execute(stmt)).scalar_one_or_none()
    if not g:
        raise HTTPException(404, "Generation not found")
    await db.delete(g)
    await db.commit()
    return {"deleted": True}
