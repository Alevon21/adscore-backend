"""Element Trend Detection — tracks which visual elements are gaining or losing popularity and CTR impact over time."""
from __future__ import annotations

import logging
from typing import Optional
from collections import defaultdict
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user, CurrentUser
from database import get_db
from db_models import Banner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/adscore", tags=["trends"])


def _extract_elements(tags: dict) -> dict[str, bool]:
    """Extract flat boolean elements from tags structure."""
    elems = {}
    if not tags:
        return elems

    # Boolean fields
    for cat in ("visual", "structural", "emotional", "accessibility"):
        section = tags.get(cat, {})
        if isinstance(section, dict):
            for k, v in section.items():
                if isinstance(v, bool):
                    elems[k] = v

    # Categorical fields as field:value
    for cat in ("visual", "text", "structural", "platform_fit"):
        section = tags.get(cat, {})
        if isinstance(section, dict):
            for k, v in section.items():
                if isinstance(v, str) and v and v not in ("нет", "отсутствует", "средняя", "средний"):
                    elems[f"{k}:{v}"] = True

    return elems


def _get_ctr(banner) -> Optional[float]:
    """Extract CTR from banner metrics."""
    m = banner.metrics if isinstance(banner.metrics, dict) else {}
    clicks = m.get("clicks")
    imps = m.get("impressions")
    if clicks is not None and imps and imps > 0:
        return clicks / imps
    ctr = m.get("ctr")
    if ctr is not None:
        return ctr / 100 if ctr > 1 else ctr
    return None


@router.get("/element-trends")
async def element_trends(
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    project: Optional[str] = None,
):
    """Detect element trends by comparing recent vs older banners."""
    tid = current_user.tenant.id

    q = select(Banner).where(
        Banner.tenant_id == tid,
        Banner.tags_status == "done",
        Banner.tags.isnot(None),
    )
    if project:
        q = q.where(Banner.project == project)

    rows = (await db.execute(q.order_by(Banner.created_at.desc()))).scalars().all()

    if len(rows) < 4:
        return {"trends": [], "total_banners": len(rows), "message": "Нужно минимум 4 протегированных баннера"}

    # Split into two halves by upload date (recent vs older)
    mid = len(rows) // 2
    recent = rows[:mid]
    older = rows[mid:]

    def compute_element_stats(banners):
        """Compute element frequency and avg CTR."""
        elem_count = defaultdict(int)
        elem_ctrs = defaultdict(list)
        total = 0

        for b in banners:
            tags = b.tags if isinstance(b.tags, dict) else {}
            elems = _extract_elements(tags)
            ctr = _get_ctr(b)
            total += 1

            for key, val in elems.items():
                if val:
                    elem_count[key] += 1
                    if ctr is not None:
                        elem_ctrs[key].append(ctr)

        stats = {}
        for key in elem_count:
            freq = elem_count[key] / total if total > 0 else 0
            ctrs = elem_ctrs.get(key, [])
            avg_ctr = sum(ctrs) / len(ctrs) if ctrs else None
            stats[key] = {"freq": freq, "count": elem_count[key], "avg_ctr": avg_ctr}

        return stats, total

    recent_stats, recent_total = compute_element_stats(recent)
    older_stats, older_total = compute_element_stats(older)

    # Compare
    all_keys = set(recent_stats.keys()) | set(older_stats.keys())
    trends = []

    for key in all_keys:
        r = recent_stats.get(key, {"freq": 0, "count": 0, "avg_ctr": None})
        o = older_stats.get(key, {"freq": 0, "count": 0, "avg_ctr": None})

        freq_delta = r["freq"] - o["freq"]
        ctr_delta = None
        if r["avg_ctr"] is not None and o["avg_ctr"] is not None and o["avg_ctr"] > 0:
            ctr_delta = (r["avg_ctr"] - o["avg_ctr"]) / o["avg_ctr"] * 100

        # Only include elements with meaningful change
        if abs(freq_delta) < 0.05 and (ctr_delta is None or abs(ctr_delta) < 3):
            continue

        # Label
        label = key.replace("_", " ").replace(":", ": ").title()

        # Determine trend direction
        if freq_delta > 0.1:
            direction = "rising"
        elif freq_delta < -0.1:
            direction = "declining"
        else:
            direction = "stable"

        trends.append({
            "element": key,
            "label": label,
            "direction": direction,
            "recent_freq": round(r["freq"] * 100, 1),
            "older_freq": round(o["freq"] * 100, 1),
            "freq_delta": round(freq_delta * 100, 1),
            "recent_ctr": round(r["avg_ctr"] * 100, 2) if r["avg_ctr"] is not None else None,
            "older_ctr": round(o["avg_ctr"] * 100, 2) if o["avg_ctr"] is not None else None,
            "ctr_delta": round(ctr_delta, 1) if ctr_delta is not None else None,
            "recent_count": r["count"],
            "older_count": o["count"],
        })

    # Sort: rising elements first, then by frequency delta magnitude
    trends.sort(key=lambda t: (-1 if t["direction"] == "rising" else 1 if t["direction"] == "declining" else 0, -abs(t["freq_delta"])))

    return {
        "trends": trends,
        "total_banners": len(rows),
        "recent_count": recent_total,
        "older_count": older_total,
        "recent_period": recent[-1].created_at.isoformat() if recent else None,
        "older_period": older[0].created_at.isoformat() if older else None,
    }
