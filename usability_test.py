"""Usability test endpoints — collect UX trust research responses."""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from db_models import UsabilityTestResponse, UserRole
from auth import get_current_user, require_role, CurrentUser
from models import UsabilityTestSubmit

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/usability-test", tags=["usability-test"])


@router.post("/submit")
async def submit_test(
    body: UsabilityTestSubmit,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Save a completed usability test response."""
    scenarios = body.scenarios
    agreed = sum(1 for s in scenarios if s.agreement == "yes")
    rate = agreed / len(scenarios) if scenarios else 0.0

    row = UsabilityTestResponse(
        tenant_id=current_user.tenant.id,
        user_id=current_user.user.id,
        scenario_responses=[s.model_dump() for s in scenarios],
        survey_responses=body.survey.model_dump(),
        agreement_rate=round(rate, 4),
        total_duration_sec=body.total_duration_sec,
    )
    db.add(row)
    await db.commit()

    logger.info("Usability test saved for user %s (agreement %.0f%%)", current_user.user.id, rate * 100)
    return {"status": "ok", "agreement_rate": round(rate, 4)}


class AggregatedResults(BaseModel):
    total_responses: int
    avg_agreement_rate: Optional[float]
    avg_usability: Optional[float]
    avg_trust: Optional[float]
    avg_transparency: Optional[float]
    avg_intelligence: Optional[float]
    avg_real_world_usage: Optional[float]
    responses: list


@router.get("/results", response_model=AggregatedResults)
async def get_results(
    current_user: CurrentUser = Depends(require_role(UserRole.owner, UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """Get aggregated usability test results (owner/admin only)."""
    result = await db.execute(
        select(UsabilityTestResponse)
        .where(UsabilityTestResponse.tenant_id == current_user.tenant.id)
        .order_by(UsabilityTestResponse.created_at.desc())
    )
    rows = result.scalars().all()

    if not rows:
        return AggregatedResults(
            total_responses=0, avg_agreement_rate=None,
            avg_usability=None, avg_trust=None, avg_transparency=None,
            avg_intelligence=None, avg_real_world_usage=None, responses=[],
        )

    surveys = [r.survey_responses for r in rows if r.survey_responses]

    def avg(key):
        vals = [s[key] for s in surveys if key in s]
        return round(sum(vals) / len(vals), 2) if vals else None

    return AggregatedResults(
        total_responses=len(rows),
        avg_agreement_rate=round(sum(r.agreement_rate or 0 for r in rows) / len(rows), 4),
        avg_usability=avg("usability"),
        avg_trust=avg("trust"),
        avg_transparency=avg("transparency"),
        avg_intelligence=avg("intelligence"),
        avg_real_world_usage=avg("real_world_usage"),
        responses=[
            {
                "id": str(r.id),
                "user_id": str(r.user_id),
                "agreement_rate": r.agreement_rate,
                "total_duration_sec": r.total_duration_sec,
                "survey": r.survey_responses,
                "created_at": r.created_at.isoformat(),
            }
            for r in rows
        ],
    )
