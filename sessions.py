"""Session history API — list, detail, results for persisted scoring sessions."""

from typing import Optional
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user, CurrentUser
from database import get_db
from db_models import ScoringSession, ScoringResult, SessionStatus, User

router = APIRouter(prefix="/sessions", tags=["sessions"])


@router.get("")
async def list_sessions(
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None),
):
    """List scoring sessions for the current tenant."""
    tid = current_user.tenant.id
    q = (
        select(ScoringSession)
        .where(ScoringSession.tenant_id == tid)
        .where(ScoringSession.status != SessionStatus.failed)  # hide deleted
        .order_by(ScoringSession.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    if status:
        try:
            s = SessionStatus(status)
            q = q.where(ScoringSession.status == s)
        except ValueError:
            pass

    result = await db.execute(q)
    sessions = result.scalars().all()

    # Get total count
    count_q = (
        select(func.count())
        .select_from(ScoringSession)
        .where(ScoringSession.tenant_id == tid)
        .where(ScoringSession.status != SessionStatus.failed)
    )
    total = (await db.execute(count_q)).scalar()

    # Get creator names
    user_ids = {s.created_by for s in sessions}
    users_map = {}
    if user_ids:
        users_result = await db.execute(select(User).where(User.id.in_(user_ids)))
        for u in users_result.scalars().all():
            users_map[u.id] = u.name or u.email

    return {
        "sessions": [
            {
                "id": str(s.id),
                "file_name": s.file_name,
                "mode": s.mode,
                "n_rows": s.n_rows,
                "status": s.status.value,
                "created_at": s.created_at.isoformat() if s.created_at else None,
                "completed_at": s.completed_at.isoformat() if s.completed_at else None,
                "created_by": users_map.get(s.created_by, "Unknown"),
            }
            for s in sessions
        ],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/{session_id}")
async def get_session_detail(
    session_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get full session details including mapping and params."""
    tid = current_user.tenant.id
    sid = uuid.UUID(session_id)

    result = await db.execute(
        select(ScoringSession)
        .where(ScoringSession.id == sid, ScoringSession.tenant_id == tid)
    )
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    return {
        "id": str(session.id),
        "file_name": session.file_name,
        "mode": session.mode,
        "n_rows": session.n_rows,
        "status": session.status.value,
        "columns_detected": session.columns_detected,
        "auto_mapped": session.auto_mapped,
        "mapping": session.mapping,
        "events_detected": session.events_detected,
        "params": session.params,
        "created_at": session.created_at.isoformat() if session.created_at else None,
        "completed_at": session.completed_at.isoformat() if session.completed_at else None,
    }


@router.get("/{session_id}/results")
async def get_session_results(
    session_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get stored scoring results for a session."""
    tid = current_user.tenant.id
    sid = uuid.UUID(session_id)

    # Verify session belongs to tenant
    session_result = await db.execute(
        select(ScoringSession.id)
        .where(ScoringSession.id == sid, ScoringSession.tenant_id == tid)
    )
    if not session_result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Session not found")

    # Get results
    result = await db.execute(
        select(ScoringResult)
        .where(ScoringResult.session_id == sid)
        .order_by(ScoringResult.created_at.desc())
        .limit(1)
    )
    scoring_result = result.scalar_one_or_none()
    if not scoring_result:
        raise HTTPException(status_code=404, detail="No results found for this session")

    return {
        "results": scoring_result.results,
        "stats": scoring_result.stats,
        "text_part_result": scoring_result.text_part_result,
        "campaign_analysis": scoring_result.campaign_analysis,
    }
