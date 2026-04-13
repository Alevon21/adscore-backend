"""Session history API — list, detail, results for persisted scoring sessions."""

from typing import Optional
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy import select, func, or_
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user, CurrentUser
from database import get_db
from db_models import ScoringSession, ScoringResult, SessionStatus, User, SessionShare
from users import log_audit

router = APIRouter(prefix="/sessions", tags=["sessions"])


class UpdateVisibilityRequest(BaseModel):
    visibility: str  # team, private, selected


class ShareSessionRequest(BaseModel):
    user_ids: list  # list of user_id strings to share with


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

    shared_session_ids = (
        select(SessionShare.session_id)
        .where(SessionShare.user_id == current_user.user.id)
        .correlate(ScoringSession)
    )

    q = (
        select(ScoringSession)
        .where(ScoringSession.tenant_id == tid)
        .where(ScoringSession.status.notin_([SessionStatus.deleted]))
        .where(
            or_(
                ScoringSession.visibility == "team",
                ScoringSession.created_by == current_user.user.id,
                ScoringSession.id.in_(shared_session_ids),
            )
        )
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

    # Get total count (with same filters as main query)
    count_q = (
        select(func.count())
        .select_from(ScoringSession)
        .where(ScoringSession.tenant_id == tid)
        .where(ScoringSession.status.notin_([SessionStatus.deleted]))
        .where(
            or_(
                ScoringSession.visibility == "team",
                ScoringSession.created_by == current_user.user.id,
                ScoringSession.id.in_(shared_session_ids),
            )
        )
    )
    if status:
        try:
            count_q = count_q.where(ScoringSession.status == SessionStatus(status))
        except ValueError:
            pass
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
                "visibility": s.visibility if hasattr(s, 'visibility') else "team",
                "created_by_id": str(s.created_by),
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

    # Visibility check
    if session.visibility != "team" and session.created_by != current_user.user.id:
        share_check = await db.execute(
            select(SessionShare.id)
            .where(SessionShare.session_id == sid, SessionShare.user_id == current_user.user.id)
        )
        if not share_check.scalar_one_or_none():
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
        "visibility": session.visibility if hasattr(session, 'visibility') else "team",
        "created_by_id": str(session.created_by),
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
        select(ScoringSession)
        .where(ScoringSession.id == sid, ScoringSession.tenant_id == tid)
    )
    session = session_result.scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # Visibility check
    if session.visibility != "team" and session.created_by != current_user.user.id:
        share_check = await db.execute(
            select(SessionShare.id)
            .where(SessionShare.session_id == sid, SessionShare.user_id == current_user.user.id)
        )
        if not share_check.scalar_one_or_none():
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


@router.patch("/{session_id}/visibility")
async def update_session_visibility(
    session_id: str,
    body: UpdateVisibilityRequest,
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update session visibility (creator only)."""
    tid = current_user.tenant.id
    sid = uuid.UUID(session_id)

    if body.visibility not in ("team", "private", "selected"):
        raise HTTPException(400, "Invalid visibility value")

    result = await db.execute(
        select(ScoringSession)
        .where(ScoringSession.id == sid, ScoringSession.tenant_id == tid)
    )
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(404, "Session not found")
    if session.created_by != current_user.user.id:
        raise HTTPException(403, "Only the session creator can change visibility")

    session.visibility = body.visibility
    await db.commit()

    await log_audit(db, tid, current_user.user.id, "update_session_visibility", request,
        resource_type="session", resource_id=session_id,
        details={"visibility": body.visibility})
    await db.commit()

    return {"ok": True, "visibility": body.visibility}


@router.post("/{session_id}/share")
async def share_session(
    session_id: str,
    body: ShareSessionRequest,
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Share session with specific users. Also sets visibility to 'selected' if it was 'private'."""
    tid = current_user.tenant.id
    sid = uuid.UUID(session_id)

    result = await db.execute(
        select(ScoringSession)
        .where(ScoringSession.id == sid, ScoringSession.tenant_id == tid)
    )
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(404, "Session not found")
    if session.created_by != current_user.user.id:
        raise HTTPException(403, "Only the session creator can share")

    # Validate user_ids belong to same tenant
    from sqlalchemy import delete as sa_delete

    user_uuids = [uuid.UUID(uid) for uid in body.user_ids]
    valid_users = await db.execute(
        select(User.id).where(User.tenant_id == tid, User.id.in_(user_uuids), User.is_active == True)
    )
    valid_ids = {row[0] for row in valid_users.all()}

    # Remove old shares and add new ones
    await db.execute(
        sa_delete(SessionShare).where(SessionShare.session_id == sid)
    )

    for uid in valid_ids:
        if uid != current_user.user.id:  # Don't share with self
            share = SessionShare(
                session_id=sid,
                user_id=uid,
                shared_by=current_user.user.id,
            )
            db.add(share)

    # Auto-set visibility to selected if it was private
    if session.visibility == "private" and valid_ids:
        session.visibility = "selected"

    await db.commit()

    await log_audit(db, tid, current_user.user.id, "share_session", request,
        resource_type="session", resource_id=session_id,
        details={"shared_with": [str(uid) for uid in valid_ids]})
    await db.commit()

    return {"ok": True, "shared_with": len(valid_ids)}


@router.get("/{session_id}/shares")
async def get_session_shares(
    session_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get list of users a session is shared with."""
    tid = current_user.tenant.id
    sid = uuid.UUID(session_id)

    # Verify session belongs to tenant and user is creator
    result = await db.execute(
        select(ScoringSession)
        .where(ScoringSession.id == sid, ScoringSession.tenant_id == tid)
    )
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(404, "Session not found")

    shares_result = await db.execute(
        select(SessionShare, User)
        .join(User, SessionShare.user_id == User.id)
        .where(SessionShare.session_id == sid)
    )
    shares = shares_result.all()

    return {
        "visibility": session.visibility if hasattr(session, 'visibility') else "team",
        "shares": [
            {
                "user_id": str(share.SessionShare.user_id),
                "user_name": share.User.name or share.User.email,
                "user_email": share.User.email,
                "shared_at": share.SessionShare.created_at.isoformat(),
            }
            for share in shares
        ],
    }


@router.delete("/{session_id}/shares/{user_id}")
async def remove_session_share(
    session_id: str,
    user_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Remove a user from session shares."""
    tid = current_user.tenant.id
    sid = uuid.UUID(session_id)

    result = await db.execute(
        select(ScoringSession)
        .where(ScoringSession.id == sid, ScoringSession.tenant_id == tid)
    )
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(404, "Session not found")
    if session.created_by != current_user.user.id:
        raise HTTPException(403, "Only the session creator can manage shares")

    from sqlalchemy import delete as sa_delete
    await db.execute(
        sa_delete(SessionShare).where(
            SessionShare.session_id == sid,
            SessionShare.user_id == uuid.UUID(user_id),
        )
    )
    await db.commit()

    return {"ok": True}
