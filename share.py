"""Share by link API — create and view shared reports without auth."""

import uuid as uuid_mod
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user, get_current_user_optional, CurrentUser
from database import get_db
from db_models import SharedLink, User, Tenant
from users import log_audit

router = APIRouter(prefix="/share", tags=["sharing"])


class CreateShareRequest(BaseModel):
    report_type: str  # stakeholder_report, budget, brief
    filters: Optional[dict] = None
    expires_in_days: Optional[int] = 30


class ShareLinkOut(BaseModel):
    token: str
    report_type: str
    filters: Optional[dict]
    expires_at: Optional[str]
    created_at: str


@router.post("/create")
async def create_share_link(
    body: CreateShareRequest,
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    valid_types = {"stakeholder_report", "budget", "brief"}
    if body.report_type not in valid_types:
        raise HTTPException(400, f"report_type must be one of {valid_types}")

    token = uuid_mod.uuid4()
    expires_at = None
    if body.expires_in_days and body.expires_in_days > 0:
        expires_at = datetime.now(timezone.utc) + timedelta(days=body.expires_in_days)

    link = SharedLink(
        token=token,
        report_type=body.report_type,
        filters=body.filters or {},
        tenant_id=current_user.tenant.id,
        created_by=current_user.user.id,
        expires_at=expires_at,
    )
    db.add(link)
    await db.commit()
    await db.refresh(link)

    await log_audit(
        db, current_user.tenant.id, current_user.user.id,
        "create_share_link", request=request,
        resource_type="shared_link", resource_id=str(link.id),
        details={"report_type": body.report_type, "filters": body.filters},
    )
    await db.commit()

    return {
        "token": str(link.token),
        "report_type": link.report_type,
        "filters": link.filters,
        "expires_at": link.expires_at.isoformat() if link.expires_at else None,
        "created_at": link.created_at.isoformat(),
    }


@router.get("/my-links")
async def list_my_links(
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(SharedLink)
        .where(SharedLink.tenant_id == current_user.tenant.id)
        .order_by(SharedLink.created_at.desc())
        .limit(50)
    )
    links = result.scalars().all()
    return {
        "links": [
            {
                "id": str(l.id),
                "token": str(l.token),
                "report_type": l.report_type,
                "filters": l.filters,
                "expires_at": l.expires_at.isoformat() if l.expires_at else None,
                "created_at": l.created_at.isoformat(),
            }
            for l in links
        ]
    }


@router.get("/{token}")
async def get_shared_report(
    token: str,
    db: AsyncSession = Depends(get_db),
):
    """Public endpoint — fetch report data by share token (no auth)."""
    try:
        token_uuid = uuid_mod.UUID(token)
    except ValueError:
        raise HTTPException(404, "Invalid token")

    result = await db.execute(
        select(SharedLink).where(SharedLink.token == token_uuid)
    )
    link = result.scalar_one_or_none()
    if not link:
        raise HTTPException(404, "Share link not found")

    if link.expires_at and datetime.now(timezone.utc) > link.expires_at:
        raise HTTPException(410, "This share link has expired")

    # Load tenant for context
    tenant_result = await db.execute(select(Tenant).where(Tenant.id == link.tenant_id))
    tenant = tenant_result.scalar_one_or_none()
    if not tenant or not tenant.is_active:
        raise HTTPException(404, "Share link not found")

    # Load creator user
    user_result = await db.execute(select(User).where(User.id == link.created_by))
    user = user_result.scalar_one_or_none()
    if not user:
        raise HTTPException(404, "Share link not found")

    # Create a pseudo CurrentUser to reuse existing report functions
    pseudo_user = CurrentUser(user=user, tenant=tenant)

    filters = link.filters or {}
    project = filters.get("project") or None

    if link.report_type == "stakeholder_report":
        from creative_analytics import get_stakeholder_report
        return await get_stakeholder_report(
            project=project,
            current_user=pseudo_user,
            db=db,
        )
    elif link.report_type == "budget":
        from creative_analytics import get_budget_recommendations
        return await get_budget_recommendations(
            project=project,
            current_user=pseudo_user,
            db=db,
        )
    elif link.report_type == "brief":
        from creative_analytics import generate_creative_brief
        return await generate_creative_brief(
            project=project,
            target_platform=filters.get("target_platform"),
            current_user=pseudo_user,
            db=db,
        )
    else:
        raise HTTPException(400, "Unknown report type")
