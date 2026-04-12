"""Activity Feed API — human-readable feed of recent team actions."""

from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from auth import get_current_user, CurrentUser
from database import get_db
from db_models import AuditLog, User

router = APIRouter(prefix="/activity", tags=["activity"])

# Human-readable action descriptions (Russian)
ACTION_LABELS = {
    "upload": "загрузил(а) файл",
    "upload_banners": "загрузил(а) баннеры",
    "score": "запустил(а) скоринг",
    "delete_file": "удалил(а) файл",
    "delete_banner": "удалил(а) баннер",
    "tag_banners": "запустил(а) AI-тегирование",
    "create_project": "создал(а) проект",
    "update_project": "обновил(а) проект",
    "create_hypothesis": "создал(а) гипотезу",
    "update_hypothesis": "обновил(а) гипотезу",
    "delete_hypothesis": "удалил(а) гипотезу",
    "add_comment": "оставил(а) комментарий",
    "update_comment": "отредактировал(а) комментарий",
    "delete_comment": "удалил(а) комментарий",
    "create_share_link": "создал(а) ссылку для шеринга",
    "register": "зарегистрировался",
    "login": "вошёл в систему",
    "invite_user": "пригласил(а) пользователя",
    "accept_invite": "принял(а) приглашение",
    "update_role": "изменил(а) роль пользователя",
    "remove_user": "удалил(а) пользователя",
    "mmp_upload": "загрузил(а) MMP-данные",
    "assign_concept_group": "назначил(а) группу концепции",
    "create_concept_group": "создал(а) группу концепции",
}

# Icons for frontend (action -> icon name)
ACTION_ICONS = {
    "upload": "upload",
    "upload_banners": "upload",
    "score": "chart",
    "delete_file": "delete",
    "delete_banner": "delete",
    "tag_banners": "ai",
    "create_project": "folder",
    "create_hypothesis": "lightbulb",
    "update_hypothesis": "lightbulb",
    "delete_hypothesis": "delete",
    "add_comment": "comment",
    "update_comment": "comment",
    "delete_comment": "delete",
    "create_share_link": "share",
    "register": "user",
    "login": "login",
    "invite_user": "user",
    "mmp_upload": "upload",
    "assign_concept_group": "group",
    "create_concept_group": "group",
}


class ActivityItem(BaseModel):
    id: int
    user_name: str
    user_id: str
    action: str
    action_label: str
    icon: str
    resource_type: Optional[str] = None
    resource_id: Optional[str] = None
    details: Optional[dict] = None
    created_at: str


@router.get("")
async def get_activity_feed(
    limit: int = Query(default=30, le=100),
    offset: int = Query(default=0, ge=0),
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get recent activity feed for the tenant — all team members can see."""
    # Exclude noisy actions from feed
    excluded_actions = {"login"}

    result = await db.execute(
        select(AuditLog, User.name, User.email)
        .join(User, AuditLog.user_id == User.id)
        .where(
            AuditLog.tenant_id == current_user.tenant.id,
            AuditLog.action.notin_(excluded_actions),
        )
        .order_by(AuditLog.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    rows = result.all()

    items = []
    for log, user_name, user_email in rows:
        display_name = user_name or user_email or "Unknown"
        items.append(ActivityItem(
            id=log.id,
            user_name=display_name,
            user_id=str(log.user_id),
            action=log.action,
            action_label=ACTION_LABELS.get(log.action, log.action),
            icon=ACTION_ICONS.get(log.action, "default"),
            resource_type=log.resource_type,
            resource_id=log.resource_id,
            details=log.details,
            created_at=log.created_at.isoformat(),
        ))

    return {"items": items, "total": len(items)}
