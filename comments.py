"""Comments API — add and list comments on banners and hypotheses."""

import uuid as uuid_mod
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from auth import get_current_user, CurrentUser
from database import get_db
from db_models import Comment
from users import log_audit

router = APIRouter(prefix="/comments", tags=["comments"])

VALID_TARGET_TYPES = {"banner", "hypothesis"}


class CommentCreate(BaseModel):
    target_type: str
    target_id: str
    text: str


class CommentOut(BaseModel):
    id: str
    target_type: str
    target_id: str
    text: str
    user_name: str
    user_id: str
    created_at: str
    updated_at: Optional[str] = None


class CommentUpdate(BaseModel):
    text: str


def _comment_to_out(c: Comment) -> dict:
    user_name = "Unknown"
    if c.user:
        user_name = c.user.name or c.user.email or "Unknown"
    return {
        "id": str(c.id),
        "target_type": c.target_type,
        "target_id": str(c.target_id),
        "text": c.text,
        "user_name": user_name,
        "user_id": str(c.user_id),
        "created_at": c.created_at.isoformat(),
        "updated_at": c.updated_at.isoformat() if c.updated_at else None,
    }


@router.get("/{target_type}/{target_id}")
async def list_comments(
    target_type: str,
    target_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if target_type not in VALID_TARGET_TYPES:
        raise HTTPException(400, f"target_type must be one of {VALID_TARGET_TYPES}")

    try:
        tid_uuid = uuid_mod.UUID(target_id)
    except ValueError:
        raise HTTPException(400, "Invalid target_id")

    result = await db.execute(
        select(Comment)
        .options(joinedload(Comment.user))
        .where(
            Comment.tenant_id == current_user.tenant.id,
            Comment.target_type == target_type,
            Comment.target_id == tid_uuid,
        )
        .order_by(Comment.created_at.asc())
    )
    comments = result.scalars().unique().all()
    return {"comments": [_comment_to_out(c) for c in comments]}


@router.post("")
async def create_comment(
    body: CommentCreate,
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if body.target_type not in VALID_TARGET_TYPES:
        raise HTTPException(400, f"target_type must be one of {VALID_TARGET_TYPES}")

    try:
        tid_uuid = uuid_mod.UUID(body.target_id)
    except ValueError:
        raise HTTPException(400, "Invalid target_id")

    text = body.text.strip()
    if not text:
        raise HTTPException(400, "Comment text cannot be empty")

    comment = Comment(
        tenant_id=current_user.tenant.id,
        user_id=current_user.user.id,
        target_type=body.target_type,
        target_id=tid_uuid,
        text=text,
    )
    db.add(comment)
    await log_audit(db, current_user.tenant.id, current_user.user.id, "add_comment", request,
                    resource_type=body.target_type, resource_id=body.target_id,
                    details={"text": text[:100]})
    await db.commit()
    await db.refresh(comment, attribute_names=["user"])

    # Re-fetch with joined user
    result = await db.execute(
        select(Comment)
        .options(joinedload(Comment.user))
        .where(Comment.id == comment.id)
    )
    comment = result.scalar_one()

    return _comment_to_out(comment)


@router.patch("/{comment_id}")
async def update_comment(
    comment_id: str,
    body: CommentUpdate,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        cid = uuid_mod.UUID(comment_id)
    except ValueError:
        raise HTTPException(400, "Invalid comment_id")

    result = await db.execute(
        select(Comment)
        .options(joinedload(Comment.user))
        .where(Comment.id == cid, Comment.tenant_id == current_user.tenant.id)
    )
    comment = result.scalar_one_or_none()
    if not comment:
        raise HTTPException(404, "Comment not found")

    if comment.user_id != current_user.user.id:
        raise HTTPException(403, "Can only edit your own comments")

    text = body.text.strip()
    if not text:
        raise HTTPException(400, "Comment text cannot be empty")

    comment.text = text
    await db.commit()
    await db.refresh(comment)

    result = await db.execute(
        select(Comment)
        .options(joinedload(Comment.user))
        .where(Comment.id == comment.id)
    )
    comment = result.scalar_one()
    return _comment_to_out(comment)


@router.delete("/{comment_id}")
async def delete_comment(
    comment_id: str,
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        cid = uuid_mod.UUID(comment_id)
    except ValueError:
        raise HTTPException(400, "Invalid comment_id")

    result = await db.execute(
        select(Comment).where(
            Comment.id == cid,
            Comment.tenant_id == current_user.tenant.id,
        )
    )
    comment = result.scalar_one_or_none()
    if not comment:
        raise HTTPException(404, "Comment not found")

    # Owner/admin can delete any comment, others only their own
    from db_models import UserRole
    if comment.user_id != current_user.user.id and current_user.user.role not in (UserRole.owner, UserRole.admin):
        raise HTTPException(403, "Can only delete your own comments")

    await db.delete(comment)
    await log_audit(db, current_user.tenant.id, current_user.user.id, "delete_comment", request,
                    resource_type=comment.target_type, resource_id=str(comment.target_id))
    await db.commit()
    return {"deleted": True}
