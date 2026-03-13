import re
import uuid
from datetime import datetime, timezone
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from db_models import User, Tenant, AuditLog, UserRole
from auth import get_current_user, require_role, CurrentUser

router = APIRouter(prefix="/auth", tags=["auth"])
tenant_router = APIRouter(prefix="/tenants", tags=["tenants"])


# ── Schemas ──────────────────────────────────────────────

class RegisterRequest(BaseModel):
    supabase_uid: str
    email: str
    name: Optional[str] = None
    company_name: str


class UserResponse(BaseModel):
    id: str
    email: str
    name: Optional[str]
    role: str
    tenant_id: str
    tenant_name: str
    tenant_slug: str
    tenant_plan: str

    class Config:
        from_attributes = True


class InviteRequest(BaseModel):
    supabase_uid: str
    email: str
    name: Optional[str] = None
    role: str = "analyst"


class RoleUpdateRequest(BaseModel):
    role: str


class AuditLogResponse(BaseModel):
    id: int
    user_id: str
    action: str
    resource_type: Optional[str]
    resource_id: Optional[str]
    ip: Optional[str]
    details: Optional[dict]
    created_at: str


# ── Helpers ──────────────────────────────────────────────

def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text[:100]


def user_to_response(user: User) -> UserResponse:
    return UserResponse(
        id=str(user.id),
        email=user.email,
        name=user.name,
        role=user.role.value,
        tenant_id=str(user.tenant_id),
        tenant_name=user.tenant.name,
        tenant_slug=user.tenant.slug,
        tenant_plan=user.tenant.plan.value,
    )


async def log_audit(db: AsyncSession, tenant_id, user_id, action, request: Request = None, **kwargs):
    log = AuditLog(
        tenant_id=tenant_id,
        user_id=user_id,
        action=action,
        resource_type=kwargs.get("resource_type"),
        resource_id=kwargs.get("resource_id"),
        ip=request.client.host if request and request.client else None,
        user_agent=request.headers.get("user-agent", "")[:500] if request else None,
        details=kwargs.get("details"),
    )
    db.add(log)
    await db.commit()


# ── Auth Routes ──────────────────────────────────────────

@router.post("/register", response_model=UserResponse)
async def register(
    body: RegisterRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Register a new user + tenant after Supabase signup."""
    # Check if user already exists — return existing profile (idempotent)
    existing = await db.execute(
        select(User).where(User.supabase_uid == body.supabase_uid)
    )
    existing_user = existing.scalar_one_or_none()
    if existing_user:
        # Ensure user is active
        if not existing_user.is_active:
            existing_user.is_active = True
            db.add(existing_user)
            await db.commit()
            await db.refresh(existing_user)
        return user_to_response(existing_user)

    # Create tenant
    slug = slugify(body.company_name)
    # Ensure slug uniqueness
    slug_exists = await db.execute(select(Tenant).where(Tenant.slug == slug))
    if slug_exists.scalar_one_or_none():
        slug = f"{slug}-{uuid.uuid4().hex[:6]}"

    tenant = Tenant(name=body.company_name, slug=slug)
    db.add(tenant)
    await db.flush()  # get tenant.id

    # Create user as owner
    user = User(
        supabase_uid=body.supabase_uid,
        email=body.email,
        name=body.name,
        tenant_id=tenant.id,
        role=UserRole.owner,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    await db.refresh(tenant)

    # Audit
    await log_audit(db, tenant.id, user.id, "register", request, details={"company": body.company_name})

    return user_to_response(user)


@router.get("/me", response_model=UserResponse)
async def get_me(
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get current user profile."""
    user = current_user.user
    # Update last_login
    user.last_login = datetime.now(timezone.utc)
    db.add(user)
    await db.commit()
    return user_to_response(user)


# ── Tenant Management ───────────────────────────────────

@tenant_router.get("/{tenant_id}/users", response_model=List[UserResponse])
async def list_users(
    tenant_id: str,
    current_user: CurrentUser = Depends(require_role(UserRole.owner, UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """List all users in the tenant (owner/admin only)."""
    if str(current_user.tenant.id) != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")

    result = await db.execute(
        select(User).where(User.tenant_id == current_user.tenant.id, User.is_active == True)
    )
    users = result.scalars().all()
    return [user_to_response(u) for u in users]


@tenant_router.post("/{tenant_id}/invite", response_model=UserResponse)
async def invite_user(
    tenant_id: str,
    body: InviteRequest,
    request: Request,
    current_user: CurrentUser = Depends(require_role(UserRole.owner, UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """Invite a user to the tenant (owner/admin only)."""
    if str(current_user.tenant.id) != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")

    # Validate role
    try:
        role = UserRole(body.role)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid role: {body.role}")

    # Cannot invite as owner
    if role == UserRole.owner:
        raise HTTPException(status_code=400, detail="Cannot assign owner role via invite")

    # Check if already exists
    existing = await db.execute(
        select(User).where(User.supabase_uid == body.supabase_uid)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="User already exists")

    user = User(
        supabase_uid=body.supabase_uid,
        email=body.email,
        name=body.name,
        tenant_id=current_user.tenant.id,
        role=role,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    await log_audit(
        db, current_user.tenant.id, current_user.user.id, "invite_user", request,
        resource_type="user", resource_id=str(user.id),
        details={"email": body.email, "role": body.role},
    )

    return user_to_response(user)


@tenant_router.patch("/{tenant_id}/users/{user_id}/role", response_model=UserResponse)
async def update_role(
    tenant_id: str,
    user_id: str,
    body: RoleUpdateRequest,
    request: Request,
    current_user: CurrentUser = Depends(require_role(UserRole.owner)),
    db: AsyncSession = Depends(get_db),
):
    """Change a user's role (owner only)."""
    if str(current_user.tenant.id) != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")

    try:
        new_role = UserRole(body.role)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid role: {body.role}")

    result = await db.execute(
        select(User).where(User.id == uuid.UUID(user_id), User.tenant_id == current_user.tenant.id)
    )
    target_user = result.scalar_one_or_none()
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")

    if target_user.id == current_user.user.id:
        raise HTTPException(status_code=400, detail="Cannot change your own role")

    old_role = target_user.role.value
    target_user.role = new_role
    db.add(target_user)
    await db.commit()
    await db.refresh(target_user)

    await log_audit(
        db, current_user.tenant.id, current_user.user.id, "update_role", request,
        resource_type="user", resource_id=str(target_user.id),
        details={"old_role": old_role, "new_role": body.role},
    )

    return user_to_response(target_user)


# ── Audit Log ────────────────────────────────────────────

@tenant_router.get("/{tenant_id}/audit-log", response_model=List[AuditLogResponse])
async def get_audit_log(
    tenant_id: str,
    limit: int = 50,
    offset: int = 0,
    current_user: CurrentUser = Depends(require_role(UserRole.owner, UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """Get audit log for the tenant (owner/admin only)."""
    if str(current_user.tenant.id) != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")

    result = await db.execute(
        select(AuditLog)
        .where(AuditLog.tenant_id == current_user.tenant.id)
        .order_by(AuditLog.created_at.desc())
        .limit(min(limit, 200))
        .offset(offset)
    )
    logs = result.scalars().all()
    return [
        AuditLogResponse(
            id=log.id,
            user_id=str(log.user_id),
            action=log.action,
            resource_type=log.resource_type,
            resource_id=log.resource_id,
            ip=log.ip,
            details=log.details,
            created_at=log.created_at.isoformat(),
        )
        for log in logs
    ]
