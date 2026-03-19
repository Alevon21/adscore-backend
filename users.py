import re
import uuid
from datetime import datetime, timezone
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from db_models import User, Tenant, AuditLog, UserRole, PendingInvite
from auth import get_current_user, require_role, require_feature, get_user_features, CurrentUser, VALID_FEATURES

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
    features: List[str] = []

    class Config:
        from_attributes = True


class InviteRequest(BaseModel):
    supabase_uid: str
    email: str
    name: Optional[str] = None
    role: str = "analyst"


class RoleUpdateRequest(BaseModel):
    role: str


class FeaturesUpdateRequest(BaseModel):
    features: List[str]


class InviteByEmailRequest(BaseModel):
    email: str
    role: str = "analyst"


class PendingInviteResponse(BaseModel):
    id: str
    email: str
    role: str
    invited_by: str
    created_at: str

    class Config:
        from_attributes = True


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
        features=get_user_features(user),
    )


async def log_audit(db: AsyncSession, tenant_id, user_id, action, request: Request = None, **kwargs):
    """Add an audit log entry to the session. Caller is responsible for commit."""
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

    # Check for pending invite — join existing tenant instead of creating new one
    invite_result = await db.execute(
        select(PendingInvite).where(
            func.lower(PendingInvite.email) == body.email.lower()
        ).order_by(PendingInvite.created_at.desc()).limit(1)
    )
    pending = invite_result.scalar_one_or_none()

    if pending:
        # Join existing tenant via invite
        user = User(
            supabase_uid=body.supabase_uid,
            email=body.email,
            name=body.name,
            tenant_id=pending.tenant_id,
            role=pending.role,
        )
        db.add(user)
        # Delete all invites for this email
        from sqlalchemy import delete as sa_delete
        await db.execute(
            sa_delete(PendingInvite).where(
                func.lower(PendingInvite.email) == body.email.lower()
            )
        )
        await db.commit()
        await db.refresh(user)

        await log_audit(
            db, pending.tenant_id, user.id, "register_via_invite", request,
            details={"email": body.email, "role": pending.role.value},
        )
        await db.commit()
        return user_to_response(user)

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
    await db.commit()

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
    await db.commit()

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
    await db.commit()

    return user_to_response(target_user)


# ── Feature Management ───────────────────────────────────

@tenant_router.patch("/{tenant_id}/users/{user_id}/features", response_model=UserResponse)
async def update_features(
    tenant_id: str,
    user_id: str,
    body: FeaturesUpdateRequest,
    request: Request,
    current_user: CurrentUser = Depends(require_role(UserRole.owner, UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """Update a user's feature access (owner/admin only)."""
    if str(current_user.tenant.id) != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")

    # Validate all features
    invalid = set(body.features) - VALID_FEATURES
    if invalid:
        raise HTTPException(status_code=400, detail=f"Invalid features: {invalid}")

    # Ensure calculators is always included (default feature)
    features = list(set(body.features) | {"calculators"})

    result = await db.execute(
        select(User).where(User.id == uuid.UUID(user_id), User.tenant_id == current_user.tenant.id)
    )
    target_user = result.scalar_one_or_none()
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")

    # Cannot modify owner/admin features (they always have all)
    if target_user.role in (UserRole.owner, UserRole.admin):
        raise HTTPException(status_code=400, detail="Owner/admin always have all features")

    old_features = target_user.features or ["calculators", "research"]
    target_user.features = features
    db.add(target_user)
    await db.commit()
    await db.refresh(target_user)

    await log_audit(
        db, current_user.tenant.id, current_user.user.id, "update_features", request,
        resource_type="user", resource_id=str(target_user.id),
        details={"old_features": old_features, "new_features": features},
    )
    await db.commit()

    return user_to_response(target_user)


# ── Pending Invites ───────────────────────────────────────

@tenant_router.post("/{tenant_id}/invites", response_model=PendingInviteResponse)
async def create_invite(
    tenant_id: str,
    body: InviteByEmailRequest,
    request: Request,
    current_user: CurrentUser = Depends(require_role(UserRole.owner, UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """Invite a user by email (owner/admin only)."""
    if str(current_user.tenant.id) != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")

    # Validate role
    try:
        role = UserRole(body.role)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid role: {body.role}")

    if role == UserRole.owner:
        raise HTTPException(status_code=400, detail="Cannot invite as owner")

    # Check if user already exists in this tenant
    existing = await db.execute(
        select(User).where(
            func.lower(User.email) == body.email.lower(),
            User.tenant_id == current_user.tenant.id,
            User.is_active == True,
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="User already in your team")

    # Check if invite already pending
    dup = await db.execute(
        select(PendingInvite).where(
            func.lower(PendingInvite.email) == body.email.lower(),
            PendingInvite.tenant_id == current_user.tenant.id,
        )
    )
    if dup.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Invite already sent to this email")

    invite = PendingInvite(
        tenant_id=current_user.tenant.id,
        email=body.email.lower().strip(),
        role=role,
        invited_by=current_user.user.id,
    )
    db.add(invite)
    await db.commit()
    await db.refresh(invite)

    await log_audit(
        db, current_user.tenant.id, current_user.user.id, "invite_user", request,
        resource_type="invite", resource_id=str(invite.id),
        details={"email": body.email, "role": body.role},
    )
    await db.commit()

    return PendingInviteResponse(
        id=str(invite.id),
        email=invite.email,
        role=invite.role.value,
        invited_by=str(invite.invited_by),
        created_at=invite.created_at.isoformat(),
    )


@tenant_router.get("/{tenant_id}/invites", response_model=List[PendingInviteResponse])
async def list_invites(
    tenant_id: str,
    current_user: CurrentUser = Depends(require_role(UserRole.owner, UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """List pending invites (owner/admin only)."""
    if str(current_user.tenant.id) != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")

    result = await db.execute(
        select(PendingInvite)
        .where(PendingInvite.tenant_id == current_user.tenant.id)
        .order_by(PendingInvite.created_at.desc())
    )
    invites = result.scalars().all()
    return [
        PendingInviteResponse(
            id=str(inv.id),
            email=inv.email,
            role=inv.role.value,
            invited_by=str(inv.invited_by),
            created_at=inv.created_at.isoformat(),
        )
        for inv in invites
    ]


@tenant_router.delete("/{tenant_id}/invites/{invite_id}")
async def cancel_invite(
    tenant_id: str,
    invite_id: str,
    request: Request,
    current_user: CurrentUser = Depends(require_role(UserRole.owner, UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """Cancel a pending invite (owner/admin only)."""
    if str(current_user.tenant.id) != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")

    result = await db.execute(
        select(PendingInvite).where(
            PendingInvite.id == uuid.UUID(invite_id),
            PendingInvite.tenant_id == current_user.tenant.id,
        )
    )
    invite = result.scalar_one_or_none()
    if not invite:
        raise HTTPException(status_code=404, detail="Invite not found")

    await db.delete(invite)
    await db.commit()

    await log_audit(
        db, current_user.tenant.id, current_user.user.id, "cancel_invite", request,
        resource_type="invite", resource_id=invite_id,
        details={"email": invite.email},
    )
    await db.commit()

    return {"ok": True}


# ── Deactivate User ──────────────────────────────────────

@tenant_router.delete("/{tenant_id}/users/{user_id}")
async def deactivate_user(
    tenant_id: str,
    user_id: str,
    request: Request,
    current_user: CurrentUser = Depends(require_role(UserRole.owner)),
    db: AsyncSession = Depends(get_db),
):
    """Deactivate a user (owner only). Cannot deactivate yourself."""
    if str(current_user.tenant.id) != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")

    result = await db.execute(
        select(User).where(User.id == uuid.UUID(user_id), User.tenant_id == current_user.tenant.id)
    )
    target_user = result.scalar_one_or_none()
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")

    if target_user.id == current_user.user.id:
        raise HTTPException(status_code=400, detail="Cannot deactivate yourself")

    target_user.is_active = False
    db.add(target_user)
    await db.commit()

    await log_audit(
        db, current_user.tenant.id, current_user.user.id, "deactivate_user", request,
        resource_type="user", resource_id=user_id,
        details={"email": target_user.email},
    )
    await db.commit()

    return {"ok": True}


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
