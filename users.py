import os
import re
import uuid
from calendar import monthrange
from datetime import datetime, timezone
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from db_models import User, Tenant, AuditLog, UserRole, TenantPlan, PendingInvite, Banner, ScoringSession
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from auth import get_current_user, require_role, require_feature, get_user_features, CurrentUser, VALID_FEATURES, verify_supabase_token

_security = HTTPBearer(auto_error=False)

limiter = Limiter(key_func=get_remote_address)

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
    is_superadmin: bool = False

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


class ProfileUpdateRequest(BaseModel):
    name: Optional[str] = None
    company_name: Optional[str] = None


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


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
        is_superadmin=getattr(user, 'is_superadmin', False),
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
@limiter.limit("5/minute")
async def register(
    body: RegisterRequest,
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(_security),
    db: AsyncSession = Depends(get_db),
):
    """Register a new user + tenant after Supabase signup."""
    # Verify the caller owns the supabase_uid they are registering
    if not credentials:
        raise HTTPException(status_code=401, detail="Authorization header required")
    verified_uid = await verify_supabase_token(credentials.credentials)
    if not verified_uid or verified_uid != body.supabase_uid:
        raise HTTPException(status_code=401, detail="Token does not match the provided supabase_uid")

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


class BrandingUpdate(BaseModel):
    logo_url: Optional[str] = None
    brand_color: Optional[str] = None


@tenant_router.get("/{tenant_id}/branding")
async def get_branding(
    tenant_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get tenant branding (logo, color)."""
    if str(current_user.tenant.id) != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied")

    tenant = await db.get(Tenant, current_user.tenant.id)
    try:
        logo = tenant.logo_url
        color = tenant.brand_color
    except Exception:
        logo = None
        color = None
    return {
        "tenant_name": tenant.name,
        "logo_url": logo,
        "brand_color": color,
    }


@tenant_router.patch("/{tenant_id}/branding")
async def update_branding(
    tenant_id: str,
    body: BrandingUpdate,
    current_user: CurrentUser = Depends(require_role(UserRole.owner, UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """Update tenant branding for white-label exports."""
    if str(current_user.tenant.id) != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied")

    tenant = await db.get(Tenant, current_user.tenant.id)
    try:
        if body.logo_url is not None:
            tenant.logo_url = body.logo_url
        if body.brand_color is not None:
            if body.brand_color and not body.brand_color.startswith("#"):
                raise HTTPException(status_code=400, detail="brand_color must be hex like #3B82F6")
            tenant.brand_color = body.brand_color
        await db.commit()
        return {
            "tenant_name": tenant.name,
            "logo_url": tenant.logo_url,
            "brand_color": tenant.brand_color,
        }
    except Exception:
        return {
            "tenant_name": tenant.name,
            "logo_url": None,
            "brand_color": None,
            "error": "Branding columns not yet migrated. Run alembic upgrade head.",
        }


# ── Profile Management ──────────────────────────────────

@router.patch("/profile", response_model=UserResponse)
async def update_profile(
    body: ProfileUpdateRequest,
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update current user's name and/or company name."""
    user = current_user.user
    changed = {}

    if body.name is not None:
        old_name = user.name
        user.name = body.name.strip()
        changed["name"] = {"old": old_name, "new": user.name}

    if body.company_name is not None:
        # Only owner can rename the company
        if user.role != UserRole.owner:
            raise HTTPException(status_code=403, detail="Only the owner can rename the company")
        tenant = await db.get(Tenant, current_user.tenant.id)
        old_company = tenant.name
        tenant.name = body.company_name.strip()
        changed["company_name"] = {"old": old_company, "new": tenant.name}

    if not changed:
        raise HTTPException(status_code=400, detail="Nothing to update")

    await db.commit()
    await db.refresh(user)

    await log_audit(
        db, current_user.tenant.id, user.id, "update_profile", request,
        details=changed,
    )
    await db.commit()

    return user_to_response(user)


@router.post("/change-password")
async def change_password(
    body: ChangePasswordRequest,
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Change password with current password verification via Supabase."""
    import httpx

    if len(body.new_password) < 6:
        raise HTTPException(status_code=400, detail="Новый пароль должен быть не менее 6 символов")

    # Step 1: Verify current password by attempting to sign in with it
    sb_url = os.getenv("SUPABASE_URL", "")
    sb_anon_key = os.getenv("SUPABASE_ANON_KEY", "")
    sb_service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

    async with httpx.AsyncClient() as client:
        verify_resp = await client.post(
            f"{sb_url}/auth/v1/token?grant_type=password",
            json={"email": current_user.user.email, "password": body.current_password},
            headers={
                "apikey": sb_anon_key,
                "Content-Type": "application/json",
            },
        )
        if verify_resp.status_code != 200:
            raise HTTPException(status_code=400, detail="Неверный текущий пароль")

    # Step 2: Update password via Supabase Admin API
    async with httpx.AsyncClient() as client:
        update_resp = await client.put(
            f"{sb_url}/auth/v1/admin/users/{current_user.user.supabase_uid}",
            json={"password": body.new_password},
            headers={
                "apikey": sb_anon_key,
                "Authorization": f"Bearer {sb_service_key}",
                "Content-Type": "application/json",
            },
        )
        if update_resp.status_code not in (200, 201):
            raise HTTPException(status_code=500, detail="Ошибка при обновлении пароля")

    await log_audit(
        db, current_user.tenant.id, current_user.user.id, "change_password", request,
    )
    await db.commit()

    return {"ok": True}


@router.delete("/account")
async def delete_own_account(
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete (deactivate) own account. Owners cannot delete themselves if they have other users."""
    user = current_user.user

    if user.role == UserRole.owner:
        # Check if there are other active users in the tenant
        result = await db.execute(
            select(func.count(User.id)).where(
                User.tenant_id == current_user.tenant.id,
                User.is_active == True,
                User.id != user.id,
            )
        )
        other_count = result.scalar()
        if other_count > 0:
            raise HTTPException(
                status_code=400,
                detail="Cannot delete owner account while other users exist. Transfer ownership or remove them first.",
            )

    user.is_active = False
    db.add(user)

    await log_audit(
        db, current_user.tenant.id, user.id, "delete_account", request,
        resource_type="user", resource_id=str(user.id),
    )
    await db.commit()

    return {"ok": True}


# ── Tenant Usage ──────────────────────────────────────────

PLAN_BANNER_LIMITS = {
    "free": 10,
    "starter": 50,
    "pro": 500,
    "enterprise": 10000,
}

PLAN_SESSION_LIMITS = {
    "free": 5,
    "starter": -1,  # unlimited
    "pro": -1,
    "enterprise": -1,
}


@tenant_router.get("/{tenant_id}/usage")
async def get_usage(
    tenant_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get tenant usage stats (banners used this month, limits, reset date)."""
    if str(current_user.tenant.id) != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied to this tenant")

    now = datetime.now(timezone.utc)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    _, last_day = monthrange(now.year, now.month)
    month_end = now.replace(day=last_day, hour=23, minute=59, second=59)
    days_until_reset = (month_end - now).days + 1

    # Count banners uploaded this month
    result = await db.execute(
        select(func.count(Banner.id)).where(
            Banner.tenant_id == current_user.tenant.id,
            Banner.created_at >= month_start,
        )
    )
    banners_used = result.scalar() or 0

    # Count scoring sessions created this month
    sessions_result = await db.execute(
        select(func.count(ScoringSession.id)).where(
            ScoringSession.tenant_id == current_user.tenant.id,
            ScoringSession.created_at >= month_start,
        )
    )
    sessions_used = sessions_result.scalar() or 0

    plan = current_user.tenant.plan.value
    is_sa = getattr(current_user.user, 'is_superadmin', False)
    banners_limit = 999999 if is_sa else PLAN_BANNER_LIMITS.get(plan, 10)

    return {
        "banners_used": banners_used,
        "banners_limit": banners_limit,
        "sessions_used": sessions_used,
        "sessions_limit": -1 if is_sa else PLAN_SESSION_LIMITS.get(plan, 5),
        "days_until_reset": days_until_reset,
        "period": f"{now.strftime('%B %Y')}",
        "plan": "unlimited" if is_sa else plan,
    }


# ── Superadmin ──────────────────────────────────────────

admin_router = APIRouter(prefix="/admin", tags=["admin"])


def _require_superadmin():
    """Dependency: require user to be a superadmin."""
    async def check(current_user: CurrentUser = Depends(get_current_user)):
        if not getattr(current_user.user, 'is_superadmin', False):
            raise HTTPException(status_code=403, detail="Superadmin access required")
        return current_user
    return check


class TenantAdminResponse(BaseModel):
    id: str
    name: str
    slug: str
    plan: str
    is_active: bool
    created_at: str
    user_count: int
    banner_count: int


class TenantPlanUpdate(BaseModel):
    plan: Optional[str] = None
    is_active: Optional[bool] = None


@admin_router.get("/tenants")
async def admin_list_tenants(
    current_user: CurrentUser = Depends(_require_superadmin()),
    db: AsyncSession = Depends(get_db),
):
    """List all tenants with user/banner counts (superadmin only)."""
    result = await db.execute(select(Tenant).order_by(Tenant.created_at.desc()))
    tenants = result.scalars().all()

    response = []
    for t in tenants:
        user_count_r = await db.execute(
            select(func.count(User.id)).where(User.tenant_id == t.id, User.is_active == True)
        )
        banner_count_r = await db.execute(
            select(func.count(Banner.id)).where(Banner.tenant_id == t.id)
        )
        response.append(TenantAdminResponse(
            id=str(t.id),
            name=t.name,
            slug=t.slug,
            plan=t.plan.value,
            is_active=t.is_active,
            created_at=t.created_at.isoformat(),
            user_count=user_count_r.scalar() or 0,
            banner_count=banner_count_r.scalar() or 0,
        ))

    return response


@admin_router.get("/users")
async def admin_list_all_users(
    current_user: CurrentUser = Depends(_require_superadmin()),
    db: AsyncSession = Depends(get_db),
):
    """List all users across all tenants (superadmin only)."""
    result = await db.execute(
        select(User).order_by(User.created_at.desc())
    )
    users = result.scalars().all()
    return [
        {
            "id": str(u.id),
            "email": u.email,
            "name": u.name,
            "role": u.role.value,
            "is_active": u.is_active,
            "is_superadmin": getattr(u, 'is_superadmin', False),
            "has_demo": "demo_data" in (u.features or []),
            "tenant_name": u.tenant.name if u.tenant else None,
            "tenant_id": str(u.tenant_id),
            "created_at": u.created_at.isoformat(),
            "last_login": u.last_login.isoformat() if u.last_login else None,
        }
        for u in users
    ]


@admin_router.get("/stats")
async def admin_platform_stats(
    current_user: CurrentUser = Depends(_require_superadmin()),
    db: AsyncSession = Depends(get_db),
):
    """Platform-level stats (superadmin only)."""
    tenants_total = (await db.execute(select(func.count(Tenant.id)))).scalar() or 0
    try:
        tenants_active = (await db.execute(
            select(func.count(Tenant.id)).where(Tenant.is_active == True)
        )).scalar() or 0
    except Exception:
        tenants_active = tenants_total
    users_total = (await db.execute(select(func.count(User.id)))).scalar() or 0
    try:
        users_active = (await db.execute(
            select(func.count(User.id)).where(User.is_active == True)
        )).scalar() or 0
    except Exception:
        users_active = users_total
    try:
        banners_total = (await db.execute(select(func.count(Banner.id)))).scalar() or 0
    except Exception:
        banners_total = 0

    # Per-plan breakdown
    plan_counts = {}
    for plan in TenantPlan:
        try:
            cnt = (await db.execute(
                select(func.count(Tenant.id)).where(Tenant.plan == plan, Tenant.is_active == True)
            )).scalar() or 0
        except Exception:
            cnt = 0
        plan_counts[plan.value] = cnt

    return {
        "tenants_total": tenants_total,
        "tenants_active": tenants_active,
        "users_total": users_total,
        "users_active": users_active,
        "banners_total": banners_total,
        "plan_breakdown": plan_counts,
    }


@admin_router.patch("/tenants/{target_tenant_id}")
async def admin_update_tenant(
    target_tenant_id: str,
    body: TenantPlanUpdate,
    request: Request,
    current_user: CurrentUser = Depends(_require_superadmin()),
    db: AsyncSession = Depends(get_db),
):
    """Update tenant plan or status (superadmin only)."""
    tenant = await db.get(Tenant, uuid.UUID(target_tenant_id))
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    changed = {}
    if body.plan is not None:
        try:
            new_plan = TenantPlan(body.plan)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid plan: {body.plan}")
        changed["plan"] = {"old": tenant.plan.value, "new": new_plan.value}
        tenant.plan = new_plan

    if body.is_active is not None:
        changed["is_active"] = {"old": tenant.is_active, "new": body.is_active}
        tenant.is_active = body.is_active

    if not changed:
        raise HTTPException(status_code=400, detail="Nothing to update")

    await db.commit()

    await log_audit(
        db, current_user.tenant.id, current_user.user.id, "admin_update_tenant", request,
        resource_type="tenant", resource_id=target_tenant_id,
        details=changed,
    )
    await db.commit()

    return {"ok": True, "changes": changed}


@admin_router.patch("/users/{target_user_id}")
async def admin_toggle_user_active(
    target_user_id: str,
    request: Request,
    current_user: CurrentUser = Depends(_require_superadmin()),
    db: AsyncSession = Depends(get_db),
):
    """Activate or deactivate a user (superadmin only). Toggles is_active."""
    target = await db.get(User, uuid.UUID(target_user_id))
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    # Prevent deactivating yourself
    if target.id == current_user.user.id:
        raise HTTPException(status_code=400, detail="Нельзя деактивировать самого себя")

    old_active = target.is_active
    target.is_active = not target.is_active
    await db.commit()

    await log_audit(
        db, current_user.tenant.id, current_user.user.id, "admin_toggle_user", request,
        resource_type="user", resource_id=target_user_id,
        details={"email": target.email, "is_active": {"old": old_active, "new": target.is_active}},
    )
    await db.commit()

    return {"ok": True, "is_active": target.is_active}


@admin_router.patch("/users/{target_user_id}/demo")
async def admin_toggle_user_demo(
    target_user_id: str,
    request: Request,
    current_user: CurrentUser = Depends(_require_superadmin()),
    db: AsyncSession = Depends(get_db),
):
    """Toggle demo_data feature for a user (superadmin only).

    By default users don't have the demo_data feature — superadmin can grant
    or revoke it with this endpoint. Owner/admin roles bypass feature checks
    on the backend, so toggling has effect only for analyst/manager users.
    """
    target = await db.get(User, uuid.UUID(target_user_id))
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    current_features = list(target.features or ["calculators", "research"])
    had_demo = "demo_data" in current_features
    if had_demo:
        new_features = [f for f in current_features if f != "demo_data"]
    else:
        new_features = current_features + ["demo_data"]

    target.features = new_features
    # SQLAlchemy needs explicit flag to pick up JSONB list mutation
    from sqlalchemy.orm.attributes import flag_modified
    flag_modified(target, "features")
    await db.commit()

    await log_audit(
        db, current_user.tenant.id, current_user.user.id, "admin_toggle_demo", request,
        resource_type="user", resource_id=target_user_id,
        details={"email": target.email, "demo_data": {"old": had_demo, "new": not had_demo}},
    )
    await db.commit()

    return {"ok": True, "has_demo": not had_demo, "features": new_features}
