import os
import time
import httpx
from dataclasses import dataclass
from typing import Optional, Dict

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt, jwk
from jose.utils import base64url_decode
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from db_models import User, Tenant, UserRole

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
JWKS_URL = f"{SUPABASE_URL}/auth/v1/.well-known/jwks.json"

# Cache JWKS keys for 1 hour
_jwks_cache: Dict = {"keys": {}, "fetched_at": 0}
JWKS_CACHE_TTL = 3600

security = HTTPBearer(auto_error=False)


def _fetch_jwks() -> Dict:
    """Fetch JWKS from Supabase and cache them."""
    now = time.time()
    if _jwks_cache["keys"] and (now - _jwks_cache["fetched_at"]) < JWKS_CACHE_TTL:
        return _jwks_cache["keys"]

    resp = httpx.get(JWKS_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    keys = {}
    for key_data in data.get("keys", []):
        kid = key_data.get("kid")
        if kid:
            keys[kid] = key_data

    _jwks_cache["keys"] = keys
    _jwks_cache["fetched_at"] = now
    return keys


def _get_signing_key(token: str):
    """Extract kid from token header and find the matching JWKS key."""
    headers = jwt.get_unverified_header(token)
    kid = headers.get("kid")
    alg = headers.get("alg", "ES256")

    keys = _fetch_jwks()

    if kid and kid in keys:
        key_data = keys[kid]
        return jwk.construct(key_data, alg), alg

    # If kid not found, refresh cache and retry
    _jwks_cache["fetched_at"] = 0
    keys = _fetch_jwks()

    if kid and kid in keys:
        key_data = keys[kid]
        return jwk.construct(key_data, alg), alg

    raise JWTError(f"Unable to find signing key for kid={kid}")


@dataclass
class CurrentUser:
    user: User
    tenant: Tenant


async def get_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: AsyncSession = Depends(get_db),
) -> CurrentUser:
    if not credentials:
        raise HTTPException(status_code=401, detail="Authorization header missing")

    token = credentials.credentials
    try:
        signing_key, alg = _get_signing_key(token)
        payload = jwt.decode(
            token,
            signing_key,
            algorithms=[alg],
            options={"verify_aud": False},
        )
    except JWTError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")

    supabase_uid = payload.get("sub")
    if not supabase_uid:
        raise HTTPException(status_code=401, detail="Token missing sub claim")

    # Try with is_active filter first
    result = await db.execute(
        select(User).where(User.supabase_uid == supabase_uid, User.is_active == True)
    )
    user = result.scalar_one_or_none()

    # If not found, check without is_active filter (debug + auto-activate)
    if not user:
        result2 = await db.execute(
            select(User).where(User.supabase_uid == supabase_uid)
        )
        inactive_user = result2.scalar_one_or_none()
        if inactive_user:
            # Auto-activate the user
            inactive_user.is_active = True
            db.add(inactive_user)
            await db.commit()
            await db.refresh(inactive_user)
            user = inactive_user
        else:
            import logging
            logging.warning(f"User not found for supabase_uid={supabase_uid}")
            raise HTTPException(status_code=404, detail="User not found. Please register first.")

    tenant = user.tenant
    if not tenant or not tenant.is_active:
        raise HTTPException(status_code=403, detail="Tenant is inactive")

    # Set tenant context for isolation helpers
    from database import tenant_context
    tenant_context.set(tenant.id)

    return CurrentUser(user=user, tenant=tenant)


def require_role(*allowed_roles):
    """Dependency factory: require user to have one of the specified roles."""
    async def check_role(current_user: CurrentUser = Depends(get_current_user)):
        if current_user.user.role not in allowed_roles:
            raise HTTPException(
                status_code=403,
                detail=f"Role '{current_user.user.role.value}' not allowed. Required: {[r.value for r in allowed_roles]}"
            )
        return current_user
    return check_role


# ── Feature-based access control ─────────────────────────

VALID_FEATURES = {"calculators", "research", "analysis", "adscore"}
ALL_FEATURES = list(VALID_FEATURES)
DEFAULT_FEATURES = ["calculators", "research"]


def get_user_features(user: User) -> list:
    """Get effective features for a user. Owner/admin always get all features."""
    if user.role in (UserRole.owner, UserRole.admin):
        return ALL_FEATURES
    return user.features or DEFAULT_FEATURES


def require_feature(feature: str):
    """Dependency factory: require user to have access to a specific feature."""
    async def check_feature(current_user: CurrentUser = Depends(get_current_user)):
        # Owner/admin bypass — always have all features
        if current_user.user.role in (UserRole.owner, UserRole.admin):
            return current_user
        user_features = current_user.user.features or DEFAULT_FEATURES
        if feature not in user_features:
            raise HTTPException(
                status_code=403,
                detail=f"Feature '{feature}' is not available for your account"
            )
        return current_user
    return check_feature
