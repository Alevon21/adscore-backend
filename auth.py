import os
import time
import httpx
import logging
from dataclasses import dataclass
from typing import Optional, Dict

import jwt as pyjwt
from jwt import PyJWKClient, PyJWK
from jwt.exceptions import InvalidTokenError

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from db_models import User, Tenant, UserRole

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
JWKS_URL = f"{SUPABASE_URL}/auth/v1/.well-known/jwks.json"

# Hardcoded allowed algorithms to prevent algorithm confusion attacks.
_ALLOWED_ALGORITHMS = ["RS256", "ES256"]

# PyJWT's built-in JWKS client with caching
_jwk_client: Optional[PyJWKClient] = None


def _get_jwk_client() -> PyJWKClient:
    """Lazily create the JWKS client (caches keys internally)."""
    global _jwk_client
    if _jwk_client is None:
        _jwk_client = PyJWKClient(
            JWKS_URL,
            cache_keys=True,
            lifespan=3600,  # cache for 1 hour
        )
    return _jwk_client


security = HTTPBearer(auto_error=False)


async def _get_signing_key(token: str) -> PyJWK:
    """Get the signing key for the given token using JWKS."""
    import asyncio
    client = _get_jwk_client()
    try:
        return await asyncio.to_thread(client.get_signing_key_from_jwt, token)
    except Exception:
        # Force cache refresh and retry
        client = _get_jwk_client()
        return await asyncio.to_thread(client.get_signing_key_from_jwt, token)


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
        signing_key = await _get_signing_key(token)
        payload = pyjwt.decode(
            token,
            signing_key.key,
            algorithms=_ALLOWED_ALGORITHMS,
            options={
                "verify_aud": False,
                "verify_exp": True,
                "require": ["exp", "sub"],
            },
        )
    except InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    supabase_uid = payload.get("sub")
    if not supabase_uid:
        raise HTTPException(status_code=401, detail="Token missing sub claim")

    # Try with is_active filter first
    result = await db.execute(
        select(User).where(User.supabase_uid == supabase_uid, User.is_active == True)
    )
    user = result.scalar_one_or_none()

    if not user:
        # Check if user exists but is deactivated
        result2 = await db.execute(
            select(User).where(User.supabase_uid == supabase_uid)
        )
        inactive_user = result2.scalar_one_or_none()
        if inactive_user:
            raise HTTPException(status_code=403, detail="Account deactivated. Contact your admin.")
        raise HTTPException(status_code=404, detail="User not found. Please register first.")

    tenant = user.tenant
    if not tenant or not tenant.is_active:
        raise HTTPException(status_code=403, detail="Tenant is inactive")

    # Set tenant context for isolation helpers
    from database import tenant_context
    tenant_context.set(tenant.id)

    return CurrentUser(user=user, tenant=tenant)


async def verify_supabase_token(token: str) -> Optional[str]:
    """Verify a Supabase JWT and return the sub (user ID) claim.

    Returns None if verification fails.
    """
    try:
        signing_key = await _get_signing_key(token)
        payload = pyjwt.decode(
            token,
            signing_key.key,
            algorithms=_ALLOWED_ALGORITHMS,
            options={
                "verify_aud": False,
                "verify_exp": True,
                "require": ["exp", "sub"],
            },
        )
        return payload.get("sub")
    except Exception:
        return None


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
ALL_FEATURES = sorted(VALID_FEATURES)
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
