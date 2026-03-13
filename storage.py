"""Supabase Storage service — upload, download, delete files via REST API."""

import os
import uuid
import logging
from typing import Optional

import httpx
from fastapi import HTTPException
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from db_models import StoredFile, Tenant, FileStatus

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
BUCKET = "uploads"
STORAGE_BASE = f"{SUPABASE_URL}/storage/v1"


def _headers():
    return {
        "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
        "apikey": SERVICE_ROLE_KEY,
    }


async def upload_file(
    tenant_id: uuid.UUID,
    session_id: uuid.UUID,
    filename: str,
    content: bytes,
    mime_type: str,
) -> str:
    """Upload file to Supabase Storage. Returns the storage key (path)."""
    storage_key = f"{tenant_id}/originals/{session_id}/{filename}"
    url = f"{STORAGE_BASE}/object/{BUCKET}/{storage_key}"

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            url,
            content=content,
            headers={
                **_headers(),
                "Content-Type": mime_type,
                "x-upsert": "true",
            },
        )
        if resp.status_code not in (200, 201):
            logger.error("Storage upload failed: %s %s", resp.status_code, resp.text)
            raise HTTPException(status_code=502, detail=f"File storage error: {resp.text}")

    return storage_key


async def download_file(storage_key: str) -> bytes:
    """Download file from Supabase Storage."""
    url = f"{STORAGE_BASE}/object/{BUCKET}/{storage_key}"

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(url, headers=_headers())
        if resp.status_code != 200:
            raise HTTPException(status_code=404, detail="File not found in storage")
        return resp.content


async def delete_file(storage_key: str) -> None:
    """Delete file from Supabase Storage."""
    url = f"{STORAGE_BASE}/object/{BUCKET}"

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.delete(
            url,
            json={"prefixes": [storage_key]},
            headers=_headers(),
        )
        if resp.status_code not in (200, 204):
            logger.warning("Storage delete failed: %s %s", resp.status_code, resp.text)


async def get_signed_url(storage_key: str, expires_in: int = 3600) -> str:
    """Create a signed URL for client-side download."""
    url = f"{STORAGE_BASE}/object/sign/{BUCKET}/{storage_key}"

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            url,
            json={"expiresIn": expires_in},
            headers=_headers(),
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail="Could not create signed URL")
        data = resp.json()
        return f"{SUPABASE_URL}/storage/v1{data['signedURL']}"


async def get_signed_urls(
    storage_keys: list,
    expires_in: int = 3600,
) -> dict:
    """Create signed URLs for multiple files in one request. Returns {key: url} mapping."""
    if not storage_keys:
        return {}

    url = f"{STORAGE_BASE}/object/sign/{BUCKET}"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            url,
            json={"expiresIn": expires_in, "paths": storage_keys},
            headers=_headers(),
        )
        if resp.status_code != 200:
            logger.warning("Batch sign failed: %s %s", resp.status_code, resp.text)
            return {}

        result = {}
        for item in resp.json():
            path = item.get("path", "")
            signed = item.get("signedURL", "")
            if path and signed:
                result[path] = f"{SUPABASE_URL}/storage/v1{signed}"
        return result


async def check_storage_quota(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    additional_bytes: int,
) -> None:
    """Raise 413 if adding additional_bytes would exceed tenant storage quota."""
    # Get current usage
    result = await db.execute(
        select(func.coalesce(func.sum(StoredFile.size_bytes), 0))
        .where(StoredFile.tenant_id == tenant_id)
        .where(StoredFile.status != FileStatus.deleted)
    )
    current_bytes = result.scalar()

    # Get tenant quota
    result = await db.execute(
        select(Tenant.storage_quota_mb).where(Tenant.id == tenant_id)
    )
    quota_mb = result.scalar()
    if quota_mb is None:
        return  # no tenant found, skip check

    quota_bytes = quota_mb * 1024 * 1024
    if current_bytes + additional_bytes > quota_bytes:
        used_mb = round(current_bytes / (1024 * 1024), 1)
        raise HTTPException(
            status_code=413,
            detail=f"Storage quota exceeded. Used: {used_mb} MB / {quota_mb} MB",
        )
