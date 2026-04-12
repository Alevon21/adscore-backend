"""AdScore API router — creative banner analytics with AI tagging (tenant-isolated)."""

import ipaddress
import json
import logging
import os
import re
import uuid as uuid_mod
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import numpy as np
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import StreamingResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import select, update, delete, func, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from auth import get_current_user, CurrentUser
from database import get_db
from db_models import Banner
import storage as file_storage

from adscore_models import (
    BannerListResponse,
    BannerMetrics,
    BannerRecord,
    BannerTags,
    BannerUploadResponse,
    CSVUploadResponse,
    ElementMetricStats,
    ElementPerformance,
    ExplainResponse,
    InsightsResponse,
    TagResponse,
)

logger = logging.getLogger(__name__)

adscore_router = APIRouter(prefix="/adscore", tags=["adscore"])
limiter = Limiter(key_func=get_remote_address)

MAX_IMAGE_SIZE = 10 * 1024 * 1024   # 10 MB
MAX_VIDEO_SIZE = 100 * 1024 * 1024  # 100 MB

VALID_IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".webp")
VALID_VIDEO_EXTENSIONS = (".mp4", ".webm", ".mov")
VALID_EXTENSIONS = VALID_IMAGE_EXTENSIONS + VALID_VIDEO_EXTENSIONS

MIME_MAP = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".mp4": "video/mp4",
    ".webm": "video/webm",
    ".mov": "video/quicktime",
}

_SSRF_BLOCKED_HOSTS = {
    'localhost', '127.0.0.1', '0.0.0.0', '::1',
    '169.254.169.254', 'metadata.google.internal',
    'metadata.internal', '100.100.100.200',
}


def _is_safe_url(url: str) -> bool:
    """Validate URL is safe for server-side fetch (blocks SSRF)."""
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in ('http', 'https'):
        return False
    hostname = parsed.hostname
    if not hostname:
        return False
    if hostname in _SSRF_BLOCKED_HOSTS:
        return False
    try:
        ip = ipaddress.ip_address(hostname)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            return False
    except ValueError:
        pass
    return True


def _sanitize_filename(name: str) -> str:
    """Sanitize filename — strip path separators and special characters."""
    name = Path(name).name  # strip directory components
    name = re.sub(r'[^\w.\-]', '_', name)
    return name[:200] or "file"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_metrics(metrics: Optional[dict]) -> dict:
    """Return a copy of metrics with CTR/CR values normalised to fractions.

    Raw data may store CTR as a percentage (e.g. 6.0 meaning 6%).
    This helper ensures values > 1 are divided by 100, and prefers
    recomputing CTR from clicks/impressions when available.
    """
    if not metrics:
        return {}
    m = dict(metrics)
    # Recompute CTR from raw counts if possible
    try:
        impr = m.get("impressions")
        clicks = m.get("clicks")
        if impr and clicks is not None:
            impr_f = float(impr)
            if impr_f > 0:
                m["ctr"] = float(clicks) / impr_f
                return m
    except (ValueError, TypeError):
        pass
    # Normalise stored ctr
    for key in ("ctr", "cr_install", "cr_event"):
        v = m.get(key)
        if v is None:
            continue
        try:
            val = float(v)
            if val > 1:
                m[key] = val / 100.0
        except (ValueError, TypeError):
            pass
    return m


def _banner_to_record(b: Banner, image_url: Optional[str] = None) -> dict:
    """Convert a DB Banner row to a dict matching BannerRecord schema."""
    return {
        "id": str(b.id),
        "filename": b.original_filename or "",
        "original_filename": b.original_filename or "",
        "upload_date": b.created_at.isoformat() if b.created_at else "",
        "file_size_bytes": b.file_size_bytes or 0,
        "width": b.width or 0,
        "height": b.height or 0,
        "metrics": _normalize_metrics(b.metrics),
        "tags": b.tags,
        "tags_status": b.tags_status or "pending",
        "tags_error": b.tags_error,
        "tagged_at": b.tagged_at.isoformat() if b.tagged_at else None,
        "explanation": b.explanation,
        "explained_at": b.explained_at.isoformat() if b.explained_at else None,
        "image_url": image_url,
        "project": b.project,
        "concept_group": b.concept_group,
        "media_type": getattr(b, "media_type", None) or "image",
        "video_meta": getattr(b, "video_meta", None),
        "keyframes": getattr(b, "keyframes", None),
    }


async def _get_banner_or_404(
    db: AsyncSession, banner_id: str, tenant_id: uuid_mod.UUID
) -> Banner:
    """Fetch banner scoped to tenant, raise 404 if not found."""
    try:
        bid = uuid_mod.UUID(banner_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Banner {banner_id} not found")

    result = await db.execute(
        select(Banner).where(Banner.id == bid, Banner.tenant_id == tenant_id)
    )
    banner = result.scalar_one_or_none()
    if not banner:
        raise HTTPException(status_code=404, detail=f"Banner {banner_id} not found")
    return banner


async def _sign_banners(banners: list[Banner]) -> dict:
    """Get signed URLs for a list of banners. Returns {storage_key: signed_url}."""
    keys = [b.storage_key for b in banners if b.storage_key]
    if not keys:
        return {}
    return await file_storage.get_signed_urls(keys)


def _get_image_dimensions(content: bytes):
    """Extract image width/height from bytes."""
    try:
        from PIL import Image
        from io import BytesIO
        Image.MAX_IMAGE_PIXELS = 25_000_000  # ~5000x5000 max, prevents decompression bombs
        with Image.open(BytesIO(content)) as img:
            return img.size
    except Exception:
        return 0, 0


async def create_banner_from_url(
    db: AsyncSession,
    tenant_id: uuid_mod.UUID,
    user_id: uuid_mod.UUID,
    image_url: str,
    metrics_dict: dict,
) -> Optional[uuid_mod.UUID]:
    """Download image from URL, upload to Supabase Storage, create Banner row.

    Returns banner UUID on success, None on failure.
    """
    import httpx as httpx_lib
    from urllib.parse import unquote  # noqa: local import for clarity

    if not _is_safe_url(image_url):
        logger.warning("SSRF blocked: %s", image_url)
        return None

    try:
        async with httpx_lib.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            resp = await client.get(image_url)
            resp.raise_for_status()
    except Exception as e:
        logger.warning("Banner download failed for %s: %s", image_url, e)
        return None

    content = resp.content
    if len(content) > MAX_IMAGE_SIZE:
        logger.warning("Banner too large (%d bytes) from %s", len(content), image_url)
        return None

    content_type = resp.headers.get("content-type", "")
    ext_map = {"image/png": ".png", "image/jpeg": ".jpg", "image/gif": ".gif", "image/webp": ".webp"}
    ext = ext_map.get(content_type.split(";")[0].strip(), "")
    if not ext:
        url_path = urlparse(image_url).path
        ext = Path(url_path).suffix.lower()
    if ext not in VALID_EXTENSIONS:
        ext = ".jpg"

    width, height = _get_image_dimensions(content)
    mime_type = MIME_MAP.get(ext, "image/jpeg")
    banner_id = uuid_mod.uuid4()

    url_path_str = urlparse(image_url).path
    original_name = _sanitize_filename(unquote(Path(url_path_str).name) or "image_from_url")

    storage_key = f"{tenant_id}/banners/{banner_id}/{original_name}"
    try:
        import httpx
        sup_url = f"{file_storage.STORAGE_BASE}/object/{file_storage.BUCKET}/{storage_key}"
        async with httpx.AsyncClient(timeout=60) as client:
            resp2 = await client.post(
                sup_url, content=content,
                headers={**file_storage._headers(), "Content-Type": mime_type, "x-upsert": "true"},
            )
            if resp2.status_code not in (200, 201):
                logger.warning("Banner storage upload failed for %s: %s", image_url, resp2.text)
                storage_key = None
    except Exception as e:
        logger.warning("Banner storage upload error for %s: %s", image_url, e)
        storage_key = None

    db_banner = Banner(
        id=banner_id,
        tenant_id=tenant_id,
        created_by=user_id,
        original_filename=original_name,
        storage_key=storage_key,
        file_size_bytes=len(content),
        width=width,
        height=height,
        mime_type=mime_type,
        metrics=metrics_dict,
        tags_status="pending",
    )
    db.add(db_banner)

    logger.info("Created banner from URL %s → %s", image_url, banner_id)
    return banner_id


# ---------------------------------------------------------------------------
# Upload endpoints
# ---------------------------------------------------------------------------


@adscore_router.post("/upload", response_model=BannerUploadResponse)
async def upload_banner(
    image: UploadFile = File(...),
    metrics: str = Form("{}"),
    project: str = Form(""),
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Upload a single banner image with optional metrics JSON."""
    tid = current_user.tenant.id
    uid = current_user.user.id

    ext = Path(image.filename or "image.png").suffix.lower()
    if ext not in VALID_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Supported formats: PNG, JPG, GIF, WebP, MP4, WebM, MOV")

    is_video = ext in VALID_VIDEO_EXTENSIONS
    max_size = MAX_VIDEO_SIZE if is_video else MAX_IMAGE_SIZE

    content = await image.read()
    if len(content) > max_size:
        raise HTTPException(status_code=400, detail=f"File too large (max {max_size // 1024 // 1024} MB)")

    try:
        metrics_dict = json.loads(metrics)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid metrics JSON")

    banner_metrics = BannerMetrics(**metrics_dict)
    # Use normalised values (rates auto-divided by 100 if > 1)
    metrics_dict = banner_metrics.model_dump(exclude_none=True)

    if is_video:
        width, height = 0, 0  # will be set after video processing
    else:
        width, height = _get_image_dimensions(content)
    mime_type = MIME_MAP.get(ext, "image/png")
    banner_id = uuid_mod.uuid4()
    original_name = _sanitize_filename(image.filename or "unknown")

    # Upload to Supabase Storage
    storage_key = f"{tid}/banners/{banner_id}/{original_name}"
    try:
        await file_storage.upload_file(tid, banner_id, f"banners/{banner_id}/{original_name}",
                                        content, mime_type)
        # upload_file builds its own path, so use a direct upload instead
    except Exception:
        pass

    # Direct upload to Supabase Storage
    storage_key = f"{tid}/banners/{banner_id}/{original_name}"
    try:
        import httpx
        url = f"{file_storage.STORAGE_BASE}/object/{file_storage.BUCKET}/{storage_key}"
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                url,
                content=content,
                headers={
                    **file_storage._headers(),
                    "Content-Type": mime_type,
                    "x-upsert": "true",
                },
            )
            if resp.status_code not in (200, 201):
                logger.warning("Banner storage upload failed: %s", resp.text)
                storage_key = None
    except Exception as e:
        logger.warning("Banner storage upload error: %s", e)
        storage_key = None

    # Create DB row
    project_name = project.strip()[:200] if project else None
    db_banner = Banner(
        id=banner_id,
        tenant_id=tid,
        created_by=uid,
        original_filename=original_name,
        storage_key=storage_key,
        file_size_bytes=len(content),
        width=width,
        height=height,
        mime_type=mime_type,
        metrics=metrics_dict,
        tags_status="pending",
        project=project_name,
        media_type="video" if is_video else "image",
    )
    db.add(db_banner)
    await db.commit()

    # Get signed URL
    image_url = None
    if storage_key:
        try:
            image_url = await file_storage.get_signed_url(storage_key)
        except Exception:
            pass

    logger.info("Uploaded banner %s (%s, %dx%d)", banner_id, original_name, width, height)

    return BannerUploadResponse(
        id=str(banner_id),
        filename=original_name,
        metrics=banner_metrics,
        tags_status="pending",
        image_url=image_url,
    )


@adscore_router.post("/upload-csv", response_model=CSVUploadResponse)
async def upload_csv(
    file: UploadFile = File(...),
    project: str = Form(""),
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Bulk upload banner metrics from CSV. Images should be uploaded separately."""
    import pandas as pd
    from io import BytesIO

    tid = current_user.tenant.id
    uid = current_user.user.id
    project_name = project.strip()[:200] if project else None

    content = await file.read()
    try:
        df = pd.read_csv(BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail="Failed to parse CSV. Please check the format.")

    imported = []
    errors = []

    for idx, row in df.iterrows():
        try:
            banner_id = uuid_mod.uuid4()
            metrics_dict = {}
            # Float rate fields
            for k in ("ctr", "cr_install", "cr_event", "spend", "revenue"):
                if k in row and pd.notna(row[k]):
                    metrics_dict[k] = float(row[k])
            # Integer count fields
            for k in ("impressions", "clicks", "installs",
                       "event_1", "event_2", "event_3", "event_4"):
                if k in row and pd.notna(row[k]):
                    metrics_dict[k] = int(row[k])
            # String metadata fields
            for k in ("platform", "campaign", "date_from", "date_to"):
                if k in row and pd.notna(row[k]) and str(row[k]).strip():
                    metrics_dict[k] = str(row[k]).strip()

            # Auto-compute rates if raw counts are present but rates are missing
            impr = metrics_dict.get("impressions")
            clks = metrics_dict.get("clicks")
            instl = metrics_dict.get("installs")
            if impr and impr > 0:
                if "ctr" not in metrics_dict and clks is not None:
                    metrics_dict["ctr"] = round(clks / impr, 6)
                if "cr_install" not in metrics_dict and instl is not None:
                    metrics_dict["cr_install"] = round(instl / impr, 6)

            banner_metrics = BannerMetrics(**metrics_dict)
            # Use normalised values (rates auto-divided by 100 if > 1)
            metrics_dict = banner_metrics.model_dump(exclude_none=True)
            fname = str(row.get("filename", "")) or ""

            # Use per-row project if present in CSV, else fallback to form-level project
            row_project = None
            if "project" in row and pd.notna(row["project"]) and str(row["project"]).strip():
                row_project = str(row["project"]).strip()[:200]
            else:
                row_project = project_name

            db_banner = Banner(
                id=banner_id,
                tenant_id=tid,
                created_by=uid,
                original_filename=fname,
                metrics=metrics_dict,
                tags_status="no_image",
                project=row_project,
            )
            db.add(db_banner)

            imported.append(BannerUploadResponse(
                id=str(banner_id),
                filename=fname,
                metrics=banner_metrics,
                tags_status="no_image",
            ))
        except Exception as e:
            errors.append(f"Row {idx + 1}: {e}")

    await db.commit()
    logger.info("CSV upload: %d imported, %d errors", len(imported), len(errors))

    return CSVUploadResponse(imported=len(imported), errors=errors, banners=imported)


@adscore_router.post("/upload-url", response_model=BannerUploadResponse)
@limiter.limit("10/minute")
async def upload_banner_url(
    request: Request,
    url: str = Form(...),
    metrics: str = Form("{}"),
    project: str = Form(""),
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Upload a banner image by URL — download and store it."""
    tid = current_user.tenant.id
    uid = current_user.user.id

    try:
        metrics_dict = json.loads(metrics)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid metrics JSON")

    banner_metrics = BannerMetrics(**metrics_dict)

    if not _is_safe_url(url):
        raise HTTPException(status_code=400, detail="URL is not allowed (private/internal addresses blocked)")

    banner_id = await create_banner_from_url(db, tid, uid, url, metrics_dict)
    if not banner_id:
        raise HTTPException(status_code=400, detail="Failed to download or store banner image")

    # Set project if provided
    project_name = project.strip()[:200] if project else None
    if project_name:
        banner_obj = await _get_banner_or_404(db, str(banner_id), tid)
        banner_obj.project = project_name

    await db.commit()

    # Fetch created banner for response
    banner = await _get_banner_or_404(db, str(banner_id), tid)
    image_url = None
    if banner.storage_key:
        try:
            image_url = await file_storage.get_signed_url(banner.storage_key)
        except Exception:
            pass

    return BannerUploadResponse(
        id=str(banner_id),
        filename=banner.original_filename,
        metrics=banner_metrics,
        tags_status="pending",
        image_url=image_url,
    )


@adscore_router.get("/csv-template")
async def get_csv_template():
    """Return a CSV template file for bulk banner metrics upload."""
    import io

    csv_content = (
        "filename,impressions,clicks,spend,installs,revenue,ctr,cr_install,cr_event,platform,campaign,date_from,date_to\n"
        "banner_001.png,125000,4000,5200.00,600,12000.00,0.032,0.015,0.008,google,Summer Sale,2026-01-01,2026-01-31\n"
        "banner_002.jpg,98000,2744,3800.00,329,8500.00,0.028,0.012,0.006,facebook,Black Friday,2026-02-01,2026-02-28\n"
    )
    return StreamingResponse(
        io.BytesIO(csv_content.encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=adscore_template.csv"},
    )


# ---------------------------------------------------------------------------
# Banner CRUD
# ---------------------------------------------------------------------------


@adscore_router.get("/banners", response_model=BannerListResponse)
async def list_banners(
    sort_by: str = "upload_date",
    sort_order: str = "desc",
    platform: Optional[str] = None,
    campaign: Optional[str] = None,
    tags_status: Optional[str] = None,
    project: Optional[str] = None,
    element: Optional[str] = None,
    element_value: Optional[str] = None,
    page: int = 1,
    per_page: int = 50,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List all banners for the current tenant with optional filters and sorting."""
    tid = current_user.tenant.id

    # Build query
    q = select(Banner).where(Banner.tenant_id == tid)

    # Apply DB-level filters where possible
    if tags_status:
        q = q.where(Banner.tags_status == tags_status)
    if project == "__none__":
        q = q.where(Banner.project.is_(None))
    elif project:
        q = q.where(Banner.project == project)

    # Sort
    if sort_by == "upload_date":
        order_col = Banner.created_at
    elif sort_by == "ctr":
        order_col = Banner.metrics["ctr"].as_float()
    elif sort_by == "cr_install":
        order_col = Banner.metrics["cr_install"].as_float()
    elif sort_by == "cr_event":
        order_col = Banner.metrics["cr_event"].as_float()
    elif sort_by == "impressions":
        order_col = Banner.metrics["impressions"].as_integer()
    elif sort_by == "clicks":
        order_col = Banner.metrics["clicks"].as_integer()
    elif sort_by == "spend":
        order_col = Banner.metrics["spend"].as_float()
    elif sort_by == "revenue":
        order_col = Banner.metrics["revenue"].as_float()
    else:
        order_col = Banner.created_at

    if sort_order == "desc":
        q = q.order_by(order_col.desc().nulls_last())
    else:
        q = q.order_by(order_col.asc().nulls_last())

    result = await db.execute(q)
    all_banners = list(result.scalars().all())

    # Apply in-memory filters for JSONB fields
    if platform:
        all_banners = [b for b in all_banners if (b.metrics or {}).get("platform") == platform]
    if campaign:
        all_banners = [b for b in all_banners if (b.metrics or {}).get("campaign") == campaign]
    if element and element_value is not None:
        def _has_element(b):
            tags = b.tags
            if not tags:
                return False
            for cat in tags.values():
                if isinstance(cat, dict) and element in cat:
                    val = cat[element]
                    if element_value.lower() in ("true", "false"):
                        return val == (element_value.lower() == "true")
                    return str(val) == element_value
            return False
        all_banners = [b for b in all_banners if _has_element(b)]

    total = len(all_banners)

    # Paginate
    start = (page - 1) * per_page
    page_banners = all_banners[start:start + per_page]

    # Get signed URLs in batch
    signed = await _sign_banners(page_banners)

    records = []
    for b in page_banners:
        image_url = signed.get(b.storage_key) if b.storage_key else None
        records.append(_banner_to_record(b, image_url))

    return BannerListResponse(banners=records, total=total)


@adscore_router.get("/banner/{banner_id}")
async def get_banner(
    banner_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get full details of a single banner."""
    tid = current_user.tenant.id
    banner = await _get_banner_or_404(db, banner_id, tid)

    image_url = None
    if banner.storage_key:
        try:
            image_url = await file_storage.get_signed_url(banner.storage_key)
        except Exception:
            pass

    return _banner_to_record(banner, image_url)


@adscore_router.patch("/banner/{banner_id}/metrics")
async def update_banner_metrics(
    banner_id: str,
    payload: BannerMetrics,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update metrics for a banner (merge with existing)."""
    tid = current_user.tenant.id
    banner = await _get_banner_or_404(db, banner_id, tid)

    existing = banner.metrics or {}
    updated = payload.model_dump(exclude_none=True)

    # Merge: only overwrite fields that were explicitly sent
    merged = {**existing, **updated}

    # Auto-compute rates from raw counts
    impr = merged.get("impressions")
    clks = merged.get("clicks")
    instl = merged.get("installs")
    if impr and impr > 0:
        if clks is not None:
            merged["ctr"] = round(clks / impr, 6)
        if instl is not None:
            merged["cr_install"] = round(instl / impr, 6)

    banner.metrics = merged
    flag_modified(banner, "metrics")
    await db.commit()
    await db.refresh(banner)

    image_url = None
    if banner.storage_key:
        try:
            image_url = await file_storage.get_signed_url(banner.storage_key)
        except Exception:
            pass

    logger.info("Updated metrics for banner %s", banner_id)
    return _banner_to_record(banner, image_url)


@adscore_router.delete("/banner/{banner_id}")
async def delete_banner(
    banner_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a banner and its image file."""
    tid = current_user.tenant.id
    banner = await _get_banner_or_404(db, banner_id, tid)

    # Delete from storage
    if banner.storage_key:
        try:
            await file_storage.delete_file(banner.storage_key)
        except Exception as e:
            logger.warning("Failed to delete banner file from storage: %s", e)

    await db.delete(banner)
    await db.commit()
    logger.info("Deleted banner %s", banner_id)

    return {"status": "deleted", "banner_id": banner_id}


# ---------------------------------------------------------------------------
# Projects & Bulk operations
# ---------------------------------------------------------------------------


@adscore_router.get("/projects")
async def list_projects(
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List unique project names for the current tenant."""
    tid = current_user.tenant.id
    result = await db.execute(
        select(Banner.project, func.count(Banner.id).label("cnt"))
        .where(Banner.tenant_id == tid)
        .group_by(Banner.project)
        .order_by(func.count(Banner.id).desc())
    )
    projects = []
    no_project_count = 0
    for row in result:
        if row.project is None:
            no_project_count = row.cnt
        else:
            projects.append({"name": row.project, "count": row.cnt})
    return {"projects": projects, "no_project_count": no_project_count}


@adscore_router.post("/banners/bulk-delete")
async def bulk_delete_banners(
    payload: dict,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete multiple banners at once. Body: {banner_ids: [str]}"""
    tid = current_user.tenant.id
    banner_ids = payload.get("banner_ids", [])
    if not banner_ids:
        raise HTTPException(status_code=400, detail="No banner_ids provided")

    uuids = []
    for bid in banner_ids:
        try:
            uuids.append(uuid_mod.UUID(bid))
        except ValueError:
            pass

    # Fetch banners to delete storage files
    result = await db.execute(
        select(Banner).where(Banner.id.in_(uuids), Banner.tenant_id == tid)
    )
    banners_to_delete = list(result.scalars().all())

    # Delete storage files
    for b in banners_to_delete:
        if b.storage_key:
            try:
                await file_storage.delete_file(b.storage_key)
            except Exception as e:
                logger.warning("Failed to delete banner file %s: %s", b.storage_key, e)

    # Delete from DB
    if banners_to_delete:
        await db.execute(
            delete(Banner).where(Banner.id.in_([b.id for b in banners_to_delete]))
        )
        await db.commit()

    logger.info("Bulk deleted %d banners", len(banners_to_delete))
    return {"deleted": len(banners_to_delete)}


@adscore_router.post("/projects/rename")
async def rename_project(
    payload: dict,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Rename a project across all banners. Body: {old_name: str, new_name: str}"""
    tid = current_user.tenant.id
    old_name = (payload.get("old_name") or "").strip()
    new_name = (payload.get("new_name") or "").strip()[:200]

    if not old_name or not new_name:
        raise HTTPException(status_code=400, detail="old_name and new_name are required")
    if old_name == new_name:
        return {"updated": 0, "old_name": old_name, "new_name": new_name}

    result = await db.execute(
        update(Banner)
        .where(Banner.tenant_id == tid, Banner.project == old_name)
        .values(project=new_name)
    )
    await db.commit()

    logger.info("Renamed project '%s' -> '%s' for %d banners", old_name, new_name, result.rowcount)
    return {"updated": result.rowcount, "old_name": old_name, "new_name": new_name}


@adscore_router.post("/banners/bulk-set-project")
async def bulk_set_project(
    payload: dict,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Set project for multiple banners. Body: {banner_ids: [str], project: str|null}"""
    tid = current_user.tenant.id
    banner_ids = payload.get("banner_ids", [])
    project_name = payload.get("project")  # None means unassign

    if not banner_ids:
        raise HTTPException(status_code=400, detail="No banner_ids provided")

    # Trim project name
    if project_name:
        project_name = project_name.strip()[:200]

    uuids = []
    for bid in banner_ids:
        try:
            uuids.append(uuid_mod.UUID(bid))
        except ValueError:
            pass

    result = await db.execute(
        update(Banner)
        .where(Banner.id.in_(uuids), Banner.tenant_id == tid)
        .values(project=project_name or None)
    )
    await db.commit()

    logger.info("Bulk set project='%s' for %d banners", project_name, result.rowcount)
    return {"updated": result.rowcount, "project": project_name}


# ---------------------------------------------------------------------------
# AI Tagging
# ---------------------------------------------------------------------------


@adscore_router.post("/tag/{banner_id}", response_model=TagResponse)
@limiter.limit("20/minute")
async def tag_banner_endpoint(
    request: Request,
    banner_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Run AI tagging on a single banner."""
    from adscore_tagger import tag_banner

    tid = current_user.tenant.id
    banner = await _get_banner_or_404(db, banner_id, tid)

    if not banner.storage_key:
        raise HTTPException(status_code=404, detail="No image file for this banner, cannot tag")

    # Update status
    banner.tags_status = "processing"
    await db.commit()

    try:
        import asyncio
        import tempfile

        # Download file from Supabase
        file_bytes = await file_storage.download_file(banner.storage_key)
        media_type = getattr(banner, "media_type", None) or "image"

        if media_type == "video":
            # Video: extract keyframes → tag each → aggregate
            from video_processor import extract_keyframes, video_meta_to_dict
            from video_scorer import score_video_keyframes

            # Write video to temp file for FFmpeg
            with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
                tmp.write(file_bytes)
                tmp_path = tmp.name

            try:
                meta, kf_list = await asyncio.to_thread(extract_keyframes, tmp_path)
                result = await asyncio.to_thread(score_video_keyframes, kf_list, meta)

                # Upload keyframe images to storage + build keyframes list
                keyframes_data = []
                for kf_data in result["keyframes"]:
                    kf_obj = next((k for k in kf_list if k.index == kf_data["index"]), None)
                    kf_image_url = None
                    if kf_obj and os.path.exists(kf_obj.image_path):
                        kf_storage_key = f"{tid}/banners/{banner.id}/keyframes/frame_{kf_data['index']:03d}.jpg"
                        with open(kf_obj.image_path, "rb") as f:
                            kf_bytes = f.read()
                        try:
                            import httpx
                            url = f"{file_storage.STORAGE_BASE}/object/{file_storage.BUCKET}/{kf_storage_key}"
                            async with httpx.AsyncClient(timeout=60) as client:
                                resp = await client.post(url, content=kf_bytes, headers={
                                    **file_storage._headers(),
                                    "Content-Type": "image/jpeg",
                                    "x-upsert": "true",
                                })
                            if resp.status_code in (200, 201):
                                kf_image_url = await file_storage.get_signed_url(kf_storage_key)
                        except Exception as e:
                            logger.warning("Failed to upload keyframe %d: %s", kf_data["index"], e)

                    keyframes_data.append({
                        **kf_data,
                        "image_url": kf_image_url,
                    })

                # Use hook frame tags as the banner's primary tags
                hook_tags = next((kf["tags"] for kf in result["keyframes"] if kf["frame_type"] == "hook" and kf["tags"]), {})
                tags_dict = hook_tags or (result["keyframes"][0]["tags"] if result["keyframes"] else {})

                banner.tags = tags_dict
                banner.video_meta = {
                    **video_meta_to_dict(meta),
                    "scene_count": result["scene_count"],
                    "video_cqs": result["video_cqs"],
                    "hook_cqs": result["hook_cqs"],
                    "cta_cqs": result["cta_cqs"],
                }
                banner.keyframes = keyframes_data
                banner.width = meta.width
                banner.height = meta.height
                flag_modified(banner, "video_meta")
                flag_modified(banner, "keyframes")
            finally:
                os.unlink(tmp_path)
                # Clean up keyframe temp files
                for kf in kf_list:
                    if os.path.exists(kf.image_path):
                        os.unlink(kf.image_path)
        else:
            # Image: standard Claude Vision tagging
            tags_dict = await asyncio.to_thread(tag_banner, file_bytes)
            banner.tags = tags_dict

        banner.tags_status = "done"
        banner.tags_error = None
        banner.tagged_at = datetime.now(timezone.utc)
        flag_modified(banner, "tags")
        await db.commit()

        logger.info("Tagged banner %s successfully (type=%s)", banner_id, media_type)
        return TagResponse(
            banner_id=banner_id,
            tags=BannerTags(**tags_dict) if tags_dict else None,
            tags_status="done",
        )
    except Exception as e:
        banner.tags_status = "error"
        banner.tags_error = str(e)
        await db.commit()

        logger.error("Failed to tag banner %s: %s", banner_id, e)
        return TagResponse(
            banner_id=banner_id,
            tags=None,
            tags_status="error",
            tags_error=str(e),
        )


@adscore_router.post("/tag-all")
@limiter.limit("3/minute")
async def tag_all_banners(
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Tag all pending banners for the current tenant."""
    from adscore_tagger import tag_banner

    tid = current_user.tenant.id
    result = await db.execute(
        select(Banner).where(
            Banner.tenant_id == tid,
            Banner.tags_status.in_(["pending", "error"]),
            Banner.storage_key.isnot(None),
        )
    )
    pending = list(result.scalars().all())

    import asyncio
    results = {"tagged": 0, "errors": 0, "total_pending": len(pending)}

    for banner in pending:
        try:
            image_bytes = await file_storage.download_file(banner.storage_key)
            tags_dict = await asyncio.to_thread(tag_banner, image_bytes)
            banner.tags = tags_dict
            banner.tags_status = "done"
            banner.tags_error = None
            banner.tagged_at = datetime.now(timezone.utc)
            results["tagged"] += 1
        except Exception as e:
            banner.tags_status = "error"
            banner.tags_error = str(e)
            results["errors"] += 1

    await db.commit()
    logger.info("Tag-all: %d tagged, %d errors out of %d pending",
                results["tagged"], results["errors"], len(pending))

    return results


# ---------------------------------------------------------------------------
# Insights & Element Performance
# ---------------------------------------------------------------------------


def _extract_boolean_elements(tags: dict) -> dict:
    """Extract all boolean tag fields as flat dict."""
    elements = {}
    bool_fields = {
        "visual": ["has_faces", "rule_of_thirds"],
        "text_elements": ["has_urgency_words"],
        "structural": ["has_cta_button", "has_logo", "product_visible", "price_visible", "before_after", "safe_zones_clear"],
        "emotional": ["has_smiling_face"],
        "accessibility": ["contrast_adequate", "min_font_readable", "color_blind_safe"],
    }
    for category, fields in bool_fields.items():
        cat_data = tags.get(category, {})
        for field in fields:
            if field in cat_data:
                elements[field] = bool(cat_data[field])

    cat_fields = {
        "visual": {"color_scheme": None, "background_type": None, "visual_clutter": None, "focal_point": None, "visual_hierarchy": None},
        "text_elements": {"text_readability": None, "font_size_hierarchy": None, "font_style": None},
        "structural": {"price_prominence": None},
        "emotional": {"tonality": None, "energy_level": None, "personalization_level": None},
        "accessibility": {"information_density": None},
        "platform_fit": {"thumb_stop_potential": None, "format_type": None, "first_impression_strength": None},
    }
    for category, fields in cat_fields.items():
        cat_data = tags.get(category, {})
        for field in fields:
            val = cat_data.get(field)
            if val:
                elements[f"{field}_{val}"] = True

    return elements


def _compute_element_performance(banners_data: list, platform_filter: Optional[str] = None) -> list:
    """Compute element performance from tagged banners (accepts list of dicts)."""
    valid = []
    for b in banners_data:
        if b.get("tags_status") != "done" or not b.get("tags"):
            continue
        metrics = b.get("metrics", {})
        if not any(metrics.get(k) for k in ("ctr", "cr_install", "cr_event")):
            continue
        if platform_filter and metrics.get("platform") != platform_filter:
            continue
        valid.append(b)

    if not valid:
        return []

    metric_keys = ["ctr", "cr_install", "cr_event"]
    all_elements = {}

    element_names = set()
    for b in valid:
        elements = _extract_boolean_elements(b["tags"])
        element_names.update(elements.keys())

    for elem_name in element_names:
        with_metrics = {k: [] for k in metric_keys}
        without_metrics = {k: [] for k in metric_keys}
        banners_with = 0
        banners_without = 0

        for b in valid:
            elements = _extract_boolean_elements(b["tags"])
            has_elem = elements.get(elem_name, False)
            metrics = b.get("metrics", {})

            if has_elem:
                banners_with += 1
            else:
                banners_without += 1

            target = with_metrics if has_elem else without_metrics
            # Normalise rate values to fractions (0-1 range)
            impr = metrics.get("impressions")
            normalised = {}
            if impr and float(impr) > 0:
                impr_f = float(impr)
                clicks = metrics.get("clicks")
                installs = metrics.get("installs")
                events = sum(float(metrics.get(f"event_{i}") or 0) for i in range(1, 5))
                if clicks is not None:
                    normalised["ctr"] = float(clicks) / impr_f
                if installs is not None:
                    normalised["cr_install"] = float(installs) / impr_f
                if events > 0:
                    normalised["cr_event"] = events / impr_f
            for mk in metric_keys:
                val = normalised.get(mk) or metrics.get(mk)
                if val is not None:
                    fval = float(val)
                    # Safety: if still > 1 after normalisation, treat as percentage
                    if fval > 1:
                        fval = fval / 100.0
                    target[mk].append(fval)

        metric_stats = {}
        for mk in metric_keys:
            w = with_metrics[mk]
            wo = without_metrics[mk]
            if not w and not wo:
                continue

            avg_w = float(np.mean(w)) if w else 0.0
            avg_wo = float(np.mean(wo)) if wo else 0.0
            delta = avg_w - avg_wo
            delta_pct = (delta / avg_wo * 100) if avg_wo != 0 else 0.0

            corr, p_val = 0.0, 1.0
            if len(w) >= 1 and len(wo) >= 1:
                all_vals = w + wo
                all_flags = [1.0] * len(w) + [0.0] * len(wo)
                if len(all_vals) >= 3:
                    try:
                        from scipy.stats import pearsonr
                        corr, p_val = pearsonr(all_flags, all_vals)
                        corr = float(corr) if not np.isnan(corr) else 0.0
                        p_val = float(p_val) if not np.isnan(p_val) else 1.0
                    except Exception:
                        pass

            metric_stats[mk] = ElementMetricStats(
                avg_with=round(avg_w, 6),
                avg_without=round(avg_wo, 6),
                delta=round(delta, 6),
                delta_pct=round(delta_pct, 2),
                correlation=round(corr, 4),
                p_value=round(p_val, 4),
            )

        category = "visual"
        if elem_name.startswith(("has_urgency", "headline", "subtitle", "offer", "cta_text", "text_readability", "font_size", "font_style")):
            category = "text_elements"
        elif elem_name.startswith(("has_cta", "has_logo", "text_image", "product_", "price_", "before_after", "safe_zones")):
            category = "structural"
        elif elem_name.startswith(("tonality", "has_smiling", "energy", "personalization")):
            category = "emotional"
        elif elem_name.startswith(("contrast_", "min_font", "color_blind", "information_density")):
            category = "accessibility"
        elif elem_name.startswith(("thumb_stop", "format_type", "first_impression")):
            category = "platform_fit"

        all_elements[elem_name] = ElementPerformance(
            element_name=elem_name,
            element_category=category,
            n_with=banners_with,
            n_without=banners_without,
            metrics=metric_stats,
        )

    return list(all_elements.values())


async def _load_tenant_banners_data(
    db: AsyncSession, tenant_id: uuid_mod.UUID, project: Optional[str] = None,
    media_type: Optional[str] = None,
) -> list:
    """Load all banners for a tenant as list of dicts (for analytics functions)."""
    q = select(Banner).where(Banner.tenant_id == tenant_id)
    if project == "__none__":
        q = q.where(Banner.project.is_(None))
    elif project:
        q = q.where(Banner.project == project)
    if media_type == "image":
        q = q.where(or_(Banner.media_type == "image", Banner.media_type.is_(None)))
    elif media_type == "video":
        q = q.where(Banner.media_type == "video")
    result = await db.execute(q)
    banners = result.scalars().all()
    return [_banner_to_record(b) for b in banners]


@adscore_router.get("/insights", response_model=InsightsResponse)
async def get_insights(
    platform: Optional[str] = None,
    project: Optional[str] = None,
    media_type: Optional[str] = None,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get element performance insights for the current tenant."""
    tid = current_user.tenant.id
    banners_data = await _load_tenant_banners_data(db, tid, project=project, media_type=media_type)

    elements = _compute_element_performance(banners_data, platform_filter=platform)

    platform_slices = {}
    if not platform:
        platforms = set()
        for b in banners_data:
            p = (b.get("metrics") or {}).get("platform")
            if p:
                platforms.add(p)
        for p in platforms:
            platform_slices[p] = [
                e.model_dump() for e in _compute_element_performance(banners_data, platform_filter=p)
            ]

    n_tagged = sum(1 for b in banners_data if b.get("tags_status") == "done")

    return InsightsResponse(
        elements=elements,
        n_banners=n_tagged,
        generated_at=datetime.now(timezone.utc).isoformat(),
        platform_slices=platform_slices,
    )


@adscore_router.get("/insights/video-hooks")
async def get_video_hook_insights(
    project: Optional[str] = None,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Analyze hook frame performance across video creatives."""
    tid = current_user.tenant.id
    banners_data = await _load_tenant_banners_data(db, tid, project=project, media_type="video")

    hook_groups = {}  # hook_type -> list of {ctr, banner_id, image_url, ...}
    duration_groups = {}  # bucket label -> list of {ctr, ...}

    for b in banners_data:
        m = b.get("metrics") or {}
        ctr = m.get("ctr")
        if ctr is None or b.get("tags_status") != "done":
            continue

        kfs = b.get("keyframes") or []
        vmeta = b.get("video_meta") or {}
        dur = vmeta.get("duration", 0)

        # Classify duration bucket
        if dur <= 0:
            dur_bucket = "unknown"
        elif dur <= 6:
            dur_bucket = "0-6s"
        elif dur <= 15:
            dur_bucket = "6-15s"
        elif dur <= 30:
            dur_bucket = "15-30s"
        elif dur <= 60:
            dur_bucket = "30-60s"
        else:
            dur_bucket = "60s+"

        entry = {
            "banner_id": b.get("id"),
            "ctr": ctr,
            "image_url": b.get("image_url"),
            "filename": b.get("original_filename"),
            "duration": dur,
        }

        duration_groups.setdefault(dur_bucket, []).append(entry)

        # Hook frame analysis
        hook_frame = next((kf for kf in kfs if kf.get("frame_type") == "hook"), None)
        if hook_frame:
            tags = hook_frame.get("tags") or {}
            # Determine hook type from tags
            hook_type = "other"
            if tags.get("text_elements", {}).get("headline"):
                hook_type = "text"
            elif tags.get("structural", {}).get("human_presence") in ("face_closeup", "person"):
                hook_type = "face"
            elif tags.get("visual", {}).get("product_prominence") in ("high", "dominant"):
                hook_type = "product"
            elif tags.get("emotional", {}).get("action_dynamic") in ("high", "medium"):
                hook_type = "action"

            entry["hook_image_url"] = hook_frame.get("image_url")
            entry["hook_cqs"] = hook_frame.get("cqs_score")
            hook_groups.setdefault(hook_type, []).append(entry)

    # Aggregate hook groups
    hook_summary = []
    for htype, entries in sorted(hook_groups.items()):
        ctrs = [e["ctr"] for e in entries]
        avg_ctr = sum(ctrs) / len(ctrs) if ctrs else 0
        best = max(entries, key=lambda e: e["ctr"])
        hook_summary.append({
            "hook_type": htype,
            "count": len(entries),
            "avg_ctr": round(avg_ctr, 6),
            "best_ctr": round(best["ctr"], 6),
            "best_banner_id": best["banner_id"],
            "best_image_url": best.get("hook_image_url") or best.get("image_url"),
        })
    hook_summary.sort(key=lambda x: x["avg_ctr"], reverse=True)

    # Aggregate duration groups
    duration_summary = []
    for bucket, entries in sorted(duration_groups.items()):
        ctrs = [e["ctr"] for e in entries]
        avg_ctr = sum(ctrs) / len(ctrs) if ctrs else 0
        duration_summary.append({
            "bucket": bucket,
            "count": len(entries),
            "avg_ctr": round(avg_ctr, 6),
        })

    # CTA endcard analysis
    cta_yes = []
    cta_no = []
    for b in banners_data:
        m = b.get("metrics") or {}
        ctr = m.get("ctr")
        if ctr is None or b.get("tags_status") != "done":
            continue
        kfs = b.get("keyframes") or []
        cta_frame = next((kf for kf in kfs if kf.get("frame_type") == "cta"), None)
        if cta_frame and cta_frame.get("tags", {}).get("structural", {}).get("cta_presence"):
            cta_yes.append(ctr)
        else:
            cta_no.append(ctr)

    cta_impact = {
        "with_cta_count": len(cta_yes),
        "without_cta_count": len(cta_no),
        "with_cta_avg_ctr": round(sum(cta_yes) / len(cta_yes), 6) if cta_yes else None,
        "without_cta_avg_ctr": round(sum(cta_no) / len(cta_no), 6) if cta_no else None,
    }

    # Scene count analysis
    scene_groups = {}
    for b in banners_data:
        m = b.get("metrics") or {}
        ctr = m.get("ctr")
        if ctr is None or b.get("tags_status") != "done":
            continue
        vmeta = b.get("video_meta") or {}
        sc = vmeta.get("scene_count") or len([kf for kf in (b.get("keyframes") or []) if kf.get("frame_type") == "scene_change"])
        scene_groups.setdefault(sc, []).append(ctr)

    scene_summary = [
        {"scene_count": sc, "count": len(ctrs), "avg_ctr": round(sum(ctrs) / len(ctrs), 6)}
        for sc, ctrs in sorted(scene_groups.items())
    ]

    return {
        "hook_types": hook_summary,
        "duration_buckets": duration_summary,
        "cta_impact": cta_impact,
        "scene_counts": scene_summary,
        "n_videos": len([b for b in banners_data if b.get("tags_status") == "done"]),
    }


@adscore_router.get("/elements")
async def get_elements_table(
    platform: Optional[str] = None,
    project: Optional[str] = None,
    media_type: Optional[str] = None,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get flat element→KPI table for DataTable rendering."""
    tid = current_user.tenant.id
    banners_data = await _load_tenant_banners_data(db, tid, project=project, media_type=media_type)
    elements = _compute_element_performance(banners_data, platform_filter=platform)

    rows = []
    for ep in elements:
        row = {
            "element_name": ep.element_name,
            "element_category": ep.element_category,
            "n_with": ep.n_with,
            "n_without": ep.n_without,
        }
        for metric_key, stats in ep.metrics.items():
            row[f"{metric_key}_avg_with"] = stats.avg_with
            row[f"{metric_key}_avg_without"] = stats.avg_without
            row[f"{metric_key}_delta_pct"] = stats.delta_pct
            row[f"{metric_key}_correlation"] = stats.correlation
            row[f"{metric_key}_p_value"] = stats.p_value
        rows.append(row)

    return {"rows": rows, "total": len(rows)}


# ---------------------------------------------------------------------------
# LLM Explanation ("Explain Why")
# ---------------------------------------------------------------------------


EXPLAIN_SYSTEM_PROMPT = """Ты — эксперт по performance-маркетингу и аналитике рекламных креативов.
Твоя задача — объяснить, почему конкретный рекламный баннер показал определённые результаты,
на основе его визуальных элементов и статистических данных.

Правила:
1. Отвечай на русском языке
2. Будь конкретным: ссылайся на реальные элементы баннера и статистику
3. Структурируй ответ по разделам с заголовками ###
4. Давай actionable рекомендации
5. Не придумывай данные — используй только предоставленный контекст
6. Длина ответа: 300-500 слов"""

EXPLAIN_USER_PROMPT = """Проанализируй рекламный баннер и объясни его результаты.

{context}

Ответь строго в следующем формате:

### Общая оценка
Кратко: хороший/средний/слабый результат и почему.

### Что работает хорошо
Какие элементы баннера вероятно положительно влияют на метрики (со ссылкой на статистику).

### Что можно улучшить
Какие элементы отсутствуют или негативно влияют, и что стоит добавить/изменить.

### Рекомендации
3-5 конкретных actionable шагов для улучшения результатов этого баннера."""


def _build_explain_context(banner: dict, banners_data: list, element_perf: list) -> str:
    """Build a text context for the LLM explain prompt."""
    metrics = banner.get("metrics", {})
    tags = banner.get("tags", {})
    lines = []

    lines.append("## Данные анализируемого баннера")
    lines.append(f"- Файл: {banner.get('original_filename', '?')}")

    metric_labels = {"ctr": "CTR", "cr_install": "CR Install", "cr_event": "CR Event"}
    for key, label in metric_labels.items():
        val = metrics.get(key)
        lines.append(f"- {label}: {f'{val * 100:.2f}%' if val is not None else 'не указан'}")
    lines.append(f"- Показы: {metrics.get('impressions') or 'не указаны'}")
    lines.append(f"- Платформа: {metrics.get('platform') or 'не указана'}")
    lines.append(f"- Кампания: {metrics.get('campaign') or 'не указана'}")

    valid_banners = [b for b in banners_data if b.get("metrics", {}).get("ctr") is not None]
    if valid_banners and metrics.get("ctr") is not None:
        ctr_values = sorted([b["metrics"]["ctr"] for b in valid_banners])
        rank = sum(1 for v in ctr_values if v <= metrics["ctr"])
        percentile = round(rank / len(ctr_values) * 100)
        lines.append(f"\n## Ранг баннера")
        lines.append(f"- CTR перцентиль: {percentile}% (ранг {rank} из {len(ctr_values)} баннеров)")

    lines.append("\n## Визуальные элементы этого баннера")
    category_names = {
        "visual": "Визуальные",
        "text_elements": "Текстовые",
        "structural": "Структурные",
        "emotional": "Эмоциональные",
        "accessibility": "Доступность",
        "platform_fit": "Платформа",
    }
    for category in ["visual", "text_elements", "structural", "emotional", "accessibility", "platform_fit"]:
        cat_data = tags.get(category, {}) if tags else {}
        if not cat_data:
            continue
        cat_items = []
        for key, value in cat_data.items():
            if value is None or value == "" or value == []:
                continue
            if isinstance(value, bool):
                cat_items.append(f"{key}: {'да' if value else 'нет'}")
            elif isinstance(value, list):
                cat_items.append(f"{key}: {', '.join(str(v) for v in value)}")
            else:
                cat_items.append(f"{key}: {value}")
        if cat_items:
            lines.append(f"**{category_names.get(category, category)}:** {'; '.join(cat_items)}")

    if element_perf and tags:
        lines.append("\n## Статистическое влияние элементов на CTR (по всем баннерам в базе)")
        banner_elements = _extract_boolean_elements(tags)
        ep_lookup = {ep.element_name: ep for ep in element_perf}

        for elem_name, has_elem in sorted(banner_elements.items()):
            ep = ep_lookup.get(elem_name)
            if not ep or "ctr" not in ep.metrics:
                continue
            stat = ep.metrics["ctr"]
            direction = "↑ ПОЛОЖИТЕЛЬНО" if stat.delta_pct > 0 else "↓ ОТРИЦАТЕЛЬНО"
            sig = "значимо" if stat.p_value < 0.05 else "не значимо"
            lines.append(
                f"- {elem_name} ({'ЕСТЬ' if has_elem else 'НЕТ'}): "
                f"delta CTR {stat.delta_pct:+.1f}%, корреляция {stat.correlation:.3f}, "
                f"p={stat.p_value:.3f} ({sig}) [{direction}]"
            )

    if valid_banners and len(valid_banners) >= 3:
        sorted_by_ctr = sorted(valid_banners, key=lambda b: b["metrics"]["ctr"], reverse=True)
        top3 = sorted_by_ctr[:3]
        bottom3 = sorted_by_ctr[-3:]

        lines.append("\n## Топ-3 баннеры по CTR (для контраста)")
        for b in top3:
            bm = b.get("metrics", {})
            lines.append(f"- {b.get('original_filename', '?')}: CTR={bm.get('ctr', 0) * 100:.2f}%")

        lines.append("\n## Боттом-3 баннеры по CTR")
        for b in bottom3:
            bm = b.get("metrics", {})
            lines.append(f"- {b.get('original_filename', '?')}: CTR={bm.get('ctr', 0) * 100:.2f}%")

    total_tagged = sum(1 for b in banners_data if b.get("tags_status") == "done")
    lines.append(f"\nВсего баннеров в базе: {len(banners_data)}, протегировано: {total_tagged}")

    return "\n".join(lines)


@adscore_router.post("/banner/{banner_id}/explain", response_model=ExplainResponse)
@limiter.limit("10/minute")
async def explain_banner(
    request: Request,
    banner_id: str,
    force: bool = False,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Generate an AI explanation for a banner's performance."""
    import anthropic

    tid = current_user.tenant.id
    banner = await _get_banner_or_404(db, banner_id, tid)

    if banner.tags_status != "done":
        raise HTTPException(
            status_code=400,
            detail="Баннер должен быть протегирован перед генерацией объяснения",
        )

    # Cache check
    if banner.explanation and not force:
        return ExplainResponse(
            banner_id=banner_id,
            explanation=banner.explanation,
            explained_at=banner.explained_at.isoformat() if banner.explained_at else "",
            cached=True,
        )

    # Build context from tenant's banners
    banners_data = await _load_tenant_banners_data(db, tid)
    element_perf = _compute_element_performance(banners_data)
    banner_dict = _banner_to_record(banner)
    context = _build_explain_context(banner_dict, banners_data, element_perf)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not configured")

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2048,
            system=EXPLAIN_SYSTEM_PROMPT,
            messages=[
                {"role": "user", "content": EXPLAIN_USER_PROMPT.format(context=context)},
            ],
        )
        explanation = response.content[0].text
    except Exception as e:
        logger.error("Failed to generate explanation for %s: %s", banner_id, e)
        raise HTTPException(status_code=500, detail="Ошибка генерации объяснения. Попробуйте позже.")

    explained_at = datetime.now(timezone.utc)
    banner.explanation = explanation
    banner.explained_at = explained_at
    await db.commit()

    logger.info("Generated explanation for banner %s (%d chars)", banner_id, len(explanation))

    return ExplainResponse(
        banner_id=banner_id,
        explanation=explanation,
        explained_at=explained_at.isoformat(),
        cached=False,
    )
