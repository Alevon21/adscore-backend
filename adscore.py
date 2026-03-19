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
from sqlalchemy import select, update, delete, func
from sqlalchemy.ext.asyncio import AsyncSession

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

MAX_IMAGE_SIZE = 10 * 1024 * 1024  # 10 MB

VALID_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".webp")
MIME_MAP = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
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
        "metrics": b.metrics or {},
        "tags": b.tags,
        "tags_status": b.tags_status or "pending",
        "tags_error": b.tags_error,
        "tagged_at": b.tagged_at.isoformat() if b.tagged_at else None,
        "explanation": b.explanation,
        "explained_at": b.explained_at.isoformat() if b.explained_at else None,
        "image_url": image_url,
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
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Upload a single banner image with optional metrics JSON."""
    tid = current_user.tenant.id
    uid = current_user.user.id

    ext = Path(image.filename or "image.png").suffix.lower()
    if ext not in VALID_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Only image files (PNG, JPG, GIF, WebP) are allowed")

    content = await image.read()
    if len(content) > MAX_IMAGE_SIZE:
        raise HTTPException(status_code=400, detail=f"Image too large (max {MAX_IMAGE_SIZE // 1024 // 1024} MB)")

    try:
        metrics_dict = json.loads(metrics)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid metrics JSON")

    banner_metrics = BannerMetrics(**metrics_dict)
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
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Bulk upload banner metrics from CSV. Images should be uploaded separately."""
    import pandas as pd
    from io import BytesIO

    tid = current_user.tenant.id
    uid = current_user.user.id

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

            banner_metrics = BannerMetrics(**metrics_dict)
            fname = str(row.get("filename", "")) or ""

            db_banner = Banner(
                id=banner_id,
                tenant_id=tid,
                created_by=uid,
                original_filename=fname,
                metrics=metrics_dict,
                tags_status="no_image",
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
        # Download image from Supabase
        image_bytes = await file_storage.download_file(banner.storage_key)
        tags_dict = await asyncio.to_thread(tag_banner, image_bytes)

        banner.tags = tags_dict
        banner.tags_status = "done"
        banner.tags_error = None
        banner.tagged_at = datetime.now(timezone.utc)
        await db.commit()

        logger.info("Tagged banner %s successfully", banner_id)
        return TagResponse(
            banner_id=banner_id,
            tags=BannerTags(**tags_dict),
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
        "visual": ["has_faces"],
        "text_elements": ["has_urgency_words"],
        "structural": ["has_cta_button", "has_logo"],
        "emotional": ["has_smiling_face"],
    }
    for category, fields in bool_fields.items():
        cat_data = tags.get(category, {})
        for field in fields:
            if field in cat_data:
                elements[field] = bool(cat_data[field])

    cat_fields = {
        "visual": {"color_scheme": None, "background_type": None},
        "emotional": {"tonality": None, "energy_level": None},
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

        for b in valid:
            elements = _extract_boolean_elements(b["tags"])
            has_elem = elements.get(elem_name, False)
            metrics = b.get("metrics", {})

            target = with_metrics if has_elem else without_metrics
            for mk in metric_keys:
                val = metrics.get(mk)
                if val is not None:
                    target[mk].append(val)

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
            if len(w) >= 2 and len(wo) >= 2:
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
        if elem_name.startswith(("has_urgency", "headline", "subtitle", "offer", "cta_text")):
            category = "text_elements"
        elif elem_name.startswith(("has_cta", "has_logo", "text_image")):
            category = "structural"
        elif elem_name.startswith(("tonality", "has_smiling", "energy")):
            category = "emotional"

        all_elements[elem_name] = ElementPerformance(
            element_name=elem_name,
            element_category=category,
            n_with=sum(len(with_metrics[k]) for k in metric_keys) // len(metric_keys) if metric_keys else 0,
            n_without=sum(len(without_metrics[k]) for k in metric_keys) // len(metric_keys) if metric_keys else 0,
            metrics=metric_stats,
        )

    return list(all_elements.values())


async def _load_tenant_banners_data(db: AsyncSession, tenant_id: uuid_mod.UUID) -> list:
    """Load all banners for a tenant as list of dicts (for analytics functions)."""
    result = await db.execute(
        select(Banner).where(Banner.tenant_id == tenant_id)
    )
    banners = result.scalars().all()
    return [_banner_to_record(b) for b in banners]


@adscore_router.get("/insights", response_model=InsightsResponse)
async def get_insights(
    platform: Optional[str] = None,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get element performance insights for the current tenant."""
    tid = current_user.tenant.id
    banners_data = await _load_tenant_banners_data(db, tid)

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


@adscore_router.get("/elements")
async def get_elements_table(
    platform: Optional[str] = None,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get flat element→KPI table for DataTable rendering."""
    tid = current_user.tenant.id
    banners_data = await _load_tenant_banners_data(db, tid)
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
    }
    for category in ["visual", "text_elements", "structural", "emotional"]:
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
