"""Migrate banners from banners.json + local files to PostgreSQL + Supabase Storage.

Usage:
    python migrate_banners_to_db.py --tenant-id <uuid> --user-id <uuid>

Reads data/banners.json, uploads images to Supabase Storage,
inserts Banner rows into the database.
"""

import argparse
import asyncio
import json
import os
import sys
import uuid
import logging
from pathlib import Path
from datetime import datetime, timezone

import httpx
from sqlalchemy import select

# Ensure backend is on path
sys.path.insert(0, os.path.dirname(__file__))

from database import async_session, engine
from db_models import Banner
import storage as file_storage

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

BANNERS_JSON = Path(__file__).parent / "data" / "banners.json"
BANNERS_DIR = Path(__file__).parent / "data" / "banners"


async def migrate(tenant_id: uuid.UUID, user_id: uuid.UUID, dry_run: bool = False):
    if not BANNERS_JSON.exists():
        logger.error("banners.json not found at %s", BANNERS_JSON)
        return

    with open(BANNERS_JSON) as f:
        banners = json.load(f)

    logger.info("Found %d banners to migrate", len(banners))

    if dry_run:
        for b in banners:
            logger.info("  [DRY] %s — %s", b["id"], b.get("original_filename", "?"))
        return

    migrated = 0
    errors = 0

    async with async_session() as db:
        for b in banners:
            try:
                banner_id = uuid.uuid4()
                original_filename = b.get("original_filename", b.get("filename", "unknown"))
                local_filename = b.get("filename")
                local_path = BANNERS_DIR / local_filename if local_filename else None

                storage_key = None
                file_size = b.get("file_size_bytes")

                # Upload image to Supabase Storage
                if local_path and local_path.exists():
                    content = local_path.read_bytes()
                    file_size = len(content)
                    storage_key = f"{tenant_id}/banners/{banner_id}/{original_filename}"

                    # Detect mime type
                    ext = local_path.suffix.lower()
                    mime_map = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                                ".gif": "image/gif", ".webp": "image/webp"}
                    mime_type = mime_map.get(ext, "image/png")

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
                            logger.warning("  Storage upload failed for %s: %s", b["id"], resp.text)
                            storage_key = None
                else:
                    mime_type = None

                # Parse upload date
                upload_date = b.get("upload_date")
                created_at = datetime.fromisoformat(upload_date) if upload_date else datetime.now(timezone.utc)

                # Determine tags_status
                tags = b.get("tags")
                tags_status = b.get("tags_status", "pending")

                banner = Banner(
                    id=banner_id,
                    tenant_id=tenant_id,
                    created_by=user_id,
                    original_filename=original_filename,
                    storage_key=storage_key,
                    file_size_bytes=file_size,
                    width=b.get("width"),
                    height=b.get("height"),
                    mime_type=mime_type or b.get("mime_type"),
                    metrics=b.get("metrics"),
                    tags=tags,
                    tags_status=tags_status,
                    tags_error=b.get("tags_error"),
                    tagged_at=datetime.fromisoformat(b["tagged_at"]) if b.get("tagged_at") else None,
                    explanation=b.get("explanation"),
                    explained_at=datetime.fromisoformat(b["explained_at"]) if b.get("explained_at") else None,
                    created_at=created_at,
                )
                db.add(banner)
                migrated += 1
                logger.info("  [OK] %s → %s (%s)", b["id"], banner_id, "with image" if storage_key else "no image")

            except Exception as e:
                logger.error("  [FAIL] %s: %s", b.get("id", "?"), e)
                errors += 1

        await db.commit()

    logger.info("Migration complete: %d migrated, %d errors", migrated, errors)

    # Rename old file
    if errors == 0:
        migrated_path = BANNERS_JSON.with_suffix(".json.migrated")
        BANNERS_JSON.rename(migrated_path)
        logger.info("Renamed banners.json → banners.json.migrated")


def main():
    parser = argparse.ArgumentParser(description="Migrate banners from JSON to DB + Supabase")
    parser.add_argument("--tenant-id", required=True, help="Target tenant UUID")
    parser.add_argument("--user-id", required=True, help="User UUID (created_by)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without changes")
    args = parser.parse_args()

    try:
        tenant_id = uuid.UUID(args.tenant_id)
        user_id = uuid.UUID(args.user_id)
    except ValueError as e:
        logger.error("Invalid UUID: %s", e)
        sys.exit(1)

    asyncio.run(migrate(tenant_id, user_id, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
