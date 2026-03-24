"""
Demo data endpoints — serves test datasets for users with the demo_data feature.
Includes server-side "run" endpoints that skip the download+upload round-trip.
"""

import io
import logging
import uuid as uuid_mod
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession

from auth import require_feature, CurrentUser
from database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/demo", tags=["demo"])

DEMO_DIR = Path(__file__).parent / "demo_data"

DATASETS = [
    {
        "id": "scoring_template",
        "module": "analysis",
        "name": "Скоринг: шаблон с кампаниями",
        "description": "90 текстов, 15 колонок. Расход, клики, конверсии, CPA по 31 кампании.",
        "filename": "scoring_template_2.xlsx",
    },
    {
        "id": "mmp_adjust_installs",
        "module": "mmp",
        "name": "MMP: установки Adjust",
        "description": "~143K строк. Данные установок с трекерами, CTIT, device_id, странами.",
        "filename": "demo_mmp_installs.csv",
    },
]


def _get_dataset(dataset_id: str) -> dict:
    ds = next((ds for ds in DATASETS if ds["id"] == dataset_id), None)
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")
    filepath = DEMO_DIR / ds["filename"]
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="Dataset file missing on server")
    return {**ds, "filepath": filepath}


@router.get("/datasets")
async def list_datasets(
    module: str = Query(None, description="Filter by module: analysis, mmp"),
    current_user: CurrentUser = Depends(require_feature("demo_data")),
):
    """List available demo datasets."""
    results = []
    for ds in DATASETS:
        if module and ds["module"] != module:
            continue
        filepath = DEMO_DIR / ds["filename"]
        size = filepath.stat().st_size if filepath.exists() else 0
        results.append({
            "id": ds["id"],
            "module": ds["module"],
            "name": ds["name"],
            "description": ds["description"],
            "filename": ds["filename"],
            "size_bytes": size,
        })
    return results


@router.get("/download/{dataset_id}")
async def download_dataset(
    dataset_id: str,
    current_user: CurrentUser = Depends(require_feature("demo_data")),
):
    """Download a demo dataset file."""
    ds = _get_dataset(dataset_id)
    return FileResponse(
        path=str(ds["filepath"]),
        filename=ds["filename"],
        media_type="application/octet-stream",
    )


# ── Server-side demo run (no download+upload round-trip) ──────────


@router.post("/run/{dataset_id}")
async def run_demo_dataset(
    dataset_id: str,
    current_user: CurrentUser = Depends(require_feature("demo_data")),
    db: AsyncSession = Depends(get_db),
):
    """Run a demo dataset directly on the server — no file transfer needed."""
    ds = _get_dataset(dataset_id)

    if ds["module"] == "analysis":
        return await _run_scoring_demo(ds, current_user, db)
    elif ds["module"] == "mmp":
        return await _run_mmp_demo(ds, current_user, db)
    else:
        raise HTTPException(400, f"Unknown module: {ds['module']}")


async def _run_scoring_demo(ds: dict, current_user: CurrentUser, db: AsyncSession):
    """Server-side scoring upload: read file from disk, create session."""
    from mapper import ColumnMapper
    from models import EventConfig, ScoringParams
    from scorer import TextScorer
    from main import SESSION_STORE, _session_lock, _schedule_ttl_locked

    filepath = ds["filepath"]
    filename = ds["filename"]

    if filename.endswith(".csv"):
        df = pd.read_csv(filepath)
    else:
        df = pd.read_excel(filepath, engine="openpyxl")

    if df.empty:
        raise HTTPException(400, "Demo file is empty")

    mapper = ColumnMapper()
    columns_detected = list(df.columns.astype(str))
    auto_mapped = mapper.auto_map(columns_detected)
    unmapped = mapper.get_unmapped_columns(columns_detected, auto_mapped)
    auto_events = mapper.detect_events(auto_mapped)

    temp_df = mapper.apply_mapping(df.copy(), auto_mapped)
    event_configs = [EventConfig(**e) for e in auto_events]
    scorer_tmp = TextScorer(ScoringParams(events=event_configs))
    mode = scorer_tmp.detect_mode(temp_df)

    session_id = str(uuid_mod.uuid4())
    with _session_lock:
        SESSION_STORE[session_id] = {
            "tenant_id": current_user.tenant.id,
            "df_original": df,
            "df_mapped": None,
            "columns_detected": columns_detected,
            "auto_mapped": auto_mapped,
            "mapping": auto_mapped.copy(),
            "events": auto_events,
            "mode": mode,
            "scoring_result": None,
            "params": None,
            "text_part_result": None,
        }
        _schedule_ttl_locked(session_id)

    logger.info("Demo scoring: session=%s, rows=%d, mode=%s", session_id, len(df), mode)

    return {
        "session_id": session_id,
        "columns_detected": columns_detected,
        "auto_mapped": auto_mapped,
        "unmapped": unmapped,
        "n_rows": len(df),
        "mode_detected": mode,
        "events_detected": auto_events,
    }


async def _run_mmp_demo(ds: dict, current_user: CurrentUser, db: AsyncSession):
    """Server-side MMP upload: read file from disk, parse, create session."""
    from mmp import _mmp_lock, _MMP_STORE, _schedule_cleanup
    from mmp_parser import validate_columns, parse_timestamps, compute_derived_fields
    from db_models import MmpSession

    filepath = ds["filepath"]
    filename = ds["filename"]

    if filename.endswith(".csv"):
        df = pd.read_csv(filepath, low_memory=False)
    else:
        df = pd.read_excel(filepath, engine="openpyxl")

    validation = validate_columns(df)
    if not validation["ok"]:
        raise HTTPException(400, f"Demo file missing columns: {validation['missing']}")

    df = parse_timestamps(df)
    df = compute_derived_fields(df)

    session_id = str(uuid_mod.uuid4())

    trackers = sorted(df["adjust_tracker"].dropna().unique().tolist())
    campaigns = sorted(df["adjust_campaign"].dropna().unique().tolist())
    countries = sorted(df["country"].dropna().unique().tolist())
    platforms = sorted(df["adjust_platform"].dropna().unique().tolist()) if "adjust_platform" in df.columns else []
    date_min = df["installed_at"].min()
    date_max = df["installed_at"].max()

    tracker_counts = df["adjust_tracker"].dropna().value_counts()
    tracker_stats = [
        {"tracker": t, "rows": int(tracker_counts.get(t, 0))}
        for t in trackers
    ]

    with _mmp_lock:
        _MMP_STORE[session_id] = {"df": df, "tenant_id": str(current_user.tenant.id)}
    _schedule_cleanup(session_id)

    # Persist to DB
    try:
        mmp_session = MmpSession(
            id=uuid_mod.UUID(session_id),
            tenant_id=current_user.tenant.id,
            created_by=current_user.user.id,
            status="uploaded",
            file_names=[filename],
            total_rows=len(df),
            date_range_min=date_min if pd.notna(date_min) else None,
            date_range_max=date_max if pd.notna(date_max) else None,
            trackers=trackers,
            campaigns=campaigns,
            countries=countries,
            platforms=platforms,
        )
        db.add(mmp_session)
        await db.commit()
    except Exception as e:
        logger.warning(f"Failed to persist demo MMP session: {e}")

    logger.info("Demo MMP: session=%s, rows=%d, trackers=%d", session_id, len(df), len(trackers))

    return {
        "session_id": session_id,
        "files": [{"filename": filename, "size_bytes": filepath.stat().st_size, "n_rows": len(df)}],
        "total_rows": len(df),
        "trackers": trackers,
        "tracker_stats": tracker_stats,
        "campaigns": campaigns,
        "countries": countries,
        "platforms": platforms,
        "date_range": {
            "min": str(date_min) if pd.notna(date_min) else None,
            "max": str(date_max) if pd.notna(date_max) else None,
        },
    }
