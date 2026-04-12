"""
MMP (Mobile Measurement Partner) data analysis router.
Supports Adjust and AppsFlyer CSV exports with auto-detection.
"""

import csv
import logging
import threading
import uuid as uuid_mod
from datetime import datetime, timezone
from io import BytesIO, StringIO
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user, CurrentUser, require_feature
from database import get_db
from db_models import MmpSession
from mmp_parser import (
    validate_columns, parse_timestamps, compute_derived_fields,
    detect_mmp_type, normalise_columns, REQUIRED_COLUMNS,
)
from mmp_fraud import run_fraud_analysis

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/mmp", tags=["mmp"])

# In-memory store (same pattern as main.py SESSION_STORE)
_MMP_STORE: dict = {}
_mmp_lock = threading.Lock()
MMP_TTL_MINUTES = 60


def _schedule_cleanup(session_id: str):
    def cleanup():
        with _mmp_lock:
            _MMP_STORE.pop(session_id, None)
        logger.info(f"MMP session {session_id} cleaned up (TTL)")
    threading.Timer(MMP_TTL_MINUTES * 60, cleanup).start()


@router.post("/upload")
async def upload_mmp_files(
    files: List[UploadFile] = File(...),
    current_user: CurrentUser = Depends(require_feature("mmp")),
    db: AsyncSession = Depends(get_db),
):
    """Upload one or more MMP CSV files (Adjust or AppsFlyer) for fraud analysis."""
    if not files:
        raise HTTPException(400, "No files provided")

    raw_contents = []
    file_infos = []

    for f in files:
        fname_lower = f.filename.lower()
        if not (fname_lower.endswith(".csv") or fname_lower.endswith(".xlsx") or fname_lower.endswith(".xls")):
            raise HTTPException(400, f"File {f.filename} is not CSV or Excel")
        content = await f.read()
        if len(content) > 200 * 1024 * 1024:
            raise HTTPException(400, f"File {f.filename} exceeds 200MB limit")
        raw_contents.append(content)
        file_infos.append({"filename": f.filename, "size_bytes": len(content)})

    # Parse and validate each file
    dfs = []
    detected_mmp = None
    for i, content in enumerate(raw_contents):
        try:
            fname_lower = file_infos[i]["filename"].lower()
            if fname_lower.endswith(".xlsx") or fname_lower.endswith(".xls"):
                df = pd.read_excel(BytesIO(content), engine="openpyxl")
            else:
                df = pd.read_csv(BytesIO(content), low_memory=False)
        except Exception as e:
            raise HTTPException(400, f"Failed to parse {file_infos[i]['filename']}: {str(e)}")

        # Auto-detect MMP type from first file
        if i == 0:
            detected_mmp = detect_mmp_type(set(df.columns))
            if not detected_mmp:
                raise HTTPException(400, {
                    "error": "Не удалось определить тип MMP. Поддерживаются Adjust и AppsFlyer.",
                    "detected_columns": sorted(df.columns.tolist())[:20],
                })
            logger.info(f"Auto-detected MMP type: {detected_mmp}")

        validation = validate_columns(df, mmp_type=detected_mmp)
        if not validation["ok"]:
            raise HTTPException(400, {
                "file": file_infos[i]["filename"],
                "mmp_type": detected_mmp,
                "missing_columns": validation["missing"],
            })
        file_infos[i]["n_rows"] = len(df)
        dfs.append(df)

    # Check column consistency across files
    ref_cols = set(dfs[0].columns)
    for i, df in enumerate(dfs[1:], 1):
        diff = set(df.columns).symmetric_difference(ref_cols)
        if diff:
            raise HTTPException(400, f"File {file_infos[i]['filename']} has different columns: {sorted(diff)}")

    # Merge, normalise columns, parse timestamps, compute derived fields
    merged = pd.concat(dfs, ignore_index=True)
    merged = normalise_columns(merged, detected_mmp)
    merged = parse_timestamps(merged)
    merged = compute_derived_fields(merged)

    session_id = str(uuid_mod.uuid4())

    # Extract metadata — use adjust_tracker/adjust_campaign (set by normalise_columns for compat)
    tracker_col = "adjust_tracker" if "adjust_tracker" in merged.columns else "tracker"
    campaign_col = "adjust_campaign" if "adjust_campaign" in merged.columns else "campaign"
    trackers = sorted(merged[tracker_col].dropna().unique().tolist()) if tracker_col in merged.columns else []
    campaigns = sorted(merged[campaign_col].dropna().unique().tolist()) if campaign_col in merged.columns else []
    countries = sorted(merged["country"].dropna().unique().tolist()) if "country" in merged.columns else []
    platform_col = "adjust_platform" if "adjust_platform" in merged.columns else "platform"
    platforms = sorted(merged[platform_col].dropna().unique().tolist()) if platform_col in merged.columns else []
    date_min = merged["installed_at"].min() if "installed_at" in merged.columns else None
    date_max = merged["installed_at"].max() if "installed_at" in merged.columns else None

    # Per-tracker row counts for Smart Benchmark recommendation
    tracker_counts = merged[tracker_col].dropna().value_counts() if tracker_col in merged.columns else pd.Series(dtype=int)
    tracker_stats = [
        {"tracker": t, "rows": int(tracker_counts.get(t, 0))}
        for t in trackers
    ]

    # Store in memory with TTL
    with _mmp_lock:
        _MMP_STORE[session_id] = {"df": merged, "tenant_id": str(current_user.tenant.id), "mmp_type": detected_mmp}
    _schedule_cleanup(session_id)

    # Persist to DB
    try:
        mmp_session = MmpSession(
            id=uuid_mod.UUID(session_id),
            tenant_id=current_user.tenant.id,
            created_by=current_user.user.id,
            status="uploaded",
            mmp_type=detected_mmp,
            file_names=[fi["filename"] for fi in file_infos],
            total_rows=len(merged),
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
        logger.warning(f"Failed to persist MMP session to DB: {e}")

    logger.info(f"MMP upload: session={session_id}, type={detected_mmp}, files={len(file_infos)}, rows={len(merged)}")

    MMP_LABELS = {"adjust": "Adjust", "appsflyer": "AppsFlyer", "singular": "Singular", "branch": "Branch"}

    return {
        "session_id": session_id,
        "mmp_type": detected_mmp,
        "mmp_label": MMP_LABELS.get(detected_mmp, detected_mmp),
        "files": file_infos,
        "total_rows": len(merged),
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


@router.post("/analyze")
async def analyze_mmp_data(
    request: dict,
    current_user: CurrentUser = Depends(require_feature("mmp")),
    db: AsyncSession = Depends(get_db),
):
    """Run fraud analysis on uploaded MMP data."""
    session_id = request.get("session_id")
    benchmark_trackers = request.get("benchmark_trackers", [])
    hourly_benchmark_trackers = request.get("hourly_benchmark_trackers") or None
    thresholds = request.get("thresholds")

    if not session_id or not benchmark_trackers:
        raise HTTPException(400, "session_id and benchmark_trackers required")

    with _mmp_lock:
        session = _MMP_STORE.get(session_id)
    if not session:
        raise HTTPException(404, "Session not found or expired. Re-upload files.")

    df = session["df"]

    # Convert thresholds from UI percentages to ratios for backend
    # Frontend sends: ctit_lt15s_pct=1 (meaning 1%), backend expects 0.01
    if thresholds:
        pct_to_ratio_keys = ["ctit_lt15s_pct", "ctit_gt5h_pct", "vta_pct", "dup_device_pct"]
        converted = {}
        for k, v in thresholds.items():
            if k in pct_to_ratio_keys and v is not None:
                converted[k] = v / 100.0
            else:
                converted[k] = v
        thresholds = converted

    result = run_fraud_analysis(df, benchmark_trackers, thresholds, hourly_benchmark_trackers)

    # Store analysis in memory
    with _mmp_lock:
        if session_id in _MMP_STORE:
            _MMP_STORE[session_id]["analysis"] = result

    # Persist to DB
    try:
        stmt = select(MmpSession).where(MmpSession.id == uuid_mod.UUID(session_id))
        db_session = (await db.execute(stmt)).scalar_one_or_none()
        if db_session:
            db_session.status = "completed"
            db_session.benchmark_trackers = benchmark_trackers
            db_session.thresholds = thresholds
            db_session.analysis_result = result
            db_session.completed_at = datetime.now(timezone.utc)
            await db.commit()
    except Exception as e:
        logger.warning(f"Failed to persist MMP analysis to DB: {e}")

    logger.info(f"MMP analysis: session={session_id}, trackers={len(result.get('tracker_passports', []))}")
    return result


@router.get("/trackers/{session_id}")
async def get_tracker_passports(
    session_id: str,
    current_user: CurrentUser = Depends(require_feature("mmp")),
):
    """Get tracker passport cards with risk levels and markers."""
    with _mmp_lock:
        session = _MMP_STORE.get(session_id)
    if not session or "analysis" not in session:
        raise HTTPException(404, "Analysis not found. Run /mmp/analyze first.")
    a = session["analysis"]
    return {
        "tracker_passports": a["tracker_passports"],
        "fraud_summary": a["fraud_summary"],
        "tracker_aggregates": a["tracker_aggregates"],
        "benchmark": a["benchmark"],
        "ctit_distributions": a.get("ctit_distributions", {}),
        "vtit_distributions": a.get("vtit_distributions", {}),
    }


@router.get("/workbench/{session_id}")
async def get_workbench_data(
    session_id: str,
    current_user: CurrentUser = Depends(require_feature("mmp")),
):
    """Get all aggregated data for Workbench tools."""
    with _mmp_lock:
        session = _MMP_STORE.get(session_id)
    if not session or "analysis" not in session:
        raise HTTPException(404, "Analysis not found.")
    a = session["analysis"]
    return {
        "tracker_aggregates": a["tracker_aggregates"],
        "tracker_passports": a["tracker_passports"],
        "hourly_installs": a["hourly_installs"],
        "hourly_clicks": a["hourly_clicks"],
        "hourly_profiles_pct": a.get("hourly_profiles_pct", {}),
        "hourly_deviations": a.get("hourly_deviations", {}),
        "critical_hours_map": a.get("critical_hours_map", {}),
        "daily_volumes": a["daily_volumes"],
        "ctit_distributions": a["ctit_distributions"],
        "vtit_distributions": a.get("vtit_distributions", {}),
        "fraud_summary": a["fraud_summary"],
        "benchmark": a["benchmark"],
        "multi_geo": a["multi_geo"],
    }


@router.get("/sessions")
async def list_mmp_sessions(
    current_user: CurrentUser = Depends(require_feature("mmp")),
    db: AsyncSession = Depends(get_db),
):
    """List past MMP analysis sessions for this tenant."""
    stmt = (
        select(MmpSession)
        .where(MmpSession.tenant_id == current_user.tenant.id)
        .order_by(MmpSession.created_at.desc())
        .limit(50)
    )
    result = await db.execute(stmt)
    sessions = result.scalars().all()
    return [
        {
            "id": str(s.id),
            "file_names": s.file_names,
            "total_rows": s.total_rows,
            "status": s.status,
            "trackers": s.trackers,
            "created_at": str(s.created_at),
        }
        for s in sessions
    ]


@router.get("/template")
async def download_mmp_template():
    """Download CSV template for Adjust data export."""
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(REQUIRED_COLUMNS)
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=adjust_template.csv"},
    )
