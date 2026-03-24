"""
Demo data endpoints — serves test datasets for users with the demo_data feature.
"""

import os
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse

from auth import require_feature, CurrentUser

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
    dataset = next((ds for ds in DATASETS if ds["id"] == dataset_id), None)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    filepath = DEMO_DIR / dataset["filename"]
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="Dataset file missing on server")

    return FileResponse(
        path=str(filepath),
        filename=dataset["filename"],
        media_type="application/octet-stream",
    )
