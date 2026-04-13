import uuid as uuid_mod
import logging
from datetime import datetime, date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from auth import get_current_user, CurrentUser
from users import log_audit
from insight_generator import generate_insights, generate_hypotheses
from database import get_db
from db_models import Banner
from creative_history_models import (
    CreativePlacement,
    Hypothesis,
    CreativeInsight,
    PlacementCreate,
    PlacementRecord,
    PlacementListResponse,
    HypothesisCreate,
    HypothesisUpdate,
    HypothesisRecord,
    HypothesisListResponse,
    InsightRecord,
    InsightListResponse,
)

logger = logging.getLogger(__name__)

creative_history_router = APIRouter(prefix="/adscore", tags=["creative_history"])


# ---------------------------------------------------------------------------
# Helper converters
# ---------------------------------------------------------------------------

def _placement_to_record(p: CreativePlacement) -> PlacementRecord:
    return PlacementRecord(
        id=str(p.id),
        creative_id=str(p.creative_id),
        platform=p.platform,
        campaign=p.campaign,
        ad_group=p.ad_group,
        geo=p.geo,
        period_start=p.period_start.isoformat() if isinstance(p.period_start, (datetime, date)) else str(p.period_start) if p.period_start else None,
        period_end=p.period_end.isoformat() if isinstance(p.period_end, (datetime, date)) else str(p.period_end) if p.period_end else None,
        metrics=p.metrics or {},
        verdict=p.verdict,
        decision_score=float(p.decision_score) if p.decision_score is not None else None,
        fatigue_score=float(p.fatigue_score) if p.fatigue_score is not None else None,
        source=p.source,
        audience_segment=p.audience_segment,
        created_at=p.created_at.isoformat() if isinstance(p.created_at, datetime) else str(p.created_at) if p.created_at else None,
    )


def _hypothesis_to_record(h: Hypothesis) -> HypothesisRecord:
    return HypothesisRecord(
        id=str(h.id),
        title=h.title,
        description=h.description,
        hypothesis_type=h.hypothesis_type,
        status=h.status,
        confidence=float(h.confidence) if h.confidence is not None else None,
        impact_score=float(h.impact_score) if h.impact_score is not None else None,
        supporting_data=h.supporting_data or {},
        validation_result=h.validation_result or {},
        tags=h.tags or [],
        source=h.source,
        project=h.project,
        created_at=h.created_at.isoformat() if isinstance(h.created_at, datetime) else str(h.created_at) if h.created_at else None,
        updated_at=h.updated_at.isoformat() if isinstance(h.updated_at, datetime) else str(h.updated_at) if h.updated_at else None,
    )


def _insight_to_record(i: CreativeInsight) -> InsightRecord:
    return InsightRecord(
        id=str(i.id),
        insight_type=i.insight_type,
        severity=i.severity,
        title=i.title,
        description=i.description,
        action_text=i.action_text,
        supporting_data=i.supporting_data or {},
        creative_ids=[str(c) for c in i.creative_ids] if i.creative_ids else [],
        hypothesis_id=str(i.hypothesis_id) if i.hypothesis_id else None,
        is_read=i.is_read,
        is_dismissed=i.is_dismissed,
        project=i.project,
        created_at=i.created_at.isoformat() if isinstance(i.created_at, datetime) else str(i.created_at) if i.created_at else None,
    )


# ---------------------------------------------------------------------------
# Placement endpoints
# ---------------------------------------------------------------------------

@creative_history_router.post("/banner/{banner_id}/placements", response_model=PlacementRecord)
async def create_placement(
    banner_id: str,
    body: PlacementCreate,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id

    # Parse date strings to date objects for DB Date column
    p_start = date.fromisoformat(body.period_start) if isinstance(body.period_start, str) else body.period_start
    p_end = date.fromisoformat(body.period_end) if isinstance(body.period_end, str) else body.period_end

    placement = CreativePlacement(
        id=uuid_mod.uuid4(),
        tenant_id=tid,
        creative_id=uuid_mod.UUID(banner_id),
        platform=body.platform,
        campaign=body.campaign,
        ad_group=body.ad_group,
        geo=body.geo,
        period_start=p_start,
        period_end=p_end,
        metrics=body.metrics,
        verdict=body.verdict,
        decision_score=body.decision_score,
        fatigue_score=body.fatigue_score,
        source=body.source,
        audience_segment=body.audience_segment,
        created_at=datetime.utcnow(),
    )

    db.add(placement)
    await db.commit()
    await db.refresh(placement)

    logger.info("Created placement %s for banner %s (tenant %s)", placement.id, banner_id, tid)
    return _placement_to_record(placement)


@creative_history_router.get("/banner/{banner_id}/placements", response_model=PlacementListResponse)
async def list_placements(
    banner_id: str,
    sort_by: str = "period_end",
    sort_order: str = "desc",
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id

    stmt = (
        select(CreativePlacement)
        .where(CreativePlacement.tenant_id == tid)
        .where(CreativePlacement.creative_id == uuid_mod.UUID(banner_id))
    )

    order_col = getattr(CreativePlacement, sort_by, CreativePlacement.period_end)
    if sort_order == "desc":
        stmt = stmt.order_by(order_col.desc())
    else:
        stmt = stmt.order_by(order_col.asc())

    result = await db.execute(stmt)
    placements = result.scalars().all()

    return PlacementListResponse(
        placements=[_placement_to_record(p) for p in placements],
        total=len(placements),
    )


@creative_history_router.post("/banner/{banner_id}/placements/import-csv")
async def import_placements_csv(
    banner_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    body: dict = None,
):
    """Import placements from CSV data. Accepts JSON body with {rows: [...]}."""
    from fastapi import Body
    tid = current_user.tenant.id

    # Verify banner exists
    stmt = select(Banner).where(Banner.id == uuid_mod.UUID(banner_id), Banner.tenant_id == tid)
    result = await db.execute(stmt)
    banner = result.scalar_one_or_none()
    if not banner:
        raise HTTPException(404, "Banner not found")

    if not body or "rows" not in body:
        raise HTTPException(400, "Expected JSON body with 'rows' array")

    rows = body["rows"]
    if not rows:
        raise HTTPException(400, "No rows to import")

    created = 0
    errors = []
    METRIC_FIELDS = {"impressions", "clicks", "spend", "installs", "revenue", "ctr", "cr_install", "cr_event"}
    RATE_FIELDS = {"ctr", "cr_install", "cr_event"}

    for i, row in enumerate(rows):
        try:
            period_start = row.get("period_start") or row.get("date_from") or row.get("start")
            period_end = row.get("period_end") or row.get("date_to") or row.get("end")
            if not period_start or not period_end:
                errors.append(f"Row {i + 1}: missing period_start/period_end")
                continue

            metrics = {}
            for key in METRIC_FIELDS:
                val = row.get(key)
                if val is not None and val != "":
                    try:
                        fval = float(val)
                        if key in RATE_FIELDS and fval > 1:
                            fval = fval / 100.0
                        metrics[key] = fval
                    except (ValueError, TypeError):
                        pass

            # Recompute CTR from clicks/impressions if both present
            if "clicks" in metrics and "impressions" in metrics and metrics["impressions"] > 0:
                metrics["ctr"] = metrics["clicks"] / metrics["impressions"]

            placement = CreativePlacement(
                id=uuid_mod.uuid4(),
                tenant_id=tid,
                creative_id=uuid_mod.UUID(banner_id),
                platform=row.get("platform") or None,
                campaign=row.get("campaign") or None,
                ad_group=row.get("ad_group") or None,
                geo=row.get("geo") or None,
                audience_segment=row.get("audience_segment") or None,
                period_start=date.fromisoformat(str(period_start)),
                period_end=date.fromisoformat(str(period_end)),
                metrics=metrics,
                verdict=row.get("verdict") or None,
                source="csv_import",
                created_at=datetime.utcnow(),
            )
            db.add(placement)
            created += 1
        except Exception as e:
            errors.append(f"Row {i + 1}: {str(e)}")

    if created > 0:
        await db.commit()

    logger.info("CSV import: %d created, %d errors for banner %s", created, len(errors), banner_id)
    return {"created": created, "errors": errors, "total_rows": len(rows)}


@creative_history_router.get("/placements/compare")
async def compare_placements(
    period_a_start: str,
    period_a_end: str,
    period_b_start: str,
    period_b_end: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id

    # Parse date strings to date objects
    pa_start = date.fromisoformat(period_a_start)
    pa_end = date.fromisoformat(period_a_end)
    pb_start = date.fromisoformat(period_b_start)
    pb_end = date.fromisoformat(period_b_end)

    # Fetch placements overlapping period A (not strict containment)
    stmt_a = (
        select(CreativePlacement)
        .where(CreativePlacement.tenant_id == tid)
        .where(CreativePlacement.period_start <= pa_end)
        .where(CreativePlacement.period_end >= pa_start)
    )
    result_a = await db.execute(stmt_a)
    placements_a = result_a.scalars().all()

    # Fetch placements overlapping period B
    stmt_b = (
        select(CreativePlacement)
        .where(CreativePlacement.tenant_id == tid)
        .where(CreativePlacement.period_start <= pb_end)
        .where(CreativePlacement.period_end >= pb_start)
    )
    result_b = await db.execute(stmt_b)
    placements_b = result_b.scalars().all()

    # Aggregate metrics per creative for each period
    def _aggregate(placements):
        agg = {}
        for p in placements:
            cid = str(p.creative_id)
            if cid not in agg:
                agg[cid] = {
                    "creative_id": cid,
                    "impressions": 0,
                    "clicks": 0,
                    "conversions": 0,
                    "spend": 0.0,
                }
            m = p.metrics or {}
            agg[cid]["impressions"] += m.get("impressions", 0) or 0
            agg[cid]["clicks"] += m.get("clicks", 0) or 0
            agg[cid]["conversions"] += m.get("conversions", 0) or 0
            agg[cid]["spend"] += float(m.get("spend", 0) or 0)
        # Compute derived metrics
        for entry in agg.values():
            imp = entry["impressions"]
            entry["ctr"] = round(entry["clicks"] / imp, 6) if imp > 0 else 0.0
            entry["cr"] = round(entry["conversions"] / imp, 6) if imp > 0 else 0.0
        return list(agg.values())

    agg_a = _aggregate(placements_a)
    agg_b = _aggregate(placements_b)

    # Compute deltas
    map_a = {e["creative_id"]: e for e in agg_a}
    map_b = {e["creative_id"]: e for e in agg_b}
    all_ids = set(map_a.keys()) | set(map_b.keys())

    deltas = []
    for cid in all_ids:
        a = map_a.get(cid, {"impressions": 0, "clicks": 0, "conversions": 0, "spend": 0.0, "ctr": 0.0, "cr": 0.0})
        b = map_b.get(cid, {"impressions": 0, "clicks": 0, "conversions": 0, "spend": 0.0, "ctr": 0.0, "cr": 0.0})
        deltas.append({
            "creative_id": cid,
            "impressions_delta": b["impressions"] - a["impressions"],
            "clicks_delta": b["clicks"] - a["clicks"],
            "conversions_delta": b["conversions"] - a["conversions"],
            "spend_delta": round(b["spend"] - a["spend"], 2),
            "ctr_delta": round(b["ctr"] - a["ctr"], 6),
            "cr_delta": round(b["cr"] - a["cr"], 6),
        })

    return {
        "period_a": agg_a,
        "period_b": agg_b,
        "deltas": deltas,
    }


# ---------------------------------------------------------------------------
# Hypothesis endpoints
# ---------------------------------------------------------------------------

@creative_history_router.get("/hypotheses", response_model=HypothesisListResponse)
async def list_hypotheses(
    status: Optional[str] = None,
    hypothesis_type: Optional[str] = None,
    project: Optional[str] = None,
    sort_by: str = "created_at",
    sort_order: str = "desc",
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id

    stmt = select(Hypothesis).where(Hypothesis.tenant_id == tid)

    if status is not None:
        stmt = stmt.where(Hypothesis.status == status)
    if hypothesis_type is not None:
        stmt = stmt.where(Hypothesis.hypothesis_type == hypothesis_type)
    if project is not None:
        if project == "__none__":
            stmt = stmt.where(Hypothesis.project.is_(None))
        else:
            stmt = stmt.where(Hypothesis.project == project)

    order_col = getattr(Hypothesis, sort_by, Hypothesis.created_at)
    if sort_order == "desc":
        stmt = stmt.order_by(order_col.desc())
    else:
        stmt = stmt.order_by(order_col.asc())

    result = await db.execute(stmt)
    hypotheses = result.scalars().all()

    return HypothesisListResponse(
        hypotheses=[_hypothesis_to_record(h) for h in hypotheses],
        total=len(hypotheses),
    )


@creative_history_router.post("/hypotheses", response_model=HypothesisRecord)
async def create_hypothesis(
    body: HypothesisCreate,
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id

    hypothesis = Hypothesis(
        id=uuid_mod.uuid4(),
        tenant_id=tid,
        user_id=current_user.user.id,
        title=body.title,
        description=body.description,
        hypothesis_type=body.hypothesis_type,
        status="proposed",
        confidence=body.confidence,
        impact_score=body.impact_score,
        supporting_data=body.supporting_data,
        tags=body.tags,
        source=body.source,
        project=body.project,
        created_at=datetime.utcnow(),
    )

    db.add(hypothesis)
    await log_audit(db, tid, current_user.user.id, "create_hypothesis", request,
                    resource_type="hypothesis", resource_id=str(hypothesis.id),
                    details={"title": body.title})
    await db.commit()
    await db.refresh(hypothesis)

    logger.info("Created hypothesis %s (tenant %s)", hypothesis.id, tid)
    return _hypothesis_to_record(hypothesis)


@creative_history_router.patch("/hypotheses/{hypothesis_id}", response_model=HypothesisRecord)
async def update_hypothesis(
    hypothesis_id: str,
    body: HypothesisUpdate,
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id

    stmt = (
        select(Hypothesis)
        .where(Hypothesis.id == uuid_mod.UUID(hypothesis_id))
        .where(Hypothesis.tenant_id == tid)
    )
    result = await db.execute(stmt)
    hypothesis = result.scalar_one_or_none()

    if hypothesis is None:
        raise HTTPException(status_code=404, detail="Hypothesis not found")

    update_data = body.model_dump(exclude_none=True)

    old_status = hypothesis.status
    for field, value in update_data.items():
        setattr(hypothesis, field, value)

    # If status changed, update the timestamp
    if "status" in update_data and update_data["status"] != old_status:
        hypothesis.updated_at = datetime.utcnow()

    # Flag JSONB fields as modified so SQLAlchemy tracks the change
    if "supporting_data" in update_data:
        flag_modified(hypothesis, "supporting_data")
    if "validation_result" in update_data:
        flag_modified(hypothesis, "validation_result")

    hypothesis.updated_at = datetime.utcnow()

    await log_audit(db, tid, current_user.user.id, "update_hypothesis", request,
                    resource_type="hypothesis", resource_id=hypothesis_id,
                    details={"fields": list(update_data.keys())})
    await db.commit()
    await db.refresh(hypothesis)

    logger.info("Updated hypothesis %s (tenant %s)", hypothesis_id, tid)
    return _hypothesis_to_record(hypothesis)


@creative_history_router.delete("/hypotheses/{hypothesis_id}")
async def delete_hypothesis(
    hypothesis_id: str,
    request: Request,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id
    stmt = (
        select(Hypothesis)
        .where(Hypothesis.id == uuid_mod.UUID(hypothesis_id))
        .where(Hypothesis.tenant_id == tid)
    )
    result = await db.execute(stmt)
    hypothesis = result.scalar_one_or_none()
    if hypothesis is None:
        raise HTTPException(status_code=404, detail="Hypothesis not found")
    await db.delete(hypothesis)
    await log_audit(db, tid, current_user.user.id, "delete_hypothesis", request,
                    resource_type="hypothesis", resource_id=hypothesis_id,
                    details={"title": hypothesis.title})
    await db.commit()
    logger.info("Deleted hypothesis %s (tenant %s)", hypothesis_id, tid)
    return {"deleted": True}


@creative_history_router.delete("/hypotheses")
async def delete_all_hypotheses(
    project: Optional[str] = None,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id
    from sqlalchemy import delete as sa_delete
    stmt = sa_delete(Hypothesis).where(Hypothesis.tenant_id == tid)
    if project:
        stmt = stmt.where(Hypothesis.project == project)
    result = await db.execute(stmt)
    await db.commit()
    count = result.rowcount
    logger.info("Deleted %d hypotheses (tenant %s, project=%s)", count, tid, project)
    return {"deleted": count}


# ---------------------------------------------------------------------------
# Insight endpoints
# ---------------------------------------------------------------------------

@creative_history_router.get("/insights/creative", response_model=InsightListResponse)
async def list_creative_insights(
    insight_type: Optional[str] = None,
    severity: Optional[str] = None,
    project: Optional[str] = None,
    include_dismissed: bool = False,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id

    stmt = select(CreativeInsight).where(CreativeInsight.tenant_id == tid)

    if insight_type is not None:
        stmt = stmt.where(CreativeInsight.insight_type == insight_type)
    if severity is not None:
        stmt = stmt.where(CreativeInsight.severity == severity)
    if project is not None:
        if project == "__none__":
            stmt = stmt.where(CreativeInsight.project.is_(None))
        else:
            stmt = stmt.where(CreativeInsight.project == project)
    if not include_dismissed:
        stmt = stmt.where(CreativeInsight.is_dismissed == False)  # noqa: E712

    stmt = stmt.order_by(CreativeInsight.created_at.desc())

    result = await db.execute(stmt)
    insights = result.scalars().all()

    return InsightListResponse(
        insights=[_insight_to_record(i) for i in insights],
        total=len(insights),
    )


@creative_history_router.patch("/insights/creative/{insight_id}", response_model=InsightRecord)
async def update_insight(
    insight_id: str,
    body: dict,
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id

    stmt = (
        select(CreativeInsight)
        .where(CreativeInsight.id == uuid_mod.UUID(insight_id))
        .where(CreativeInsight.tenant_id == tid)
    )
    result = await db.execute(stmt)
    insight = result.scalar_one_or_none()

    if insight is None:
        raise HTTPException(status_code=404, detail="Insight not found")

    if "is_read" in body:
        insight.is_read = body["is_read"]
    if "is_dismissed" in body:
        insight.is_dismissed = body["is_dismissed"]

    await db.commit()
    await db.refresh(insight)

    logger.info("Updated insight %s (tenant %s)", insight_id, tid)
    return _insight_to_record(insight)


# ---------------------------------------------------------------------------
# Generation endpoints
# ---------------------------------------------------------------------------

@creative_history_router.post("/insights/generate")
async def generate_creative_insights(
    body: dict = {},
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id
    project = body.get("project") if isinstance(body, dict) else None

    # Clear old unread & non-dismissed insights before regenerating
    # Preserve insights the user has interacted with (read or dismissed)
    from sqlalchemy import delete as sa_delete, and_, or_
    del_stmt = (
        sa_delete(CreativeInsight)
        .where(CreativeInsight.tenant_id == tid)
        .where(CreativeInsight.is_read == False)   # noqa: E712
        .where(CreativeInsight.is_dismissed == False)  # noqa: E712
    )
    if project:
        del_stmt = del_stmt.where(CreativeInsight.project == project)
    await db.execute(del_stmt)
    await db.flush()

    new_insights = await generate_insights(db, tid, project=project)
    # Stamp project on generated insights
    if project:
        for ins in new_insights:
            ins.project = project
    if new_insights:
        db.add_all(new_insights)
        await db.commit()

    # Count total active
    stmt = (
        select(func.count(CreativeInsight.id))
        .where(CreativeInsight.tenant_id == tid)
        .where(CreativeInsight.is_dismissed == False)  # noqa: E712
    )
    result = await db.execute(stmt)
    total = result.scalar() or 0

    logger.info("Generated %d insights for tenant %s (total active: %d)", len(new_insights), tid, total)
    return {"generated": len(new_insights), "total": total}


@creative_history_router.post("/hypotheses/generate")
async def generate_creative_hypotheses(
    body: dict = {},
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    tid = current_user.tenant.id
    uid = current_user.user.id
    project = body.get("project") if isinstance(body, dict) else None
    new_hypotheses = await generate_hypotheses(db, tid, uid, project=project)
    # Stamp project on generated hypotheses
    if project:
        for hyp in new_hypotheses:
            hyp.project = project
    if new_hypotheses:
        db.add_all(new_hypotheses)
        await db.commit()

    # Count total
    stmt = (
        select(func.count(Hypothesis.id))
        .where(Hypothesis.tenant_id == tid)
    )
    result = await db.execute(stmt)
    total = result.scalar() or 0

    # Diagnostic counts
    banner_count_stmt = select(func.count(Banner.id)).where(Banner.tenant_id == tid)
    banner_count = (await db.execute(banner_count_stmt)).scalar() or 0

    tagged_count_stmt = select(func.count(Banner.id)).where(Banner.tenant_id == tid, Banner.tags_status == 'done')
    tagged_count = (await db.execute(tagged_count_stmt)).scalar() or 0

    placement_count_stmt = select(func.count(CreativePlacement.id)).where(CreativePlacement.tenant_id == tid)
    placement_count = (await db.execute(placement_count_stmt)).scalar() or 0

    logger.info("Generated %d hypotheses for tenant %s (total: %d)", len(new_hypotheses), tid, total)
    return {
        "generated": len(new_hypotheses),
        "total": total,
        "diagnostics": {
            "banners": banner_count,
            "tagged": tagged_count,
            "placements": placement_count,
        }
    }
