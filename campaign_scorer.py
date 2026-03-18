"""
Campaign-level aggregation and scoring.

Takes already-scored TextResult objects, aggregates by campaign,
recomputes metrics from raw sums, runs a full z-score pipeline
at the campaign level, and generates campaign verdicts + insights.
"""

import logging
from typing import Dict, List, Optional, Set

import numpy as np
import pandas as pd

from models import (
    CampaignAnalysisResult,
    CampaignInsight,
    CampaignResult,
    CampaignVerdict,
    EventConfig,
    ScoringParams,
    TextResult,
)
from scorer import TextScorer

logger = logging.getLogger(__name__)


def analyze_campaigns(
    results: List[TextResult],
    df_mapped: Optional[pd.DataFrame],
    params: ScoringParams,
    stats: Dict,
) -> CampaignAnalysisResult:
    """Main entry point for campaign-level analysis."""
    events = params.events or []
    event_labels = stats.get("event_labels", {})
    weights = stats.get("weights_used", dict(params.weights))

    # Group texts by campaign
    texts_by_campaign: Dict[str, List[TextResult]] = {}
    for r in results:
        if r.campaign:
            texts_by_campaign.setdefault(r.campaign, []).append(r)

    if len(texts_by_campaign) < 2:
        return CampaignAnalysisResult(campaigns=[], n_campaigns=0)

    # 1. Aggregate raw data per campaign
    agg_df = _aggregate_campaigns(df_mapped, texts_by_campaign, events)

    if agg_df is None or len(agg_df) < 2:
        return CampaignAnalysisResult(campaigns=[], n_campaigns=0)

    # 2. Compute campaign-level raw metrics
    metric_df = _compute_campaign_metrics(agg_df, events)

    # 3. Determine available metrics
    all_metrics = _get_available_metrics(metric_df, events)
    cost_metrics = _get_cost_metrics(events)

    # 4. Z-score pipeline (reusing TextScorer)
    scorer = TextScorer(params)

    if len(metric_df) >= 2 and len(all_metrics) >= 1:
        metric_df = scorer.winsorize(metric_df, all_metrics)
        metric_df = scorer.compute_zscores(metric_df, all_metrics)
        metric_df = scorer.compute_composite(metric_df, all_metrics, weights=weights)
        metric_df = scorer.assign_categories(metric_df)
    else:
        metric_df["composite_score"] = 0.5
        metric_df["category"] = "AVERAGE"
        metric_df["alt_category"] = "≈Средний"

    # 5. Campaign reliability threshold: max(500, min(median, 3000))
    # Кампании агрегируют клики многих текстов → порог выше чем у текстов, но с потолком
    campaign_clicks = metric_df["_clicks"].values
    median_clicks = int(np.median(campaign_clicks)) if len(campaign_clicks) > 0 else 500
    reliability_threshold = max(500, min(median_clicks, 3000))

    # 6. Build CampaignResult objects
    campaigns: List[CampaignResult] = []

    for _, row in metric_df.iterrows():
        camp_name = str(row.get("campaign", ""))
        camp_texts = texts_by_campaign.get(camp_name, [])

        # Impression-weighted avg text score
        avg_text_score = _impression_weighted_avg(camp_texts)

        # Text verdict distribution
        vd: Dict[str, int] = {}
        for t in camp_texts:
            v = t.verdict.verdict if t.verdict else "Нет"
            vd[v] = vd.get(v, 0) + 1

        # Budget waste: % impressions on "Исключить" texts
        total_imp = sum(t.n_impressions for t in camp_texts)
        exclude_imp = sum(
            t.n_impressions for t in camp_texts
            if t.verdict and t.verdict.verdict == "Исключить"
        )
        waste_pct = (exclude_imp / total_imp * 100) if total_imp > 0 else 0.0

        # Score spread
        text_scores = [t.composite_score for t in camp_texts]
        spread = float(np.std(text_scores, ddof=1)) if len(text_scores) > 1 else 0.0

        # Best/worst text
        best_t = max(camp_texts, key=lambda t: t.composite_score)
        worst_t = min(camp_texts, key=lambda t: t.composite_score)

        # Extract metrics and z-scores
        camp_metrics: Dict[str, Optional[float]] = {}
        camp_z: Dict[str, Optional[float]] = {}
        for m in all_metrics:
            if m in row.index and pd.notna(row[m]):
                camp_metrics[m] = round(float(row[m]), 6)
            z_col = f"z_{m}"
            if z_col in row.index and pd.notna(row[z_col]):
                camp_z[m] = round(float(row[z_col]), 4)

        # Event totals
        event_totals: Dict[str, int] = {}
        for ev in events:
            col = ev.slot
            if col in row.index and pd.notna(row[col]):
                event_totals[ev.slot] = int(row[col])

        n_clicks = int(row.get("_clicks", 0))
        is_reliable = n_clicks >= reliability_threshold
        warnings: List[str] = []
        if not is_reliable:
            warnings.append("insufficient_campaign_data")

        # Financial impact aggregation from text-level results (v2.1)
        camp_excess_cost = sum(t.excess_cost or 0 for t in camp_texts)
        camp_real_savings = sum(t.real_savings or 0 for t in camp_texts)
        camp_real_savings_adj = sum(t.real_savings_adjusted or 0 for t in camp_texts)

        cr = CampaignResult(
            campaign=camp_name,
            n_texts=len(camp_texts),
            n_texts_scored=len(camp_texts),
            total_impressions=int(row.get("_impressions", 0)),
            total_clicks=n_clicks,
            total_spend=round(float(row.get("_spend", 0)), 2),
            total_revenue=round(float(row.get("_revenue", 0)), 2),
            total_events=event_totals,
            metrics=camp_metrics,
            z_scores=camp_z,
            composite_score=round(float(row.get("composite_score", 0.5)), 4),
            avg_text_score=round(avg_text_score, 4),
            category=str(row.get("category", "AVERAGE")),
            alt_category=str(row.get("alt_category", "")),
            is_reliable=is_reliable,
            warnings=warnings,
            text_verdict_distribution=vd,
            budget_waste_pct=round(waste_pct, 1),
            score_spread=round(spread, 4),
            best_text_id=best_t.text_id,
            best_text_score=best_t.composite_score,
            worst_text_id=worst_t.text_id,
            worst_text_score=worst_t.composite_score,
            excess_cost=round(camp_excess_cost, 2) if camp_excess_cost > 0 else None,
            real_savings=round(camp_real_savings, 2) if camp_real_savings > 0 else None,
            real_savings_adjusted=round(camp_real_savings_adj, 2) if camp_real_savings_adj > 0 else None,
        )

        # Verdict
        cr.verdict = _campaign_verdict(cr, camp_z, event_labels, params)
        campaigns.append(cr)

    # Sort by composite_score descending
    campaigns.sort(key=lambda c: c.composite_score, reverse=True)

    # 7. Generate insights
    insights = _generate_campaign_insights(campaigns, event_labels)

    return CampaignAnalysisResult(
        campaigns=campaigns,
        insights=insights,
        n_campaigns=len(campaigns),
        overall_best_campaign=campaigns[0].campaign if campaigns else "",
        overall_worst_campaign=campaigns[-1].campaign if campaigns else "",
        reliability_threshold=reliability_threshold,
    )


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def _aggregate_campaigns(
    df_mapped: Optional[pd.DataFrame],
    texts_by_campaign: Dict[str, List[TextResult]],
    events: List[EventConfig],
) -> Optional[pd.DataFrame]:
    """Aggregate raw data per campaign from df_mapped or fallback to TextResult."""

    if df_mapped is not None and "campaign" in df_mapped.columns:
        # Use original raw data for precision
        df = df_mapped.copy()
        # Keep only rows whose text_id is in scored results
        scored_ids = set()
        for texts in texts_by_campaign.values():
            for t in texts:
                scored_ids.add(t.text_id)

        if "text_id" in df.columns:
            df["text_id"] = df["text_id"].astype(str)
            df = df[df["text_id"].isin(scored_ids)]

        # Numeric conversion
        num_cols = ["impressions", "clicks", "spend", "revenue", "installs"]
        for ev in events:
            num_cols.append(ev.slot)
        for c in num_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        # Group by campaign
        agg_dict = {"text_id": "count"}
        for c in num_cols:
            if c in df.columns:
                agg_dict[c] = "sum"

        grouped = df.groupby("campaign", as_index=False).agg(agg_dict)
        grouped = grouped.rename(columns={"text_id": "n_texts"})

        # Rename to internal columns
        rename_map = {
            "impressions": "_impressions",
            "clicks": "_clicks",
            "spend": "_spend",
            "revenue": "_revenue",
        }
        grouped = grouped.rename(columns={k: v for k, v in rename_map.items() if k in grouped.columns})

        return grouped

    # Fallback: reconstruct from TextResult metrics
    rows = []
    for camp_name, texts in texts_by_campaign.items():
        total_imp = sum(t.n_impressions for t in texts)
        total_clk = sum(t.n_clicks for t in texts)
        # Derive spend from CPC * clicks per text
        total_spend = sum(
            (t.metrics.get("CPC", 0) or 0) * t.n_clicks
            for t in texts
        )
        total_rev = sum(
            (t.metrics.get("RPC", 0) or 0) * t.n_clicks
            for t in texts
        )
        row_data: Dict = {
            "campaign": camp_name,
            "n_texts": len(texts),
            "_impressions": total_imp,
            "_clicks": total_clk,
            "_spend": total_spend,
            "_revenue": total_rev,
        }
        for ev in events:
            slot = ev.slot
            cr_key = f"CR_{slot}"
            total_ev = sum(
                (t.metrics.get(cr_key, 0) or 0) * t.n_clicks
                for t in texts
            )
            row_data[slot] = total_ev
        # Derive installs from CR_install * clicks per text
        total_installs = sum(
            (t.metrics.get("CR_install", 0) or 0) * t.n_clicks
            for t in texts
        )
        if total_installs > 0:
            row_data["installs"] = total_installs
        rows.append(row_data)

    return pd.DataFrame(rows)


def _compute_campaign_metrics(
    agg_df: pd.DataFrame,
    events: List[EventConfig],
) -> pd.DataFrame:
    """Compute raw metrics from aggregated campaign sums."""
    df = agg_df.copy()

    imp = df.get("_impressions", pd.Series(0, index=df.index))
    clk = df.get("_clicks", pd.Series(0, index=df.index))
    spend = df.get("_spend", pd.Series(0, index=df.index))
    rev = df.get("_revenue", pd.Series(0, index=df.index))

    # CTR
    df["CTR"] = np.where(imp > 0, clk / imp, np.nan)
    # CPC
    df["CPC"] = np.where(clk > 0, spend / clk, np.nan)
    # CPM
    df["CPM"] = np.where(imp > 0, spend / imp * 1000, np.nan)

    # Install metrics (auto-detected)
    installs = df.get("installs", pd.Series(0, index=df.index))
    df["CR_install"] = np.where(clk > 0, installs / clk, np.nan)
    df["CPI"] = np.where(installs > 0, spend / installs, np.nan)

    has_rev = rev > 0

    if events:
        for ev in events:
            slot = ev.slot
            if slot not in df.columns:
                continue
            ev_vals = pd.to_numeric(df[slot], errors="coerce").fillna(0)
            df[f"CR_{slot}"] = np.where(clk > 0, ev_vals / clk, np.nan)
            df[f"CPA_{slot}"] = np.where(ev_vals > 0, spend / ev_vals, np.nan)

        df["ROI"] = np.where((spend > 0) & has_rev, (rev - spend) / spend, np.nan)
        df["RPM"] = np.where((imp > 0) & has_rev, rev / imp * 1000, np.nan)
        df["RPC"] = np.where((clk > 0) & has_rev, rev / clk, np.nan)
    else:
        # No events — basic mode
        df["ROI"] = np.where((spend > 0) & has_rev, (rev - spend) / spend, np.nan)
        df["RPM"] = np.where((imp > 0) & has_rev, rev / imp * 1000, np.nan)
        df["RPC"] = np.where((clk > 0) & has_rev, rev / clk, np.nan)

    return df


def _get_available_metrics(df: pd.DataFrame, events: List[EventConfig]) -> List[str]:
    """Determine which metrics are available in the campaign DataFrame."""
    metrics = ["CTR"]
    has_spend = "_spend" in df.columns and (df["_spend"] > 0).any()
    has_rev = "_revenue" in df.columns and (df["_revenue"] > 0).any()

    if events:
        for ev in events:
            if f"CR_{ev.slot}" in df.columns:
                metrics.append(f"CR_{ev.slot}")
                if has_spend:
                    metrics.append(f"CPA_{ev.slot}")
    if has_rev:
        metrics.extend(["ROI", "RPM", "RPC"])
    if has_spend:
        metrics.extend(["CPC", "CPM"])

    # Install metrics
    has_installs = "installs" in df.columns and (df["installs"] > 0).any()
    if has_installs:
        metrics.append("CR_install")
        if has_spend:
            metrics.append("CPI")

    return [m for m in metrics if m in df.columns and df[m].notna().any()]


def _get_cost_metrics(events: List[EventConfig]) -> Set[str]:
    """Return set of cost metrics."""
    cost = {"CPC", "CPM", "CPI"}
    if events:
        for ev in events:
            cost.add(f"CPA_{ev.slot}")
    else:
        cost.add("CPA")
    return cost


def _impression_weighted_avg(texts: List[TextResult]) -> float:
    """Compute impression-weighted average of text composite scores."""
    total_imp = sum(t.n_impressions for t in texts)
    if total_imp == 0:
        return sum(t.composite_score for t in texts) / max(len(texts), 1)
    return sum(t.composite_score * t.n_impressions for t in texts) / total_imp


# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------

def _campaign_verdict(
    cr: CampaignResult,
    z_scores: Dict[str, Optional[float]],
    event_labels: Dict[str, str],
    params: ScoringParams = None,
) -> CampaignVerdict:
    """Assign verdict to a campaign based on composite score and z-scores."""
    scale_threshold = params.scale_threshold if params else 0.68
    exclude_threshold = params.exclude_threshold if params else 0.30
    strong_z = params.strong_z if params else 0.60
    weak_z = params.weak_z if params else 0.35

    # Strengths / weaknesses from z-scores
    strengths = [m for m, z in z_scores.items() if z is not None and z >= strong_z]
    weaknesses = [m for m, z in z_scores.items() if z is not None and z <= weak_z]

    score = cr.composite_score

    # Insufficient data
    if "insufficient_campaign_data" in cr.warnings:
        return CampaignVerdict(
            verdict="Мало данных",
            reason=f"Кампания набрала {cr.total_clicks} кликов — недостаточно для надёжной оценки",
            reason_type="объёмы",
            reason_detail=f"Текстов: {cr.n_texts}, кликов: {cr.total_clicks}",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # Scale — block if ROI is weak (prevents scaling money-losing campaigns)
    roi_z = z_scores.get("ROI")
    roi_ok = roi_z is None or roi_z >= weak_z
    if score >= scale_threshold and cr.budget_waste_pct < 20 and roi_ok:
        return CampaignVerdict(
            verdict="Масштабировать",
            reason=f"Лидер среди кампаний. Лучший текст: {cr.best_text_id} ({cr.best_text_score:.2f})",
            reason_type="эффективность",
            reason_detail=f"Средний балл текстов: {cr.avg_text_score:.2f}, потери: {cr.budget_waste_pct:.0f}%",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # Exclude
    if score <= exclude_threshold:
        return CampaignVerdict(
            verdict="Исключить",
            reason=f"Низкая эффективность. {cr.budget_waste_pct:.0f}% бюджета на слабые тексты",
            reason_type="эффективность",
            reason_detail=f"Худший текст: {cr.worst_text_id} ({cr.worst_text_score:.2f}), разброс: {cr.score_spread:.2f}",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # Optimize
    if cr.budget_waste_pct > 30 or cr.score_spread > 0.15 or len(weaknesses) > len(strengths):
        reasons = []
        if cr.budget_waste_pct > 30:
            reasons.append(f"{cr.budget_waste_pct:.0f}% бюджета на тексты «Исключить»")
        if cr.score_spread > 0.15:
            reasons.append(f"высокий разброс качества (σ={cr.score_spread:.2f})")
        if len(weaknesses) > len(strengths):
            reasons.append("больше слабых метрик чем сильных")
        return CampaignVerdict(
            verdict="Оптимизировать",
            reason=". ".join(reasons).capitalize() if reasons else "Есть потенциал улучшения",
            reason_type="качество",
            reason_detail=f"Лучший: {cr.best_text_id} ({cr.best_text_score:.2f}), худший: {cr.worst_text_id} ({cr.worst_text_score:.2f})",
            strengths=strengths,
            weaknesses=weaknesses,
        )

    # OK
    return CampaignVerdict(
        verdict="ОК",
        reason=f"Стабильная работа. Средний балл текстов: {cr.avg_text_score:.2f}",
        reason_type="эффективность",
        reason_detail=f"Потери: {cr.budget_waste_pct:.0f}%, разброс: {cr.score_spread:.2f}",
        strengths=strengths,
        weaknesses=weaknesses,
    )


# ---------------------------------------------------------------------------
# Insights
# ---------------------------------------------------------------------------

def _generate_campaign_insights(
    campaigns: List[CampaignResult],
    event_labels: Dict[str, str],
) -> List[CampaignInsight]:
    """Generate cross-campaign insights."""
    insights: List[CampaignInsight] = []
    reliable = [c for c in campaigns if c.is_reliable]

    if len(reliable) < 2:
        return insights

    best = reliable[0]  # already sorted desc
    worst = reliable[-1]

    # 1. Best campaign
    insights.append(CampaignInsight(
        type="campaign_best",
        icon="trophy",
        title=f"Лучшая кампания: «{best.campaign}»",
        description=(
            f"Балл: {best.composite_score:.2f}, "
            f"{best.n_texts} текстов, "
            f"{best.total_clicks:,} кликов. "
            f"Рекомендуем увеличить бюджет."
        ),
        severity="success",
    ))

    # 2. Worst campaign
    if worst.composite_score < best.composite_score - 0.05:
        insights.append(CampaignInsight(
            type="campaign_worst",
            icon="alert_triangle",
            title=f"Слабая кампания: «{worst.campaign}»",
            description=(
                f"Балл: {worst.composite_score:.2f} "
                f"(на {best.composite_score - worst.composite_score:.2f} ниже лидера). "
                f"Потери бюджета: {worst.budget_waste_pct:.0f}%. "
                f"Требует оптимизации текстов."
            ),
            severity="warning",
        ))

    # 3. Budget reallocation
    total_imp_all = sum(c.total_impressions for c in reliable)
    if total_imp_all > 0:
        worst_share = worst.total_impressions / total_imp_all * 100
        if worst_share > 20 and worst.composite_score < 0.45:
            insights.append(CampaignInsight(
                type="budget_realloc",
                icon="alert_triangle",
                title="Рекомендация: перераспределить бюджет",
                description=(
                    f"«{worst.campaign}» получает {worst_share:.0f}% показов "
                    f"при низком балле ({worst.composite_score:.2f}). "
                    f"Перенесите бюджет на «{best.campaign}» ({best.composite_score:.2f})."
                ),
                severity="warning",
            ))

    # 4. High diversity campaigns
    for c in reliable:
        if c.score_spread > 0.15 and c.n_texts >= 3:
            insights.append(CampaignInsight(
                type="campaign_diversity",
                icon="chart",
                title=f"Высокий разброс: «{c.campaign}»",
                description=(
                    f"Разброс баллов σ={c.score_spread:.2f} "
                    f"(лучший: {c.best_text_score:.2f}, худший: {c.worst_text_score:.2f}). "
                    f"Отключите слабые тексты для повышения среднего."
                ),
                severity="info",
            ))

    return insights
