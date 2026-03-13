"""
Counterfactual financial impact computation.
Computes excess_cost, missed_conversions, revenue_gap, real_savings.

Must run AFTER verdicts are assigned (needs SCALE texts for benchmark CPA).

Spec: AdScore v2.1 Sections 14-15.
"""

import logging
from typing import List, Optional

import numpy as np

from models import ScoringParams, TextResult

logger = logging.getLogger(__name__)

_DEFAULT_SATURATION_DISCOUNT = 0.7


def compute_financial_impact(
    results: List[TextResult],
    events: list,
    params: ScoringParams = None,
) -> None:
    """
    Compute financial impact fields on each TextResult (mutates in-place).

    Uses batch-level benchmarks (median CPA of SCALE texts, or all).
    """
    if not results:
        return

    saturation_discount = params.saturation_discount if params else _DEFAULT_SATURATION_DISCOUNT

    # Determine primary CPA and conversion keys
    cpa_key = _get_primary_cpa_key(events)

    # Find SCALE texts for benchmark CPA
    scale_results = [
        r for r in results
        if r.verdict and r.verdict.verdict in ("Масштабировать", "SCALE")
        and not r.anomaly_detected
    ]
    benchmark_pool = scale_results if len(scale_results) >= 2 else results
    target_source = "batch_top_median" if len(scale_results) >= 2 else "batch_median"

    # Compute CPA target (median of benchmark pool)
    # Fix: use explicit None checks instead of `or` (which treats 0.0 as falsy)
    cpa_values = []
    for r in benchmark_pool:
        val = r.metrics.get(cpa_key)
        if val is None:
            val = r.metrics.get("CPA")
        if val is not None and val > 0:
            cpa_values.append(val)
    cpa_target = float(np.median(cpa_values)) if cpa_values else None

    # Compute ROI target (median of benchmark pool)
    roi_values = [
        r.metrics.get("ROI")
        for r in benchmark_pool
        if r.metrics.get("ROI") is not None
    ]
    roi_target = float(np.median(roi_values)) if roi_values else None

    n_computed = 0
    for r in results:
        metrics = r.metrics or {}
        spend = _estimate_spend(r)

        if spend <= 0:
            continue

        r.target_source = target_source
        actual_conv = _get_conversions(r, events)
        # Fix: explicit None check instead of `or` (0.0 is valid CPA)
        actual_cpa = metrics.get(cpa_key)
        if actual_cpa is None:
            actual_cpa = metrics.get("CPA")

        # --- excess_cost: overspend vs CPA target ---
        if cpa_target and actual_cpa and actual_conv and actual_conv > 0:
            r.excess_cost = round(max(0.0, (actual_cpa - cpa_target) * actual_conv), 2)
            if r.excess_cost == 0:
                r.excess_cost = None

        # --- missed_conversions: conversions left on table ---
        # Fix: only compute when actual_conv is known (not None)
        if cpa_target and cpa_target > 0 and actual_conv is not None:
            potential_conv = spend / cpa_target
            missed = potential_conv - actual_conv
            if missed > 0:
                r.missed_conversions = round(missed, 2)

        # --- revenue_gap: difference from target ROAS ---
        actual_revenue_total = _estimate_revenue(r)
        if roi_target is not None and spend > 0 and actual_revenue_total is not None:
            target_revenue = (1 + roi_target) * spend
            gap = target_revenue - actual_revenue_total
            if gap > 0:
                r.revenue_gap = round(gap, 2)

        # --- real_savings: counterfactual ---
        if cpa_target and actual_conv and actual_conv > 0:
            savings = spend - actual_conv * cpa_target
            if savings > 0:
                r.real_savings = round(savings, 2)
                r.real_savings_adjusted = round(savings * saturation_discount, 2)

        n_computed += 1

    if n_computed > 0:
        logger.info(
            "Financial impact computed: %d texts, CPA_target=%.2f, source=%s",
            n_computed, cpa_target or 0, target_source,
        )


def _get_primary_cpa_key(events: list) -> str:
    """Get CPA metric key for primary event."""
    if events:
        for ev in events:
            if ev.is_primary:
                return f"CPA_{ev.slot}"
        return f"CPA_{events[0].slot}"
    return "CPA"


def _estimate_spend(r: TextResult) -> float:
    """Estimate total spend from metrics."""
    # Try direct spend from metrics
    spend = r.metrics.get("spend")
    if spend is not None and spend > 0:
        return float(spend)

    # Fallback: CPC * clicks
    cpc = r.metrics.get("CPC")
    if cpc is not None and cpc > 0 and r.n_clicks > 0:
        return cpc * r.n_clicks

    # Fallback: CPM * impressions / 1000
    cpm = r.metrics.get("CPM")
    if cpm is not None and cpm > 0 and r.n_impressions > 0:
        return cpm * r.n_impressions / 1000

    return 0.0


def _get_conversions(r: TextResult, events: list) -> Optional[float]:
    """Get actual conversion count."""
    # Try to get from CR * clicks
    if events:
        for ev in events:
            if ev.is_primary:
                cr = r.metrics.get(f"CR_{ev.slot}")
                if cr is not None and r.n_clicks > 0:
                    return cr * r.n_clicks
        cr = r.metrics.get(f"CR_{events[0].slot}")
        if cr is not None and r.n_clicks > 0:
            return cr * r.n_clicks

    cr = r.metrics.get("CR")
    if cr is not None and r.n_clicks > 0:
        return cr * r.n_clicks

    return None


def _estimate_revenue(r: TextResult) -> Optional[float]:
    """Estimate total revenue."""
    rpc = r.metrics.get("RPC")
    if rpc is not None and r.n_clicks > 0:
        return rpc * r.n_clicks

    rpm = r.metrics.get("RPM")
    if rpm is not None and r.n_impressions > 0:
        return rpm * r.n_impressions / 1000

    return None
