"""
Fatigue / lifecycle detection module.
Detects creative fatigue when temporal data (days_active) is available.

Spec: AdScore v2.1 Section 9.
"""

import logging
from typing import List

import numpy as np
from scipy import stats as scipy_stats

from models import ScoringParams, TextResult

logger = logging.getLogger(__name__)

_DEFAULT_COLD_START_DAYS = 14
_DEFAULT_MAX_FATIGUE_PENALTY = 0.15
_DEFAULT_P_VALUE_THRESHOLD = 0.15


def compute_fatigue(
    results: List[TextResult],
    df_scored,  # pd.DataFrame with optional days_active column
    params: ScoringParams = None,
) -> None:
    """
    Compute fatigue penalty for each text (mutates in-place).

    Requires 'days_active' column in df_scored.
    If not present, this is a no-op.

    Logic:
    - days_active < cold_start_days: cold start, no penalty
    - Older texts with below-average performance get fatigue penalty
    - fatigue_penalty = base_penalty * max(0, 1 - p_value / p_value_threshold)
    - decision_score_adjusted = decision_score * (1 - fatigue_penalty)
    """
    if df_scored is None or "days_active" not in df_scored.columns:
        return

    cold_start_days = params.cold_start_days if params else _DEFAULT_COLD_START_DAYS
    max_fatigue_penalty = params.max_fatigue_penalty if params else _DEFAULT_MAX_FATIGUE_PENALTY
    p_value_threshold = params.fatigue_p_value_threshold if params else _DEFAULT_P_VALUE_THRESHOLD

    # Build lookup: text_id -> days_active
    days_lookup = {}
    for _, row in df_scored.iterrows():
        text_id = str(row.get("text_id", row.name))
        days = row.get("days_active")
        try:
            days_val = float(days)
            if not np.isnan(days_val):
                days_lookup[text_id] = days_val
        except (TypeError, ValueError):
            continue

    if not days_lookup:
        return

    # Batch-level statistics
    # Fix: filter out NaN values to prevent poisoning the entire batch
    all_scores = []
    for r in results:
        s = r.decision_score if r.decision_score is not None else r.composite_score
        if s is not None and not np.isnan(s):
            all_scores.append(s)
    if not all_scores:
        return

    mean_score = float(np.mean(all_scores))
    std_score = float(np.std(all_scores, ddof=1)) if len(all_scores) > 2 else 0.0
    max_days = max(days_lookup.values()) if days_lookup else 1.0

    n_fatigue = 0
    for result in results:
        days_active = days_lookup.get(result.text_id)
        if days_active is None:
            continue

        # Cold start: no penalty for new texts
        if days_active < cold_start_days:
            result.fatigue_score = 0.0
            result.fatigue_penalty = 0.0
            result.declining_recently = False
            continue

        # Fix: use original (pre-fatigue) score to ensure idempotency
        # If fatigue was already applied, _original_score preserves the unpenalized value
        if not hasattr(result, '_original_score') or result._original_score is None:
            result._original_score = (
                result.decision_score
                if result.decision_score is not None
                else result.composite_score
            )
        score = result._original_score

        # Age factor: normalized 0-1
        age_factor = min(1.0, days_active / max(max_days, 1.0))

        # Performance gap from mean
        perf_gap = max(0.0, mean_score - score)

        # Base penalty: scales with age and underperformance
        base_penalty = min(max_fatigue_penalty, age_factor * perf_gap * 2)

        # P-value proxy via z-test
        if std_score > 0 and len(all_scores) > 2:
            z_stat = (mean_score - score) / std_score
            p_value = float(scipy_stats.norm.sf(z_stat))
        else:
            p_value = 1.0

        # Penalty with p-value dampening
        fatigue_penalty = base_penalty * max(0.0, 1.0 - p_value / p_value_threshold)
        fatigue_penalty = min(fatigue_penalty, max_fatigue_penalty)

        # Fatigue score (0-1 severity)
        fatigue_score = min(1.0, fatigue_penalty / max_fatigue_penalty) if max_fatigue_penalty > 0 else 0.0

        result.fatigue_score = round(fatigue_score, 4)
        result.fatigue_penalty = round(fatigue_penalty, 4)
        result.declining_recently = fatigue_penalty > 0.05

        # Adjust decision_score
        if fatigue_penalty > 0:
            adj_score = score * (1 - fatigue_penalty)
            result.decision_score = round(adj_score, 4)
            # Keep composite_score in sync for backward compat
            result.composite_score = result.decision_score

        if fatigue_penalty > 0:
            n_fatigue += 1

    if n_fatigue > 0:
        logger.info(
            "Fatigue computed: %d/%d texts with penalty, %d had days_active data",
            n_fatigue, len(results), len(days_lookup),
        )
