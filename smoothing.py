"""
Empirical Bayes smoothing for rate metrics (CTR, CR variants).
Uses Beta-Binomial conjugate model with hierarchical prior fallback.

Spec: AdScore v2.1 Section 5.
"""

import logging
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Rate metrics: metric_name -> (successes_col, trials_col)
RATE_METRICS: Dict[str, Tuple[str, str]] = {
    "CTR": ("_clicks", "_impressions"),
    "CR_install": ("_installs", "_clicks"),
}

MIN_WEIGHT_CLICKS = 30  # w_i = min(1.0, clicks_i / 30)
MIN_TEXTS_FOR_PRIOR = 5
MIN_TEXTS_FOR_PLATFORM = 10


def _estimate_beta_prior(
    successes: np.ndarray,
    trials: np.ndarray,
) -> Tuple[float, float]:
    """
    Estimate Beta prior (alpha, beta) via method of moments.

    variance_ok check: v < p_bar * (1 - p_bar)
    Falls back to uninformative (1, 1) if estimation fails.
    """
    mask = trials > 0
    if mask.sum() < 3:
        return 1.0, 1.0

    rates = successes[mask] / trials[mask]
    p_bar = float(np.mean(rates))
    v = float(np.var(rates, ddof=1))

    # variance_ok: v must be < p_bar*(1-p_bar) for MoM to work
    if v <= 0 or p_bar <= 0 or p_bar >= 1 or v >= p_bar * (1 - p_bar):
        return 1.0, 1.0

    factor = (p_bar * (1 - p_bar) / v) - 1.0
    if factor <= 0:
        return 1.0, 1.0

    alpha = max(0.01, p_bar * factor)
    beta = max(0.01, (1 - p_bar) * factor)

    return alpha, beta


def _compute_hierarchical_prior(
    df: pd.DataFrame,
    succ_col: str,
    trial_col: str,
) -> Tuple[float, float, str]:
    """
    Hierarchical prior: campaign+platform -> campaign -> platform -> global.
    Returns (alpha, beta, source_level).
    """
    # Level 1: campaign + platform
    has_campaign = "campaign" in df.columns
    has_platform = "platform" in df.columns

    if has_campaign and has_platform:
        grouped = df.groupby(["campaign", "platform"]).agg(
            s=(succ_col, "sum"), t=(trial_col, "sum"),
        )
        if len(grouped) >= MIN_TEXTS_FOR_PRIOR:
            a, b = _estimate_beta_prior(grouped["s"].values, grouped["t"].values)
            if a != 1.0 or b != 1.0:
                return a, b, "campaign_platform"

    # Level 2: campaign only
    if has_campaign:
        grouped = df.groupby("campaign").agg(
            s=(succ_col, "sum"), t=(trial_col, "sum"),
        )
        if len(grouped) >= MIN_TEXTS_FOR_PRIOR:
            a, b = _estimate_beta_prior(grouped["s"].values, grouped["t"].values)
            if a != 1.0 or b != 1.0:
                return a, b, "campaign"

    # Level 3: platform only
    if has_platform:
        grouped = df.groupby("platform").agg(
            s=(succ_col, "sum"), t=(trial_col, "sum"),
        )
        if len(grouped) >= MIN_TEXTS_FOR_PLATFORM:
            a, b = _estimate_beta_prior(grouped["s"].values, grouped["t"].values)
            if a != 1.0 or b != 1.0:
                return a, b, "platform"

    # Level 4: global
    a, b = _estimate_beta_prior(
        df[succ_col].values.astype(float),
        df[trial_col].values.astype(float),
    )
    return a, b, "global"


def smooth_rates(
    df: pd.DataFrame,
    events: list,
) -> pd.DataFrame:
    """
    Apply Empirical Bayes smoothing to rate metrics.

    For each rate metric:
      smoothed = (successes + alpha) / (trials + alpha + beta)
      weight = min(1.0, trials / 30)
      blended = weight * observed + (1-weight) * smoothed

    Adds *_smoothed columns. Original raw values preserved.

    Parameters
    ----------
    df : DataFrame with raw metric columns (_clicks, _impressions, etc.)
    events : list of EventConfig objects

    Returns
    -------
    DataFrame with *_smoothed columns added
    """
    df = df.copy()

    # Build rate metric map dynamically
    rate_map = dict(RATE_METRICS)

    # Add event-specific CR metrics
    for ev in events:
        slot = ev.slot
        conv_col = f"_conv_{slot}"
        if conv_col in df.columns:
            rate_map[f"CR_{slot}"] = (conv_col, "_clicks")

    # Legacy CR
    if "_conversions" in df.columns:
        rate_map["CR"] = ("_conversions", "_clicks")

    for metric_name, (succ_col, trial_col) in rate_map.items():
        if succ_col not in df.columns or trial_col not in df.columns:
            continue

        successes = pd.to_numeric(df[succ_col], errors="coerce").fillna(0).values
        trials = pd.to_numeric(df[trial_col], errors="coerce").fillna(0).values

        if trials.sum() == 0:
            continue

        # Estimate prior via hierarchy
        alpha, beta, source = _compute_hierarchical_prior(df, succ_col, trial_col)

        # Smoothed rate: Bayesian posterior mean
        smoothed = (successes + alpha) / (trials + alpha + beta)

        # Observed rate
        observed = np.where(trials > 0, successes / trials, 0.0)

        # Confidence weight: how much to trust observed vs prior
        weights = np.minimum(1.0, trials / MIN_WEIGHT_CLICKS)

        # Blended: weighted mix
        blended = weights * observed + (1 - weights) * smoothed

        col_name = f"{metric_name}_smoothed"
        df[col_name] = np.where(trials > 0, blended, np.nan)

        logger.info(
            "Smoothing %s: alpha=%.3f, beta=%.3f, source=%s, n=%d",
            metric_name, alpha, beta, source, int((trials > 0).sum()),
        )

    return df
