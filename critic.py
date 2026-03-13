"""
CRITIC (Criteria Importance Through Intercriteria Correlation) method
for objective weight computation.

Reference: Diakoulaki, D., Mavrotas, G., & Papayannakis, L. (1995).
"Determining objective weights in multiple criteria problems: The CRITIC method."
Computers & Operations Research, 22(7), 763-770.
"""

import numpy as np
import pandas as pd
from typing import Dict, Set


def compute_critic_weights(
    z_scores_df: pd.DataFrame,
    cost_metrics: Set[str],
) -> Dict[str, float]:
    """
    Compute objective weights via the CRITIC method.

    The method combines two dimensions of information:
    1. Contrast (standard deviation) — metrics with higher σ differentiate texts better
    2. Conflict (1 - correlation) — metrics weakly correlated with others carry unique info

    Weight_j = σ_j × Σ(1 - r_jk) for all k ≠ j, then normalized to sum to 1.

    Parameters
    ----------
    z_scores_df : pd.DataFrame
        DataFrame with z_{metric} columns (after sigmoid normalization, values in 0..1).
        Cost metrics are already inverted. Rows are texts.
    cost_metrics : Set[str]
        Set of cost metric names (for reference; already inverted in z-scores).

    Returns
    -------
    Dict[str, float]
        Metric name → weight (sums to 1.0).
    """
    # 1. Extract z_{metric} columns
    z_cols = [c for c in z_scores_df.columns if c.startswith("z_")]
    if not z_cols:
        return {}

    metric_names = [c[2:] for c in z_cols]  # "z_CTR" -> "CTR"
    matrix = z_scores_df[z_cols].copy()
    matrix.columns = metric_names

    # Edge case: single metric
    if len(metric_names) == 1:
        return {metric_names[0]: 1.0}

    # Edge case: single row or no valid rows — no variance/correlation possible
    valid_rows = matrix.dropna(how="all")
    if len(valid_rows) <= 1:
        w = 1.0 / len(metric_names)
        return {m: round(w, 6) for m in metric_names}

    # 2. Standard deviation for each metric (pairwise: drop NaN per-column)
    sigmas = {}
    for m in metric_names:
        col = matrix[m].dropna()
        if len(col) < 2:
            sigmas[m] = 0.0
        else:
            sigmas[m] = float(col.std(ddof=1))

    # 3. Pearson correlation matrix (pairwise complete observations)
    corr_matrix = matrix.corr(method="pearson", min_periods=2)
    corr_matrix = corr_matrix.fillna(0.0)

    # 4. Information content C_j for each metric
    C = {}
    for j in metric_names:
        sigma_j = sigmas.get(j, 0.0)
        if sigma_j == 0.0:
            # Zero variance → metric provides no differentiation → zero weight
            C[j] = 0.0
            continue

        conflict_sum = 0.0
        for k in metric_names:
            if k != j and j in corr_matrix.index and k in corr_matrix.columns:
                conflict_sum += 1.0 - corr_matrix.loc[j, k]

        C[j] = sigma_j * conflict_sum

    # 5. Normalize to weights summing to 1.0
    total_C = sum(C.values())
    if total_C == 0:
        # Fallback: equal weights (all metrics have zero variance)
        w = 1.0 / len(metric_names)
        return {m: round(w, 6) for m in metric_names}

    weights = {m: round(C[m] / total_C, 6) for m in metric_names}
    return weights
