"""
Statistical enrichment for text scoring results (v2.2).

Per-text additions for each proportion metric (CTR, CR, CR_install):
  {prefix}_ci_low / {prefix}_ci_high  — 95 % Bayesian credible interval
                                         (Beta posterior, uniform prior)
  prob_{prefix}_better                — Bayesian P(text rate > batch median rate)

Cross-text:
  ctr_pvalue        — two-sided binomial p-value vs batch mean CTR
  is_significant_bh — True if significant after BH correction (FDR 5 %)

The module requires only scipy (already a project dependency).
"""

import numpy as np
from scipy.stats import beta as beta_dist, binomtest


# ── Generic Beta CI for any proportion metric ────────────────────────────────

def _beta_ci_for_proportion(results: list, successes_fn, trials_fn, prefix: str) -> None:
    """
    In-place: set {prefix}_ci_low, {prefix}_ci_high, prob_{prefix}_better
    on each result using Beta(successes+1, trials-successes+1) posterior.

    successes_fn(r) → int  number of successes (clicks, conversions, installs)
    trials_fn(r)    → int  number of trials   (impressions, clicks)
    """
    rates = []
    for r in results:
        trials = trials_fn(r)
        if trials and trials > 0:
            k = max(0, min(successes_fn(r), trials))
            rates.append(k / trials)
        else:
            rates.append(0.0)
    if not rates:
        return
    median_rate = float(np.median(rates))

    for r in results:
        trials = trials_fn(r)
        if not trials or trials <= 0:
            continue
        k = max(0, min(successes_fn(r), trials))
        a = k + 1              # Beta posterior alpha (uniform prior)
        b = trials - k + 1    # Beta posterior beta

        setattr(r, f"{prefix}_ci_low", round(float(beta_dist.ppf(0.025, a, b)), 6))
        setattr(r, f"{prefix}_ci_high", round(float(beta_dist.ppf(0.975, a, b)), 6))
        setattr(r, f"prob_{prefix}_better", round(
            float(1.0 - beta_dist.cdf(median_rate, a, b)), 4
        ))


# ── Main entry point ─────────────────────────────────────────────────────────

def enrich_with_statistics(results: list) -> None:
    """
    In-place statistical enrichment of TextResult objects.

    1. Beta CIs + P(better) for CTR (clicks/impressions)
    2. Beta CIs + P(better) for CR  (conversions/clicks)  — if CR metric exists
    3. Beta CIs + P(better) for CR_install (installs/clicks) — if metric exists
    4. Binomial p-values for CTR vs batch mean
    5. Benjamini-Hochberg correction across all CTR p-values
    """
    n = len(results)
    if n == 0:
        return

    total_imps = sum(r.n_impressions for r in results)
    if total_imps == 0:
        return

    # ── 1. CTR: clicks / impressions ─────────────────────────────────────────
    _beta_ci_for_proportion(
        results,
        successes_fn=lambda r: r.n_clicks,
        trials_fn=lambda r: r.n_impressions,
        prefix="ctr",
    )

    # ── 2. CR: conversions / clicks ──────────────────────────────────────────
    # Recover integer conversions from CR rate: n_conv ≈ round(CR * n_clicks)
    has_cr = any(r.metrics.get("CR") is not None and r.n_clicks > 0 for r in results)
    if has_cr:
        _beta_ci_for_proportion(
            results,
            successes_fn=lambda r: round((r.metrics.get("CR") or 0) * r.n_clicks),
            trials_fn=lambda r: r.n_clicks,
            prefix="cr",
        )

    # ── 3. CR_install: installs / clicks ─────────────────────────────────────
    has_cri = any(r.metrics.get("CR_install") is not None and r.n_clicks > 0 for r in results)
    if has_cri:
        _beta_ci_for_proportion(
            results,
            successes_fn=lambda r: round((r.metrics.get("CR_install") or 0) * r.n_clicks),
            trials_fn=lambda r: r.n_clicks,
            prefix="cr_install",
        )

    # ── 4. Binomial p-values for CTR vs batch mean ──────────────────────────
    total_clicks = sum(r.n_clicks for r in results)
    batch_mean_ctr = total_clicks / total_imps

    p_values: list[float] = []
    valid_indices: list[int] = []

    for i, r in enumerate(results):
        if r.n_impressions < 10:
            continue
        clicks = max(0, min(r.n_clicks, r.n_impressions))
        res = binomtest(clicks, r.n_impressions, batch_mean_ctr, alternative="two-sided")
        r.ctr_pvalue = round(float(res.pvalue), 6)
        p_values.append(res.pvalue)
        valid_indices.append(i)

    # ── 5. Benjamini-Hochberg correction (FDR = 5 %) ────────────────────────
    if not p_values:
        return

    p_arr = np.array(p_values)
    n_tests = len(p_arr)
    sorted_idx = np.argsort(p_arr)
    sorted_p = p_arr[sorted_idx]
    thresholds = (np.arange(1, n_tests + 1) / n_tests) * 0.05

    below = sorted_p <= thresholds
    if below.any():
        max_k = int(np.where(below)[0].max())
        final_sig = np.zeros(n_tests, dtype=bool)
        final_sig[: max_k + 1] = True
    else:
        final_sig = np.zeros(n_tests, dtype=bool)

    for rank, orig_rank in enumerate(sorted_idx):
        results[valid_indices[orig_rank]].is_significant_bh = bool(final_sig[rank])
