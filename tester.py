"""
A/B testing module with z-test for proportions, Welch's t-test for means,
Benjamini-Hochberg FDR correction, and MDE (minimum detectable effect) calculator.
"""

import logging
import math
from typing import Dict, List, Optional, Any

import numpy as np
from scipy.stats import norm, t as t_dist

logger = logging.getLogger(__name__)


class ABTester:
    """Performs A/B comparisons between two ad texts."""

    def ztest_proportions(
        self, p1: float, n1: int, p2: float, n2: int
    ) -> Dict[str, Any]:
        """
        Two-proportion z-test.
        p1, p2: conversion rates (proportions)
        n1, n2: sample sizes (clicks)

        Returns dict with z_stat, p_value, ci_lower, ci_upper, effect_size.
        """
        warning: Optional[str] = None

        if n1 < 30 or n2 < 30:
            warning = "insufficient_sample"

        # Edge case: pool proportion is 0 or 1
        total_conversions = p1 * n1 + p2 * n2
        total_n = n1 + n2

        if total_n == 0:
            return {
                "z_stat": 0.0,
                "p_value": 1.0,
                "ci_lower": 0.0,
                "ci_upper": 0.0,
                "effect_size": 0.0,
                "warning": warning,
            }

        p_pool = total_conversions / total_n

        # Edge case: p_pool == 0 or p_pool == 1 → no information
        if p_pool <= 0 or p_pool >= 1:
            return {
                "z_stat": 0.0,
                "p_value": 1.0,
                "ci_lower": 0.0,
                "ci_upper": 0.0,
                "effect_size": 0.0,
                "warning": warning,
            }

        se = math.sqrt(p_pool * (1 - p_pool) * (1 / n1 + 1 / n2))

        if se == 0:
            return {
                "z_stat": 0.0,
                "p_value": 1.0,
                "ci_lower": 0.0,
                "ci_upper": 0.0,
                "effect_size": 0.0,
                "warning": warning,
            }

        z_stat = (p1 - p2) / se
        p_value = 2 * (1 - norm.cdf(abs(z_stat)))

        # 95% confidence interval for the difference
        se_diff = math.sqrt(p1 * (1 - p1) / n1 + p2 * (1 - p2) / n2)
        diff = p1 - p2
        ci_lower = diff - 1.96 * se_diff
        ci_upper = diff + 1.96 * se_diff

        effect_size = round(diff, 6)

        return {
            "z_stat": round(z_stat, 4),
            "p_value": round(p_value, 6),
            "ci_lower": round(ci_lower, 6),
            "ci_upper": round(ci_upper, 6),
            "effect_size": effect_size,
            "warning": warning,
            "test_type": "z_proportions",
        }

    def test_means(
        self,
        m1: float, s1: float, n1: int,
        m2: float, s2: float, n2: int,
    ) -> Dict[str, Any]:
        """
        Two-sample test for means.
        Uses Welch's t-test when n1 < 30 or n2 < 30, z-test otherwise.
        m1, m2: means
        s1, s2: standard deviations
        n1, n2: sample sizes
        """
        warning: Optional[str] = None
        use_welch = n1 < 30 or n2 < 30

        if n1 < 30 or n2 < 30:
            warning = "insufficient_sample"

        if n1 <= 1 or n2 <= 1:
            return {
                "z_stat": 0.0,
                "p_value": 1.0,
                "ci_lower": 0.0,
                "ci_upper": 0.0,
                "effect_size": 0.0,
                "warning": warning,
                "test_type": "welch_t" if use_welch else "z_means",
            }

        se = math.sqrt(s1**2 / n1 + s2**2 / n2)

        if se == 0:
            return {
                "z_stat": 0.0,
                "p_value": 1.0,
                "ci_lower": 0.0,
                "ci_upper": 0.0,
                "effect_size": 0.0,
                "warning": warning,
                "test_type": "welch_t" if use_welch else "z_means",
            }

        stat = (m1 - m2) / se
        diff = m1 - m2

        if use_welch:
            # Welch-Satterthwaite degrees of freedom
            num = (s1**2 / n1 + s2**2 / n2) ** 2
            den = (s1**2 / n1) ** 2 / (n1 - 1) + (s2**2 / n2) ** 2 / (n2 - 1)
            if den == 0:
                df = max(n1, n2) - 1
            else:
                df = num / den
            p_value = 2 * (1 - t_dist.cdf(abs(stat), df))
            t_crit = t_dist.ppf(0.975, df)
            ci_lower = diff - t_crit * se
            ci_upper = diff + t_crit * se
            test_type = "welch_t"
        else:
            p_value = 2 * (1 - norm.cdf(abs(stat)))
            ci_lower = diff - 1.96 * se
            ci_upper = diff + 1.96 * se
            test_type = "z_means"

        effect_size = round(diff, 6)

        return {
            "z_stat": round(stat, 4),
            "p_value": round(p_value, 6),
            "ci_lower": round(ci_lower, 6),
            "ci_upper": round(ci_upper, 6),
            "effect_size": effect_size,
            "warning": warning,
            "test_type": test_type,
        }

    # Keep backward-compatible alias
    def ztest_means(self, m1, s1, n1, m2, s2, n2):
        return self.test_means(m1, s1, n1, m2, s2, n2)

    def apply_fdr(
        self, p_values: List[float], fdr_level: float = 0.01
    ) -> List[bool]:
        """
        Benjamini-Hochberg FDR correction.
        Returns list of booleans: True if H0 should be rejected.
        """
        k = len(p_values)
        if k == 0:
            return []

        # Sort p-values with original indices
        indexed = sorted(enumerate(p_values), key=lambda x: x[1])
        reject = [False] * k

        for rank, (orig_idx, p) in enumerate(indexed, 1):
            threshold = (rank / k) * fdr_level
            if p <= threshold:
                reject[orig_idx] = True

        return reject

    def compare(
        self,
        result_a: Dict[str, Any],
        result_b: Dict[str, Any],
        metric: str,
        fdr_level: float = 0.01,
    ) -> Dict[str, Any]:
        """
        Compare two texts on a given metric.
        result_a, result_b: dicts with keys matching TextResult fields
            (metrics, n_clicks, n_impressions, std_metrics, etc.)
        metric: 'CR', 'CTR', 'CPA', etc.

        Returns full comparison result with MDE.
        """
        metrics_a = result_a.get("metrics", {})
        metrics_b = result_b.get("metrics", {})

        val_a = metrics_a.get(metric)
        val_b = metrics_b.get(metric)

        n_clicks_a = result_a.get("n_clicks", 0)
        n_clicks_b = result_b.get("n_clicks", 0)
        n_impressions_a = result_a.get("n_impressions", 0)
        n_impressions_b = result_b.get("n_impressions", 0)
        std_a = result_a.get("std_metrics", {})
        std_b = result_b.get("std_metrics", {})

        if val_a is None or val_b is None:
            return {
                "z_stat": 0.0,
                "p_value": 1.0,
                "p_value_fdr_corrected": 1.0,
                "significant": False,
                "winner": None,
                "ci_diff_lower": 0.0,
                "ci_diff_upper": 0.0,
                "effect_size": 0.0,
                "warning": "metric_not_available",
                "test_type": None,
                "mde": None,
            }

        # Choose test based on metric type
        # CR and CR_event_N are proportions (conversions/clicks)
        # CTR is a proportion (clicks/impressions)
        is_cr = metric == "CR" or metric.startswith("CR_")
        is_ctr = metric == "CTR"
        is_proportion = is_cr or is_ctr

        if is_proportion:
            if is_ctr:
                n1, n2 = n_impressions_a, n_impressions_b
            else:
                n1, n2 = n_clicks_a, n_clicks_b
            test_result = self.ztest_proportions(val_a, n1, val_b, n2)
        else:
            # FIX: For continuous metrics (CPA, CPC, CPM, ROI), use test_means
            n1, n2 = n_clicks_a, n_clicks_b
            s1 = std_a.get(metric, abs(val_a) * 0.5) if std_a else abs(val_a) * 0.5
            s2 = std_b.get(metric, abs(val_b) * 0.5) if std_b else abs(val_b) * 0.5
            test_result = self.test_means(val_a, s1, n1, val_b, s2, n2)

        # Apply FDR correction (single comparison => rank = 1/1)
        p_raw = test_result["p_value"]
        fdr_results = self.apply_fdr([p_raw], fdr_level)
        p_fdr = p_raw  # For a single test, FDR-corrected = raw
        significant = fdr_results[0] if fdr_results else False

        # Determine winner (cost metrics: lower = better)
        is_cost_metric = metric in ("CPA", "CPC", "CPM") or metric.startswith("CPA_")
        if significant:
            if is_cost_metric:
                winner = "A" if val_a < val_b else "B"
            else:
                winner = "A" if val_a > val_b else "B"
        else:
            winner = None

        # MDE calculation
        mde = None
        if is_proportion:
            baseline = (val_a + val_b) / 2
            mde = calculate_mde(n1, n2, baseline_rate=baseline, is_proportion=True)
        else:
            s1_val = std_a.get(metric) if std_a else None
            s2_val = std_b.get(metric) if std_b else None
            if s1_val and s2_val:
                baseline = (abs(val_a) + abs(val_b)) / 2
                mde = calculate_mde(
                    n1, n2, s1=s1_val, s2=s2_val,
                    baseline_mean=baseline, is_proportion=False,
                )

        return {
            "z_stat": test_result["z_stat"],
            "p_value": test_result["p_value"],
            "p_value_fdr_corrected": round(p_fdr, 6),
            "significant": significant,
            "winner": winner,
            "ci_diff_lower": test_result["ci_lower"],
            "ci_diff_upper": test_result["ci_upper"],
            "effect_size": test_result["effect_size"],
            "warning": test_result.get("warning"),
            "test_type": test_result.get("test_type"),
            "mde": mde,
        }


def calculate_mde(
    n1: int,
    n2: int,
    baseline_rate: float = None,
    s1: float = None,
    s2: float = None,
    baseline_mean: float = None,
    alpha: float = 0.05,
    power: float = 0.80,
    is_proportion: bool = True,
) -> Dict[str, Any]:
    """
    Calculate Minimum Detectable Effect (MDE).
    For proportions: MDE = (z_α + z_β) × sqrt(p(1-p)(1/n1 + 1/n2))
    For means: MDE = (z_α + z_β) × sqrt(s1²/n1 + s2²/n2)
    """
    if n1 <= 0 or n2 <= 0:
        return {
            "mde_absolute": None,
            "mde_percent": None,
            "power": power,
            "alpha": alpha,
            "interpretation": "Недостаточно данных для расчёта МДЭ",
        }

    z_alpha = norm.ppf(1 - alpha / 2)  # 1.96 for α=0.05
    z_beta = norm.ppf(power)            # 0.84 for power=0.80

    if is_proportion and baseline_rate is not None:
        p = max(baseline_rate, 1e-6)
        se = math.sqrt(p * (1 - p) * (1 / n1 + 1 / n2))
        mde_abs = (z_alpha + z_beta) * se
        mde_pct = (mde_abs / p * 100) if p > 0 else 0
    elif s1 is not None and s2 is not None:
        se = math.sqrt(s1**2 / n1 + s2**2 / n2)
        mde_abs = (z_alpha + z_beta) * se
        base = baseline_mean if baseline_mean and baseline_mean > 0 else 1.0
        mde_pct = (mde_abs / base * 100)
    else:
        return {
            "mde_absolute": None,
            "mde_percent": None,
            "power": power,
            "alpha": alpha,
            "interpretation": "Нет данных для расчёта МДЭ (нужен std dev)",
        }

    return {
        "mde_absolute": round(mde_abs, 6),
        "mde_percent": round(mde_pct, 2),
        "n1": n1,
        "n2": n2,
        "power": power,
        "alpha": alpha,
        "interpretation": (
            f"С текущими объёмами ({n1} и {n2}) можно обнаружить "
            f"разницу ≥{mde_pct:.1f}% с {int(power * 100)}% вероятностью"
        ),
    }
