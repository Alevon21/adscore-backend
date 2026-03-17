"""
Core scoring algorithm — 7 steps from Part 2 of the specification.

Steps:
1. Mapping (handled by mapper.py)
2. Validation & cleaning
3. Raw metrics computation
4. Winsorization
5. Z-score normalization (sigmoid)
6. Composite score
7. Category assignment (hybrid: percentile + quality floor)
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from models import EventConfig, ExcludedText, ScoringParams, ScoringResult, TextResult
from verdict import generate_verdicts

logger = logging.getLogger(__name__)

# Goal-based weight presets (all 10 metrics, each sums to 1.00)
GOAL_PRESETS: Dict[str, Dict[str, float]] = {
    "goal_traffic": {
        "CTR": 0.25, "CR": 0.05, "CPA": 0.05, "ROI": 0.05,
        "RPM": 0.05, "RPC": 0.05, "CPC": 0.20, "CPM": 0.10,
        "CPI": 0.10, "CR_install": 0.10,
    },
    "goal_conversions": {
        "CTR": 0.10, "CR": 0.20, "CPA": 0.20, "ROI": 0.05,
        "RPM": 0.05, "RPC": 0.05, "CPC": 0.05, "CPM": 0.05,
        "CPI": 0.10, "CR_install": 0.15,
    },
    "goal_revenue": {
        "CTR": 0.05, "CR": 0.10, "CPA": 0.10, "ROI": 0.20,
        "RPM": 0.15, "RPC": 0.15, "CPC": 0.05, "CPM": 0.05,
        "CPI": 0.05, "CR_install": 0.10,
    },
    "goal_installs": {
        "CTR": 0.05, "CR": 0.05, "CPA": 0.05, "ROI": 0.05,
        "RPM": 0.05, "RPC": 0.05, "CPC": 0.10, "CPM": 0.05,
        "CPI": 0.25, "CR_install": 0.30,
    },
}


def _sigmoid(z: pd.Series) -> pd.Series:
    """Sigmoid normalization: maps z-scores to (0, 1). z=0 → 0.5."""
    return 1.0 / (1.0 + np.exp(-z))


class TextScorer:
    """Implements the full 7-step scoring pipeline."""

    def __init__(self, params: ScoringParams):
        self.params = params

    def _get_events(self) -> List[EventConfig]:
        """Return configured events or empty list."""
        return self.params.events or []

    def _get_primary_event(self) -> Optional[EventConfig]:
        """Return the primary event (first is_primary, or first event)."""
        events = self._get_events()
        for e in events:
            if e.is_primary:
                return e
        return events[0] if events else None

    def _get_all_metrics(self, df: pd.DataFrame) -> List[str]:
        """Build list of all metrics based on available columns and events."""
        metrics = ["CTR"]
        cols = set(df.columns)

        events = self._get_events()
        has_spend = "spend" in cols and df["spend"].notna().any()
        has_revenue = "revenue" in cols and df["revenue"].notna().any() and (df["revenue"] > 0).any()

        if events:
            for ev in events:
                slot = ev.slot
                if slot in cols:
                    metrics.append(f"CR_{slot}")
                    if has_spend:
                        metrics.append(f"CPA_{slot}")
            if has_revenue:
                metrics.append("ROI")
                metrics.append("RPM")
                metrics.append("RPC")
        else:
            # Legacy: check for registrations/conversions
            has_conv = (
                ("registrations" in cols and df["registrations"].notna().any())
                or ("conversions" in cols and df["conversions"].notna().any())
            )
            if has_conv:
                metrics.append("CR")
                if has_spend:
                    metrics.append("CPA")
            if has_revenue:
                metrics.append("ROI")
                metrics.append("RPM")
                metrics.append("RPC")

        if has_spend:
            metrics.append("CPC")
            metrics.append("CPM")

        # Install metrics (auto-detected)
        has_installs = "installs" in cols and df["installs"].notna().any() and (df["installs"] > 0).any()
        if has_installs:
            metrics.append("CR_install")
            if has_spend:
                metrics.append("CPI")

        return metrics

    def _get_cost_metrics(self) -> set:
        """Return set of cost metrics (lower = better)."""
        cost = {"CPC", "CPM"}
        events = self._get_events()
        if events:
            for ev in events:
                cost.add(f"CPA_{ev.slot}")
        else:
            cost.add("CPA")
        cost.add("CPI")
        return cost

    def _resolve_weights(
        self, all_metrics: List[str], df_scored: pd.DataFrame
    ) -> Dict[str, float]:
        """
        Resolve final weights dict based on weight_mode.

        - "manual": use self.params.weights as-is (current behavior)
        - "auto": compute via CRITIC method from z-score data
        - "goal_*": expand preset weights, distributing CR/CPA across event slots
        """
        mode = self.params.weight_mode

        if mode == "manual":
            return dict(self.params.weights)

        if mode == "auto":
            from critic import compute_critic_weights
            cost_metrics = self._get_cost_metrics()
            weights = compute_critic_weights(df_scored, cost_metrics)
            return weights

        if mode in GOAL_PRESETS:
            # If frontend already sent pre-expanded weights with slot-specific keys,
            # use them directly (depth-weighted expansion done on frontend)
            provided = dict(self.params.weights)
            has_slot_keys = any(
                k.startswith("CR_") or k.startswith("CPA_")
                for k in provided
            )
            if has_slot_keys:
                return provided

            # Fallback: expand preset with equal distribution (legacy/API callers)
            preset = GOAL_PRESETS[mode]
            expanded: Dict[str, float] = {}

            for metric in all_metrics:
                if metric in preset:
                    # Direct match (CTR, ROI, RPM, RPC, CPC, CPM, or legacy CR/CPA)
                    expanded[metric] = preset[metric]
                elif metric.startswith("CR_") and metric != "CR_install" and "CR" in preset:
                    # Distribute CR weight equally across CR_event_* slots (not CR_install)
                    cr_slots = [m for m in all_metrics if m.startswith("CR_") and m != "CR_install"]
                    if cr_slots:
                        expanded[metric] = preset["CR"] / len(cr_slots)
                elif metric.startswith("CPA_") and "CPA" in preset:
                    # Distribute CPA weight equally across all CPA_event_* slots
                    cpa_slots = [m for m in all_metrics if m.startswith("CPA_")]
                    if cpa_slots:
                        expanded[metric] = preset["CPA"] / len(cpa_slots)

            return expanded

        # Fallback to manual
        return dict(self.params.weights)

    def detect_mode(self, df: pd.DataFrame) -> str:
        """
        Determine scoring mode based on available columns.
        - 'full': revenue column present
        - 'basic': spend + events/conversions present
        - 'minimal': only impressions + clicks
        """
        cols = set(df.columns)
        has_revenue = "revenue" in cols and df["revenue"].notna().any() and (df["revenue"] > 0).any()
        has_spend = "spend" in cols and df["spend"].notna().any()

        events = self._get_events()
        if events:
            has_conversions = any(
                ev.slot in cols and df[ev.slot].notna().any()
                for ev in events
            )
        else:
            has_conversions = (
                ("registrations" in cols and df["registrations"].notna().any())
                or ("conversions" in cols and df["conversions"].notna().any())
            )
            # Also check event_N columns without explicit config
            for i in range(1, 5):
                slot = f"event_{i}"
                if slot in cols and df[slot].notna().any():
                    has_conversions = True
                    break

        if has_revenue:
            return "full"
        if has_spend and has_conversions:
            return "basic"
        return "minimal"

    def compute_raw_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Step 3: Compute raw metrics with division-by-zero protection.
        Supports dynamic event columns (event_1..event_4) or legacy registrations.
        """
        df = df.copy()

        impressions = pd.to_numeric(df.get("impressions"), errors="coerce").fillna(0)
        clicks = pd.to_numeric(df.get("clicks"), errors="coerce").fillna(0)
        spend = pd.to_numeric(df.get("spend"), errors="coerce").fillna(0) if "spend" in df.columns else pd.Series(0, index=df.index)
        revenue = pd.to_numeric(df.get("revenue"), errors="coerce").fillna(0) if "revenue" in df.columns else pd.Series(0, index=df.index)

        df["_impressions"] = impressions
        df["_clicks"] = clicks
        df["_spend"] = spend
        df["_revenue"] = revenue

        # CTR = clicks / impressions
        df["CTR"] = np.where(impressions > 0, clicks / impressions, np.nan)

        # CPC = spend / clicks
        df["CPC"] = np.where(clicks > 0, spend / clicks, np.nan)

        # CPM = spend / impressions * 1000
        df["CPM"] = np.where(impressions > 0, spend / impressions * 1000, np.nan)

        # Install metrics (auto-detected from data)
        installs = pd.to_numeric(df.get("installs"), errors="coerce").fillna(0) if "installs" in df.columns else pd.Series(0, index=df.index)
        df["_installs"] = installs
        # CR_install = installs / clicks
        df["CR_install"] = np.where(clicks > 0, installs / clicks, np.nan)
        # CPI = spend / installs
        df["CPI"] = np.where(installs > 0, spend / installs, np.nan)

        has_rev = revenue > 0

        events = self._get_events()
        if events:
            # Dynamic event metrics
            for ev in events:
                slot = ev.slot
                if slot not in df.columns:
                    continue
                ev_values = pd.to_numeric(df[slot], errors="coerce").fillna(0)
                df[f"_conv_{slot}"] = ev_values

                # CR_event_N = event / clicks
                df[f"CR_{slot}"] = np.where(clicks > 0, ev_values / clicks, np.nan)
                # CPA_event_N = spend / event
                df[f"CPA_{slot}"] = np.where(ev_values > 0, spend / ev_values, np.nan)

            # Revenue metrics
            df["ROI"] = np.where((spend > 0) & has_rev, (revenue - spend) / spend, np.nan)
            df["RPM"] = np.where((impressions > 0) & has_rev, revenue / impressions * 1000, np.nan)
            df["RPC"] = np.where((clicks > 0) & has_rev, revenue / clicks, np.nan)
        else:
            # Legacy: registrations/conversions
            if "registrations" in df.columns:
                conversions = pd.to_numeric(df["registrations"], errors="coerce").fillna(0)
            elif "conversions" in df.columns:
                conversions = pd.to_numeric(df["conversions"], errors="coerce").fillna(0)
            else:
                conversions = pd.Series(0, index=df.index)

            df["_conversions"] = conversions

            # CR = conversions / clicks
            df["CR"] = np.where(clicks > 0, conversions / clicks, np.nan)
            # CPA = spend / conversions
            df["CPA"] = np.where(conversions > 0, spend / conversions, np.nan)
            # Revenue metrics (legacy)
            df["ROI"] = np.where((spend > 0) & has_rev, (revenue - spend) / spend, np.nan)
            df["RPM"] = np.where((impressions > 0) & has_rev, revenue / impressions * 1000, np.nan)
            df["RPC"] = np.where((clicks > 0) & has_rev, revenue / clicks, np.nan)

        return df

    def _filter_by_thresholds(self, df: pd.DataFrame) -> tuple:
        """Step 2: Apply minimum threshold filters. Returns (filtered_df, excluded_texts)."""
        mask = pd.Series(True, index=df.index)

        # Track per-row exclusion reasons
        imp_fail = pd.Series(False, index=df.index)
        clk_fail = pd.Series(False, index=df.index)
        conv_fail = pd.Series(False, index=df.index)

        if "_impressions" in df.columns:
            imp_fail = df["_impressions"] < self.params.min_impressions
            mask &= ~imp_fail

        if "_clicks" in df.columns:
            clk_fail = df["_clicks"] < self.params.min_clicks
            mask &= ~clk_fail

        # Filter by min_conversions on the selected event (or primary by default)
        conv_col = None
        if self.params.min_conversions > 0:
            if self.params.min_conversions_event:
                conv_col = f"_conv_{self.params.min_conversions_event}"
            else:
                primary = self._get_primary_event()
                if primary:
                    conv_col = f"_conv_{primary.slot}"

            if conv_col and conv_col in df.columns:
                conv_fail = df[conv_col] < self.params.min_conversions
                mask &= ~conv_fail
            elif "_conversions" in df.columns:
                conv_col = "_conversions"
                conv_fail = df["_conversions"] < self.params.min_conversions
                mask &= ~conv_fail

        # Build excluded texts list
        excluded_texts: List[ExcludedText] = []
        excluded_df = df[~mask]
        for idx, row in excluded_df.iterrows():
            reasons = []
            if imp_fail.loc[idx]:
                reasons.append(f"Показов: {int(row.get('_impressions', 0))} < {self.params.min_impressions}")
            if clk_fail.loc[idx]:
                reasons.append(f"Кликов: {int(row.get('_clicks', 0))} < {self.params.min_clicks}")
            if conv_fail.loc[idx]:
                n_conv = int(row.get(conv_col, 0)) if conv_col and conv_col in row.index else 0
                reasons.append(f"Конверсий: {n_conv} < {self.params.min_conversions}")

            # Get primary conversion count
            n_conv_val = 0
            primary = self._get_primary_event()
            if primary and f"_conv_{primary.slot}" in row.index:
                n_conv_val = int(row.get(f"_conv_{primary.slot}", 0))
            elif "_conversions" in row.index:
                n_conv_val = int(row.get("_conversions", 0))

            excluded_texts.append(ExcludedText(
                text_id=str(row.get("text_id", idx)),
                headline=str(row.get("headline", "")),
                reason="; ".join(reasons) if reasons else "Не прошёл фильтры",
                n_impressions=int(row.get("_impressions", 0)),
                n_clicks=int(row.get("_clicks", 0)),
                n_conversions=n_conv_val,
            ))

        filtered = df[mask].copy()
        n_excluded = len(df) - len(filtered)
        if n_excluded > 0:
            logger.info("Filtered out %d rows below thresholds", n_excluded)

        return filtered, excluded_texts

    def winsorize(self, df: pd.DataFrame, all_metrics: List[str]) -> pd.DataFrame:
        """
        Step 4: Winsorize each metric to clip outliers.
        """
        df = df.copy()
        lower = self.params.winsorize_lower
        upper = self.params.winsorize_upper

        for metric in all_metrics:
            if metric in df.columns:
                series = df[metric].dropna()
                if len(series) < 2:
                    continue
                lo = series.quantile(lower)
                hi = series.quantile(upper)
                df[metric] = df[metric].clip(lo, hi)

        return df

    def compute_zscores(self, df: pd.DataFrame, all_metrics: List[str]) -> pd.DataFrame:
        """
        Step 5: Z-score normalization with sigmoid.
        - z = (x - mean) / std (ddof=1, sample std)
        - Invert cost metrics: multiply z by -1
        - Normalize via sigmoid: z_norm = 1 / (1 + exp(-z))
        """
        df = df.copy()
        cost_metrics = self._get_cost_metrics()

        for metric in all_metrics:
            z_col = f"z_{metric}"
            if metric not in df.columns:
                continue

            series = df[metric]
            valid = series.dropna()

            if len(valid) == 0:
                df[z_col] = np.nan
                continue

            mean_val = valid.mean()
            std_val = valid.std(ddof=1)  # Sample std (Bessel's correction)

            if std_val > 0:
                z = (series - mean_val) / std_val
            else:
                z = pd.Series(0.0, index=df.index)
                z[series.isna()] = np.nan

            # Invert cost metrics
            if metric in cost_metrics:
                z = -z

            # Sigmoid normalization to (0, 1)
            z_norm = _sigmoid(z)
            z_norm[series.isna()] = np.nan

            # For conversion rate metrics (CR_*), treat exact zero as "no data"
            # rather than "worst performer" — zero conversions usually means
            # insufficient funnel depth, not a meaningful measurement
            if metric.startswith("CR_") or metric == "CR":
                z_norm[series == 0] = np.nan

            df[z_col] = z_norm

        return df

    def compute_composite(
        self,
        df: pd.DataFrame,
        all_metrics: List[str],
        weights: Optional[Dict[str, float]] = None,
    ) -> pd.DataFrame:
        """
        Step 6: Weighted composite score.
        Weights are renormalized to sum to 1.0 across available metrics.
        If no metrics available for a row, score = 0.5.
        """
        df = df.copy()
        weights = weights or self.params.weights

        def _row_composite(row: pd.Series) -> float:
            available = {}
            for metric in all_metrics:
                z_col = f"z_{metric}"
                if z_col in row.index and pd.notna(row[z_col]):
                    # Find matching weight
                    w = weights.get(metric, 0)
                    if w == 0:
                        # Try base metric name (e.g., CR_event_1 → CR)
                        base = metric.split("_")[0] if "_" in metric else metric
                        w = weights.get(base, 0)
                    if w > 0:
                        available[metric] = (row[z_col], w)

            if not available:
                return 0.5

            total_w = sum(w for _, w in available.values())
            if total_w == 0:
                return 0.5

            score = sum(z * (w / total_w) for z, w in available.values())
            return round(float(score), 4)

        df["composite_score"] = df.apply(_row_composite, axis=1)
        return df

    def _compute_score_layers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Step 6.5 (v2.1): Compute 4 score layers from composite_score.
        - decision_score: = composite_score (sigmoid-based, for verdicts)
        - ranking_score:  fractional rank (rank-1)/(n-1) for UI sorting
        - benchmark_score: sigmoid((score - median) / std)
        - relative_score: percentile rank within batch
        - reliability_score: min(1, clicks / 200)
        - decision_confidence: "high"/"medium"/"low" based on clicks
        """
        df = df.copy()
        n = len(df)

        # decision_score = composite_score (identity for now, fatigue adjusts later)
        df["decision_score"] = df["composite_score"]

        # ranking_score: fractional rank (0 to 1, uniform distribution)
        if n > 1:
            ranks = df["composite_score"].rank(method="average")
            df["ranking_score"] = (ranks - 1) / (n - 1)
        else:
            df["ranking_score"] = 0.5

        # benchmark_score: distance from batch median, normalized via sigmoid
        median_score = df["composite_score"].median()
        std_score = df["composite_score"].std(ddof=1) if n > 1 else 1.0
        if std_score > 0:
            z_from_median = (df["composite_score"] - median_score) / std_score
            df["benchmark_score"] = 1.0 / (1.0 + np.exp(-z_from_median))
        else:
            df["benchmark_score"] = 0.5

        # relative_score: percentile within batch
        if n > 1:
            df["relative_score"] = df["composite_score"].rank(pct=True)
        else:
            df["relative_score"] = 0.5

        # reliability_score: click-based reliability (0 to 1)
        df["reliability_score"] = np.minimum(1.0, df["_clicks"].astype(float) / 200.0)

        # decision_confidence: categorical confidence level
        df["decision_confidence"] = df["_clicks"].apply(
            lambda c: "high" if c >= 200 else ("medium" if c >= 50 else "low")
        )

        return df

    def assign_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Step 7: Hybrid category assignment — percentile + quality floor.
        Prevents bad texts from getting TOP/GOOD just because of percentile.
        Also assigns alt_category with 5-level Russian labels.
        """
        df = df.copy()
        scores = df["composite_score"].values
        top_floor = self.params.score_quality_floor
        good_floor = self.params.good_quality_floor

        def _percentile_rank(score: float) -> float:
            if len(scores) <= 1:
                return 0.5
            return float(np.sum(scores <= score)) / len(scores)

        def _assign(score: float) -> str:
            pctile = _percentile_rank(score)
            if pctile >= self.params.top_threshold and score >= top_floor:
                return "TOP"
            if pctile >= 0.50 and score >= good_floor:
                return "GOOD"
            if pctile >= self.params.low_threshold:
                return "AVERAGE"
            return "LOW"

        def _alt_category(score: float) -> str:
            """5-level Russian labels from the Google Sheet approach."""
            # Map sigmoid score to z-score-like range for thresholds
            # sigmoid(0.8) ≈ 0.69, sigmoid(-0.8) ≈ 0.31
            if score >= 0.69:
                return "Сильный +"
            if score >= 0.55:
                return "Слабый +"
            if score >= 0.45:
                return "≈Средний"
            if score >= 0.31:
                return "Слабый -"
            return "Сильный -"

        df["category"] = df["composite_score"].apply(_assign)
        df["alt_category"] = df["composite_score"].apply(_alt_category)
        return df

    def _categorize_score(self, score: float, all_scores: list) -> str:
        """Re-categorize a single score against the full batch (post-fatigue)."""
        scores = np.array(all_scores)
        top_floor = self.params.score_quality_floor
        good_floor = self.params.good_quality_floor
        if len(scores) <= 1:
            pctile = 0.5
        else:
            pctile = float(np.sum(scores <= score)) / len(scores)
        if pctile >= self.params.top_threshold and score >= top_floor:
            return "TOP"
        if pctile >= 0.50 and score >= good_floor:
            return "GOOD"
        if pctile >= self.params.low_threshold:
            return "AVERAGE"
        return "LOW"

    @staticmethod
    def _alt_category(score: float) -> str:
        """5-level Russian alt-category for a single score."""
        if score >= 0.69:
            return "Сильный +"
        if score >= 0.55:
            return "Слабый +"
        if score >= 0.45:
            return "≈Средний"
        if score >= 0.31:
            return "Слабый -"
        return "Сильный -"

    def _get_available_metrics(self, df: pd.DataFrame, all_metrics: List[str]) -> List[str]:
        """Return list of metrics that have at least some non-NaN z_scores."""
        available = []
        for metric in all_metrics:
            z_col = f"z_{metric}"
            if z_col in df.columns and df[z_col].notna().any():
                available.append(metric)
        return available

    def score(self, df: pd.DataFrame) -> ScoringResult:
        """
        Execute the full 7-step scoring pipeline.
        Returns ScoringResult with per-text results and summary stats.
        """
        n_total = len(df)
        mode = self.detect_mode(df)
        logger.info("Scoring mode: %s, rows: %d", mode, n_total)

        # Step 3: Compute raw metrics
        df = self.compute_raw_metrics(df)

        # Determine all metrics for this run
        all_metrics = self._get_all_metrics(df)
        cost_metrics = self._get_cost_metrics()

        # Step 2: Filter by thresholds
        df_scored, excluded_texts = self._filter_by_thresholds(df)
        n_excluded = n_total - len(df_scored)

        # Edge case: no rows after filtering
        if len(df_scored) == 0:
            return ScoringResult(
                results=[],
                stats={
                    "n_total": n_total,
                    "n_scored": 0,
                    "n_excluded": n_excluded,
                    "n_top": 0,
                    "n_low": 0,
                    "mode": mode,
                    "score_mean": 0,
                    "score_std": 0,
                    "excluded_texts": [et.model_dump() for et in excluded_texts],
                },
            )

        # Edge case: single row after filtering
        if len(df_scored) == 1:
            row = df_scored.iloc[0]
            warnings_list = ["insufficient_data"]
            text_id = str(row.get("text_id", row.name))
            headline_val = str(row.get("headline", ""))

            raw_metrics = {}
            for m in all_metrics:
                if m in row.index and pd.notna(row[m]):
                    raw_metrics[m] = round(float(row[m]), 6)

            result = TextResult(
                text_id=text_id,
                headline=headline_val,
                composite_score=0.5,
                category="AVERAGE",
                alt_category="≈Средний",
                mode=mode,
                metrics=raw_metrics,
                z_scores={},
                warnings=warnings_list,
                n_impressions=int(row.get("_impressions", 0)),
                n_clicks=int(row.get("_clicks", 0)),
            )
            return ScoringResult(
                results=[result],
                stats={
                    "n_total": n_total,
                    "n_scored": 1,
                    "n_excluded": n_excluded,
                    "n_top": 0,
                    "n_low": 0,
                    "mode": mode,
                    "score_mean": 0.5,
                    "score_std": 0.0,
                },
            )

        # Step 4: Winsorize
        df_scored = self.winsorize(df_scored, all_metrics)

        # Step 4.5: Empirical Bayes smoothing (v2.1)
        from smoothing import smooth_rates
        df_scored = smooth_rates(df_scored, self._get_events())
        # Replace raw rate metrics with smoothed versions for z-score input
        for metric in list(all_metrics):
            smoothed_col = f"{metric}_smoothed"
            if smoothed_col in df_scored.columns:
                df_scored[f"{metric}_raw"] = df_scored[metric]
                df_scored[metric] = df_scored[smoothed_col]

        # Step 5: Z-score normalization (sigmoid)
        df_scored = self.compute_zscores(df_scored, all_metrics)

        # Step 5.5: Resolve weights based on weight_mode
        resolved_weights = self._resolve_weights(all_metrics, df_scored)

        # Step 6: Composite score (using resolved weights)
        df_scored = self.compute_composite(df_scored, all_metrics, weights=resolved_weights)

        # Step 6.5: Compute score layers (v2.1)
        df_scored = self._compute_score_layers(df_scored)

        # Step 7: Assign categories (hybrid)
        df_scored = self.assign_categories(df_scored)

        available_metrics = self._get_available_metrics(df_scored, all_metrics)
        logger.info("Available metrics for scoring: %s", available_metrics)

        # Pre-compute per-metric std devs for A/B t-tests
        metric_stds: Dict[str, float] = {}
        for m in all_metrics:
            if m in df_scored.columns:
                col = df_scored[m].dropna()
                if len(col) > 1:
                    metric_stds[m] = round(float(col.std(ddof=1)), 6)

        # Dynamic reliability threshold: max(100, min(Q1, 300))
        # Floor = 100 (minimum for any metric), cap = 300 (достаточно для надёжной статистики)
        all_clicks = df_scored["_clicks"].dropna().values
        q1_clicks = int(np.percentile(all_clicks, 25)) if len(all_clicks) > 0 else 100
        reliability_threshold = max(100, min(q1_clicks, 300))

        # Build results list
        results: List[TextResult] = []
        for _, row in df_scored.iterrows():
            text_id = str(row.get("text_id", row.name))
            headline_val = str(row.get("headline", ""))

            raw_metrics: Dict[str, Optional[float]] = {}
            z_scores_dict: Dict[str, Optional[float]] = {}
            warnings_list: List[str] = []

            for m in all_metrics:
                if m in row.index and pd.notna(row[m]):
                    raw_metrics[m] = round(float(row[m]), 6)
                z_col = f"z_{m}"
                if z_col in row.index and pd.notna(row[z_col]):
                    z_scores_dict[m] = round(float(row[z_col]), 4)

            # Warning for small samples (dynamic threshold)
            n_imp = int(row.get("_impressions", 0))
            n_clk = int(row.get("_clicks", 0))
            if n_clk < reliability_threshold:
                warnings_list.append("insufficient_sample")

            # Segment fields
            campaign_val = str(row.get("campaign", "")) if "campaign" in row.index and pd.notna(row.get("campaign")) else ""
            platform_val = str(row.get("platform", "")) if "platform" in row.index and pd.notna(row.get("platform")) else ""
            device_val = str(row.get("device", "")) if "device" in row.index and pd.notna(row.get("device")) else ""

            results.append(
                TextResult(
                    text_id=text_id,
                    headline=headline_val,
                    composite_score=round(float(row["composite_score"]), 4),
                    # v2.1 score layers
                    decision_score=round(float(row.get("decision_score", row["composite_score"])), 4),
                    ranking_score=round(float(row.get("ranking_score", row["composite_score"])), 4),
                    benchmark_score=round(float(row.get("benchmark_score", row["composite_score"])), 4),
                    relative_score=round(float(row.get("relative_score", 0.5)), 4),
                    reliability_score=round(float(row.get("reliability_score", 0)), 4),
                    decision_confidence=str(row.get("decision_confidence", "low")),
                    category=str(row["category"]),
                    alt_category=str(row.get("alt_category", "")),
                    mode=mode,
                    metrics=raw_metrics,
                    z_scores=z_scores_dict,
                    warnings=warnings_list,
                    n_impressions=n_imp,
                    n_clicks=n_clk,
                    campaign=campaign_val,
                    platform=platform_val,
                    device=device_val,
                    std_metrics=metric_stds,
                )
            )

        # Step 7.5: Anomaly detection (v2.1 — BEFORE verdicts)
        from anomaly import detect_anomalies
        detect_anomalies(results, self._get_events())

        # Step 7.6: Problem type classification (v2.1)
        from problem_type import classify_problem_types, classify_traffic_proxy
        classify_problem_types(results, self._get_events())

        # Step 7.7: Traffic quality proxy typing (v2.1 G1)
        classify_traffic_proxy(results, self._get_events())

        # Step 8: Generate verdicts (thresholds from params)
        generate_verdicts(results, self._get_events(), params=self.params)

        # Step 8.5: Financial impact (v2.1 — needs verdicts for SCALE pool)
        from financial import compute_financial_impact
        compute_financial_impact(results, self._get_events(), params=self.params)

        # Step 8.6: Fatigue detection (v2.1 — adjusts decision_score)
        from fatigue import compute_fatigue
        compute_fatigue(results, df_scored, params=self.params)
        # Post-fatigue: re-assign categories based on adjusted composite_score
        # (fatigue may have lowered scores, categories must reflect that)
        for r in results:
            r.category = self._categorize_score(r.composite_score, [x.composite_score for x in results])
            r.alt_category = self._alt_category(r.composite_score)

        # Step 8.7: Statistical enrichment (v2.2 — CTR CI, p-values, BH correction)
        from stats_enrichment import enrich_with_statistics
        enrich_with_statistics(results)

        # Post-fatigue verdict override: SCALE + fatigue → OPTIMIZE_FATIGUE
        for r in results:
            if (r.fatigue_penalty and r.fatigue_penalty > 0.05
                    and r.verdict and r.verdict.verdict == "Масштабировать"):
                r.verdict.verdict = "OPTIMIZE_FATIGUE"
                r.verdict.reason = "Усталость креатива — рекомендуется обновить"
                r.verdict.reason_type = "усталость"

        # Summary stats — use actual result scores (may be fatigue-adjusted)
        scores_array = np.array([r.composite_score for r in results])
        category_counts = pd.Series([r.category for r in results]).value_counts()
        stats = {
            "n_total": n_total,
            "n_scored": len(df_scored),
            "n_excluded": n_excluded,
            "n_top": int(category_counts.get("TOP", 0)),
            "n_low": int(category_counts.get("LOW", 0)),
            "mode": mode,
            "score_mean": round(float(np.mean(scores_array)), 4),
            "score_std": round(float(np.std(scores_array, ddof=1)), 4) if len(scores_array) > 1 else 0.0,
            "weights_used": resolved_weights,
            "weight_mode": self.params.weight_mode,
            "reliability_threshold": reliability_threshold,
        }
        events = self._get_events()
        if events:
            stats["event_labels"] = {ev.slot: ev.label for ev in events}

        # Verdict distribution
        verdict_counts = {}
        for r in results:
            if r.verdict:
                v = r.verdict.verdict
                verdict_counts[v] = verdict_counts.get(v, 0) + 1
        stats["verdict_distribution"] = verdict_counts

        # Segment unique values (for UI filters)
        segments = {}
        for seg_col in ("campaign", "platform", "device"):
            if seg_col in df_scored.columns:
                vals = df_scored[seg_col].dropna().astype(str).unique().tolist()
                vals = sorted([v for v in vals if v and v != "nan"])
                if vals:
                    segments[seg_col] = vals
        if segments:
            stats["segments"] = segments

        # Excluded texts info
        stats["excluded_texts"] = [et.model_dump() for et in excluded_texts]

        # Step 9: Generate cross-text insights
        from insights import generate_insights
        insights_list = generate_insights(results, stats)
        stats["insights"] = [ins.model_dump() for ins in insights_list]

        return ScoringResult(results=results, stats=stats)
