"""
Run 12 CSV datasets through the scoring pipeline DIRECTLY (no HTTP/auth needed).

Usage:
  1. Generate CSVs: python3 generate_usability_data.py
  2. Run scoring: python3 run_usability_scoring.py
"""

import json
import os
import sys

# Add backend to path
BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)

import pandas as pd
from models import ScoringParams, EventConfig
from scorer import TextScorer

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "usability_scoring_results.json")
CSV_DIR = os.path.join(os.path.dirname(__file__), "usability_csvs")

SCENARIO_CONFIGS = {
    "01_best_performer.csv": {"weight_mode": "goal_conversions"},
    "02_budget_cut.csv": {"weight_mode": "goal_conversions"},
    "03_creative_fatigue.csv": {"weight_mode": "goal_conversions"},
    "04_anomaly_detection.csv": {"weight_mode": "goal_conversions"},
    "05_campaign_verdict.csv": {"weight_mode": "goal_conversions", "campaign_analysis": True},
    "06_high_ctr_trap.csv": {"weight_mode": "goal_conversions"},
    "07_roi_vs_volume.csv": {"weight_mode": "goal_revenue"},
    "08_borderline_decision.csv": {"weight_mode": "goal_conversions"},
    "09_new_vs_proven.csv": {"weight_mode": "goal_conversions"},
    "10_platform_split.csv": {"weight_mode": "goal_conversions"},
    "11_cost_outlier.csv": {"weight_mode": "goal_revenue"},
    "12_mixed_signals.csv": {"weight_mode": "goal_conversions"},
}


def result_to_dict(r):
    """Convert a TextResult to a JSON-serializable dict."""
    d = {}
    for attr in [
        "text_id", "headline", "composite_score", "decision_score",
        "ranking_score", "benchmark_score", "relative_score", "reliability_score",
        "decision_confidence", "anomaly_detected", "anomaly_code",
        "excess_cost", "missed_conversions", "revenue_gap",
        "real_savings", "real_savings_adjusted",
        "target_source", "problem_type", "metric_pattern", "pattern_confidence",
        "fatigue_score", "fatigue_penalty", "declining_recently",
        "category", "alt_category", "mode",
        "n_impressions", "n_clicks",
        "campaign", "platform", "device",
        "ctr_ci_low", "ctr_ci_high", "prob_ctr_better", "ctr_pvalue",
        "is_significant_bh",
        "cr_ci_low", "cr_ci_high", "prob_cr_better",
    ]:
        val = getattr(r, attr, None)
        if val is not None:
            # Handle numpy types
            if hasattr(val, "item"):
                val = val.item()
            if isinstance(val, float) and (val != val):  # NaN check
                val = None
            d[attr] = val

    # Dict fields
    for attr in ["metrics", "z_scores", "std_metrics"]:
        raw = getattr(r, attr, None)
        if raw:
            cleaned = {}
            for k, v in raw.items():
                if hasattr(v, "item"):
                    v = v.item()
                if isinstance(v, float) and (v != v):
                    v = None
                cleaned[k] = v
            d[attr] = cleaned

    # Verdict
    v = getattr(r, "verdict", None)
    if v:
        if hasattr(v, "__dict__"):
            vd = {}
            for k in ["verdict", "reason", "reason_type", "reason_detail", "strengths", "weaknesses"]:
                val = getattr(v, k, None)
                if val is not None:
                    vd[k] = val
            d["verdict"] = vd
        elif isinstance(v, dict):
            d["verdict"] = v

    # Warnings
    w = getattr(r, "warnings", None)
    if w:
        d["warnings"] = list(w)

    return d


def process_scenario(csv_filename, config):
    filepath = os.path.join(CSV_DIR, csv_filename)
    if not os.path.exists(filepath):
        print(f"  ✗ File not found: {filepath}")
        return None

    df = pd.read_csv(filepath)
    print(f"  📁 Loaded: {len(df)} rows, columns: {list(df.columns)}")

    # Build events config
    events = []
    if "event_1" in df.columns:
        events.append(EventConfig(slot="event_1", label="Конверсии", column="event_1", is_primary=True))

    params = ScoringParams(
        weight_mode=config.get("weight_mode", "goal_conversions"),
        events=events,
    )

    scorer = TextScorer(params)
    scoring_result = scorer.score(df)

    results_dicts = [result_to_dict(r) for r in scoring_result.results]

    # Verdicts summary
    verdicts = {}
    for r in results_dicts:
        v = r.get("verdict", {})
        vname = v.get("verdict", "?") if isinstance(v, dict) else "?"
        verdicts[vname] = verdicts.get(vname, 0) + 1
    print(f"  ✓ Scored: {len(results_dicts)} ads")
    print(f"  📊 Verdicts: {verdicts}")

    # Print details
    for r in results_dicts:
        v = r.get("verdict", {})
        score = r.get("composite_score")
        score_str = f"{score:.3f}" if score is not None else "N/A"
        print(f"    {r.get('text_id', '?'):>8} | score={score_str} | {v.get('verdict', '?'):20s} | {r.get('headline', '')}")

    result = {
        "csv": csv_filename,
        "weight_mode": config.get("weight_mode"),
        "stats": scoring_result.stats,
        "results": results_dicts,
    }

    # Campaign analysis
    if config.get("campaign_analysis") and "campaign" in df.columns:
        try:
            from campaign import CampaignAnalyzer
            analyzer = CampaignAnalyzer(params)
            camp_result = analyzer.analyze(df, scoring_result.results)
            # Convert campaign results
            camp_dicts = []
            for c in camp_result.get("campaigns", []):
                if hasattr(c, "__dict__"):
                    cd = {}
                    for k, v in c.__dict__.items():
                        if hasattr(v, "item"):
                            v = v.item()
                        if hasattr(v, "__dict__"):
                            v = v.__dict__
                        cd[k] = v
                    camp_dicts.append(cd)
                elif isinstance(c, dict):
                    camp_dicts.append(c)
            result["campaign_analysis"] = {
                "campaigns": camp_dicts,
                "n_campaigns": len(camp_dicts),
            }
            print(f"  🏢 Campaigns: {len(camp_dicts)} analyzed")
        except Exception as e:
            print(f"  ⚠ Campaign analysis error: {e}")

    return result


def main():
    if not os.path.exists(CSV_DIR):
        print(f"✗ CSV directory not found: {CSV_DIR}")
        print("  Run generate_usability_data.py first")
        sys.exit(1)

    all_results = {}
    csv_files = sorted(SCENARIO_CONFIGS.keys())

    for i, csv_file in enumerate(csv_files, 1):
        config = SCENARIO_CONFIGS[csv_file]
        scenario_id = csv_file.replace(".csv", "").split("_", 1)[1]
        print(f"\n{'='*60}")
        print(f"[{i}/12] Scenario: {scenario_id} ({csv_file})")
        print(f"{'='*60}")

        result = process_scenario(csv_file, config)
        if result:
            all_results[scenario_id] = result
        else:
            print(f"  ✗ FAILED for {csv_file}")

    # Save results
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n{'='*60}")
    print(f"Done! {len(all_results)}/12 scenarios processed")
    print(f"Results saved to: {OUTPUT_FILE}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
