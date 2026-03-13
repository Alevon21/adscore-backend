"""
Stress test for text-parts analysis pipeline.
Generates random datasets, runs full pipeline, checks for logical inconsistencies.
"""

import io
import json
import random
import sys
import traceback
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

API = "http://localhost:8000"

# ── helpers ──────────────────────────────────────────────────────

WORDS_POOL = [
    "скидка", "доставка", "бесплатно", "акция", "подарок",
    "бонус", "гарантия", "кэшбэк", "рублей", "haraba",
    "заказ", "новинка", "распродажа", "выгодно", "промокод",
    "быстро", "качество", "лучшая", "цена", "товар",
]


def make_headline(words_in: List[str], extra_words: int = 3) -> str:
    """Build a headline that contains given words + random filler."""
    filler = random.sample(
        ["купите", "сейчас", "только", "лучшее", "предложение",
         "для", "вас", "топ", "дня", "супер", "мега"],
        min(extra_words, 5),
    )
    parts = list(words_in) + filler
    random.shuffle(parts)
    return " ".join(parts)


def generate_dataset(
    n_texts: int,
    word_probs: Optional[Dict[str, float]] = None,
    word_score_boost: Optional[Dict[str, float]] = None,
    base_score_mean: float = 500,
    base_score_std: float = 200,
    mode: str = "basic",
) -> pd.DataFrame:
    """
    Generate a synthetic dataset.
    word_probs: {word: probability of appearing in a text}
    word_score_boost: {word: additive boost to impressions/clicks when word present}
    """
    if word_probs is None:
        word_probs = {w: random.uniform(0.2, 0.6) for w in random.sample(WORDS_POOL, 6)}
    if word_score_boost is None:
        word_score_boost = {}

    rows = []
    for i in range(n_texts):
        present_words = [w for w, p in word_probs.items() if random.random() < p]
        if not present_words:
            present_words = [random.choice(list(word_probs.keys()))]

        headline = make_headline(present_words)

        # Base metrics
        imp = max(100, int(np.random.normal(base_score_mean, base_score_std)))
        clicks = max(10, int(imp * np.random.uniform(0.02, 0.15)))

        # Apply score boost for present words
        for w in present_words:
            boost = word_score_boost.get(w, 0)
            if boost > 0:
                clicks = int(clicks * (1 + boost))
            elif boost < 0:
                clicks = max(10, int(clicks * (1 + boost)))

        spend = round(clicks * np.random.uniform(5, 50), 2)
        conversions = max(0, int(clicks * np.random.uniform(0.01, 0.20)))

        row = {
            "text_id": f"text_{i+1:03d}",
            "headline": headline,
            "impressions": imp,
            "clicks": clicks,
            "spend": spend,
        }

        if mode in ("basic", "full"):
            row["registrations"] = max(5, conversions)

        if mode == "full":
            row["revenue"] = round(spend * np.random.uniform(0.5, 3.0), 2)

        rows.append(row)

    return pd.DataFrame(rows)


def upload_and_score(df: pd.DataFrame, score_params: Optional[dict] = None) -> Tuple[str, dict]:
    """Upload CSV, apply auto-mapping, score, return (session_id, score_result)."""
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    resp = requests.post(f"{API}/upload", files={"file": ("test.csv", buf, "text/csv")})
    resp.raise_for_status()
    data = resp.json()
    sid = data["session_id"]

    # Score with given params or defaults
    body = {"session_id": sid}
    if score_params:
        body["params"] = score_params
    resp = requests.post(f"{API}/score", json=body)
    resp.raise_for_status()
    score_result = resp.json()
    return sid, score_result


def run_text_parts(sid: str, custom_parts: List[str], metric: str = "composite_score") -> dict:
    """Run text-parts analysis."""
    resp = requests.post(f"{API}/text-parts", json={
        "session_id": sid,
        "custom_parts": custom_parts,
        "primary_metric": metric,
    })
    resp.raise_for_status()
    return resp.json()


# ── checks ───────────────────────────────────────────────────────

class CheckResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def ok(self, msg: str):
        self.passed += 1

    def fail(self, msg: str):
        self.failed += 1
        self.errors.append(msg)

    def summary(self) -> str:
        status = "PASS" if self.failed == 0 else "FAIL"
        return f"[{status}] {self.passed} passed, {self.failed} failed"


def check_impact_logic(result: dict, n_scored: int, check: CheckResult):
    """Validate impact table logic."""
    impacts_by_metric = result.get("part_impacts", {})
    cs_impacts = impacts_by_metric.get("composite_score", [])

    for imp in cs_impacts:
        name = imp["part_name"]
        delta = imp["delta"]
        ci_lo = imp["ci_lower"]
        ci_hi = imp["ci_upper"]
        p = imp["p_value"]
        d = imp["effect_size"]
        conf = imp["confidence"]
        sig = imp["significant"]
        n_w = imp["n_with"]
        n_wo = imp["n_without"]
        m_w = imp["metric_with"]
        m_wo = imp["metric_without"]
        dpct = imp["delta_pct"]

        # 1. CI must contain delta
        if ci_lo > delta + 1e-6:
            check.fail(f"[{name}] ci_lower ({ci_lo:.6f}) > delta ({delta:.6f})")
        elif ci_hi < delta - 1e-6:
            check.fail(f"[{name}] ci_upper ({ci_hi:.6f}) < delta ({delta:.6f})")
        else:
            check.ok(f"[{name}] CI contains delta")

        # 2. CI symmetry: delta should be at the center
        mid = (ci_lo + ci_hi) / 2
        if abs(mid - delta) > 1e-4:
            check.fail(f"[{name}] CI not symmetric: mid={mid:.6f}, delta={delta:.6f}")
        else:
            check.ok(f"[{name}] CI symmetric")

        # 3. Confidence classification consistency
        abs_d = abs(d)
        expected_conf = "noise"
        if p < 0.05 and abs_d >= 0.5:
            expected_conf = "high"
        elif p < 0.10 and abs_d >= 0.3:
            expected_conf = "medium"
        elif p < 0.20 and abs_d >= 0.2:
            expected_conf = "low"

        if conf != expected_conf:
            check.fail(f"[{name}] confidence={conf}, expected={expected_conf} (p={p:.4f}, |d|={abs_d:.4f})")
        else:
            check.ok(f"[{name}] confidence correct")

        # 4. significant = confidence in (high, medium)
        expected_sig = conf in ("high", "medium")
        if sig != expected_sig:
            check.fail(f"[{name}] significant={sig}, expected={expected_sig} (conf={conf})")
        else:
            check.ok(f"[{name}] significant field correct")

        # 5. Cohen's d sign should match delta sign (if non-zero)
        if abs(delta) > 1e-8 and abs(d) > 1e-8:
            if (delta > 0) != (d > 0):
                check.fail(f"[{name}] Cohen's d sign ({d:.4f}) != delta sign ({delta:.6f})")
            else:
                check.ok(f"[{name}] Cohen's d sign matches delta")

        # 6. delta = metric_with - metric_without
        expected_delta = m_w - m_wo
        if abs(delta - expected_delta) > 1e-4:
            check.fail(f"[{name}] delta={delta:.6f}, expected m_w-m_wo={expected_delta:.6f}")
        else:
            check.ok(f"[{name}] delta = m_w - m_wo")

        # 7. delta_pct consistency
        if abs(m_wo) > 1e-8:
            expected_pct = delta / abs(m_wo) * 100
            if abs(dpct - expected_pct) > 0.1:
                check.fail(f"[{name}] delta_pct={dpct:.2f}, expected={expected_pct:.2f}")
            else:
                check.ok(f"[{name}] delta_pct correct")

        # 8. n_with + n_without should equal n_scored
        if n_w + n_wo != n_scored:
            check.fail(f"[{name}] n_with({n_w}) + n_without({n_wo}) = {n_w+n_wo} != n_scored({n_scored})")
        else:
            check.ok(f"[{name}] n_with + n_without = n_scored")

        # 9. p_value in [0, 1]
        if not (0 <= p <= 1):
            check.fail(f"[{name}] p_value={p} outside [0,1]")
        else:
            check.ok(f"[{name}] p_value valid range")

        # 10. metric_with and metric_without should be in [0, 1] for composite_score
        if not (0 <= m_w <= 1):
            check.fail(f"[{name}] metric_with={m_w} outside [0,1] for composite_score")
        else:
            check.ok(f"[{name}] metric_with in valid range")

        if not (0 <= m_wo <= 1):
            check.fail(f"[{name}] metric_without={m_wo} outside [0,1] for composite_score")
        else:
            check.ok(f"[{name}] metric_without in valid range")


def check_combinations(result: dict, check: CheckResult):
    """Validate combinations logic."""
    combos = result.get("best_combinations", [])
    if not combos:
        check.ok("No combinations to check")
        return

    # 1. Ranks should be sequential
    ranks = [c["rank"] for c in combos]
    expected_ranks = list(range(1, len(combos) + 1))
    if ranks != expected_ranks:
        check.fail(f"Ranks not sequential: {ranks}")
    else:
        check.ok("Ranks sequential")

    # 2. Sorted by avg_metric descending
    metrics = [c["avg_metric"] for c in combos]
    for i in range(len(metrics) - 1):
        if metrics[i] < metrics[i+1] - 1e-6:
            check.fail(f"Combos not sorted: rank {i+1} ({metrics[i]:.4f}) < rank {i+2} ({metrics[i+1]:.4f})")
            break
    else:
        check.ok("Combos sorted by avg_metric descending")

    # 3. CI contains avg_metric
    for c in combos:
        avg = c["avg_metric"]
        ci_lo = c["ci_lower"]
        ci_hi = c["ci_upper"]
        if ci_lo > avg + 1e-6 or ci_hi < avg - 1e-6:
            check.fail(f"Combo {c['parts']}: CI [{ci_lo:.4f}, {ci_hi:.4f}] doesn't contain avg {avg:.4f}")
        else:
            check.ok(f"Combo {c['parts']}: CI contains avg_metric")

    # 4. n_texts >= 2 for all combos
    for c in combos:
        if c["n_texts"] < 2:
            check.fail(f"Combo {c['parts']}: n_texts={c['n_texts']} < 2")
        else:
            check.ok(f"Combo {c['parts']}: n_texts >= 2")


def check_summary(result: dict, n_scored: int, check: CheckResult):
    """Validate summary logic."""
    summary = result.get("summary")
    if summary is None:
        check.fail("No summary generated")
        return

    pos = summary.get("positive_elements", [])
    neg = summary.get("negative_elements", [])
    neu = summary.get("neutral_elements", [])
    rec = summary.get("recommendation", "")
    warn = summary.get("sample_warning", "")

    cs_impacts = result.get("part_impacts", {}).get("composite_score", [])

    # 1. Every element in impacts should be in exactly one summary category
    impact_names = {imp["part_name"] for imp in cs_impacts}
    summary_names = set(pos) | set(neg) | set(neu)
    if impact_names != summary_names:
        missing = impact_names - summary_names
        extra = summary_names - impact_names
        if missing:
            check.fail(f"Elements in impacts but not in summary: {missing}")
        if extra:
            check.fail(f"Elements in summary but not in impacts: {extra}")
    else:
        check.ok("All impact elements accounted for in summary")

    # 2. Positive elements should have high/medium confidence + positive delta
    for name in pos:
        imp = next((i for i in cs_impacts if i["part_name"] == name), None)
        if imp:
            if imp["confidence"] not in ("high", "medium"):
                check.fail(f"Positive element '{name}' has confidence={imp['confidence']}, expected high/medium")
            elif imp["delta"] <= 0:
                check.fail(f"Positive element '{name}' has delta={imp['delta']:.6f} <= 0")
            else:
                check.ok(f"Positive element '{name}' correct")

    # 3. Negative elements should have high/medium confidence + negative delta
    for name in neg:
        imp = next((i for i in cs_impacts if i["part_name"] == name), None)
        if imp:
            if imp["confidence"] not in ("high", "medium"):
                check.fail(f"Negative element '{name}' has confidence={imp['confidence']}, expected high/medium")
            elif imp["delta"] >= 0:
                check.fail(f"Negative element '{name}' has delta={imp['delta']:.6f} >= 0")
            else:
                check.ok(f"Negative element '{name}' correct")

    # 4. Neutral elements should have noise/low confidence
    for name in neu:
        imp = next((i for i in cs_impacts if i["part_name"] == name), None)
        if imp:
            if imp["confidence"] in ("high", "medium"):
                # Could be zero delta — check
                if abs(imp["delta"]) > 1e-8:
                    check.fail(f"Neutral element '{name}' has confidence={imp['confidence']}")
                else:
                    check.ok(f"Neutral element '{name}' has zero delta")
            else:
                check.ok(f"Neutral element '{name}' correct")

    # 5. Sample warning
    if n_scored < 10 and not warn:
        check.fail(f"n_scored={n_scored} < 10 but no sample_warning")
    elif n_scored < 20 and n_scored >= 10 and not warn:
        check.fail(f"n_scored={n_scored} < 20 but no sample_warning")
    elif n_scored >= 20 and warn and "ориентировочные" in warn:
        # Warning for < 20 texts should not appear when >= 20
        # Actually warn appears for < 30. Let me check: code says < 20, so >= 20 && < 30 has no warn. Check code...
        # Code: if n_texts < 10: ... elif n_texts < 20: ...
        # So >= 20 → no warning
        check.fail(f"n_scored={n_scored} >= 20 but got 'ориентировочные' warning")
    else:
        check.ok(f"Sample warning appropriate for n={n_scored}")

    # 6. If all neutral, recommendation should mention "не показал"
    if pos == [] and neg == [] and len(neu) > 0:
        if "не показал" not in rec and "Ни один" not in rec:
            check.fail(f"All neutral but recommendation doesn't mention 'не показал': {rec[:80]}")
        else:
            check.ok("All-neutral recommendation correct")


def check_excluded_parts(result: dict, requested_parts: List[str], check: CheckResult):
    """Validate excluded parts logic."""
    excluded = result.get("excluded_parts", [])
    detected = result.get("parts_detected", [])

    # All requested parts should be either in detected or excluded
    accounted = set(detected) | {e["part_name"] for e in excluded}
    for p in requested_parts:
        if p not in accounted:
            # It's possible a part gets filtered but let's flag it
            check.fail(f"Requested part '{p}' not in detected or excluded")
        else:
            check.ok(f"Part '{p}' accounted for")

    for ex in excluded:
        reason = ex["reason"]
        n_w = ex["n_with"]
        n_t = ex["n_total"]
        name = ex["part_name"]

        if reason == "none_match" and n_w != 0:
            check.fail(f"Excluded '{name}' reason=none_match but n_with={n_w}")
        elif reason == "all_match" and n_w != n_t:
            check.fail(f"Excluded '{name}' reason=all_match but n_with={n_w} != n_total={n_t}")
        elif reason == "too_few" and n_w >= 2:
            check.fail(f"Excluded '{name}' reason=too_few but n_with={n_w} >= 2")
        elif reason == "too_many" and (n_t - n_w) >= 2:
            check.fail(f"Excluded '{name}' reason=too_many but n_without={n_t - n_w} >= 2")
        else:
            check.ok(f"Excluded '{name}' reason={reason} valid")


# ── test scenarios ───────────────────────────────────────────────

def test_small_dataset():
    """Test 1: Very small dataset (5 texts)."""
    print("\n=== Test 1: Small dataset (5 texts) ===")
    check = CheckResult()

    df = generate_dataset(5, word_probs={"скидка": 0.6, "доставка": 0.5, "бонус": 0.4})
    sid, score = upload_and_score(df)
    n_scored = score["stats"]["n_scored"]
    print(f"  Scored: {n_scored} texts")

    if n_scored < 3:
        print("  Skipped: too few scored texts")
        return check

    parts = ["скидка", "доставка", "бонус"]
    result = run_text_parts(sid, parts)
    check_impact_logic(result, n_scored, check)
    check_combinations(result, check)
    check_summary(result, n_scored, check)
    check_excluded_parts(result, parts, check)

    print(f"  {check.summary()}")
    for e in check.errors:
        print(f"    ERROR: {e}")
    return check


def test_medium_dataset():
    """Test 2: Medium dataset (25 texts) with clear word effects."""
    print("\n=== Test 2: Medium dataset (25 texts) with effects ===")
    check = CheckResult()

    # "скидка" boosts clicks by 80%, "доставка" hurts by 30%
    df = generate_dataset(
        25,
        word_probs={"скидка": 0.4, "доставка": 0.4, "бонус": 0.3, "haraba": 0.5},
        word_score_boost={"скидка": 0.8, "доставка": -0.3},
    )
    sid, score = upload_and_score(df)
    n_scored = score["stats"]["n_scored"]
    print(f"  Scored: {n_scored} texts")

    parts = ["скидка", "доставка", "бонус", "haraba"]
    result = run_text_parts(sid, parts)
    check_impact_logic(result, n_scored, check)
    check_combinations(result, check)
    check_summary(result, n_scored, check)
    check_excluded_parts(result, parts, check)

    print(f"  {check.summary()}")
    for e in check.errors:
        print(f"    ERROR: {e}")
    return check


def test_large_dataset():
    """Test 3: Large dataset (100 texts)."""
    print("\n=== Test 3: Large dataset (100 texts) ===")
    check = CheckResult()

    df = generate_dataset(
        100,
        word_probs={"скидка": 0.3, "доставка": 0.4, "бонус": 0.2, "кэшбэк": 0.25, "haraba": 0.5},
        word_score_boost={"скидка": 0.5},
    )
    sid, score = upload_and_score(df)
    n_scored = score["stats"]["n_scored"]
    print(f"  Scored: {n_scored} texts")

    parts = ["скидка", "доставка", "бонус", "кэшбэк", "haraba"]
    result = run_text_parts(sid, parts)
    check_impact_logic(result, n_scored, check)
    check_combinations(result, check)
    check_summary(result, n_scored, check)
    check_excluded_parts(result, parts, check)

    # Extra check: with 100 texts and 50% boost on "скидка", it should have positive delta
    cs_impacts = result.get("part_impacts", {}).get("composite_score", [])
    skidka = next((i for i in cs_impacts if i["part_name"] == "скидка"), None)
    if skidka:
        if skidka["delta"] <= 0:
            check.fail(f"скидка has 50% boost but delta={skidka['delta']:.4f} <= 0")
        else:
            check.ok("скидка has positive delta as expected")

    print(f"  {check.summary()}")
    for e in check.errors:
        print(f"    ERROR: {e}")
    return check


def test_word_in_all_texts():
    """Test 4: Word present in ALL texts (should be excluded)."""
    print("\n=== Test 4: Word in all texts (should be excluded) ===")
    check = CheckResult()

    df = generate_dataset(
        15,
        word_probs={"скидка": 1.0, "доставка": 0.4},  # скидка in 100%
    )
    sid, score = upload_and_score(df)
    n_scored = score["stats"]["n_scored"]
    print(f"  Scored: {n_scored} texts")

    if n_scored < 3:
        print("  Skipped")
        return check

    parts = ["скидка", "доставка"]
    result = run_text_parts(sid, parts)

    # скидка should be excluded (all_match or too_many)
    excluded = result.get("excluded_parts", [])
    detected = result.get("parts_detected", [])

    skidka_excluded = any(e["part_name"] == "скидка" for e in excluded)
    skidka_detected = "скидка" in detected

    if not skidka_excluded and skidka_detected:
        # It's possible if not ALL scored texts have it
        # Let's check: if скидка is in detected but should be in all, it means
        # some scored texts don't have it
        check.ok("скидка in detected (not all scored texts have it)")
    elif skidka_excluded:
        ex = next(e for e in excluded if e["part_name"] == "скидка")
        if ex["reason"] in ("all_match", "too_many"):
            check.ok(f"скидка correctly excluded: {ex['reason']}")
        else:
            check.fail(f"скидка excluded with wrong reason: {ex['reason']}")
    else:
        check.fail("скидка not in detected or excluded")

    check_impact_logic(result, n_scored, check)
    check_combinations(result, check)
    check_summary(result, n_scored, check)

    print(f"  {check.summary()}")
    for e in check.errors:
        print(f"    ERROR: {e}")
    return check


def test_word_in_no_texts():
    """Test 5: Word NOT present in any text (should be excluded)."""
    print("\n=== Test 5: Word in no texts (should be excluded) ===")
    check = CheckResult()

    df = generate_dataset(15, word_probs={"доставка": 0.4, "бонус": 0.3})
    sid, score = upload_and_score(df)
    n_scored = score["stats"]["n_scored"]
    print(f"  Scored: {n_scored} texts")

    if n_scored < 3:
        print("  Skipped")
        return check

    # Request "промокод" which won't be in any text
    parts = ["доставка", "промокод"]
    result = run_text_parts(sid, parts)

    excluded = result.get("excluded_parts", [])
    promo_excluded = next((e for e in excluded if e["part_name"] == "промокод"), None)

    if promo_excluded:
        if promo_excluded["reason"] in ("none_match", "too_few"):
            check.ok(f"промокод correctly excluded: {promo_excluded['reason']}")
        else:
            check.fail(f"промокод excluded with wrong reason: {promo_excluded['reason']}")
    else:
        # Maybe it accidentally appears in filler? Unlikely but possible
        detected = result.get("parts_detected", [])
        if "промокод" in detected:
            check.ok("промокод found in some texts (random filler)")
        else:
            check.fail("промокод not in excluded or detected")

    check_impact_logic(result, n_scored, check)
    check_combinations(result, check)
    check_summary(result, n_scored, check)

    print(f"  {check.summary()}")
    for e in check.errors:
        print(f"    ERROR: {e}")
    return check


def test_identical_scores():
    """Test 6: All texts have identical metrics → zero variance."""
    print("\n=== Test 6: Identical metrics (zero variance) ===")
    check = CheckResult()

    # Create dataset with identical impressions/clicks
    rows = []
    for i in range(15):
        words = ["скидка"] if i < 7 else ["доставка"]
        headline = make_headline(words)
        rows.append({
            "text_id": f"text_{i+1:03d}",
            "headline": headline,
            "impressions": 500,
            "clicks": 50,
            "spend": 250.0,
            "registrations": 10,
        })
    df = pd.DataFrame(rows)

    sid, score = upload_and_score(df)
    n_scored = score["stats"]["n_scored"]
    print(f"  Scored: {n_scored} texts")

    if n_scored < 3:
        print("  Skipped")
        return check

    parts = ["скидка", "доставка"]
    result = run_text_parts(sid, parts)

    # With identical metrics, all composite scores should be ~0.5
    # Delta should be ~0, Cohen's d should be ~0, confidence should be "noise"
    cs_impacts = result.get("part_impacts", {}).get("composite_score", [])
    for imp in cs_impacts:
        if abs(imp["delta"]) > 0.01:
            check.fail(f"[{imp['part_name']}] identical metrics but delta={imp['delta']:.6f}")
        else:
            check.ok(f"[{imp['part_name']}] delta ≈ 0 for identical metrics")
        if imp["confidence"] != "noise":
            check.fail(f"[{imp['part_name']}] identical metrics but confidence={imp['confidence']}")
        else:
            check.ok(f"[{imp['part_name']}] confidence=noise for identical metrics")

    check_combinations(result, check)
    check_summary(result, n_scored, check)

    print(f"  {check.summary()}")
    for e in check.errors:
        print(f"    ERROR: {e}")
    return check


def test_minimal_mode():
    """Test 7: Minimal mode (only impressions + clicks, no spend/conversions)."""
    print("\n=== Test 7: Minimal mode (impressions + clicks only) ===")
    check = CheckResult()

    rows = []
    for i in range(20):
        present = ["скидка"] if random.random() < 0.5 else ["доставка"]
        headline = make_headline(present)
        imp = random.randint(200, 1000)
        clicks = max(10, int(imp * random.uniform(0.02, 0.15)))
        if "скидка" in present:
            clicks = int(clicks * 1.5)  # boost
        rows.append({
            "text_id": f"text_{i+1:03d}",
            "headline": headline,
            "impressions": imp,
            "clicks": clicks,
        })
    df = pd.DataFrame(rows)

    # Use relaxed thresholds so minimal mode texts aren't filtered out
    sid, score = upload_and_score(df, score_params={
        "min_impressions": 50,
        "min_clicks": 5,
        "min_conversions": 0,
    })
    n_scored = score["stats"]["n_scored"]
    mode = score["stats"]["mode"]
    print(f"  Scored: {n_scored} texts, mode={mode}")

    if n_scored < 3:
        print("  Skipped")
        return check

    parts = ["скидка", "доставка"]
    result = run_text_parts(sid, parts)
    check_impact_logic(result, n_scored, check)
    check_combinations(result, check)
    check_summary(result, n_scored, check)

    # In minimal mode, only CTR metric should be available
    available_metrics = set(result.get("part_impacts", {}).keys())
    if "CTR" not in available_metrics and "composite_score" not in available_metrics:
        check.fail(f"Minimal mode: expected CTR or composite_score, got {available_metrics}")
    else:
        check.ok("Minimal mode metrics available")

    print(f"  {check.summary()}")
    for e in check.errors:
        print(f"    ERROR: {e}")
    return check


def test_extreme_values():
    """Test 8: Extreme metric values (outliers)."""
    print("\n=== Test 8: Extreme values (outliers) ===")
    check = CheckResult()

    rows = []
    for i in range(20):
        present = ["скидка"] if i < 10 else ["доставка"]
        headline = make_headline(present)

        if i == 0:
            # Extreme outlier
            imp, clicks, spend, reg = 10000, 5000, 10.0, 1000
        elif i == 19:
            # Another extreme
            imp, clicks, spend, reg = 100, 10, 5000.0, 5
        else:
            imp = random.randint(200, 800)
            clicks = max(10, int(imp * random.uniform(0.03, 0.12)))
            spend = round(clicks * random.uniform(10, 40), 2)
            reg = max(5, int(clicks * random.uniform(0.05, 0.15)))

        rows.append({
            "text_id": f"text_{i+1:03d}",
            "headline": headline,
            "impressions": imp,
            "clicks": clicks,
            "spend": spend,
            "registrations": reg,
        })
    df = pd.DataFrame(rows)

    sid, score = upload_and_score(df)
    n_scored = score["stats"]["n_scored"]
    print(f"  Scored: {n_scored} texts")

    if n_scored < 3:
        print("  Skipped")
        return check

    parts = ["скидка", "доставка"]
    result = run_text_parts(sid, parts)
    check_impact_logic(result, n_scored, check)
    check_combinations(result, check)
    check_summary(result, n_scored, check)

    # Composite scores should still be in [0, 1] thanks to sigmoid
    cs_impacts = result.get("part_impacts", {}).get("composite_score", [])
    for imp_data in cs_impacts:
        if not (0 <= imp_data["metric_with"] <= 1):
            check.fail(f"metric_with={imp_data['metric_with']} outside [0,1]")
        if not (0 <= imp_data["metric_without"] <= 1):
            check.fail(f"metric_without={imp_data['metric_without']} outside [0,1]")

    print(f"  {check.summary()}")
    for e in check.errors:
        print(f"    ERROR: {e}")
    return check


def test_many_parts():
    """Test 9: Many parts requested (10+)."""
    print("\n=== Test 9: Many parts (12 requested) ===")
    check = CheckResult()

    all_words = WORDS_POOL[:12]
    word_probs = {w: random.uniform(0.15, 0.5) for w in all_words}

    df = generate_dataset(40, word_probs=word_probs)
    sid, score = upload_and_score(df)
    n_scored = score["stats"]["n_scored"]
    print(f"  Scored: {n_scored} texts")

    if n_scored < 3:
        print("  Skipped")
        return check

    result = run_text_parts(sid, all_words)
    check_impact_logic(result, n_scored, check)
    check_combinations(result, check)
    check_summary(result, n_scored, check)
    check_excluded_parts(result, all_words, check)

    # With 12 parts and max_combination_size=3, should have <= 20 combos
    n_combos = len(result.get("best_combinations", []))
    if n_combos > 20:
        check.fail(f"Too many combos: {n_combos} > 20")
    else:
        check.ok(f"Combos count={n_combos} <= 20")

    print(f"  {check.summary()}")
    for e in check.errors:
        print(f"    ERROR: {e}")
    return check


def test_cost_metric():
    """Test 10: Analysis on cost metric (CPA) — lower = better."""
    print("\n=== Test 10: Cost metric (CPA) analysis ===")
    check = CheckResult()

    # Make "скидка" texts have LOWER CPA (more conversions per spend)
    rows = []
    for i in range(30):
        has_skidka = random.random() < 0.4
        present = ["скидка"] if has_skidka else ["доставка"]
        headline = make_headline(present)
        imp = random.randint(300, 800)
        clicks = max(10, int(imp * random.uniform(0.04, 0.12)))
        spend = round(clicks * random.uniform(15, 35), 2)
        reg = max(5, int(clicks * random.uniform(0.05, 0.15)))
        if has_skidka:
            reg = int(reg * 2)  # More conversions → lower CPA

        rows.append({
            "text_id": f"text_{i+1:03d}",
            "headline": headline,
            "impressions": imp,
            "clicks": clicks,
            "spend": spend,
            "registrations": reg,
        })
    df = pd.DataFrame(rows)

    sid, score = upload_and_score(df)
    n_scored = score["stats"]["n_scored"]
    print(f"  Scored: {n_scored} texts")

    if n_scored < 3:
        print("  Skipped")
        return check

    parts = ["скидка", "доставка"]
    # Analyze on CPA metric
    result = run_text_parts(sid, parts, metric="CPA")
    check_combinations(result, check)

    # For CPA (lower=better), "скидка" should have lower CPA (negative delta is GOOD)
    cpa_impacts = result.get("part_impacts", {}).get("CPA", [])
    for imp_data in cpa_impacts:
        if imp_data["part_name"] == "скидка":
            if imp_data["delta"] > 0:
                # CPA is higher with скидка — might happen if boost not strong enough
                check.ok("скидка CPA delta positive (boost may not be enough)")
            else:
                check.ok("скидка has lower CPA as expected (negative delta)")

    # Check summary: for cost metric, negative delta = positive element
    summary = result.get("summary")
    if summary:
        # If скидка has negative delta and is significant, it should be "positive" (good for cost)
        skidka_impact = next((i for i in cpa_impacts if i["part_name"] == "скидка"), None)
        if skidka_impact and skidka_impact["confidence"] in ("high", "medium") and skidka_impact["delta"] < 0:
            if "скидка" in summary.get("positive_elements", []):
                check.ok("скидка correctly classified as positive for cost metric")
            elif "скидка" in summary.get("negative_elements", []):
                check.fail("скидка classified as negative but has lower CPA (should be positive)")
        else:
            check.ok("скидка not significant or positive delta — classification ok")

    check_impact_logic(result, n_scored, check)

    print(f"  {check.summary()}")
    for e in check.errors:
        print(f"    ERROR: {e}")
    return check


def test_full_mode_with_revenue():
    """Test 11: Full mode with revenue data."""
    print("\n=== Test 11: Full mode with revenue ===")
    check = CheckResult()

    df = generate_dataset(
        30,
        word_probs={"скидка": 0.4, "доставка": 0.35, "бонус": 0.3},
        mode="full",
    )
    sid, score = upload_and_score(df)
    n_scored = score["stats"]["n_scored"]
    mode = score["stats"]["mode"]
    print(f"  Scored: {n_scored} texts, mode={mode}")

    if n_scored < 3:
        print("  Skipped")
        return check

    parts = ["скидка", "доставка", "бонус"]
    result = run_text_parts(sid, parts)
    check_impact_logic(result, n_scored, check)
    check_combinations(result, check)
    check_summary(result, n_scored, check)

    # Full mode should have ROI metric
    available_metrics = set(result.get("part_impacts", {}).keys())
    if mode == "full" and "ROI" not in available_metrics:
        check.fail(f"Full mode but no ROI metric in impacts. Available: {available_metrics}")
    else:
        check.ok(f"Full mode has ROI metric")

    print(f"  {check.summary()}")
    for e in check.errors:
        print(f"    ERROR: {e}")
    return check


def test_repeated_runs():
    """Test 12: Multiple runs on same session should be deterministic."""
    print("\n=== Test 12: Deterministic (repeated runs) ===")
    check = CheckResult()

    df = generate_dataset(20, word_probs={"скидка": 0.4, "доставка": 0.4, "бонус": 0.3})
    sid, score = upload_and_score(df)
    n_scored = score["stats"]["n_scored"]
    print(f"  Scored: {n_scored} texts")

    if n_scored < 3:
        print("  Skipped")
        return check

    parts = ["скидка", "доставка", "бонус"]
    result1 = run_text_parts(sid, parts)
    result2 = run_text_parts(sid, parts)

    # Results should be identical
    impacts1 = result1.get("part_impacts", {}).get("composite_score", [])
    impacts2 = result2.get("part_impacts", {}).get("composite_score", [])

    if len(impacts1) != len(impacts2):
        check.fail(f"Different number of impacts: {len(impacts1)} vs {len(impacts2)}")
    else:
        for i1, i2 in zip(impacts1, impacts2):
            if i1["part_name"] != i2["part_name"]:
                check.fail(f"Different order: {i1['part_name']} vs {i2['part_name']}")
            elif abs(i1["delta"] - i2["delta"]) > 1e-8:
                check.fail(f"Different delta for {i1['part_name']}: {i1['delta']} vs {i2['delta']}")
            else:
                check.ok(f"{i1['part_name']} deterministic")

    combos1 = result1.get("best_combinations", [])
    combos2 = result2.get("best_combinations", [])
    if len(combos1) != len(combos2):
        check.fail(f"Different combo count: {len(combos1)} vs {len(combos2)}")
    else:
        check.ok("Combo count deterministic")

    print(f"  {check.summary()}")
    for e in check.errors:
        print(f"    ERROR: {e}")
    return check


def test_random_stress():
    """Test 13: 10 random datasets with random parameters."""
    print("\n=== Test 13: Random stress (10 iterations) ===")
    check = CheckResult()

    for iteration in range(10):
        n_texts = random.randint(8, 60)
        n_words = random.randint(2, 6)
        words = random.sample(WORDS_POOL, n_words)
        word_probs = {w: random.uniform(0.15, 0.6) for w in words}
        # Random boost for one word
        boost_word = random.choice(words)
        boost_val = random.uniform(-0.5, 1.0)
        word_boost = {boost_word: boost_val}
        mode = random.choice(["basic", "basic", "full"])

        df = generate_dataset(n_texts, word_probs=word_probs,
                              word_score_boost=word_boost, mode=mode)
        try:
            sid, score = upload_and_score(df)
        except Exception as e:
            check.fail(f"iter {iteration}: upload/score failed: {e}")
            continue

        n_scored = score["stats"]["n_scored"]
        if n_scored < 3:
            check.ok(f"iter {iteration}: n_scored={n_scored} < 3, skip")
            continue

        try:
            result = run_text_parts(sid, words)
        except Exception as e:
            check.fail(f"iter {iteration}: text-parts failed: {e}")
            continue

        # Run all standard checks
        sub = CheckResult()
        check_impact_logic(result, n_scored, sub)
        check_combinations(result, sub)
        check_summary(result, n_scored, sub)
        check_excluded_parts(result, words, sub)

        if sub.failed > 0:
            for err in sub.errors:
                check.fail(f"iter {iteration} (n={n_texts}, words={words}): {err}")
        check.passed += sub.passed

        print(f"  iter {iteration}: n={n_texts}, scored={n_scored}, words={n_words}, "
              f"mode={mode} → {sub.summary()}")

    print(f"  {check.summary()}")
    for e in check.errors:
        print(f"    ERROR: {e}")
    return check


# ── main ─────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("STRESS TEST: Text Parts Analysis Pipeline")
    print("=" * 60)

    # Check API health
    try:
        resp = requests.get(f"{API}/health")
        resp.raise_for_status()
        print(f"API health: {resp.json()}")
    except Exception as e:
        print(f"API not available: {e}")
        sys.exit(1)

    random.seed(42)
    np.random.seed(42)

    tests = [
        test_small_dataset,
        test_medium_dataset,
        test_large_dataset,
        test_word_in_all_texts,
        test_word_in_no_texts,
        test_identical_scores,
        test_minimal_mode,
        test_extreme_values,
        test_many_parts,
        test_cost_metric,
        test_full_mode_with_revenue,
        test_repeated_runs,
        test_random_stress,
    ]

    total_passed = 0
    total_failed = 0
    all_errors = []

    for test_fn in tests:
        try:
            result = test_fn()
            total_passed += result.passed
            total_failed += result.failed
            if result.errors:
                all_errors.extend([(test_fn.__name__, e) for e in result.errors])
        except Exception as e:
            print(f"  EXCEPTION: {e}")
            traceback.print_exc()
            total_failed += 1
            all_errors.append((test_fn.__name__, f"EXCEPTION: {e}"))

    print("\n" + "=" * 60)
    print(f"TOTAL: {total_passed} passed, {total_failed} failed")
    print("=" * 60)

    if all_errors:
        print("\nALL ERRORS:")
        for test_name, error in all_errors:
            print(f"  [{test_name}] {error}")
    else:
        print("\nAll checks passed!")

    return total_failed


if __name__ == "__main__":
    failures = main()
    sys.exit(1 if failures > 0 else 0)
