import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from mmp_fraud import (
    compute_benchmark,
    classify_fraud,
    compute_tracker_markers,
    compute_hourly_profile,
    detect_multi_geo_devices,
    detect_multi_geo_ips,
)


def make_installs(tracker, n, ctit_range=(30, 600), seed=42):
    """Generate n installs for a tracker with random CTIT."""
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "adjust_tracker": [tracker] * n,
        "ctit_seconds": rng.uniform(*ctit_range, size=n),
        "install_hour": rng.integers(0, 24, size=n),
        "is_impression_based": [False] * n,
        "is_reattribution": [False] * n,
        "device_id": [f"dev_{i}" for i in range(n)],
        "ip_string": [f"1.2.3.{i % 256}" for i in range(n)],
        "country": ["ru"] * n,
    })


class TestBenchmark:
    def test_computes_percentiles(self):
        df = make_installs("clean_tracker", 1000, ctit_range=(10, 3600))
        result = compute_benchmark(df, ["clean_tracker"])
        assert result["p50"] > 0
        assert result["p99"] > result["p50"]
        assert result["n"] == 1000

    def test_empty_benchmark_returns_defaults(self):
        df = make_installs("other", 100)
        result = compute_benchmark(df, ["nonexistent"])
        assert result["n"] == 0
        assert result["p99"] > 0


class TestClassifyFraud:
    def test_click_injection_detected(self):
        df = pd.DataFrame({"ctit_seconds": [5, 10, 100, 500]})
        df = classify_fraud(df, benchmark_p99=3600)
        assert list(df["fraud_type"]) == ["Click Injection", "Click Injection", "Legitimate", "Legitimate"]

    def test_click_spam_detected(self):
        df = pd.DataFrame({"ctit_seconds": [100, 5000]})
        df = classify_fraud(df, benchmark_p99=3600)
        assert df["fraud_type"].iloc[1] == "Click Spam"


class TestTrackerMarkers:
    def test_clean_tracker_low_risk(self):
        df = make_installs("clean", 500, ctit_range=(30, 600))
        bench_hourly = compute_hourly_profile(df, "clean")
        result = compute_tracker_markers(df, "clean", bench_hourly)
        assert result["risk_level"] in ("Low", "Moderate")
        assert result["risk_score"] <= 2

    def test_fraudulent_tracker_high_risk(self):
        n = 200
        df = pd.DataFrame({
            "adjust_tracker": ["fraud"] * n,
            "ctit_seconds": [5] * 100 + [100] * 100,
            "install_hour": list(range(24)) * (n // 24) + [0] * (n % 24),
            "is_impression_based": [True] * n,
            "is_reattribution": [False] * n,
            "device_id": ["same_device"] * n,
            "ip_string": ["1.2.3.4"] * n,
            "country": ["ru"] * n,
        })
        bench_hourly = pd.Series([1 / 24 * 100] * 24, index=range(24))
        result = compute_tracker_markers(df, "fraud", bench_hourly)
        assert result["risk_level"] in ("High", "Critical")
        assert result["markers"]["m_ctit_lt15s"] == 1
        assert result["markers"]["m_vta_gt50"] == 1
        assert result["markers"]["m_dup_device_high"] == 1


class TestMultiGeo:
    def test_multi_geo_devices_detected(self):
        df = pd.DataFrame({
            "adjust_tracker": ["t"] * 4,
            "device_id": ["d1", "d1", "d2", "d2"],
            "country": ["ru", "by", "ru", "ru"],
        })
        result = detect_multi_geo_devices(df)
        assert len(result) == 1
        assert result[0]["device_id"] == "d1"
        assert set(result[0]["countries"]) == {"ru", "by"}

    def test_multi_geo_ips_detected(self):
        df = pd.DataFrame({
            "adjust_tracker": ["t"] * 4,
            "ip_string": ["1.1.1.1", "1.1.1.1", "2.2.2.2", "2.2.2.2"],
            "country": ["ru", "de", "ru", "ru"],
        })
        result = detect_multi_geo_ips(df)
        assert len(result) == 1
        assert result[0]["ip"] == "1.1.1.1"

    def test_no_multi_geo_when_single_country(self):
        df = pd.DataFrame({
            "adjust_tracker": ["t"] * 4,
            "device_id": ["d1", "d1", "d2", "d2"],
            "country": ["ru", "ru", "ru", "ru"],
        })
        result = detect_multi_geo_devices(df)
        assert len(result) == 0
