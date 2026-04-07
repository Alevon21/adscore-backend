import pandas as pd
import numpy as np

DEFAULT_THRESHOLDS = {
    "ctit_lt15s_pct": 0.01,
    "ctit_gt5h_pct": 0.10,
    "vta_pct": 0.50,
    "hourly_mad_multiplier": 3.0,       # adaptive: hour is anomalous if deviation > N × MAD
    "hourly_mad_floor_pp": 0.5,         # minimum threshold in п.п. (guards against tiny MAD)
    "hourly_critical_hours": 5,
    "hourly_mad_pp": 1.5,               # MAD itself above this → anomaly signal
    "dup_device_pct": 0.05,
    "ip_dup_install_threshold": 3,
    "ip_dup_count": 20,
    "install_rate_low": 0.001,      # 0.1%
    "install_rate_high": 0.01,      # 1%
    "new_devices_high_pct": 0.20,   # 20%
}


def compute_benchmark(df: pd.DataFrame, benchmark_trackers: list) -> dict:
    mask = df["adjust_tracker"].isin(benchmark_trackers)
    ctit = df.loc[mask & (df["ctit_seconds"] >= 0), "ctit_seconds"].dropna()
    if len(ctit) == 0:
        return {"p50": 60, "p90": 600, "p95": 1800, "p99": 3600, "n": 0}
    return {
        "p50": float(ctit.quantile(0.50)),
        "p90": float(ctit.quantile(0.90)),
        "p95": float(ctit.quantile(0.95)),
        "p99": float(ctit.quantile(0.99)),
        "n": int(len(ctit)),
    }


def classify_fraud(df: pd.DataFrame, benchmark_p99: float) -> pd.DataFrame:
    conditions = [df["ctit_seconds"] < 15, df["ctit_seconds"] > benchmark_p99]
    choices = ["Click Injection", "Click Spam"]
    df["fraud_type"] = np.select(conditions, choices, default="Legitimate")
    return df


def compute_hourly_profile(df: pd.DataFrame, tracker: str) -> pd.Series:
    tdf = df[df["adjust_tracker"] == tracker]
    hourly = tdf.groupby("install_hour").size()
    total = hourly.sum()
    pct = (hourly / total * 100) if total > 0 else hourly
    return pct.reindex(range(24), fill_value=0)


def detect_multi_geo_devices(df: pd.DataFrame) -> list:
    """Find device_ids that appear in multiple countries."""
    valid = df[df["device_id"].notna() & df["country"].notna()]
    if len(valid) == 0:
        return []
    geo = valid.groupby("device_id")["country"].apply(lambda x: sorted(x.unique().tolist()))
    multi = geo[geo.apply(len) > 1]
    tracker_col = "adjust_tracker" if "adjust_tracker" in valid.columns else None
    results = []
    has_reattr = "is_reattribution" in valid.columns
    for did, countries in multi.items():
        rows = valid[valid["device_id"] == did]
        trackers = sorted(rows[tracker_col].dropna().unique().tolist()) if tracker_col else []
        reattr_count = int(rows["is_reattribution"].sum()) if has_reattr else 0
        results.append({"device_id": did, "countries": countries, "trackers": trackers, "count": int(len(rows)), "reattribution_count": reattr_count})
    return results


def detect_multi_geo_ips(df: pd.DataFrame) -> list:
    """Find IPs that appear in multiple countries."""
    valid = df[df["ip_string"].notna() & df["country"].notna()]
    if len(valid) == 0:
        return []
    geo = valid.groupby("ip_string")["country"].apply(lambda x: sorted(x.unique().tolist()))
    multi = geo[geo.apply(len) > 1]
    tracker_col = "adjust_tracker" if "adjust_tracker" in valid.columns else None
    results = []
    has_reattr = "is_reattribution" in valid.columns
    for ip, countries in multi.items():
        rows = valid[valid["ip_string"] == ip]
        trackers = sorted(rows[tracker_col].dropna().unique().tolist()) if tracker_col else []
        reattr_count = int(rows["is_reattribution"].sum()) if has_reattr else 0
        results.append({"ip": ip, "countries": countries, "trackers": trackers, "count": int(len(rows)), "reattribution_count": reattr_count})
    return results


def compute_tracker_markers(
    df: pd.DataFrame,
    tracker: str,
    benchmark_hourly: pd.Series,
    thresholds: dict = None,
    device_tracker_map: dict = None,
) -> dict:
    t = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    tdf = df[df["adjust_tracker"] == tracker]
    n = len(tdf)
    if n == 0:
        return {"tracker": tracker, "n": 0, "risk_level": "No Data", "risk_score": 0, "markers": {}, "stats": {}}

    ctit = tdf["ctit_seconds"].dropna()
    ctit_valid = ctit[ctit >= 0]
    n_ctit = len(ctit_valid)

    # Marker calculations
    lt15_share = (ctit_valid < 15).sum() / n_ctit if n_ctit > 0 else 0
    gt5h_share = (ctit_valid > 18000).sum() / n_ctit if n_ctit > 0 else 0
    gt24h_share = (ctit_valid > 86400).sum() / n_ctit if n_ctit > 0 else 0
    lte1h_share = (ctit_valid <= 3600).sum() / n_ctit if n_ctit > 0 else 0
    lte60s_share = (ctit_valid <= 60).sum() / n_ctit if n_ctit > 0 else 0
    vta_share = tdf["is_impression_based"].sum() / n if "is_impression_based" in tdf.columns else 0
    reattr_rate = tdf["is_reattribution"].sum() / n

    # VTIT stats for VTA traffic
    vtit = tdf["vtit_seconds"].dropna() if "vtit_seconds" in tdf.columns else pd.Series(dtype=float)
    vtit_valid = vtit[vtit >= 0]
    n_vtit = len(vtit_valid)
    vtit_p50 = float(vtit_valid.quantile(0.50)) if n_vtit > 0 else None
    vtit_p95 = float(vtit_valid.quantile(0.95)) if n_vtit > 0 else None

    # CTV (valid CTIT) stats
    ctit_p50 = float(ctit_valid.quantile(0.50)) if n_ctit > 0 else None
    ctit_p95 = float(ctit_valid.quantile(0.95)) if n_ctit > 0 else None

    # Hourly anomaly — fixed threshold (как в Colab: |deviation| > 1.5 п.п.)
    hourly_pct = compute_hourly_profile(df, tracker)
    fixed_threshold = t["hourly_mad_pp"]  # default 1.5 п.п.
    if benchmark_hourly is not None and len(benchmark_hourly) == 24:
        deviation = (hourly_pct - benchmark_hourly).abs()
        mad = float(deviation.mean())  # mean absolute deviation for summary stats
        critical_hours = int((deviation > fixed_threshold).sum())
        critical_hours_list = sorted([int(h) for h in range(24) if deviation.get(h, 0) > fixed_threshold])
    else:
        critical_hours, mad = 0, 0.0
        critical_hours_list = []

    # Hourly anomaly levels (based on count of critical hours)
    if critical_hours >= t["hourly_critical_hours"]:
        hourly_anomaly_level = "Сильный"
    elif critical_hours >= 2:
        hourly_anomaly_level = "Умеренный"
    else:
        hourly_anomaly_level = "Нет"

    # CTIT speed marker
    m_ctit_fast = int(lte60s_share > 0.50) if n_ctit > 0 else 0  # >50% installs within 60s

    # Device duplicates — split into clean installs vs reattributions
    device_ids = tdf["device_id"].dropna()
    dup_device_mask = device_ids.duplicated(keep=False)
    dup_device_pct = dup_device_mask.sum() / len(device_ids) if len(device_ids) > 0 else 0
    # Of duplicated devices, how many rows are reattributions?
    if dup_device_mask.sum() > 0 and "is_reattribution" in tdf.columns:
        dup_rows = tdf.loc[device_ids.index[dup_device_mask]]
        dup_reattr_count = int(dup_rows["is_reattribution"].sum())
        dup_device_reattr_pct = dup_reattr_count / int(dup_device_mask.sum())
    else:
        dup_reattr_count = 0
        dup_device_reattr_pct = 0.0
    # Only flag as fraud marker if duplicates remain after excluding reattributions
    clean_dup_pct = dup_device_pct * (1 - dup_device_reattr_pct)

    # IP duplicates
    ip_counts = tdf["ip_string"].dropna().value_counts()
    heavy_ips = int((ip_counts > t["ip_dup_install_threshold"]).sum())

    # Install Rate: clicks vs installs
    n_clicks = int((tdf["activity_kind"] == "click").sum()) if "activity_kind" in tdf.columns else 0
    n_installs_only = int((tdf["activity_kind"].isin(["install", "reattribution"])).sum()) if "activity_kind" in tdf.columns else n
    install_rate = n_installs_only / n_clicks if n_clicks > 0 else None

    # New Devices: devices exclusive to this tracker
    tracker_devices = tdf["device_id"].dropna().unique()
    n_tracker_devices = len(tracker_devices)
    if n_tracker_devices > 0 and device_tracker_map is not None:
        exclusive = sum(1 for d in tracker_devices if len(device_tracker_map.get(d, set())) == 1)
        new_device_pct = exclusive / n_tracker_devices
    else:
        new_device_pct = 0.0

    # Multi-geo per tracker
    multi_geo_devices = detect_multi_geo_devices(tdf)
    multi_geo_ips = detect_multi_geo_ips(tdf)

    markers = {
        "m_ctit_lt15s": int(lt15_share > t["ctit_lt15s_pct"]),
        "m_ctit_gt5h": int(gt5h_share > t["ctit_gt5h_pct"]),
        "m_ctit_fast": m_ctit_fast,
        "m_vta_gt50": int(vta_share >= t["vta_pct"]),
        "m_hourly_anomaly": int(critical_hours >= 2),
        "m_dup_device_high": int(clean_dup_pct >= t["dup_device_pct"]),
        "m_dup_ip_high": int(heavy_ips >= t["ip_dup_count"]),
        "m_install_rate_low": int(install_rate is not None and install_rate < t["install_rate_low"]),
        "m_install_rate_high": int(install_rate is not None and install_rate > t["install_rate_high"]),
        "m_new_devices_high": int(new_device_pct > t["new_devices_high_pct"]),
    }
    # All 7 markers contribute to risk score
    risk_score = sum(markers.values())
    risk_level = (
        "Low" if risk_score <= 1 else
        "Moderate" if risk_score == 2 else
        "High" if risk_score == 3 else
        "Critical"
    )

    return {
        "tracker": tracker,
        "n": n,
        "risk_score": risk_score,
        "risk_level": risk_level,
        "markers": markers,
        "stats": {
            "lt15s_share": round(lt15_share * 100, 2),
            "gt5h_share": round(gt5h_share * 100, 2),
            "gt24h_share": round(gt24h_share * 100, 2),
            "lte1h_share": round(lte1h_share * 100, 2),
            "lte60s_share": round(lte60s_share * 100, 2),
            "vta_share": round(vta_share * 100, 2),
            "reattribution_rate": round(reattr_rate * 100, 2),
            "ctit_count": n_ctit,
            "ctit_p50": round(ctit_p50, 1) if ctit_p50 is not None else None,
            "ctit_p95": round(ctit_p95, 1) if ctit_p95 is not None else None,
            "dup_device_pct": round(dup_device_pct * 100, 2),
            "dup_device_clean_pct": round(clean_dup_pct * 100, 2),
            "dup_device_reattr_pct": round(dup_device_reattr_pct * 100, 2),
            "heavy_ip_count": heavy_ips,
            "critical_hours": critical_hours,
            "mad_pp": round(mad, 2),
            "threshold_pp": fixed_threshold,
            "critical_hours_list": critical_hours_list,
            "hourly_anomaly_level": hourly_anomaly_level,
            "install_rate": round(install_rate * 100, 4) if install_rate is not None else None,
            "n_clicks": n_clicks,
            "vtit_count": n_vtit,
            "vtit_p50": round(vtit_p50, 1) if vtit_p50 is not None else None,
            "vtit_p95": round(vtit_p95, 1) if vtit_p95 is not None else None,
            "new_device_pct": round(new_device_pct * 100, 2),
            "multi_geo_devices": len(multi_geo_devices),
            "multi_geo_ips": len(multi_geo_ips),
        },
        "multi_geo": {
            "devices": multi_geo_devices[:20],
            "ips": multi_geo_ips[:20],
        },
    }


def run_fraud_analysis(df: pd.DataFrame, benchmark_trackers: list, thresholds: dict = None, hourly_benchmark_trackers: list = None) -> dict:
    """Run full fraud analysis pipeline. Returns aggregated results."""
    benchmark = compute_benchmark(df, benchmark_trackers)
    df = classify_fraud(df, benchmark["p99"])

    # Hourly benchmark: separate from fraud P99 benchmark
    hourly_bench_list = hourly_benchmark_trackers or benchmark_trackers
    bench_hourly = None
    if hourly_bench_list:
        bench_hourly = compute_hourly_profile(df, hourly_bench_list[0])

    trackers = sorted(df["adjust_tracker"].dropna().unique())

    # Build device-to-tracker map for New Devices metric
    device_tracker_map = df.groupby("device_id")["adjust_tracker"].apply(set).to_dict() if "device_id" in df.columns else {}

    # Per-tracker analysis
    tracker_passports = [compute_tracker_markers(df, t, bench_hourly, thresholds, device_tracker_map) for t in trackers]

    # Fraud summary crosstab
    fraud_summary = {}
    for tracker in trackers:
        tdf = df[df["adjust_tracker"] == tracker]
        total = len(tdf)
        if total == 0:
            continue
        counts = tdf["fraud_type"].value_counts()
        fraud_summary[tracker] = {
            ft: {"count": int(counts.get(ft, 0)), "pct": round(counts.get(ft, 0) / total * 100, 2)}
            for ft in ["Legitimate", "Click Injection", "Click Spam"]
        }

    # Per-tracker aggregates
    tracker_aggregates = []
    for tracker in trackers:
        tdf = df[df["adjust_tracker"] == tracker]
        ctit = tdf["ctit_seconds"].dropna()
        ctit_valid = ctit[ctit >= 0]
        n_clk = int((tdf["activity_kind"] == "click").sum()) if "activity_kind" in tdf.columns else 0
        n_inst_only = int((tdf["activity_kind"].isin(["install", "reattribution"])).sum()) if "activity_kind" in tdf.columns else len(tdf)
        tracker_aggregates.append({
            "tracker": tracker,
            "n_installs": len(tdf),
            "n_clicks": n_clk,
            "install_rate": round(n_inst_only / n_clk * 100, 4) if n_clk > 0 else None,
            "n_reattributions": int(tdf["is_reattribution"].sum()),
            "reattribution_rate": round(tdf["is_reattribution"].mean() * 100, 2),
            "ctit_p50": round(float(ctit_valid.quantile(0.50)), 1) if len(ctit_valid) > 0 else None,
            "ctit_p95": round(float(ctit_valid.quantile(0.95)), 1) if len(ctit_valid) > 0 else None,
            "ctit_p99": round(float(ctit_valid.quantile(0.99)), 1) if len(ctit_valid) > 0 else None,
            "fraud_rate": round((tdf["fraud_type"] != "Legitimate").sum() / len(tdf) * 100, 2) if len(tdf) > 0 else 0,
            "countries": sorted(tdf["country"].dropna().unique().tolist()),
        })

    # Hourly profiles (absolute counts + percentage)
    hourly_installs = {}
    hourly_clicks = {}
    hourly_profiles_pct = {}
    hourly_deviations = {}
    critical_hours_map = {}
    for tracker in trackers:
        tdf = df[df["adjust_tracker"] == tracker]
        hi = tdf.groupby("install_hour").size()
        hourly_installs[tracker] = {int(h): int(c) for h, c in hi.items()}
        if "click_hour" in tdf.columns:
            hc = tdf.groupby("click_hour").size()
            hourly_clicks[tracker] = {int(h): int(c) for h, c in hc.items()}
        # Percentage profile
        profile_pct = compute_hourly_profile(df, tracker)
        hourly_profiles_pct[tracker] = {int(h): round(float(v), 3) for h, v in profile_pct.items()}
        # Deviation from benchmark (fixed threshold — same as Colab)
        if bench_hourly is not None and len(bench_hourly) == 24:
            dev = profile_pct - bench_hourly
            hourly_deviations[tracker] = {int(h): round(float(v), 3) for h, v in dev.items()}
            t_thresh = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
            fixed_thresh = t_thresh["hourly_mad_pp"]  # default 1.5 п.п.
            crit_list = [int(h) for h in range(24) if abs(dev.get(h, 0)) > fixed_thresh]
            critical_hours_map[tracker] = crit_list

    # Daily volumes with fraud breakdown
    daily = df.groupby(["adjust_tracker", "install_date"]).agg(
        installs=("fraud_type", "size"),
        fraud_count=("fraud_type", lambda x: int((x != "Legitimate").sum())),
        ci_count=("fraud_type", lambda x: int((x == "Click Injection").sum())),
        cs_count=("fraud_type", lambda x: int((x == "Click Spam").sum())),
    ).reset_index()
    daily_volumes = [
        {
            "tracker": row["adjust_tracker"],
            "date": str(row["install_date"]),
            "installs": int(row["installs"]),
            "fraud_count": int(row["fraud_count"]),
            "ci_count": int(row["ci_count"]),
            "cs_count": int(row["cs_count"]),
        }
        for _, row in daily.iterrows()
    ]

    # CTIT distributions
    ctit_distributions = {}
    for tracker in trackers:
        tdf = df[(df["adjust_tracker"] == tracker) & (df["ctit_seconds"] >= 0)]
        if len(tdf) > 0 and "ctit_bucket" in tdf.columns:
            counts = tdf["ctit_bucket"].value_counts()
            ctit_distributions[tracker] = {str(k): int(v) for k, v in counts.items()}

    # VTIT distributions (VTA traffic only)
    vtit_distributions = {}
    for tracker in trackers:
        tdf_vtit = df[(df["adjust_tracker"] == tracker) & df["vtit_seconds"].notna() & (df["vtit_seconds"] >= 0)] if "vtit_seconds" in df.columns else pd.DataFrame()
        if len(tdf_vtit) > 0 and "vtit_bucket" in tdf_vtit.columns:
            counts = tdf_vtit["vtit_bucket"].value_counts()
            vtit_distributions[tracker] = {str(k): int(v) for k, v in counts.items()}

    # Global multi-geo
    multi_geo_devices = detect_multi_geo_devices(df)
    multi_geo_ips = detect_multi_geo_ips(df)

    # Benchmark hourly profile for reference
    benchmark_hourly_profile = {}
    if bench_hourly is not None:
        benchmark_hourly_profile = {int(h): round(float(v), 3) for h, v in bench_hourly.items()}

    return {
        "benchmark": {
            **benchmark,
            "benchmark_trackers": benchmark_trackers,
            "hourly_benchmark_trackers": hourly_bench_list,
            "hourly_profile": benchmark_hourly_profile,
        },
        "tracker_passports": tracker_passports,
        "fraud_summary": fraud_summary,
        "tracker_aggregates": tracker_aggregates,
        "hourly_installs": hourly_installs,
        "hourly_clicks": hourly_clicks,
        "hourly_profiles_pct": hourly_profiles_pct,
        "hourly_deviations": hourly_deviations,
        "critical_hours_map": critical_hours_map,
        "daily_volumes": daily_volumes,
        "ctit_distributions": ctit_distributions,
        "vtit_distributions": vtit_distributions,
        "multi_geo": {
            "devices": multi_geo_devices[:50],
            "ips": multi_geo_ips[:50],
        },
        "total_rows": len(df),
    }
