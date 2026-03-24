import pandas as pd
import numpy as np

REQUIRED_COLUMNS = [
    "event_time", "installed_at", "click_time", "engagement_time",
    "adjust_tracker", "adjust_campaign",
    "device_id", "ip_string", "country",
    "activity_kind", "is_impression_based", "reattributed_at",
    "conversion_duration",
]

CTIT_BINS = [-np.inf, 15, 60, 900, 3600, 10800, 21600, 86400, np.inf]
CTIT_LABELS = ["<15s", "15-60s", "1-15m", "15-60m", "1-3h", "3-6h", "6-24h", ">24h"]


def validate_columns(df: pd.DataFrame) -> dict:
    cols = set(df.columns)
    missing = sorted(set(REQUIRED_COLUMNS) - cols)
    return {"ok": len(missing) == 0, "missing": missing, "detected": sorted(cols), "n_rows": len(df)}


def parse_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["event_time", "installed_at", "click_time", "engagement_time", "reattributed_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    if "conversion_duration" in df.columns:
        df["conversion_duration"] = pd.to_numeric(df["conversion_duration"], errors="coerce")
    if "is_impression_based" in df.columns:
        df["is_impression_based"] = df["is_impression_based"].astype(str).str.lower().isin(["true", "1", "yes"])
    return df


def compute_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    # Detect reattributions first — needed for CTIT strategy
    if "reattributed_at" in df.columns:
        df["is_reattribution"] = df["reattributed_at"].notna()
    else:
        df["is_reattribution"] = False

    # CTIT: use Adjust's conversion_duration as primary source (handles
    # reattributions correctly). Fall back to manual calculation only for
    # rows where conversion_duration is missing AND it's not a reattribution
    # (manual calc gives wrong/negative values for reattributions).
    manual_ctit = None
    if "installed_at" in df.columns and "click_time" in df.columns:
        manual_ctit = (df["installed_at"] - df["click_time"]).dt.total_seconds()

    if "conversion_duration" in df.columns:
        df["ctit_seconds"] = df["conversion_duration"]
        if manual_ctit is not None:
            # Fill gaps with manual calc, but only for non-reattributions
            fill_mask = df["ctit_seconds"].isna() & ~df["is_reattribution"]
            df.loc[fill_mask, "ctit_seconds"] = manual_ctit[fill_mask]
    elif manual_ctit is not None:
        df["ctit_seconds"] = manual_ctit
        # Zero out nonsensical negative values from reattributions
        df.loc[df["is_reattribution"] & (df["ctit_seconds"] < 0), "ctit_seconds"] = np.nan

    if "installed_at" in df.columns:
        df["install_hour"] = df["installed_at"].dt.hour
        df["install_date"] = df["installed_at"].dt.date
    if "click_time" in df.columns:
        df["click_hour"] = df["click_time"].dt.hour
    if "ctit_seconds" in df.columns:
        df["ctit_bucket"] = pd.cut(df["ctit_seconds"], bins=CTIT_BINS, labels=CTIT_LABELS)
    return df


def parse_mmp_csv(content: bytes) -> pd.DataFrame:
    """Parse raw CSV bytes into processed DataFrame."""
    from io import BytesIO
    df = pd.read_csv(BytesIO(content), low_memory=False)
    validation = validate_columns(df)
    if not validation["ok"]:
        raise ValueError(f"Missing columns: {validation['missing']}")
    df = parse_timestamps(df)
    df = compute_derived_fields(df)
    return df
