import logging
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Unified schema — all MMP data is normalised to these columns
# ---------------------------------------------------------------------------

UNIFIED_COLUMNS = [
    "event_time", "installed_at", "click_time", "engagement_time",
    "tracker", "campaign",
    "device_id", "ip_string", "country",
    "activity_kind", "is_impression_based", "reattributed_at",
    "conversion_duration",
]

# Legacy name kept for backwards-compatibility with mmp_fraud.py
REQUIRED_COLUMNS = [
    "event_time", "installed_at", "click_time", "engagement_time",
    "adjust_tracker", "adjust_campaign",
    "device_id", "ip_string", "country",
    "activity_kind", "is_impression_based", "reattributed_at",
    "conversion_duration",
]

CTIT_BINS = [-np.inf, 15, 60, 900, 3600, 10800, 21600, 86400, np.inf]
CTIT_LABELS = ["<15s", "15-60s", "1-15m", "15-60m", "1-3h", "3-6h", "6-24h", ">24h"]

VTIT_BINS = [-np.inf, 60, 300, 900, 3600, 10800, 43200, 86400, np.inf]
VTIT_LABELS = ["<1m", "1-5m", "5-15m", "15-60m", "1-3h", "3-12h", "12-24h", ">24h"]


# ---------------------------------------------------------------------------
# MMP-specific column mappings
# ---------------------------------------------------------------------------

# Adjust raw column → unified column
# Note: Adjust CSV column names depend on export config (placeholders).
# We support the most common export format used by our clients.
ADJUST_COLUMN_MAP = {
    "event_time": "event_time",
    "created_at": "event_time",     # alternative Adjust placeholder
    "installed_at": "installed_at",
    "click_time": "click_time",
    "engagement_time": "engagement_time",
    "adjust_tracker": "tracker",
    "network_name": "tracker",      # alternative Adjust placeholder
    "adjust_campaign": "campaign",
    "campaign_name": "campaign",    # alternative Adjust placeholder
    "device_id": "device_id",
    "adid": "device_id",           # Adjust device ID
    "ip_string": "ip_string",
    "ip_address": "ip_string",     # alternative
    "country": "country",
    "city": "city",
    "activity_kind": "activity_kind",
    "is_impression_based": "is_impression_based",
    "match_type": "match_type",
    "reattributed_at": "reattributed_at",
    "conversion_duration": "conversion_duration",
    # Device IDs
    "idfa": "idfa",
    "gps_adid": "gaid",
    "idfv": "idfv",
    # Optional
    "adjust_platform": "platform",
    "os_name": "platform",         # alternative
    "campaign_id_name": "campaign_id",
    "adgroup_name": "adset",
    "adgroup_id": "adset_id",
    "creative_name": "creative",
    "creative_id": "creative_id",
    "channel_name": "channel",
    "publisher_id": "publisher",
    "site_id": "publisher_id",
    "app_version": "app_version",
    "revenue": "revenue",
    "currency": "currency",
}

# AppsFlyer raw column → unified column
APPSFLYER_COLUMN_MAP = {
    "event_time": "event_time",
    "install_time": "installed_at",
    "click_time": "click_time",
    "touch_time": "engagement_time",
    # Tracker / campaign
    "media_source": "tracker",
    "campaign": "campaign",
    # Device & network
    "appsflyer_id": "device_id",
    "ip": "ip_string",
    "country_code": "country",
    "city": "city",
    # Activity
    "event_name": "activity_kind",
    "is_retargeting": "is_retargeting",  # handled separately
    "attributed_touch_type": "attributed_touch_type",  # "click" | "impression"
    # Timing
    "click_to_install_time": "conversion_duration",
    # Device IDs
    "idfa": "idfa",
    "advertising_id": "gaid",
    "idfv": "idfv",
    # Revenue
    "event_revenue": "revenue",
    "event_revenue_usd": "revenue_usd",
    "event_revenue_currency": "currency",
    # Optional
    "platform": "platform",
    "channel": "channel",
    "sub_param_1": "sub_param_1",
    "campaign_id": "campaign_id",
    "adset": "adset",
    "adset_id": "adset_id",
    "ad": "creative",
    "ad_id": "creative_id",
    "af_siteid": "publisher",
    "app_version": "app_version",
}

# Singular raw column → unified column
# Docs: Singular export logs use these column names
SINGULAR_COLUMN_MAP = {
    "adjusted_timestamp": "event_time",
    "attribution_event_timestamp": "installed_at",
    "touchpoint_timestamp": "click_time",
    # Tracker / campaign
    "partner": "tracker",
    "campaign_name": "campaign",
    "singular_campaign_name": "campaign",  # alias
    # Device & network
    "device_id": "device_id",
    "ip": "ip_string",
    "country": "country",
    "city": "city",
    # Activity
    "name": "activity_kind",
    "is_reengagement": "is_retargeting",
    "is_view_through": "is_view_through",  # bool, handled in normalise
    # Revenue
    "revenue": "revenue",
    "converted_revenue": "revenue_usd",
    "currency": "currency",
    # Optional
    "platform": "platform",
    "campaign_id": "campaign_id",
    "sub_campaign_name": "adset",
    "sub_campaign_id": "adset_id",
    "creative_name": "creative",
    "creative_id": "creative_id",
    "publisher_name": "publisher",
    "publisher_id": "publisher_id",
    "app_version": "app_version",
    # Note: device_id holds IDFA or GAID depending on device_id_type column
}

# Branch raw column → unified column
# Docs: Branch export CSVs use long prefixed column names
BRANCH_COLUMN_MAP = {
    "timestamp": "event_time",
    "timestamp_iso": "event_time",  # alias
    "last_attributed_touch_timestamp": "click_time",
    "referrer_click_timestamp": "click_time",  # alias
    # Tracker / campaign
    "last_attributed_touch_data_tilde_advertising_partner_name": "tracker",
    "last_attributed_touch_data_tilde_campaign": "campaign",
    # Device & network
    "developer_identity": "device_id",
    "user_data_ip": "ip_string",
    "user_data_geo_country_code": "country",
    "user_data_geo_city": "city",
    # Activity
    "name": "activity_kind",
    "origin": "origin",  # REENGAGEMENT → retargeting, handled in normalise
    "last_attributed_touch_type": "attributed_touch_type",
    # Revenue
    "revenue": "revenue",
    # Device IDs
    "user_data_idfa": "idfa",
    "user_data_aaid": "gaid",
    "user_data_idfv": "idfv",
    # Optional
    "user_data_os": "platform",
    "user_data_app_version": "app_version",
    "last_attributed_touch_data_tilde_campaign_id": "campaign_id",
    "last_attributed_touch_data_tilde_ad_set_name": "adset",
    "last_attributed_touch_data_tilde_ad_set_id": "adset_id",
    "last_attributed_touch_data_tilde_ad_name": "creative",
    "last_attributed_touch_data_tilde_ad_id": "creative_id",
    "last_attributed_touch_data_tilde_channel": "channel",
    "last_attributed_touch_data_tilde_secondary_publisher": "publisher",
    "id": "branch_id",
}

# Signature columns unique to each MMP (used for auto-detection)
ADJUST_SIGNATURE = {"adjust_tracker", "adjust_campaign", "conversion_duration"}
APPSFLYER_SIGNATURE = {"media_source", "appsflyer_id", "attributed_touch_type"}
SINGULAR_SIGNATURE = {"partner", "touchpoint_timestamp", "attribution_event_timestamp"}
BRANCH_SIGNATURE = {"developer_identity", "last_attributed_touch_data_tilde_campaign", "user_data_geo_country_code"}


# ---------------------------------------------------------------------------
# Auto-detection
# ---------------------------------------------------------------------------

def detect_mmp_type(columns: set) -> Optional[str]:
    """Detect MMP type from column names. Returns 'adjust', 'appsflyer', 'singular', 'branch', or None."""
    cols_lower = {c.lower().strip() for c in columns}

    # Check Branch first (most specific — very long column names)
    branch_matches = sum(1 for sig in BRANCH_SIGNATURE if sig in cols_lower)
    if branch_matches >= 2:
        return "branch"

    # Check Singular
    singular_matches = sum(1 for sig in SINGULAR_SIGNATURE if sig in cols_lower)
    if singular_matches >= 2:
        return "singular"

    # Check AppsFlyer
    af_matches = sum(1 for sig in APPSFLYER_SIGNATURE if sig in cols_lower)
    if af_matches >= 2:
        return "appsflyer"

    # Check Adjust
    adj_matches = sum(1 for sig in ADJUST_SIGNATURE if sig in cols_lower)
    if adj_matches >= 2:
        return "adjust"

    return None


# ---------------------------------------------------------------------------
# Column normalisation
# ---------------------------------------------------------------------------

def normalise_columns(df: pd.DataFrame, mmp_type: str) -> pd.DataFrame:
    """Rename MMP-specific columns to unified schema. Returns new DataFrame."""
    if mmp_type == "adjust":
        col_map = ADJUST_COLUMN_MAP
    elif mmp_type == "appsflyer":
        col_map = APPSFLYER_COLUMN_MAP
    elif mmp_type == "singular":
        col_map = SINGULAR_COLUMN_MAP
    elif mmp_type == "branch":
        col_map = BRANCH_COLUMN_MAP
    else:
        return df

    # Build rename dict (only for columns that exist)
    rename = {}
    for raw, unified in col_map.items():
        if raw in df.columns and raw != unified:
            rename[raw] = unified

    df = df.rename(columns=rename)

    # AppsFlyer-specific transformations
    if mmp_type == "appsflyer":
        # is_impression_based: derived from attributed_touch_type
        if "attributed_touch_type" in df.columns:
            df["is_impression_based"] = df["attributed_touch_type"].astype(str).str.lower().str.strip() == "impression"
        else:
            df["is_impression_based"] = False

        # reattributed_at: AppsFlyer uses is_retargeting boolean instead
        if "is_retargeting" in df.columns:
            retarget = df["is_retargeting"].astype(str).str.lower().isin(["true", "1", "yes"])
            # For retargeted installs, use install time as reattributed_at marker
            if "installed_at" in df.columns:
                df["reattributed_at"] = pd.NaT
                df.loc[retarget, "reattributed_at"] = df.loc[retarget, "installed_at"]
            else:
                df["reattributed_at"] = pd.NaT
        elif "reattributed_at" not in df.columns:
            df["reattributed_at"] = pd.NaT

        # conversion_duration: AppsFlyer's click_to_install_time might be in
        # "HH:MM:SS" format or seconds — handle both
        if "conversion_duration" in df.columns:
            cd = df["conversion_duration"]
            # Try to detect HH:MM:SS format
            if cd.dtype == object:
                def _parse_ctit(val):
                    if pd.isna(val):
                        return np.nan
                    s = str(val).strip()
                    if ":" in s:
                        parts = s.split(":")
                        try:
                            if len(parts) == 3:
                                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                            elif len(parts) == 2:
                                return int(parts[0]) * 60 + int(parts[1])
                        except (ValueError, IndexError):
                            return np.nan
                    try:
                        return float(s)
                    except ValueError:
                        return np.nan

                df["conversion_duration"] = cd.apply(_parse_ctit)
            else:
                df["conversion_duration"] = pd.to_numeric(cd, errors="coerce")

        # activity_kind: normalise AppsFlyer event names
        if "activity_kind" in df.columns:
            df["activity_kind"] = df["activity_kind"].fillna("install")

    # Singular-specific transformations
    if mmp_type == "singular":
        # is_impression_based: Singular uses boolean is_view_through field
        if "is_view_through" in df.columns:
            df["is_impression_based"] = df["is_view_through"].astype(str).str.lower().isin(["true", "1", "yes"])
        else:
            df["is_impression_based"] = False

        # reattributed_at from is_reengagement
        if "is_retargeting" in df.columns:
            retarget = df["is_retargeting"].astype(str).str.lower().isin(["true", "1", "yes"])
            if "installed_at" in df.columns:
                df["reattributed_at"] = pd.NaT
                df.loc[retarget, "reattributed_at"] = df.loc[retarget, "installed_at"]
            else:
                df["reattributed_at"] = pd.NaT
        elif "reattributed_at" not in df.columns:
            df["reattributed_at"] = pd.NaT

        # conversion_duration: Singular has no direct field — compute from timestamps
        if "conversion_duration" not in df.columns:
            if "installed_at" in df.columns and "click_time" in df.columns:
                installed = pd.to_datetime(df["installed_at"], errors="coerce")
                clicked = pd.to_datetime(df["click_time"], errors="coerce")
                df["conversion_duration"] = (installed - clicked).dt.total_seconds()
                df.loc[df["conversion_duration"] < 0, "conversion_duration"] = np.nan

        if "activity_kind" in df.columns:
            df["activity_kind"] = df["activity_kind"].fillna("install")

    # Branch-specific transformations
    if mmp_type == "branch":
        # is_impression_based
        if "attributed_touch_type" in df.columns:
            df["is_impression_based"] = df["attributed_touch_type"].astype(str).str.lower().str.strip() == "impression"
        else:
            df["is_impression_based"] = False

        # reattributed_at: Branch uses origin=REENGAGEMENT
        if "origin" in df.columns:
            retarget = df["origin"].astype(str).str.upper().str.strip() == "REENGAGEMENT"
            df["reattributed_at"] = pd.NaT
            if "event_time" in df.columns:
                df.loc[retarget, "reattributed_at"] = df.loc[retarget, "event_time"]
        elif "reattributed_at" not in df.columns:
            df["reattributed_at"] = pd.NaT

        # Branch has no separate installed_at — for install events, timestamp IS install time
        if "installed_at" not in df.columns and "event_time" in df.columns:
            if "activity_kind" in df.columns:
                install_mask = df["activity_kind"].astype(str).str.lower().str.strip() == "install"
                df["installed_at"] = pd.NaT
                df.loc[install_mask, "installed_at"] = df.loc[install_mask, "event_time"]

        # Branch has no direct conversion_duration — compute from timestamps
        if "conversion_duration" not in df.columns:
            if "installed_at" in df.columns and "click_time" in df.columns:
                installed = pd.to_datetime(df["installed_at"], errors="coerce")
                clicked = pd.to_datetime(df["click_time"], errors="coerce")
                df["conversion_duration"] = (installed - clicked).dt.total_seconds()
                df.loc[df["conversion_duration"] < 0, "conversion_duration"] = np.nan

        if "activity_kind" in df.columns:
            df["activity_kind"] = df["activity_kind"].fillna("install")

    # Add legacy columns for backwards-compatibility with mmp_fraud.py
    if "tracker" in df.columns and "adjust_tracker" not in df.columns:
        df["adjust_tracker"] = df["tracker"]
    if "campaign" in df.columns and "adjust_campaign" not in df.columns:
        df["adjust_campaign"] = df["campaign"]

    return df


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_columns(df: pd.DataFrame, mmp_type: Optional[str] = None) -> dict:
    """Validate that required columns are present.

    If mmp_type is provided, checks against that MMP's expected columns.
    If not, checks against legacy REQUIRED_COLUMNS (Adjust).
    """
    cols = set(df.columns)

    if mmp_type == "appsflyer":
        af_required = {"media_source", "install_time", "appsflyer_id", "country_code"}
        missing = sorted(af_required - cols)
    elif mmp_type == "singular":
        singular_required = {"partner", "attribution_event_timestamp"}
        missing = sorted(singular_required - cols)
    elif mmp_type == "branch":
        branch_required = {"developer_identity", "last_attributed_touch_data_tilde_campaign"}
        missing = sorted(branch_required - cols)
    elif mmp_type == "adjust":
        missing = sorted(set(REQUIRED_COLUMNS) - cols)
    else:
        # Legacy: check for Adjust columns
        missing = sorted(set(REQUIRED_COLUMNS) - cols)

    return {
        "ok": len(missing) == 0,
        "missing": missing,
        "detected": sorted(cols),
        "n_rows": len(df),
        "mmp_type": mmp_type or detect_mmp_type(cols),
    }


# ---------------------------------------------------------------------------
# Timestamp parsing & derived fields (shared logic)
# ---------------------------------------------------------------------------

def parse_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["event_time", "installed_at", "click_time", "engagement_time", "reattributed_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    if "conversion_duration" in df.columns:
        df["conversion_duration"] = pd.to_numeric(df["conversion_duration"], errors="coerce")
    if "is_impression_based" in df.columns:
        if df["is_impression_based"].dtype == object:
            df["is_impression_based"] = df["is_impression_based"].astype(str).str.lower().isin(["true", "1", "yes"])
    return df


def compute_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    # Detect reattributions first — needed for CTIT strategy
    if "reattributed_at" in df.columns:
        df["is_reattribution"] = df["reattributed_at"].notna()
    else:
        df["is_reattribution"] = False

    # CTIT: use MMP's conversion_duration as primary source (handles
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

    # VTIT: View-Through Install Time (for VTA / impression-based traffic)
    if "is_impression_based" in df.columns and "installed_at" in df.columns and "engagement_time" in df.columns:
        vta_mask = df["is_impression_based"] == True
        df["vtit_seconds"] = np.nan
        df.loc[vta_mask, "vtit_seconds"] = (
            df.loc[vta_mask, "installed_at"] - df.loc[vta_mask, "engagement_time"]
        ).dt.total_seconds()
        df["vtit_bucket"] = pd.cut(df["vtit_seconds"], bins=VTIT_BINS, labels=VTIT_LABELS)

    return df


def parse_mmp_csv(content: bytes) -> pd.DataFrame:
    """Parse raw CSV bytes into processed DataFrame with auto-detection of MMP type."""
    from io import BytesIO
    df = pd.read_csv(BytesIO(content), low_memory=False)

    mmp_type = detect_mmp_type(set(df.columns))
    if mmp_type:
        logger.info("Auto-detected MMP type: %s", mmp_type)
        df = normalise_columns(df, mmp_type)

    validation = validate_columns(df, mmp_type)
    if not validation["ok"]:
        raise ValueError(f"Missing columns: {validation['missing']}")
    df = parse_timestamps(df)
    df = compute_derived_fields(df)
    return df
