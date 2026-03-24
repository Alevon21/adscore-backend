import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from mmp_parser import validate_columns, parse_timestamps, compute_derived_fields, REQUIRED_COLUMNS


def make_df(**overrides):
    """Create minimal valid Adjust DataFrame."""
    base = {col: ["test"] for col in REQUIRED_COLUMNS}
    base.update({
        "installed_at": ["2025-04-27T08:27:43"],
        "click_time": ["2025-04-27T08:25:00"],
        "event_time": ["2025-04-27T08:27:43"],
        "engagement_time": ["2025-04-27T08:24:00"],
        "reattributed_at": [None],
        "conversion_duration": ["163"],
        "is_impression_based": ["false"],
    })
    base.update(overrides)
    return pd.DataFrame(base)


class TestValidateColumns:
    def test_valid_csv_passes(self):
        df = make_df()
        result = validate_columns(df)
        assert result["ok"] is True
        assert result["missing"] == []

    def test_missing_required_column_fails(self):
        df = make_df()
        df = df.drop(columns=["adjust_tracker"])
        result = validate_columns(df)
        assert result["ok"] is False
        assert "adjust_tracker" in result["missing"]

    def test_extra_columns_ok(self):
        df = make_df(extra_col=["foo"])
        result = validate_columns(df)
        assert result["ok"] is True


class TestComputeDerivedFields:
    def test_ctit_seconds_calculated(self):
        df = make_df()
        df = parse_timestamps(df)
        df = compute_derived_fields(df)
        assert abs(df["ctit_seconds"].iloc[0] - 163.0) < 1.0

    def test_install_hour_extracted(self):
        df = make_df()
        df = parse_timestamps(df)
        df = compute_derived_fields(df)
        assert df["install_hour"].iloc[0] == 8

    def test_ctit_bucket_assigned(self):
        df = make_df()
        df = parse_timestamps(df)
        df = compute_derived_fields(df)
        assert df["ctit_bucket"].iloc[0] == "1-15m"

    def test_is_reattribution_false_when_null(self):
        df = make_df()
        df = parse_timestamps(df)
        df = compute_derived_fields(df)
        assert df["is_reattribution"].iloc[0] == False

    def test_is_reattribution_true_when_set(self):
        df = make_df(reattributed_at=["2025-04-28T10:00:00"])
        df = parse_timestamps(df)
        df = compute_derived_fields(df)
        assert df["is_reattribution"].iloc[0] == True
