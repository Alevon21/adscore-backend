"""
Shared helpers for AdScore v2.1.

NOTE: Verdict thresholds (SCALE_THRESHOLD, EXCLUDE_THRESHOLD, etc.) have been
moved to ScoringParams in models.py (K1 config extraction).
"""

from typing import Dict, Optional


def metric_display_name(key: str, event_labels: Optional[Dict[str, str]] = None) -> str:
    """Human-readable metric name, resolving event slots to labels.

    Shared helper used by verdict.py, insights.py, and campaign_scorer.py.
    """
    _NAMES = {
        "CTR": "CTR", "CPC": "CPC", "CPM": "CPM", "ROI": "ROI",
        "RPM": "RPM", "RPC": "RPC", "CR": "CR", "CPA": "CPA",
        "CPI": "CPI", "CR_install": "CR install",
    }
    if key in _NAMES:
        return _NAMES[key]
    labels = event_labels or {}
    if key.startswith("CR_"):
        slot = key[3:]
        return f"Конверсия: {labels.get(slot, slot)}"
    if key.startswith("CPA_"):
        slot = key[4:]
        return f"Стоимость: {labels.get(slot, slot)}"
    return key
