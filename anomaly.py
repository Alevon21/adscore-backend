"""
Rule-based anomaly detection for ad text scoring.
Detects suspicious metric patterns indicating data quality issues.

Must run BEFORE verdict assignment so verdicts can respect anomaly flags.

Spec: AdScore v2.1 Section 13.
"""

import logging
from typing import Dict, List, Optional

from models import TextResult

logger = logging.getLogger(__name__)

# Z-score thresholds (sigmoid-normalized, 0-1)
WEAK_Z = 0.35
STRONG_Z = 0.65
COST_EXTREME_Z = 0.05  # sigmoid(-3) ≈ 0.047
HIGH_CR_THRESHOLD = 0.50  # CR > 50% is suspicious


def detect_anomalies(
    results: List[TextResult],
    events: list,
) -> None:
    """
    Run anomaly detection on scored results (mutates in-place).

    Sets anomaly_detected=True and anomaly_code on suspicious texts.
    Must run BEFORE verdict assignment.

    Rules:
    1. Fraud signal: Low CTR + Low CR + suspiciously cheap CPA
    2. High conversion density: CR > 50%
    3. Cost metric extreme outlier: z ≤ 0.05
    """
    for result in results:
        z = result.z_scores or {}
        metrics = result.metrics or {}

        if not z:
            continue

        anomaly_code = _check_fraud_signal(z, events)
        if not anomaly_code:
            anomaly_code = _check_high_conversion_density(metrics, events)
        if not anomaly_code:
            anomaly_code = _check_cost_outlier(z)

        if anomaly_code:
            result.anomaly_detected = True
            result.anomaly_code = anomaly_code
            if "anomaly" not in (result.warnings or []):
                result.warnings.append("anomaly")
            logger.info(
                "Anomaly detected for %s: %s", result.text_id, anomaly_code
            )


def _check_fraud_signal(
    z: Dict[str, Optional[float]],
    events: list,
) -> Optional[str]:
    """
    Fraud: Low CTR (z < 0.35) + Low CR (z < 0.35) +
    suspiciously good CPA (z > 0.65, meaning very low cost).
    All three together = possible attribution fraud.
    """
    ctr_z = z.get("CTR")
    if ctr_z is None or ctr_z >= WEAK_Z:
        return None

    # Check any CR metric
    cr_z = _get_any_cr_z(z, events)
    if cr_z is None or cr_z >= WEAK_Z:
        return None

    # Check CPA (inverted: high z = low cost = suspiciously cheap)
    cpa_z = _get_any_cpa_z(z, events)
    if cpa_z is not None and cpa_z > STRONG_Z:
        return "attribution_anomaly"

    return None


def _check_high_conversion_density(
    metrics: Dict[str, Optional[float]],
    events: list,
) -> Optional[str]:
    """CR > 50% is suspicious for ad texts."""
    if events:
        for ev in events:
            cr_val = metrics.get(f"CR_{ev.slot}")
            if cr_val is not None and cr_val > HIGH_CR_THRESHOLD:
                return "high_conversion_density"

    cr_val = metrics.get("CR")
    if cr_val is not None and cr_val > HIGH_CR_THRESHOLD:
        return "high_conversion_density"

    return None


def _check_cost_outlier(z: Dict[str, Optional[float]]) -> Optional[str]:
    """
    Any cost metric z-score extremely low = extremely expensive.
    In sigmoid space, sigmoid(-3) ≈ 0.047.
    """
    cost_keys = [k for k in z if k.startswith("CPA") or k in ("CPC", "CPM", "CPI")]
    for k in cost_keys:
        val = z.get(k)
        if val is not None and val <= COST_EXTREME_Z:
            return "cost_outlier"

    return None


def _get_any_cr_z(z: dict, events: list) -> Optional[float]:
    """Get CR z-score for any event (deepest first)."""
    if events:
        for ev in reversed(events):
            val = z.get(f"CR_{ev.slot}")
            if val is not None:
                return val
    # Fix: explicit None check — 0.0 is a valid z-score
    val = z.get("CR")
    if val is not None:
        return val
    return z.get("CR_install")


def _get_any_cpa_z(z: dict, events: list) -> Optional[float]:
    """Get CPA z-score for any event (primary first)."""
    if events:
        for ev in events:
            if ev.is_primary:
                val = z.get(f"CPA_{ev.slot}")
                if val is not None:
                    return val
        val = z.get(f"CPA_{events[0].slot}")
        if val is not None:
            return val
    return z.get("CPA")
