"""Unified severity computation for detection events and canonical potholes.

Single source of truth — replaces the 4 duplicated severity calculations
that were scattered across main.py and deduplication.py.
"""
from __future__ import annotations

from typing import Any


# Severity rank for escalation comparisons.
SEVERITY_RANKS: dict[str, int] = {
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}


def compute_severity(
    objects: list[dict[str, Any]] | None = None,
    metrics: dict[str, Any] | None = None,
) -> str:
    """Derive a severity label from detected objects and/or metrics.

    Priority order:
      1. Explicit ``metrics["severity"]`` (set by the worker post-inference).
      2. Heuristic from the largest detection bbox area.
      3. Default "Low".
    """
    sev = "Low"
    for obj in objects or []:
        bbox = obj.get("bbox", [0, 0, 0, 0])
        if len(bbox) >= 4:
            area = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1])
            if area > 0.5:
                sev = "Critical"
            elif area > 0.3:
                sev = "High"
            elif area > 0.15:
                sev = "Medium"

    if metrics and "severity" in metrics:
        sev = str(metrics["severity"]).capitalize()
    return sev


def escalate_severity(current_sev: str, new_sev: str) -> str:
    """Return the highest severity between *current_sev* and *new_sev*."""
    r_cur = SEVERITY_RANKS.get(current_sev.lower(), 1)
    r_new = SEVERITY_RANKS.get(new_sev.lower(), 1)
    if r_new > r_cur:
        return new_sev.capitalize()
    return current_sev.capitalize()


def extract_class_label(record: object) -> str:
    """Extract the anomaly class label from a model instance (CanonicalPothole or DetectionEvent)."""
    if hasattr(record, "observations") and record.observations:
        for obs in record.observations:
            if isinstance(obs, dict) and "label" in obs:
                return obs["label"]
    if hasattr(record, "objects") and record.objects:
        for obj in record.objects:
            if isinstance(obj, dict):
                return obj.get("label", "pothole")
    if hasattr(record, "metrics") and isinstance(record.metrics, dict):
        return record.metrics.get("label", "pothole")
    return "pothole"
