"""Unit tests for the unified severity engine (app/severity.py)."""
from __future__ import annotations

from app.severity import (
    compute_severity,
    escalate_severity,
    extract_class_label,
    SEVERITY_RANKS,
)


def test_severity_ranks():
    assert SEVERITY_RANKS["critical"] > SEVERITY_RANKS["high"]
    assert SEVERITY_RANKS["high"] > SEVERITY_RANKS["medium"]
    assert SEVERITY_RANKS["medium"] > SEVERITY_RANKS["low"]


def test_compute_severity_default():
    assert compute_severity() == "Low"
    assert compute_severity(objects=[]) == "Low"


def test_compute_severity_from_bbox():
    # Area > 0.5 -> Critical
    obj_crit = [{"bbox": [0.0, 0.0, 0.8, 0.8]}]  # area = 0.64
    assert compute_severity(objects=obj_crit) == "Critical"

    # Area > 0.3 -> High
    obj_high = [{"bbox": [0.0, 0.0, 0.6, 0.6]}]  # area = 0.36
    assert compute_severity(objects=obj_high) == "High"

    # Area > 0.15 -> Medium
    obj_med = [{"bbox": [0.0, 0.0, 0.5, 0.4]}]  # area = 0.20
    assert compute_severity(objects=obj_med) == "Medium"

    # Area <= 0.15 -> Low
    obj_low = [{"bbox": [0.0, 0.0, 0.2, 0.2]}]  # area = 0.04
    assert compute_severity(objects=obj_low) == "Low"


def test_compute_severity_metrics_override():
    # Metrics severity overrides bbox area
    obj_low = [{"bbox": [0.0, 0.0, 0.1, 0.1]}]
    assert compute_severity(objects=obj_low, metrics={"severity": "critical"}) == "Critical"
    assert compute_severity(objects=obj_low, metrics={"severity": "high"}) == "High"


def test_escalate_severity():
    assert escalate_severity("Low", "High") == "High"
    assert escalate_severity("High", "Low") == "High"
    assert escalate_severity("Medium", "Critical") == "Critical"
    assert escalate_severity("Critical", "Medium") == "Critical"
    assert escalate_severity("low", "medium") == "Medium"


def test_extract_class_label():
    class DummyRecordWithObservations:
        observations = [{"label": "manhole"}]

    class DummyRecordWithObjects:
        observations = None
        objects = [{"label": "crack"}]

    class DummyRecordWithMetrics:
        observations = None
        objects = None
        metrics = {"label": "waterlogging"}

    class DummyRecordDefault:
        pass

    assert extract_class_label(DummyRecordWithObservations()) == "manhole"
    assert extract_class_label(DummyRecordWithObjects()) == "crack"
    assert extract_class_label(DummyRecordWithMetrics()) == "waterlogging"
    assert extract_class_label(DummyRecordDefault()) == "pothole"
