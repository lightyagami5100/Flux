"""Unit tests for the extracted SVG visualization module (app/visualization.py)."""
from __future__ import annotations

from app.visualization import render_road_snapshot_svg


def test_render_road_snapshot_svg_classes():
    classes = [
        "pothole",
        "crack",
        "manhole",
        "waterlogging",
        "sewage",
        "garbage_dump",
        "debris",
        "road_debris",
        "object",
    ]
    for cls in classes:
        svg = render_road_snapshot_svg(
            id_str="test-1234",
            severity="Critical",
            passes=3,
            lat=33.72,
            lon=73.09,
            confidence=0.95,
            anomaly_class=cls,
        )
        assert svg.startswith("<svg")
        assert svg.endswith("</svg>")
        assert "viewBox=\"0 0 480 270\"" in svg
        assert "GPS: 33.7200, 73.0900" in svg
        assert "CRITICAL (3 PASSES)" in svg
