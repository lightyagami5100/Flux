"""Integration tests for MS-007 Detection Visualization & Map API."""
from __future__ import annotations

import io
import json
import uuid
from datetime import datetime, timezone, timedelta, UTC
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models import DetectionEvent, DetectionStatus
from app.db import get_session


@pytest.fixture
def mock_detection_events():
    now = datetime.now(UTC)
    ev1 = DetectionEvent(
        event_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        device_id="cam-01",
        schema_version=1,
        captured_at=now - timedelta(minutes=10),
        received_at=now - timedelta(minutes=9),
        processed_at=now - timedelta(minutes=8),
        status=DetectionStatus.PROCESSED,
        media_kind="image",
        media_uri="minio://media/pothole_high.jpg",
        media_sha256="abc",
        latitude=33.7200,
        longitude=73.0900,
        object_count=1,
        objects=[{"label": "pothole", "confidence": 0.95, "bbox": [0.1, 0.1, 0.9, 0.9]}],
        metrics={"severity": "High"},
        processing_ms=120,
        error=None,
    )

    ev2 = DetectionEvent(
        event_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        device_id="cam-02",
        schema_version=1,
        captured_at=now - timedelta(hours=2),
        received_at=now - timedelta(hours=2),
        processed_at=now - timedelta(hours=2),
        status=DetectionStatus.PROCESSED,
        media_kind="image",
        media_uri="minio://media/pothole_low.jpg",
        media_sha256="def",
        latitude=31.5200,
        longitude=74.3500,
        object_count=1,
        objects=[{"label": "pothole", "confidence": 0.60, "bbox": [0.05, 0.05, 0.15, 0.15]}],
        metrics={"severity": "Low"},
        processing_ms=85,
        error=None,
    )
    return [ev1, ev2]


@pytest.fixture
def test_client_with_db(mock_detection_events):
    mock_session = AsyncMock()

    class FakeResult:
        def __init__(self, items):
            self._items = items
        def scalars(self):
            return self
        def all(self):
            return self._items
        def scalar_one_or_none(self):
            return self._items[0] if self._items else None

    # Setup mock session execute to return events based on query
    async def fake_execute(stmt):
        return FakeResult(mock_detection_events)

    mock_session.execute = AsyncMock(side_effect=fake_execute)

    async def override_get_session():
        yield mock_session

    app.dependency_overrides[get_session] = override_get_session

    mock_settings = MagicMock()
    mock_settings.auto_create_tables = False
    mock_settings.max_body_bytes = 10000000
    mock_settings.minio_endpoint = "localhost:9000"
    mock_settings.minio_access_key = "test"
    mock_settings.minio_secret_key = "test"
    mock_settings.minio_bucket = "media"
    mock_settings.minio_secure = False
    mock_settings.ingest_stream = "stream:detections"
    mock_settings.is_production = False
    mock_settings.missing_production_settings.return_value = []

    mock_redis = AsyncMock()

    with patch("app.main.aioredis.from_url", return_value=mock_redis), \
         patch("app.main.get_settings", return_value=mock_settings), \
         patch("app.main.UploadManager"):
        with TestClient(app) as client:
            yield client, mock_session, mock_detection_events

    app.dependency_overrides.clear()


class TestVisualizationAPI:
    def test_get_detections_feature_collection(self, test_client_with_db):
        client, _, events = test_client_with_db
        res = client.get("/detections")
        assert res.status_code == 200
        data = res.json()
        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 2
        f1 = data["features"][0]
        assert f1["geometry"]["type"] == "Point"
        assert f1["properties"]["severity"] == "High"
        assert "thumbnail_url" in f1["properties"]

    def test_get_detections_bbox_filtering(self, test_client_with_db):
        client, mock_session, events = test_client_with_db
        res = client.get("/detections?min_lat=33.0&max_lat=34.0&min_lon=73.0&max_lon=74.0")
        assert res.status_code == 200
        assert mock_session.execute.called

    def test_get_detections_severity_filter(self, test_client_with_db):
        client, _, _ = test_client_with_db
        res = client.get("/detections?severity=High")
        assert res.status_code == 200
        features = res.json()["features"]
        assert all(f["properties"]["severity"].lower() == "high" for f in features)

    def test_get_detection_detail_200(self, test_client_with_db):
        client, mock_session, events = test_client_with_db
        # Mock single item lookup
        class FakeSingleResult:
            def scalar_one_or_none(self):
                return events[0]

        mock_session.execute.side_effect = None
        mock_session.execute.return_value = FakeSingleResult()

        target_id = str(events[0].event_id)
        res = client.get(f"/detections/{target_id}")
        assert res.status_code == 200
        data = res.json()
        assert data["event_id"] == target_id
        assert data["device_id"] == "cam-01"
        assert data["severity"] == "High"
        assert data["object_count"] == 1

    def test_get_detection_detail_404(self, test_client_with_db):
        client, mock_session, _ = test_client_with_db
        class FakeEmptyResult:
            def scalar_one_or_none(self):
                return None

        mock_session.execute.side_effect = None
        mock_session.execute.return_value = FakeEmptyResult()

        nonexistent = str(uuid.uuid4())
        res = client.get(f"/detections/{nonexistent}")
        assert res.status_code == 404

    def test_get_detection_media_fallback_svg(self, test_client_with_db):
        client, mock_session, events = test_client_with_db
        class FakeSingleResult:
            def scalar_one_or_none(self):
                return events[0]

        mock_session.execute.side_effect = None
        mock_session.execute.return_value = FakeSingleResult()

        # MinIO will fail to connect in unit test, so it falls back to the high-tech SVG badge
        res = client.get(f"/detections/{events[0].event_id}/media")
        assert res.status_code == 200
        assert b"POTHOLE" in res.content or b"FLUX" in res.content or b"ANOMALY" in res.content

    def test_export_geojson_download(self, test_client_with_db):
        client, _, _ = test_client_with_db
        res = client.get("/detections/export/geojson")
        assert res.status_code == 200
        assert "application/geo+json" in res.headers["content-type"]
        assert "attachment; filename=" in res.headers["content-disposition"]
        data = res.json()
        assert data["type"] == "FeatureCollection"
