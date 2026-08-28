"""Unit & Integration Tests for MS-008: Spatial Deduplication & Canonical Pothole Clustering."""
import json
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from app.config import Settings
from app.deduplication import (
    haversine_distance,
    compute_event_severity,
    escalate_severity,
    cluster_detection,
    recluster_all_events,
)
from app.db import get_session
from app.main import app
from app.models import Base, CanonicalPothole, DetectionEvent, DetectionStatus, PotholeStatus
from app.upload_manager import UploadManager


# ── UNIT TESTS FOR MATHEMATICAL CORE ──

def test_haversine_distance_zero():
    """Identical points have 0 distance."""
    d = haversine_distance(33.7200, 73.0900, 33.7200, 73.0900)
    assert d == 0.0


def test_haversine_distance_small():
    """Points ~10 meters apart."""
    # 0.0001 deg lat is ~11.1 meters
    d = haversine_distance(33.7200, 73.0900, 33.7201, 73.0900)
    assert 10.0 <= d <= 12.5


def test_escalate_severity():
    """Severity escalation logic."""
    assert escalate_severity("Low", "High") == "High"
    assert escalate_severity("High", "Low") == "High"
    assert escalate_severity("Medium", "Critical") == "Critical"
    assert escalate_severity("Critical", "Medium") == "Critical"
    assert escalate_severity("Low", "Low") == "Low"


# ── INTEGRATION TESTS FOR CLUSTERING & APIS ──

@pytest.fixture
def test_client():
    app.state.settings = Settings(
        minio_endpoint="localhost:9000",
        minio_access_key="test",
        minio_secret_key="test",
        minio_bucket="test-bucket",
        redis_url="redis://localhost:6379/0",
        database_url="postgresql+asyncpg://postgres:postgres@localhost:5432/test",
    )
    app.state.redis = AsyncMock()
    app.state.upload_manager = UploadManager(
        endpoint=app.state.settings.minio_endpoint,
        access_key=app.state.settings.minio_access_key,
        secret_key=app.state.settings.minio_secret_key,
        bucket=app.state.settings.minio_bucket,
        secure=app.state.settings.minio_secure,
    )
    return TestClient(app)


class TestSpatialClusteringEngine:

    @pytest.mark.anyio
    async def test_cluster_single_detection(self):
        """First detection creates a new CanonicalPothole."""
        ev = DetectionEvent(
            event_id=uuid.uuid4(),
            device_id="mobile_1",
            captured_at=datetime.now(timezone.utc),
            received_at=datetime.now(timezone.utc),
            status=DetectionStatus.PROCESSED,
            media_kind="image",
            media_uri="minio://test/img1.jpg",
            latitude=33.7200,
            longitude=73.0900,
            object_count=1,
            objects=[{"label": "pothole", "confidence": 0.88, "bbox": [0.1, 0.1, 0.4, 0.4]}],
            metrics={"severity": "Medium"},
        )

        mock_session = AsyncMock(spec=AsyncSession)
        # Bounding box candidate query returns empty (no existing pothole)
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        pothole, is_new = await cluster_detection(mock_session, ev, radius_meters=10.0)

        assert is_new is True
        assert pothole.latitude == 33.7200
        assert pothole.longitude == 73.0900
        assert pothole.observation_count == 1
        assert pothole.severity == "Medium"
        assert len(pothole.observations) == 1
        assert ev.canonical_pothole_id == pothole.pothole_id
        mock_session.add.assert_called_once_with(pothole)

    @pytest.mark.anyio
    async def test_cluster_merges_nearby_detection(self):
        """Second detection within 5 meters merges into existing CanonicalPothole."""
        existing_pothole = CanonicalPothole(
            pothole_id=uuid.uuid4(),
            latitude=33.72000,
            longitude=73.09000,
            severity="Low",
            status=PotholeStatus.ACTIVE,
            observation_count=1,
            avg_confidence=0.80,
            first_detected_at=datetime(2026, 8, 25, 10, 0, 0, tzinfo=timezone.utc),
            last_detected_at=datetime(2026, 8, 25, 10, 0, 0, tzinfo=timezone.utc),
            primary_media_uri="minio://test/img1.jpg",
            observations=[{"event_id": "old_1", "confidence": 0.80}],
        )

        # New event 4 meters away with Critical severity
        new_ev = DetectionEvent(
            event_id=uuid.uuid4(),
            device_id="mobile_2",
            captured_at=datetime(2026, 8, 25, 10, 5, 0, tzinfo=timezone.utc),
            received_at=datetime(2026, 8, 25, 10, 5, 0, tzinfo=timezone.utc),
            status=DetectionStatus.PROCESSED,
            media_kind="image",
            media_uri="minio://test/img2.jpg",
            latitude=33.72003, # ~3.3 meters away
            longitude=73.09003,
            object_count=1,
            objects=[{"label": "pothole", "confidence": 0.95, "bbox": [0.0, 0.0, 0.9, 0.9]}],
            metrics={"severity": "Critical"},
        )

        mock_session = AsyncMock(spec=AsyncSession)
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [existing_pothole]
        mock_session.execute.return_value = mock_result

        pothole, is_new = await cluster_detection(mock_session, new_ev, radius_meters=10.0)

        assert is_new is False
        assert pothole.pothole_id == existing_pothole.pothole_id
        assert pothole.observation_count == 2
        # Severity escalated to Critical
        assert pothole.severity == "Critical"
        # Centroid updated
        assert pytest.approx(pothole.latitude, rel=1e-5) == (33.72000 + 33.72003) / 2
        assert len(pothole.observations) == 2
        assert new_ev.canonical_pothole_id == pothole.pothole_id


class TestPotholeEndpointsAPI:

    def test_get_detections_deduplicated_mode(self, test_client):
        """GET /detections?deduplicated=true returns canonical potholes."""
        mock_pothole = CanonicalPothole(
            pothole_id=uuid.uuid4(),
            latitude=33.7200,
            longitude=73.0900,
            severity="Critical",
            status=PotholeStatus.ACTIVE,
            observation_count=4,
            avg_confidence=0.92,
            first_detected_at=datetime(2026, 8, 25, 8, 0, 0, tzinfo=timezone.utc),
            last_detected_at=datetime(2026, 8, 25, 10, 0, 0, tzinfo=timezone.utc),
            primary_event_id=uuid.uuid4(),
            primary_media_uri="minio://test/pothole.jpg",
            observations=[],
        )

        async def override_get_session():
            mock_session = AsyncMock(spec=AsyncSession)
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = [mock_pothole]
            mock_session.execute.return_value = mock_result
            yield mock_session

        app.dependency_overrides[get_session] = override_get_session
        try:
            res = test_client.get("/detections?deduplicated=true")
            assert res.status_code == 200
            data = res.json()
            assert data["type"] == "FeatureCollection"
            assert len(data["features"]) == 1
            prop = data["features"][0]["properties"]
            assert prop["id"] == str(mock_pothole.pothole_id)
            assert prop["observation_count"] == 4
            assert prop["severity"] == "Critical"
            assert prop["status"] == "active"
            assert prop["thumbnail_url"] == f"/potholes/{mock_pothole.pothole_id}/media"
        finally:
            app.dependency_overrides.pop(get_session, None)

    def test_get_pothole_detail_endpoint(self, test_client):
        """GET /potholes/{pothole_id} returns full profile and observation history."""
        p_id = uuid.uuid4()
        mock_pothole = CanonicalPothole(
            pothole_id=p_id,
            latitude=33.7200,
            longitude=73.0900,
            severity="High",
            status=PotholeStatus.ACTIVE,
            observation_count=2,
            avg_confidence=0.89,
            first_detected_at=datetime(2026, 8, 25, 9, 0, 0, tzinfo=timezone.utc),
            last_detected_at=datetime(2026, 8, 25, 10, 0, 0, tzinfo=timezone.utc),
            primary_event_id=uuid.uuid4(),
            primary_media_uri="minio://test/best.jpg",
            observations=[
                {"event_id": "e1", "confidence": 0.85, "captured_at": "2026-08-25T09:00:00Z"},
                {"event_id": "e2", "confidence": 0.93, "captured_at": "2026-08-25T10:00:00Z"},
            ],
        )

        async def override_get_session():
            mock_session = AsyncMock(spec=AsyncSession)
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = mock_pothole
            mock_session.execute.return_value = mock_result
            yield mock_session

        app.dependency_overrides[get_session] = override_get_session
        try:
            res = test_client.get(f"/potholes/{p_id}")
            assert res.status_code == 200
            data = res.json()
            assert data["pothole_id"] == str(p_id)
            assert data["observation_count"] == 2
            assert len(data["observations"]) == 2
        finally:
            app.dependency_overrides.pop(get_session, None)

    def test_patch_pothole_status(self, test_client):
        """PATCH /potholes/{pothole_id}/status marks pothole as repaired."""
        p_id = uuid.uuid4()
        mock_pothole = CanonicalPothole(
            pothole_id=p_id,
            latitude=33.7200,
            longitude=73.0900,
            severity="High",
            status=PotholeStatus.ACTIVE,
            observation_count=1,
            avg_confidence=0.90,
            first_detected_at=datetime.now(timezone.utc),
            last_detected_at=datetime.now(timezone.utc),
            primary_media_uri="minio://test/p.jpg",
            observations=[],
        )

        async def override_get_session():
            mock_session = AsyncMock(spec=AsyncSession)
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = mock_pothole
            mock_session.execute.return_value = mock_result
            yield mock_session

        app.dependency_overrides[get_session] = override_get_session
        try:
            res = test_client.patch(f"/potholes/{p_id}/status", json={"status": "repaired"})
            assert res.status_code == 200
            data = res.json()
            assert data["status"] == "repaired"
            assert mock_pothole.status == PotholeStatus.REPAIRED
        finally:
            app.dependency_overrides.pop(get_session, None)

    def test_rebuild_clusters_endpoint(self, test_client):
        """POST /api/deduplicate/rebuild triggers batch sweep."""
        with patch("app.main.recluster_all_events", new_callable=AsyncMock) as mock_recluster:
            mock_recluster.return_value = 12

            async def override_get_session():
                mock_session = AsyncMock(spec=AsyncSession)
                yield mock_session

            app.dependency_overrides[get_session] = override_get_session
            try:
                res = test_client.post("/api/deduplicate/rebuild")
                assert res.status_code == 200
                data = res.json()
                assert data["status"] == "ok"
                mock_recluster.assert_called_once()
            finally:
                app.dependency_overrides.pop(get_session, None)
