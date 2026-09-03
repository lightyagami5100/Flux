"""Security and hardening verification tests."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.upload_manager import UploadManager


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


def test_security_headers_present(client: TestClient):
    """Ensure standard OWASP security headers are present on all responses."""
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.headers.get("X-Content-Type-Options") == "nosniff"
    assert response.headers.get("X-Frame-Options") == "SAMEORIGIN"
    assert response.headers.get("X-XSS-Protection") == "1; mode=block"
    assert response.headers.get("Referrer-Policy") == "strict-origin-when-cross-origin"


def test_payload_size_limit_enforced(client: TestClient):
    """Ensure oversized request bodies are rejected with 413 Payload Too Large."""
    oversized_headers = {
        "Content-Length": "10000000",  # 10 MB > max_body_bytes (256 KB)
        "Content-Type": "application/json",
    }
    response = client.post("/v1/ingest/detections", data=b"{}", headers=oversized_headers)
    assert response.status_code == 413
    assert response.json()["detail"] == "Payload too large"


@pytest.mark.parametrize("path", ["/v1/ingest/upload", "/api/uploads"])
def test_upload_paths_exempt_from_payload_size_limit(client: TestClient, path: str):
    """Media upload routes carry multi-MiB chunks and must bypass the 256 KiB body cap."""
    oversized_headers = {
        "Content-Length": "10000000",  # 10 MB > max_body_bytes (256 KB)
        "Content-Type": "application/json",
    }
    response = client.post(path, data=b"{}", headers=oversized_headers)
    assert response.status_code != 413
    # Request reached the handler, so it fails validation instead of the size guard.
    assert response.status_code == 422


def test_unauthorized_access_rejected(client: TestClient):
    """Ensure protected ingestion endpoints reject requests without valid X-API-Key."""
    valid_payload = {
        "latitude": 33.72,
        "longitude": 73.09,
        "captured_at": "2026-08-29T12:00:00Z",
        "media_type": "image",
    }
    response = client.post("/v1/ingest/detections", json=valid_payload)
    assert response.status_code == 401
    assert "Missing X-API-Key" in response.json()["detail"]


def test_upload_filename_path_traversal_sanitized():
    """Ensure path traversal payloads in upload filenames are neutralized."""
    mgr = UploadManager(
        endpoint="localhost:9000",
        access_key="test",
        secret_key="test",
        bucket="test",
    )
    session = mgr.create_session(
        device_id="dev-01",
        filename="../../../../../etc/passwd",
        total_chunks=5,
        latitude=33.72,
        longitude=73.09,
    )
    assert "/" not in session.filename
    assert ".." not in session.filename
    assert session.filename == "passwd"


def test_upload_coordinate_boundary_validation():
    """Ensure invalid latitude/longitude coordinates are rejected."""
    mgr = UploadManager(
        endpoint="localhost:9000",
        access_key="test",
        secret_key="test",
        bucket="test",
    )
    with pytest.raises(ValueError, match="Invalid GPS coordinates"):
        mgr.create_session(
            device_id="dev-01",
            filename="sample.mp4",
            total_chunks=5,
            latitude=195.0,  # Invalid latitude (> 90)
            longitude=73.09,
        )


def test_upload_invalid_chunk_count_validation():
    """Ensure invalid total_chunks count is rejected."""
    mgr = UploadManager(
        endpoint="localhost:9000",
        access_key="test",
        secret_key="test",
        bucket="test",
    )
    with pytest.raises(ValueError, match="Invalid total_chunks"):
        mgr.create_session(
            device_id="dev-01",
            filename="sample.mp4",
            total_chunks=0,  # Invalid chunk count
            latitude=33.72,
            longitude=73.09,
        )


def test_seed_endpoint_disabled_in_production(client: TestClient):
    """POST /seed must return 404 in production to prevent unauthorized data loss."""
    from unittest.mock import patch, MagicMock
    with patch("app.main.get_settings") as mock_get_settings:
        mock_settings = MagicMock()
        mock_settings.is_production = True
        mock_settings.max_body_bytes = 262144
        mock_get_settings.return_value = mock_settings

        res = client.post("/seed")
        assert res.status_code == 404
        assert "not available in production" in res.json()["detail"].lower()


def test_upload_auth_enforced_when_required(client: TestClient):
    """When REQUIRE_UPLOAD_AUTH is True, /api/uploads must reject requests without X-API-Key."""
    from unittest.mock import patch, MagicMock
    with patch("app.deps.get_settings") as mock_get_settings:
        mock_settings = MagicMock()
        mock_settings.require_upload_auth = True
        mock_settings.api_keys = {"secret-key": "dev-01"}
        mock_get_settings.return_value = mock_settings

        # Without X-API-Key -> 401
        res = client.post("/api/uploads", json={
            "filename": "video.mp4",
            "total_chunks": 3,
            "latitude": 33.72,
            "longitude": 73.09,
        })
        assert res.status_code == 401


def test_upload_rate_limit_enforced(client: TestClient):
    """Ensure upload rate limit returns 429 when client exceeds request limit."""
    from unittest.mock import AsyncMock, patch
    mock_redis = AsyncMock()
    mock_redis.incr.return_value = 65

    with patch.object(app.state, "redis", mock_redis, create=True):
        res = client.post("/api/uploads", json={
            "device_id": "cam-01",
            "filename": "video.mp4",
            "total_chunks": 3,
            "latitude": 33.72,
            "longitude": 73.09,
        })
        assert res.status_code == 429
        assert "Upload rate limit exceeded" in res.json()["detail"]
        assert "Retry-After" in res.headers
