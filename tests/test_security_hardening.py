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
