import uuid
from datetime import datetime, timezone
import sys
from unittest.mock import AsyncMock, patch, MagicMock

# Mock out SQLAlchemy engine creation so it doesn't try to connect to the DB
import sqlalchemy.ext.asyncio
def mock_create(*args, **kwargs):
    engine = MagicMock()
    engine.dispose = AsyncMock()
    engine.begin = MagicMock()
    return engine
orig_create = sqlalchemy.ext.asyncio.create_async_engine
sqlalchemy.ext.asyncio.create_async_engine = mock_create
sys.modules["minio"] = MagicMock()

import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_healthz():
    with patch("app.main.aioredis.from_url") as mock_from_url, \
         patch("app.main.get_settings") as mock_get_settings, \
         patch("app.main.UploadManager") as mock_upload_mgr:
        mock_settings = MagicMock()
        mock_settings.auto_create_tables = False
        mock_settings.minio_endpoint = "localhost:9000"
        mock_settings.minio_access_key = "test"
        mock_settings.minio_secret_key = "test"
        mock_settings.minio_bucket = "media"
        mock_settings.minio_secure = False
        mock_settings.ingest_stream = "stream:detections"
        mock_get_settings.return_value = mock_settings
        mock_from_url.return_value = AsyncMock()
        with TestClient(app) as client:
            response = client.get("/healthz")
            assert response.status_code == 200
            assert response.json() == {"status": "ok"}

@patch("app.main.reserve")
@patch("app.main.store")
@patch("app.deps.get_redis")
def test_ingest_detection(mock_get_redis, mock_store, mock_reserve):
    # Mock reserve to return (claimed=True, replay=None)
    mock_reserve.return_value = (True, None)
    
    mock_redis = AsyncMock()
    mock_get_redis.return_value = mock_redis

    event_id = str(uuid.uuid4())
    payload = {
        "schema_version": 1,
        "event_id": event_id,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "media": {
            "kind": "image",
            "uri": "minio://bucket/test.jpg"
        },
        "objects": [
            {
                "label": "pothole",
                "confidence": 0.95,
                "bbox": [0.1, 0.1, 0.9, 0.9]
            }
        ]
    }

    # API needs X-API-Key because of DeviceIdentity dependency
    # Let's mock get_current_device dependency or pass a dummy key if it's configured
    # We can use override_dependencies on the FastAPI app
    from app.deps import get_current_device, get_redis
    app.dependency_overrides[get_current_device] = lambda: type("DeviceIdentity", (), {"device_id": "test-device"})()
    app.dependency_overrides[get_redis] = lambda: mock_redis

    with patch("app.main.aioredis.from_url") as mock_from_url, \
         patch("app.main.get_settings") as mock_get_settings, \
         patch("app.main.UploadManager") as mock_upload_mgr:
        mock_settings = MagicMock()
        mock_settings.auto_create_tables = False
        mock_settings.max_body_bytes = 10000000
        mock_settings.minio_endpoint = "localhost:9000"
        mock_settings.minio_access_key = "test"
        mock_settings.minio_secret_key = "test"
        mock_settings.minio_bucket = "media"
        mock_settings.minio_secure = False
        mock_settings.ingest_stream = "stream:detections"
        mock_get_settings.return_value = mock_settings
        mock_from_url.return_value = mock_redis
        with TestClient(app) as client:
            response = client.post("/v1/ingest/detections", json=payload)
    
            assert response.status_code == 201
            assert response.json()["status"] == "accepted"
            assert response.json()["event_id"] == event_id

    # Clean up overrides
    app.dependency_overrides.clear()
