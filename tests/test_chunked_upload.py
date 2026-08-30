"""Integration tests for chunked upload API endpoints."""
from __future__ import annotations

import io
import json
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.upload_manager import UploadManager


class FakeMinioClient:
    """In-memory MinIO mock for chunked upload endpoint testing."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.buckets: set[str] = set()

    def bucket_exists(self, bucket: str) -> bool:
        return bucket in self.buckets

    def make_bucket(self, bucket: str) -> None:
        self.buckets.add(bucket)

    def put_object(self, bucket: str, key: str, data, length: int, content_type: str = "") -> None:
        if hasattr(data, "read"):
            self.objects[f"{bucket}/{key}"] = data.read()
        else:
            self.objects[f"{bucket}/{key}"] = bytes(data)

    def get_object(self, bucket: str, key: str):
        full_key = f"{bucket}/{key}"
        if full_key not in self.objects:
            raise Exception(f"Object not found: {full_key}")
        data = self.objects[full_key]

        class FakeResponse:
            def __init__(self, d: bytes):
                self._stream = io.BytesIO(d)

            def read(self, amt: int | None = None) -> bytes:
                if amt is None:
                    return self._stream.read()
                return self._stream.read(amt)

            def close(self):
                pass

            def release_conn(self):
                pass

        return FakeResponse(data)

    def remove_object(self, bucket: str, key: str) -> None:
        full_key = f"{bucket}/{key}"
        self.objects.pop(full_key, None)


@pytest.fixture
def mock_app_state():
    fake_minio = FakeMinioClient()
    mock_redis = AsyncMock()
    mock_settings = MagicMock()
    mock_settings.auto_create_tables = False
    mock_settings.max_body_bytes = 10_000_000
    mock_settings.minio_endpoint = "localhost:9000"
    mock_settings.minio_access_key = "test"
    mock_settings.minio_secret_key = "test"
    mock_settings.minio_bucket = "media"
    mock_settings.minio_secure = False
    mock_settings.ingest_stream = "stream:detections"
    mock_settings.stream_maxlen = 10000
    mock_settings.is_production = False
    mock_settings.missing_production_settings.return_value = []

    with patch("app.upload_manager.Minio", return_value=fake_minio):
        manager = UploadManager(
            endpoint="localhost:9000",
            access_key="test",
            secret_key="test",
            bucket="media",
            secure=False,
        )

    with patch("app.main.aioredis.from_url") as mock_from_url, \
         patch("app.main.get_settings") as mock_get_settings:
        mock_get_settings.return_value = mock_settings
        mock_from_url.return_value = mock_redis
        with TestClient(app) as test_client:
            app.state.upload_manager = manager
            app.state.settings = mock_settings
            app.state.redis = mock_redis
            yield test_client, manager, mock_redis, fake_minio


class TestChunkedUploadAPI:
    def test_create_upload_session(self, mock_app_state):
        client, manager, redis, minio = mock_app_state
        payload = {
            "device_id": "phone-1",
            "filename": "patrol_video.mp4",
            "total_chunks": 3,
            "latitude": 33.72,
            "longitude": 73.09,
            "content_type": "video/mp4",
        }
        res = client.post("/api/uploads", json=payload)
        assert res.status_code == 201
        data = res.json()
        assert "session_id" in data
        assert data["total_chunks"] == 3
        assert data["chunk_size_hint"] > 0
        assert data["upload_url_template"] == f"/api/uploads/{data['session_id']}/chunks/{{n}}"

        # Verify session is tracked in manager
        session = manager.get_session(data["session_id"])
        assert session is not None
        assert session.device_id == "phone-1"

    def test_create_upload_session_validation_error(self, mock_app_state):
        client, _, _, _ = mock_app_state
        # total_chunks <= 0 is invalid
        res = client.post("/api/uploads", json={
            "device_id": "phone-1",
            "filename": "patrol.mp4",
            "total_chunks": 0,
            "latitude": 33.72,
            "longitude": 73.09,
        })
        assert res.status_code == 422

    def test_upload_chunks_and_complete_flow(self, mock_app_state):
        client, manager, redis, minio = mock_app_state

        # 1. Create Session
        create_res = client.post("/api/uploads", json={
            "device_id": "volunteer-cam",
            "filename": "road_survey.mp4",
            "total_chunks": 2,
            "latitude": 31.52,
            "longitude": 74.35,
            "content_type": "video/mp4",
        })
        assert create_res.status_code == 201
        session_id = create_res.json()["session_id"]

        # 2. Upload Chunk 0
        chunk0_bytes = b"FIRST_CHUNK_DATA_5MB"
        chunk0_file = ("chunk_0", io.BytesIO(chunk0_bytes), "application/octet-stream")
        res0 = client.put(f"/api/uploads/{session_id}/chunks/0", files={"chunk": chunk0_file})
        assert res0.status_code == 200
        assert res0.json() == {
            "session_id": session_id,
            "chunk_index": 0,
            "received": 1,
            "remaining": 1,
        }

        # 3. Upload Chunk 1
        chunk1_bytes = b"SECOND_CHUNK_DATA_5MB"
        chunk1_file = ("chunk_1", io.BytesIO(chunk1_bytes), "application/octet-stream")
        res1 = client.put(f"/api/uploads/{session_id}/chunks/1", files={"chunk": chunk1_file})
        assert res1.status_code == 200
        assert res1.json() == {
            "session_id": session_id,
            "chunk_index": 1,
            "received": 2,
            "remaining": 0,
        }

        # 4. Complete Session
        comp_res = client.post(f"/api/uploads/{session_id}/complete")
        assert comp_res.status_code == 200
        comp_data = comp_res.json()
        assert comp_data["status"] == "completed"
        assert comp_data["session_id"] == session_id
        assert comp_data["media_uri"] == f"minio://media/uploads/{session_id}/road_survey.mp4"

        # Verify assembled file content in MinIO
        assembled = minio.objects[f"media/uploads/{session_id}/road_survey.mp4"]
        assert assembled == b"FIRST_CHUNK_DATA_5MBSECOND_CHUNK_DATA_5MB"

        # Verify Redis xadd was called with standard envelope
        redis.xadd.assert_called_once()
        call_args = redis.xadd.call_args
        assert call_args[0][0] == "stream:detections"
        stream_payload = json.loads(call_args[0][1]["data"])
        assert stream_payload["schema_version"] == 1
        assert stream_payload["device_id"] == "volunteer-cam"
        assert stream_payload["payload"]["media"]["kind"] == "video"
        assert stream_payload["payload"]["media"]["uri"] == comp_data["media_uri"]
        assert stream_payload["payload"]["latitude"] == 31.52
        assert stream_payload["payload"]["longitude"] == 74.35

    def test_upload_chunk_unknown_session_404(self, mock_app_state):
        client, _, _, _ = mock_app_state
        chunk_file = ("chunk", io.BytesIO(b"data"), "application/octet-stream")
        res = client.put("/api/uploads/nonexistent-session/chunks/0", files={"chunk": chunk_file})
        assert res.status_code == 404

    def test_upload_chunk_out_of_bounds_400(self, mock_app_state):
        client, _, _, _ = mock_app_state
        create_res = client.post("/api/uploads", json={
            "device_id": "dev",
            "filename": "f.mp4",
            "total_chunks": 1,
            "latitude": 0.0,
            "longitude": 0.0,
        })
        sid = create_res.json()["session_id"]
        chunk_file = ("chunk", io.BytesIO(b"data"), "application/octet-stream")
        res = client.put(f"/api/uploads/{sid}/chunks/5", files={"chunk": chunk_file})
        assert res.status_code == 400

    def test_complete_upload_missing_chunks_400(self, mock_app_state):
        client, _, _, _ = mock_app_state
        create_res = client.post("/api/uploads", json={
            "device_id": "dev",
            "filename": "f.mp4",
            "total_chunks": 2,
            "latitude": 0.0,
            "longitude": 0.0,
        })
        sid = create_res.json()["session_id"]
        # Only upload chunk 0
        chunk_file = ("chunk", io.BytesIO(b"data"), "application/octet-stream")
        client.put(f"/api/uploads/{sid}/chunks/0", files={"chunk": chunk_file})

        # Try to complete
        comp_res = client.post(f"/api/uploads/{sid}/complete")
        assert comp_res.status_code == 400
        assert "missing chunks" in comp_res.json()["detail"]

    def test_cancel_upload_session(self, mock_app_state):
        client, manager, _, _ = mock_app_state
        create_res = client.post("/api/uploads", json={
            "device_id": "dev",
            "filename": "f.mp4",
            "total_chunks": 2,
            "latitude": 0.0,
            "longitude": 0.0,
        })
        sid = create_res.json()["session_id"]
        del_res = client.delete(f"/api/uploads/{sid}")
        assert del_res.status_code == 200
        assert del_res.json() == {"status": "cancelled", "session_id": sid}
        assert manager.get_session(sid) is None
