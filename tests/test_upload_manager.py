"""Tests for the upload_manager module."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, call
import io

from app.upload_manager import UploadManager, UploadSession, STALE_SESSION_TTL


class FakeMinioClient:
    """In-memory MinIO mock for testing."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.buckets: set[str] = set()

    def bucket_exists(self, bucket: str) -> bool:
        return bucket in self.buckets

    def make_bucket(self, bucket: str) -> None:
        self.buckets.add(bucket)

    def put_object(self, bucket: str, key: str, data, length: int, content_type: str = "") -> None:
        self.objects[f"{bucket}/{key}"] = data.read()

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
def fake_minio():
    return FakeMinioClient()


@pytest.fixture
def manager(fake_minio):
    with patch("app.upload_manager.Minio", return_value=fake_minio):
        mgr = UploadManager(
            endpoint="localhost:9000",
            access_key="test",
            secret_key="test",
            bucket="test-bucket",
            secure=False,
        )
        return mgr


class TestCreateSession:
    def test_creates_session(self, manager):
        session = manager.create_session(
            device_id="dev-1",
            filename="test.mp4",
            total_chunks=3,
            latitude=30.2,
            longitude=70.0,
        )
        assert session.session_id
        assert session.device_id == "dev-1"
        assert session.total_chunks == 3
        assert len(session.received_chunks) == 0

    def test_session_is_retrievable(self, manager):
        session = manager.create_session(
            device_id="dev-1",
            filename="test.mp4",
            total_chunks=2,
            latitude=30.2,
            longitude=70.0,
        )
        retrieved = manager.get_session(session.session_id)
        assert retrieved is session

    def test_unknown_session_returns_none(self, manager):
        assert manager.get_session("nonexistent") is None


class TestStoreChunk:
    def test_stores_chunk(self, manager, fake_minio):
        session = manager.create_session("dev", "f.mp4", 2, 30.0, 70.0)
        manager.store_chunk(session.session_id, 0, b"chunk-0-data")
        assert 0 in session.received_chunks
        # Verify chunk was uploaded to MinIO
        key = f"test-bucket/chunks/{session.session_id}/00000"
        assert key in fake_minio.objects

    def test_rejects_unknown_session(self, manager):
        with pytest.raises(ValueError, match="Unknown session"):
            manager.store_chunk("bad-id", 0, b"data")

    def test_rejects_out_of_range_chunk(self, manager):
        session = manager.create_session("dev", "f.mp4", 2, 30.0, 70.0)
        with pytest.raises(ValueError, match="out of range"):
            manager.store_chunk(session.session_id, 5, b"data")

    def test_rejects_negative_chunk(self, manager):
        session = manager.create_session("dev", "f.mp4", 2, 30.0, 70.0)
        with pytest.raises(ValueError, match="out of range"):
            manager.store_chunk(session.session_id, -1, b"data")

    def test_duplicate_chunk_overwrites(self, manager, fake_minio):
        session = manager.create_session("dev", "f.mp4", 2, 30.0, 70.0)
        manager.store_chunk(session.session_id, 0, b"first")
        manager.store_chunk(session.session_id, 0, b"second")
        key = f"test-bucket/chunks/{session.session_id}/00000"
        assert fake_minio.objects[key] == b"second"


class TestCompleteSession:
    def test_assembles_chunks_in_order(self, manager, fake_minio):
        session = manager.create_session("dev", "video.mp4", 3, 30.0, 70.0)
        manager.store_chunk(session.session_id, 0, b"AAA")
        manager.store_chunk(session.session_id, 1, b"BBB")
        manager.store_chunk(session.session_id, 2, b"CCC")

        uri, key = manager.complete_session(session.session_id)

        assert uri == f"minio://test-bucket/uploads/{session.session_id}/video.mp4"
        # Verify assembled content
        assembled = fake_minio.objects[f"test-bucket/{key}"]
        assert assembled == b"AAABBBCCC"

    def test_cleans_up_chunks_after_assembly(self, manager, fake_minio):
        session = manager.create_session("dev", "f.mp4", 2, 30.0, 70.0)
        manager.store_chunk(session.session_id, 0, b"X")
        manager.store_chunk(session.session_id, 1, b"Y")
        manager.complete_session(session.session_id)

        # Individual chunks should be removed
        for i in range(2):
            key = f"test-bucket/chunks/{session.session_id}/{i:05d}"
            assert key not in fake_minio.objects

    def test_removes_session_after_completion(self, manager):
        session = manager.create_session("dev", "f.mp4", 1, 30.0, 70.0)
        manager.store_chunk(session.session_id, 0, b"data")
        manager.complete_session(session.session_id)
        assert manager.get_session(session.session_id) is None

    def test_fails_with_missing_chunks(self, manager):
        session = manager.create_session("dev", "f.mp4", 3, 30.0, 70.0)
        manager.store_chunk(session.session_id, 0, b"A")
        # Missing chunks 1 and 2
        with pytest.raises(ValueError, match="missing chunks"):
            manager.complete_session(session.session_id)

    def test_fails_with_unknown_session(self, manager):
        with pytest.raises(ValueError, match="Unknown session"):
            manager.complete_session("nonexistent")


class TestCleanupStale:
    def test_cleans_stale_sessions(self, manager, fake_minio):
        session = manager.create_session("dev", "f.mp4", 2, 30.0, 70.0)
        manager.store_chunk(session.session_id, 0, b"data")

        # Artificially age the session
        session.created_at -= STALE_SESSION_TTL + 1

        cleaned = manager.cleanup_stale()
        assert cleaned == 1
        assert manager.get_session(session.session_id) is None

    def test_keeps_fresh_sessions(self, manager):
        session = manager.create_session("dev", "f.mp4", 2, 30.0, 70.0)
        cleaned = manager.cleanup_stale()
        assert cleaned == 0
        assert manager.get_session(session.session_id) is not None
