"""Integration tests for MS-009: Health Probes & Prometheus Observability."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.db import get_session
from app.main import app
from app.metrics import metrics
from app.upload_manager import UploadManager


@pytest.fixture
def client():
    app.state.settings = Settings(
        minio_endpoint="localhost:9000",
        minio_access_key="test",
        minio_secret_key="test",
        minio_bucket="test-bucket",
        redis_url="redis://localhost:6379/0",
        database_url="postgresql+asyncpg://postgres:postgres@localhost:5432/test",
    )
    mock_redis = AsyncMock()
    mock_redis.ping.return_value = True
    app.state.redis = mock_redis
    app.state.upload_manager = UploadManager(
        endpoint=app.state.settings.minio_endpoint,
        access_key=app.state.settings.minio_access_key,
        secret_key=app.state.settings.minio_secret_key,
        bucket=app.state.settings.minio_bucket,
        secure=app.state.settings.minio_secure,
    )
    with patch("app.main.Minio") as mock_minio_cls:
        mock_s3 = MagicMock()
        mock_s3.bucket_exists.return_value = True
        mock_minio_cls.return_value = mock_s3
        yield TestClient(app)


class TestHealthProbes:

    def test_healthz_liveness(self, client):
        """GET /healthz returns 200 OK."""
        res = client.get("/healthz")
        assert res.status_code == 200
        assert res.json() == {"status": "ok"}

    def test_livez_probe(self, client):
        """GET /livez returns 200 Alive."""
        res = client.get("/livez")
        assert res.status_code == 200
        assert res.json() == {"status": "alive"}

    def test_readyz_healthy(self, client):
        """GET /readyz returns 200 ready when DB, Redis, and MinIO are reachable."""
        with patch("app.main.SessionLocal") as mock_session_local:
            mock_session = AsyncMock()
            mock_session_local.return_value.__aenter__.return_value = mock_session

            res = client.get("/readyz")
            assert res.status_code == 200
            data = res.json()
            assert data["status"] == "ready"
            assert data["checks"]["redis"] == "ok"
            assert data["checks"]["postgres"] == "ok"
            assert data["checks"]["minio"] == "ok"

    def test_readyz_redis_failure(self, client):
        """GET /readyz returns 503 degraded when Redis is down."""
        app.state.redis.ping.side_effect = ConnectionError("Redis connection refused")

        with patch("app.main.SessionLocal") as mock_session_local:
            mock_session = AsyncMock()
            mock_session_local.return_value.__aenter__.return_value = mock_session

            res = client.get("/readyz")
            assert res.status_code == 503
            data = res.json()
            assert data["status"] == "degraded"
            assert "error" in data["checks"]["redis"]

    def test_readyz_postgres_failure(self, client):
        """GET /readyz returns 503 degraded when Postgres is down."""
        app.state.redis.ping.side_effect = None
        app.state.redis.ping.return_value = True

        with patch("app.main.SessionLocal") as mock_session_local:
            mock_session = AsyncMock()
            mock_session.execute.side_effect = Exception("DB Connection timeout")
            mock_session_local.return_value.__aenter__.return_value = mock_session

            res = client.get("/readyz")
            assert res.status_code == 503
            data = res.json()
            assert data["status"] == "degraded"
            assert "error" in data["checks"]["postgres"]


class TestPrometheusMetrics:

    def test_metrics_endpoint_exposition(self, client):
        """GET /metrics returns standard Prometheus formatted metric text."""
        res = client.get("/metrics")
        assert res.status_code == 200
        assert "text/plain" in res.headers["content-type"]
        text = res.text
        assert "flux_http_requests_total" in text
        assert "flux_ingest_accepted_total" in text
        assert "flux_dedup_merges_total" in text

    def test_metrics_recorded_by_middleware(self, client):
        """Invoking endpoints increments Prometheus metrics."""
        client.get("/healthz")
        res = client.get("/metrics")
        assert res.status_code == 200
        assert 'flux_http_requests_total{endpoint="/healthz",method="GET",status="200"}' in res.text
