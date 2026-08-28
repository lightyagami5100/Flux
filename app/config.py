"""Application configuration (12-factor, env-driven)."""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Postgres ---
    database_url: str = "postgresql+asyncpg://flux_user:flux_password@localhost:5432/flux_db"
    db_pool_size: int = 10
    db_max_overflow: int = 20

    # --- Redis ---
    redis_url: str = "redis://localhost:6379/0"

    # --- Ingest stream topology ---
    ingest_stream: str = "stream:detections"
    ingest_consumer_group: str = "detection-workers"
    stream_maxlen: int = 1_000_000  # approximate MAXLEN cap (bounded memory)

    # --- API behaviour ---
    idempotency_ttl_seconds: int = 86_400   # 24h replay window
    max_body_bytes: int = 262_144           # 256 KiB hard cap on ingest bodies
    auto_create_tables: bool = True         # dev convenience; use Alembic in prod

    # --- Device auth: JSON object mapping api_key -> device_id ---
    # e.g. API_KEYS='{"dev-key-camera-1": "cam-01", "dev-key-edge-9": "edge-09"}'
    api_keys: dict[str, str] = Field(default_factory=dict)

    # --- MinIO (Sprint 2) ---
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "media"
    minio_secure: bool = False

    # --- Processor (Sprint 2 & 4) ---
    processor_name: str = "roboflow"
    roboflow_api_key: str = ""

    log_level: str = "INFO"

    @property
    def dead_letter_stream(self) -> str:
        return f"{self.ingest_stream}:dlq"


@lru_cache
def get_settings() -> Settings:
    return Settings()
