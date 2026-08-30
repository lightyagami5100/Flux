"""Application configuration (12-factor, env-driven)."""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Runtime environment ---
    # "development" keeps the local-friendly fallbacks; anything else makes
    # missing credentials a hard startup failure (see missing_production_settings).
    environment: str = "development"

    # --- Postgres ---
    # Local-dev default only. Production must supply DATABASE_URL.
    database_url: str = "postgresql+asyncpg://flux:flux@localhost:5432/flux"
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
    # Schema is created from the ORM metadata on boot. There is no migration
    # tool in the repo yet, so this stays on in production too.
    auto_create_tables: bool = True

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
    # Roboflow project/version to call, e.g. "pothole-detection-project/4".
    # The default is the generic COCO model, which has NO pothole class - it is
    # only useful for wiring smoke tests. Set this to a real pothole model.
    roboflow_model_id: str = "coco/3"

    # --- Video sampling ---
    # Inference is billed per frame, so video is sampled rather than decoded whole.
    video_sample_every_n_frames: int = 15   # ~2 frames/sec on 30fps footage
    video_max_frames: int = 20              # hard ceiling on API calls per clip

    log_level: str = "INFO"

    @field_validator("api_keys", mode="before")
    @classmethod
    def _blank_api_keys_mean_none(cls, v: object) -> object:
        """Treat an unset/blank API_KEYS env var as "no devices" instead of a parse error.

        Compose interpolates missing variables to an empty string, which would
        otherwise crash startup before the explicit warning can be logged.
        """
        if isinstance(v, str) and not v.strip():
            return {}
        return v

    @property
    def dead_letter_stream(self) -> str:
        return f"{self.ingest_stream}:dlq"

    @property
    def is_production(self) -> bool:
        return self.environment.lower() not in {"development", "dev", "test", "local"}

    def missing_production_settings(self) -> list[str]:
        """Names of settings that still hold dev defaults but must be explicit in prod."""
        problems: list[str] = []
        if not self.is_production:
            return problems

        if "flux:flux@" in self.database_url:
            problems.append("DATABASE_URL")
        if not self.api_keys:
            problems.append("API_KEYS")
        if self.minio_access_key in _INSECURE_MINIO_DEFAULTS:
            problems.append("MINIO_ACCESS_KEY")
        if self.minio_secret_key in _INSECURE_MINIO_DEFAULTS:
            problems.append("MINIO_SECRET_KEY")
        if self.processor_name == "roboflow" and not self.roboflow_api_key:
            problems.append("ROBOFLOW_API_KEY")
        if self.processor_name == "roboflow" and self.roboflow_model_id == "coco/3":
            problems.append("ROBOFLOW_MODEL_ID")
        return problems


_INSECURE_MINIO_DEFAULTS = frozenset({"", "minioadmin"})


@lru_cache
def get_settings() -> Settings:
    return Settings()
