"""Guards that stop production from booting on development defaults."""
from __future__ import annotations

from app.config import Settings


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "environment": "production",
        "database_url": "postgresql+asyncpg://real:s3cret@db:5432/flux",
        "api_keys": {"k": "device-1"},
        "minio_access_key": "real-access",
        "minio_secret_key": "real-secret",
        "roboflow_api_key": "rf-key",
        "roboflow_model_ids": ["potholes/7"],
        "cors_origins": ["https://flux.example.com"],
        "require_upload_auth": True,
    }
    base.update(overrides)
    return Settings(_env_file=None, **base)  # type: ignore[arg-type]


def test_development_defaults_are_allowed_outside_production() -> None:
    s = Settings(_env_file=None, environment="development")  # type: ignore[call-arg]
    assert s.is_production is False
    assert s.missing_production_settings() == []


def test_production_rejects_the_placeholder_coco_model() -> None:
    """coco/3 has no road-damage classes; shipping it would detect nothing useful."""
    assert "ROBOFLOW_MODEL_IDS" in _settings(roboflow_model_ids=["coco/3"]).missing_production_settings()
    assert "ROBOFLOW_MODEL_IDS" in _settings(roboflow_model_ids=[]).missing_production_settings()


def test_fully_configured_production_reports_no_problems() -> None:
    assert _settings().missing_production_settings() == []


def test_production_rejects_default_database_credentials() -> None:
    s = _settings(database_url="postgresql+asyncpg://flux:flux@localhost:5432/flux")
    assert "DATABASE_URL" in s.missing_production_settings()


def test_production_rejects_empty_api_keys() -> None:
    assert "API_KEYS" in _settings(api_keys={}).missing_production_settings()


def test_production_rejects_minioadmin_credentials() -> None:
    problems = _settings(
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
    ).missing_production_settings()
    assert "MINIO_ACCESS_KEY" in problems
    assert "MINIO_SECRET_KEY" in problems


def test_production_rejects_missing_roboflow_key_when_processor_selected() -> None:
    s = _settings(processor_name="roboflow", roboflow_api_key="")
    assert "ROBOFLOW_API_KEY" in s.missing_production_settings()


def test_production_rejects_wildcard_cors() -> None:
    s = _settings(cors_origins=["*"])
    assert "CORS_ORIGINS" in s.missing_production_settings()


def test_production_rejects_unauthenticated_uploads() -> None:
    s = _settings(require_upload_auth=False)
    assert "REQUIRE_UPLOAD_AUTH" in s.missing_production_settings()
