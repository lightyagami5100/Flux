"""Strict Pydantic v2 schemas for the ingest plane.

Strictness strategy (deliberate):
- `extra="forbid"` everywhere: unknown/misspelled fields are rejected with 422
  instead of being silently dropped.
- `StrictStr` / `StrictInt` per field: numbers are NEVER coerced into strings
  or ints (this is where real-world bugs hide).
- We intentionally do NOT set global `strict=True`: FastAPI validates JSON
  bodies in *python* mode, so global strict mode would reject perfectly valid
  string-encoded UUIDs and ISO-8601 timestamps. Per-field Strict* types give us
  the protection we want without breaking standard JSON representations.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator

# Bounded float in [0, 1] — used for confidences and normalized bbox coordinates.
Number01 = Annotated[float, Field(ge=0.0, le=1.0)]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MediaRef(StrictModel):
    kind: Literal["image", "video"]
    # scheme://rest — covers s3://, minio://, file://, https:// ...
    uri: StrictStr = Field(min_length=1, max_length=2048, pattern=r"^[a-zA-Z][a-zA-Z0-9+.\-]*://.+$")
    bytes: StrictInt | None = Field(default=None, ge=0)
    sha256: StrictStr | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class DetectedObject(StrictModel):
    label: StrictStr = Field(min_length=1, max_length=64)
    confidence: Number01
    # Normalized (x1, y1, x2, y2) in [0, 1], origin top-left.
    bbox: tuple[Number01, Number01, Number01, Number01]

    @field_validator("bbox")
    @classmethod
    def _bbox_must_be_ordered(cls, v: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
        x1, y1, x2, y2 = v
        if x2 < x1 or y2 < y1:
            raise ValueError("bbox must satisfy x2 >= x1 and y2 >= y1")
        return v


class DetectionEventIn(StrictModel):
    schema_version: StrictInt = Field(default=1, ge=1, le=1)
    event_id: uuid.UUID                      # client-generated, globally unique
    captured_at: datetime                    # must be timezone-aware
    media: MediaRef
    objects: list[DetectedObject] = Field(default_factory=list, max_length=200)
    latitude: float | None = None
    longitude: float | None = None
    metadata: dict[StrictStr, StrictStr] | None = None

    @field_validator("captured_at")
    @classmethod
    def _require_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("captured_at must be timezone-aware (ISO-8601 with UTC offset)")
        return v


class DetectionAccepted(StrictModel):
    status: Literal["accepted"] = "accepted"
    event_id: uuid.UUID
    device_id: str
    received_at: datetime


# ═══════════════════════════════════════════════════════════════════════
# MS-006: Chunked Upload Schemas
# ═══════════════════════════════════════════════════════════════════════

class UploadSessionRequest(BaseModel):
    """Request to create a new chunked upload session."""
    model_config = ConfigDict(extra="forbid")

    device_id: str = Field(min_length=1, max_length=128)
    filename: str = Field(min_length=1, max_length=256)
    total_chunks: int = Field(ge=1, le=1000)
    latitude: float = Field(ge=-90.0, le=90.0)
    longitude: float = Field(ge=-180.0, le=180.0)
    content_type: str = Field(default="video/mp4", max_length=64)


class UploadSessionResponse(BaseModel):
    """Response after creating an upload session."""
    session_id: str
    total_chunks: int
    chunk_size_hint: int  # recommended chunk size in bytes
    upload_url_template: str  # e.g. /api/uploads/{session_id}/chunks/{n}


class ChunkUploadResponse(BaseModel):
    """Response after uploading a single chunk."""
    session_id: str
    chunk_index: int
    received: int  # total chunks received so far
    remaining: int  # chunks still expected


class CompleteUploadResponse(BaseModel):
    """Response after completing a chunked upload session."""
    status: str  # "completed"
    session_id: str
    event_id: str
    media_uri: str
