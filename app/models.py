"""ORM models. Primary key is the client-supplied event_id (UUID), which makes
worker redelivery naturally idempotent via ON CONFLICT DO UPDATE."""
from __future__ import annotations

import enum
import uuid
from datetime import datetime

from sqlalchemy import DateTime, Enum, Index, Integer, String, Text, Float, JSON, Uuid
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

JSON_TYPE = JSON().with_variant(JSONB, "postgresql")
UUID_TYPE = Uuid().with_variant(PG_UUID(as_uuid=True), "postgresql")


class Base(DeclarativeBase):
    pass


class DetectionStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSED = "processed"
    FAILED = "failed"


class PotholeStatus(str, enum.Enum):
    ACTIVE = "active"
    REPAIRED = "repaired"
    ARCHIVED = "archived"


class CanonicalPothole(Base):
    """A deduplicated physical pothole on earth, aggregated across multi-pass detections."""
    __tablename__ = "canonical_potholes"

    pothole_id: Mapped[uuid.UUID] = mapped_column(UUID_TYPE, primary_key=True, default=uuid.uuid4)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    severity: Mapped[str] = mapped_column(String(32), nullable=False, default="Low")  # "Low", "Medium", "High", "Critical"
    status: Mapped[PotholeStatus] = mapped_column(
        Enum(
            PotholeStatus,
            name="pothole_status",
            native_enum=False,
            values_callable=lambda e: [m.value for m in e],
        ),
        nullable=False,
        default=PotholeStatus.ACTIVE,
    )
    observation_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    avg_confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    first_detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    primary_event_id: Mapped[uuid.UUID | None] = mapped_column(UUID_TYPE)
    primary_media_uri: Mapped[str | None] = mapped_column(Text)
    observations: Mapped[list[dict]] = mapped_column(JSON_TYPE, nullable=False, default=list)

    __table_args__ = (
        Index("ix_canonical_potholes_lat_lon", "latitude", "longitude"),
        Index("ix_canonical_potholes_status", "status"),
        Index("ix_canonical_potholes_last_detected", "last_detected_at"),
        Index("ix_canonical_potholes_severity", "severity"),
    )


class DetectionEvent(Base):
    __tablename__ = "detection_events"

    event_id: Mapped[uuid.UUID] = mapped_column(UUID_TYPE, primary_key=True)
    device_id: Mapped[str] = mapped_column(String(128), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    status: Mapped[DetectionStatus] = mapped_column(
        Enum(
            DetectionStatus,
            name="detection_status",
            native_enum=False,
            values_callable=lambda e: [m.value for m in e],  # store lowercase values
        ),
        nullable=False,
    )

    canonical_pothole_id: Mapped[uuid.UUID | None] = mapped_column(UUID_TYPE)
    media_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    media_uri: Mapped[str] = mapped_column(Text, nullable=False)
    media_sha256: Mapped[str | None] = mapped_column(String(64))

    latitude: Mapped[float | None] = mapped_column(Float)
    longitude: Mapped[float | None] = mapped_column(Float)

    object_count: Mapped[int] = mapped_column(Integer, nullable=False)
    objects: Mapped[list[dict]] = mapped_column(JSON_TYPE, nullable=False, default=list)
    metrics: Mapped[dict] = mapped_column(JSON_TYPE, nullable=False, default=dict)
    processing_ms: Mapped[int | None] = mapped_column(Integer)
    error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        Index("ix_detection_events_device_captured", "device_id", "captured_at"),
        Index("ix_detection_events_status", "status"),
        Index("ix_detection_events_lat_lon", "latitude", "longitude"),
        Index("ix_detection_events_captured_at", "captured_at"),
        Index("ix_detection_events_canonical", "canonical_pothole_id"),
    )
