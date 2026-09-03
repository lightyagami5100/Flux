"""Spatial Deduplication & Canonical Pothole Clustering Engine (MS-008).

Clusters nearby detection events (within 10 meters default) into unified CanonicalPothole
entities. Maintains observation history, centroid coordinates, severity escalation,
and representative media selection.
"""
from __future__ import annotations

import logging
import math
import uuid
from datetime import datetime, UTC

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import CanonicalPothole, DetectionEvent, PotholeStatus
from app.severity import compute_severity, escalate_severity

logger = logging.getLogger("deduplication")

# Default radius in meters to consider detections part of the same physical pothole
DEFAULT_DEDUP_RADIUS_METERS = 10.0


def _normalize_dt(dt: datetime | None) -> datetime:
    if dt is None:
        return datetime.now(UTC)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def _dialect_name(session: AsyncSession) -> str:
    """Best-effort dialect name of the session bind; empty string when undeterminable."""
    name = getattr(getattr(getattr(session, "bind", None), "dialect", None), "name", "")
    return name if isinstance(name, str) else ""


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the Great-Circle distance between two points in meters using Haversine formula."""
    R = 6371000.0  # Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    )
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c


def compute_event_severity(event: DetectionEvent) -> str:
    """Calculate the severity of a single detection event."""
    return compute_severity(objects=event.objects, metrics=event.metrics)


def compute_event_confidence(event: DetectionEvent) -> float:
    """Extract primary confidence score from an event."""
    if event.objects:
        return float(event.objects[0].get("confidence", 0.8))
    return 0.8


async def cluster_detection(
    session: AsyncSession,
    event: DetectionEvent,
    radius_meters: float = DEFAULT_DEDUP_RADIUS_METERS,
) -> tuple[CanonicalPothole, bool]:
    """Cluster a newly processed DetectionEvent into a CanonicalPothole.

    Returns:
        (canonical_pothole, is_new_cluster)
    """
    if event.latitude is None or event.longitude is None:
        raise ValueError(f"Cannot cluster event {event.event_id}: missing coordinates")

    event_sev = compute_event_severity(event)
    event_conf = compute_event_confidence(event)

    # 1. Bounding box pre-filter in SQL
    # ~111,000 meters per degree of latitude
    lat_delta = radius_meters / 111_000.0
    lon_delta = radius_meters / (111_000.0 * max(0.1, math.cos(math.radians(event.latitude))))

    min_lat = event.latitude - lat_delta
    max_lat = event.latitude + lat_delta
    min_lon = event.longitude - lon_delta
    max_lon = event.longitude + lon_delta

    stmt = select(CanonicalPothole).where(
        CanonicalPothole.latitude.between(min_lat, max_lat),
        CanonicalPothole.longitude.between(min_lon, max_lon),
        CanonicalPothole.status != PotholeStatus.ARCHIVED,
    )
    # SELECT ... FOR UPDATE is unsupported on SQLite (the Docker-less fallback).
    if _dialect_name(session) != "sqlite":
        stmt = stmt.with_for_update()

    result = await session.execute(stmt)
    candidates = result.scalars().all()

    best_match: CanonicalPothole | None = None
    best_dist = float("inf")

    for cand in candidates:
        dist = haversine_distance(
            event.latitude, event.longitude, cand.latitude, cand.longitude
        )
        if dist <= radius_meters and dist < best_dist:
            best_dist = dist
            best_match = cand

    event_sev = compute_event_severity(event)
    event_conf = compute_event_confidence(event)
    event_dt = _normalize_dt(event.captured_at)

    # Observation record to append
    event_label = "pothole"
    if event.objects:
        event_label = event.objects[0].get("label", "pothole") if isinstance(event.objects[0], dict) else "pothole"
    elif hasattr(event, 'metrics') and isinstance(event.metrics, dict):
        event_label = event.metrics.get("label", "pothole")

    obs_record = {
        "event_id": str(event.event_id),
        "device_id": event.device_id,
        "captured_at": event_dt.isoformat(),
        "latitude": event.latitude,
        "longitude": event.longitude,
        "severity": event_sev,
        "confidence": event_conf,
        "media_uri": event.media_uri,
        "object_count": event.object_count,
        "label": event_label,
    }

    if best_match is not None:
        # Merge into existing canonical pothole cluster
        pothole = best_match
        old_count = pothole.observation_count
        new_count = old_count + 1

        # Centroid update (weighted moving average)
        pothole.latitude = (pothole.latitude * old_count + event.latitude) / new_count
        pothole.longitude = (pothole.longitude * old_count + event.longitude) / new_count
        pothole.observation_count = new_count
        pothole.avg_confidence = (pothole.avg_confidence * old_count + event_conf) / new_count

        # Severity escalation
        pothole.severity = escalate_severity(pothole.severity, event_sev)

        # Timestamps with timezone normalization
        pothole_last_dt = _normalize_dt(pothole.last_detected_at)
        pothole_first_dt = _normalize_dt(pothole.first_detected_at)

        if event_dt > pothole_last_dt:
            pothole.last_detected_at = event.captured_at
        if event_dt < pothole_first_dt:
            pothole.first_detected_at = event.captured_at

        # Representative media (prefer higher confidence or larger severity)
        if event_conf > pothole.avg_confidence or pothole.primary_media_uri is None:
            pothole.primary_media_uri = event.media_uri
            pothole.primary_event_id = event.event_id

        # Update observations list (capped to 50 most recent to prevent DB row bloat)
        MAX_INLINE_OBSERVATIONS = 50
        current_obs = list(pothole.observations or [])
        current_obs.append(obs_record)
        if len(current_obs) > MAX_INLINE_OBSERVATIONS:
            current_obs = current_obs[-MAX_INLINE_OBSERVATIONS:]
        pothole.observations = current_obs
        event.canonical_pothole_id = pothole.pothole_id

        logger.info(
            "merged event %s into canonical pothole %s (count=%d, dist=%.1fm)",
            event.event_id, pothole.pothole_id, new_count, best_dist,
        )
        return pothole, False
    else:
        # Create new Canonical Pothole
        new_pothole_id = uuid.uuid4()
        pothole = CanonicalPothole(
            pothole_id=new_pothole_id,
            latitude=event.latitude,
            longitude=event.longitude,
            severity=event_sev,
            status=PotholeStatus.ACTIVE,
            observation_count=1,
            avg_confidence=event_conf,
            first_detected_at=event.captured_at,
            last_detected_at=event.captured_at,
            primary_event_id=event.event_id,
            primary_media_uri=event.media_uri,
            observations=[obs_record],
        )
        session.add(pothole)
        event.canonical_pothole_id = new_pothole_id

        logger.info(
            "created new canonical pothole %s for event %s at (%.4f, %.4f)",
            new_pothole_id, event.event_id, event.latitude, event.longitude,
        )
        return pothole, True


async def recluster_all_events(
    session: AsyncSession,
    radius_meters: float = DEFAULT_DEDUP_RADIUS_METERS,
    batch_size: int = 500,
) -> int:
    """Clear all canonical potholes and recluster all processed detection events in memory-bounded batches."""
    # Delete existing canonical potholes so the rebuild starts from scratch
    await session.execute(delete(CanonicalPothole))
    await session.execute(
        update(DetectionEvent).values(canonical_pothole_id=None)
    )
    await session.flush()

    clustered_count = 0
    offset = 0

    while True:
        stmt = (
            select(DetectionEvent)
            .where(
                DetectionEvent.status == "processed",
                DetectionEvent.latitude.is_not(None),
                DetectionEvent.longitude.is_not(None),
            )
            .order_by(DetectionEvent.captured_at.asc())
            .offset(offset)
            .limit(batch_size)
        )
        result = await session.execute(stmt)
        batch = result.scalars().all()
        if not batch:
            break

        for ev in batch:
            await cluster_detection(session, ev, radius_meters=radius_meters)
            clustered_count += 1

        offset += len(batch)
        await session.flush()

    await session.commit()
    return clustered_count
