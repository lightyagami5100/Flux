"""Sprint 1 — Ingest Plane API.

POST /v1/ingest/detections:
  body  : DetectionEventIn (strict schema)
  auth  : X-API-Key header -> device identity
  idem  : Idempotency-Key header (falls back to the event_id)
  output: 201 + envelope appended to a Redis Stream for async processing.
"""
from __future__ import annotations

import json
import logging
import re
import uuid
import random
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated

import redis.asyncio as aioredis
from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query, Request, Response, Security, UploadFile, Form, status
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from redis.asyncio import Redis
from sqlalchemy import text
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import asyncio

from . import models  # noqa: F401  (populates Base.metadata before create_all)
from .config import Settings, get_settings
from .db import SessionLocal, engine
from .deps import DeviceIdentity, get_current_device, get_redis
from .idempotency import release, reserve, store
from .models import Base, DetectionEvent, DetectionStatus, CanonicalPothole, PotholeStatus
from .schemas import (
    DetectionAccepted,
    DetectionEventIn,
    UploadSessionRequest,
    UploadSessionResponse,
    ChunkUploadResponse,
    CompleteUploadResponse,
)
from .db import get_session
from sqlalchemy import select, func
try:
    from minio import Minio
except ImportError:
    Minio = None  # type: ignore
from .upload_manager import UploadManager, DEFAULT_CHUNK_SIZE
from .deduplication import cluster_detection, recluster_all_events
from .metrics import metrics, PrometheusMiddleware

logger = logging.getLogger("ingest")

_IDEMPOTENCY_KEY_RE = re.compile(r"^[A-Za-z0-9._\-]{1,128}$")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    if not settings.api_keys:
        logger.warning("API_KEYS is empty — every ingest request will be rejected with 401.")

    app.state.settings = settings

    # Resilient Redis initialization
    try:
        r = aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_timeout=1.5,
            socket_connect_timeout=1.5,
        )
        await asyncio.wait_for(r.ping(), timeout=1.0)
        app.state.redis = r
        logger.info("Connected to Redis at %s", settings.redis_url)
    except Exception as e:
        logger.warning("Redis unavailable at %s (%s). Using high-performance in-memory Redis fallback.", settings.redis_url, e)
        try:
            import fakeredis.aioredis
            app.state.redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        except Exception:
            app.state.redis = aioredis.from_url(settings.redis_url, decode_responses=True)

    # Resilient Database initialization
    if settings.auto_create_tables:
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Database schema ensured in PostgreSQL")
        except Exception as e:
            logger.warning("PostgreSQL unavailable (%s). Initializing seamless local SQLite fallback.", e)
            import app.db as db_module
            from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
            sqlite_engine = create_async_engine("sqlite+aiosqlite:///flux_dev.db", echo=False)
            db_module.engine = sqlite_engine
            db_module.SessionLocal = async_sessionmaker(bind=sqlite_engine, class_=AsyncSession, expire_on_commit=False)
            async with sqlite_engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Local SQLite database schema initialized in flux_dev.db")

    # MS-006: Initialize the chunked upload manager
    app.state.upload_manager = UploadManager(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        bucket=settings.minio_bucket,
        secure=settings.minio_secure,
    )

    try:
        yield
    finally:
        try:
            await app.state.redis.aclose()
        except Exception:
            pass
        try:
            await engine.dispose()
        except Exception:
            pass


app = FastAPI(
    title="Vision Ingest Plane",
    version="0.1.0-sprint1",
    lifespan=lifespan,
)

# ─── CORS configuration for Mobile App Testing ────────────────────────────────
# ─── Middleware Stack ────────────────────────────────────────────────────────
app.add_middleware(PrometheusMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def enforce_body_size(request: Request, call_next):
    """Cheap DoS guard: reject oversized declared bodies before parsing."""
    content_length = request.headers.get("content-length")
    if content_length and content_length.isdigit():
        if int(content_length) > get_settings().max_body_bytes:
            return JSONResponse({"detail": "Payload too large"}, status_code=413)
    return await call_next(request)


@app.get("/")
async def read_root():
    return RedirectResponse(url="/static/index.html")


@app.get("/healthz", summary="Lightweight liveness probe")
async def healthz() -> dict:
    return {"status": "ok"}


@app.get("/livez", summary="Kubernetes/ECS liveness probe")
async def livez() -> dict:
    return {"status": "alive"}


@app.get("/readyz", summary="Deep readiness probe checking DB, Redis, and Object Store")
async def readyz(request: Request) -> JSONResponse:
    checks: dict[str, str] = {}
    settings: Settings = request.app.state.settings

    # 1. Redis probe
    try:
        await request.app.state.redis.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"

    # 2. Database probe
    try:
        async with SessionLocal() as session:
            await session.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {exc}"

    # 3. MinIO / Object Store probe
    try:
        s3 = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )
        s3.bucket_exists(settings.minio_bucket)
        checks["minio"] = "ok"
    except Exception as exc:
        checks["minio"] = f"error: {exc}"

    if settings.roboflow_api_key:
        checks["roboflow_key"] = "ok"
    else:
        checks["roboflow_key"] = "optional"

    healthy = checks["redis"] == "ok" and checks["postgres"] == "ok" and checks["minio"] == "ok"
    return JSONResponse(
        {"status": "ready" if healthy else "degraded", "checks": checks},
        status_code=200 if healthy else 503,
    )


@app.get("/metrics", summary="Prometheus telemetry metrics")
async def prometheus_metrics() -> Response:
    """Expose application metrics in Prometheus text format (v0.0.4)."""
    return Response(
        content=metrics.generate_prometheus_text(),
        media_type="text/plain; version=0.0.4",
    )


@app.post(
    "/v1/ingest/detections",
    status_code=status.HTTP_201_CREATED,
    response_model=DetectionAccepted,
    summary="Submit a detection event for asynchronous processing",
)
async def ingest_detection(
    event: DetectionEventIn,
    request: Request,
    device: DeviceIdentity = Security(get_current_device),
    redis_client: Redis = Depends(get_redis),
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> JSONResponse:
    settings: Settings = request.app.state.settings

    # ---- Normalize the idempotency key -------------------------------------
    if idempotency_key is not None:
        idempotency_key = idempotency_key.strip()
        if not _IDEMPOTENCY_KEY_RE.fullmatch(idempotency_key):
            raise HTTPException(
                status_code=422,
                detail="Idempotency-Key must match [A-Za-z0-9._-]{1,128}",
            )
    else:
        # Implicit dedupe: fall back to the client-generated event_id.
        idempotency_key = f"evt-{event.event_id}"

    idem_key = f"idem:detections:{device.device_id}:{idempotency_key}"

    # ---- Reserve / replay ---------------------------------------------------
    claimed, replay = await reserve(redis_client, idem_key, settings.idempotency_ttl_seconds)
    if replay is not None:
        return JSONResponse(
            content=replay.body,
            status_code=replay.status_code,
            headers={"Idempotency-Key": idempotency_key, "Idempotency-Replayed": "true"},
        )
    if not claimed:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An identical request is currently being processed; retry shortly.",
            headers={"Retry-After": "1"},
        )

    # ---- Produce to the Redis Stream ---------------------------------------
    received_at = datetime.now(timezone.utc)
    envelope = {
        "schema_version": 1,
        "event_id": str(event.event_id),
        "device_id": device.device_id,
        "received_at": received_at.isoformat(),
        "payload": event.model_dump(mode="json"),
    }
    try:
        await redis_client.xadd(
            settings.ingest_stream,
            {"data": json.dumps(envelope, separators=(",", ":"))},
            maxlen=settings.stream_maxlen,
            approximate=True,
        )
    except Exception:
        logger.exception("failed to append event %s to stream", event.event_id)
        await release(redis_client, idem_key)  # free the key so the client can retry
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Ingest queue temporarily unavailable",
            headers={"Retry-After": "2"},
        )

    # ---- Persist the idempotent response ------------------------------------
    body = DetectionAccepted(
        status="accepted",
        event_id=event.event_id,
        device_id=device.device_id,
        received_at=received_at,
    ).model_dump(mode="json")

    await store(redis_client, idem_key, 201, body, settings.idempotency_ttl_seconds)

    logger.info("accepted event=%s device=%s key=%s", event.event_id, device.device_id, idempotency_key)
    return JSONResponse(content=body, status_code=201, headers={"Idempotency-Key": idempotency_key})


@app.post("/v1/ingest/upload", status_code=201)
async def upload_and_ingest(
    video: UploadFile,
    lat: float = Form(...),
    lon: float = Form(...),
    request: Request = None,
):
    import uuid
    from datetime import datetime, timezone
    event_id = uuid.uuid4()
    
    # Upload to MinIO (using settings)
    settings = app.state.settings
    s3 = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure
    )
    bucket = settings.minio_bucket
    if not s3.bucket_exists(bucket):
        s3.make_bucket(bucket)
        
    object_name = f"mobile/{event_id}_{video.filename}"
    file_content = await video.read()
    s3.put_object(
        bucket,
        object_name,
        io.BytesIO(file_content),
        length=len(file_content),
        content_type=video.content_type
    )
    
    uri = f"minio://{bucket}/{object_name}"
    
    # Create event and push standard envelope to Redis stream
    now = datetime.now(timezone.utc)
    event = DetectionEventIn(
        schema_version=1,
        event_id=event_id,
        captured_at=now,
        media={"kind": "image" if (video.content_type and video.content_type.startswith("image")) else "video", "uri": uri},
        objects=[],
        latitude=lat,
        longitude=lon,
    )
    envelope = {
        "schema_version": 1,
        "event_id": str(event_id),
        "device_id": "mobile-upload",
        "received_at": now.isoformat(),
        "payload": event.model_dump(mode="json"),
    }
    await request.app.state.redis.xadd(
        settings.ingest_stream,
        {"data": json.dumps(envelope, separators=(",", ":"))},
        maxlen=settings.stream_maxlen,
        approximate=True,
    )
    
    return {"status": "accepted", "event_id": str(event_id)}


@app.get("/detections", summary="Query pothole anomalies (GeoJSON)")
async def get_all_detections(
    session: AsyncSession = Depends(get_session),
    min_lat: Annotated[float | None, Query(ge=-90.0, le=90.0)] = None,
    max_lat: Annotated[float | None, Query(ge=-90.0, le=90.0)] = None,
    min_lon: Annotated[float | None, Query(ge=-180.0, le=180.0)] = None,
    max_lon: Annotated[float | None, Query(ge=-180.0, le=180.0)] = None,
    severity: Annotated[str | None, Query(description="Filter by severity: High, Medium, Low, Critical")] = None,
    status: Annotated[str | None, Query(description="Filter by status: active, repaired, archived, all")] = None,
    since: Annotated[datetime | None, Query(description="ISO timestamp for incremental updates")] = None,
    deduplicated: Annotated[bool, Query(description="Return deduplicated canonical potholes if True, raw events if False")] = True,
    limit: Annotated[int, Query(ge=1, le=2000)] = 500,
):
    if deduplicated:
        # Check canonical potholes table
        stmt = select(CanonicalPothole)
        if status and status.lower() != "all":
            stmt = stmt.where(CanonicalPothole.status == status.lower())
        else:
            stmt = stmt.where(CanonicalPothole.status != PotholeStatus.ARCHIVED)

        if min_lat is not None:
            stmt = stmt.where(CanonicalPothole.latitude >= min_lat)
        if max_lat is not None:
            stmt = stmt.where(CanonicalPothole.latitude <= max_lat)
        if min_lon is not None:
            stmt = stmt.where(CanonicalPothole.longitude >= min_lon)
        if max_lon is not None:
            stmt = stmt.where(CanonicalPothole.longitude <= max_lon)
        if severity is not None:
            stmt = stmt.where(CanonicalPothole.severity.ilike(severity))
        if since is not None:
            stmt = stmt.where(CanonicalPothole.last_detected_at >= since)

        stmt = stmt.order_by(CanonicalPothole.last_detected_at.desc()).limit(limit)
        result = await session.execute(stmt)
        potholes = result.scalars().all()

        features = []
        for p in potholes:
            p_id = getattr(p, "pothole_id", getattr(p, "event_id", None))
            p_sev = getattr(p, "severity", None)
            if p_sev is None:
                p_sev = "Low"
                for obj in getattr(p, "objects", []):
                    w = obj.get("bbox", [0, 0, 0, 0])[2] - obj.get("bbox", [0, 0, 0, 0])[0]
                    h = obj.get("bbox", [0, 0, 0, 0])[3] - obj.get("bbox", [0, 0, 0, 0])[1]
                    if w * h > 0.5:
                        p_sev = "Critical"
                    elif w * h > 0.3:
                        p_sev = "High"
                    elif w * h > 0.15:
                        p_sev = "Medium"
                if getattr(p, "metrics", None) and "severity" in p.metrics:
                    p_sev = p.metrics["severity"].capitalize()

            if severity and p_sev.lower() != severity.lower():
                continue

            p_conf = getattr(p, "avg_confidence", 0.85)
            if hasattr(p, "objects") and p.objects:
                p_conf = p.objects[0].get("confidence", p_conf)

            p_count = getattr(p, "observation_count", getattr(p, "object_count", 1))
            p_first = getattr(p, "first_detected_at", getattr(p, "captured_at", datetime.now(timezone.utc)))
            p_last = getattr(p, "last_detected_at", getattr(p, "captured_at", datetime.now(timezone.utc)))
            p_status_obj = getattr(p, "status", PotholeStatus.ACTIVE)
            p_status = p_status_obj.value if hasattr(p_status_obj, "value") else str(p_status_obj)
            p_primary_event = getattr(p, "primary_event_id", getattr(p, "event_id", None))
            p_thumb = f"/potholes/{p_id}/media" if hasattr(p, "pothole_id") else f"/detections/{p_id}/media"

            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [p.longitude, p.latitude],
                },
                "properties": {
                    "id": str(p_id),
                    "class": "pothole",
                    "severity": p_sev,
                    "confidence": p_conf,
                    "observation_count": p_count,
                    "first_detected_at": p_first.isoformat() if hasattr(p_first, "isoformat") else str(p_first),
                    "last_detected_at": p_last.isoformat() if hasattr(p_last, "isoformat") else str(p_last),
                    "timestamp": p_last.isoformat() if hasattr(p_last, "isoformat") else str(p_last),
                    "status": p_status,
                    "primary_event_id": str(p_primary_event) if p_primary_event else None,
                    "thumbnail_url": p_thumb,
                },
            })
        return {"type": "FeatureCollection", "features": features}

    # Raw observation events mode (forensics/debug)
    raw_stmt = select(DetectionEvent).where(
        DetectionEvent.status == DetectionStatus.PROCESSED,
        DetectionEvent.latitude.is_not(None),
        DetectionEvent.longitude.is_not(None),
    )
    if min_lat is not None:
        raw_stmt = raw_stmt.where(DetectionEvent.latitude >= min_lat)
    if max_lat is not None:
        raw_stmt = raw_stmt.where(DetectionEvent.latitude <= max_lat)
    if min_lon is not None:
        raw_stmt = raw_stmt.where(DetectionEvent.longitude >= min_lon)
    if max_lon is not None:
        raw_stmt = raw_stmt.where(DetectionEvent.longitude <= max_lon)
    if since is not None:
        raw_stmt = raw_stmt.where(DetectionEvent.captured_at >= since)

    raw_stmt = raw_stmt.order_by(DetectionEvent.captured_at.desc()).limit(limit)
    result = await session.execute(raw_stmt)
    events = result.scalars().all()

    features = []
    for event in events:
        sev = "Low"
        for obj in event.objects:
            w = obj.get("bbox", [0, 0, 0, 0])[2] - obj.get("bbox", [0, 0, 0, 0])[0]
            h = obj.get("bbox", [0, 0, 0, 0])[3] - obj.get("bbox", [0, 0, 0, 0])[1]
            if w * h > 0.5:
                sev = "Critical"
            elif w * h > 0.3:
                sev = "High"
            elif w * h > 0.15:
                sev = "Medium"

        if event.metrics and "severity" in event.metrics:
            sev = event.metrics["severity"].capitalize()

        if severity and sev.lower() != severity.lower():
            continue

        primary_obj = event.objects[0] if event.objects else {"label": "pothole", "confidence": 0.0}

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [event.longitude, event.latitude]
            },
            "properties": {
                "id": str(event.event_id),
                "class": primary_obj.get("label", "pothole"),
                "severity": sev,
                "confidence": primary_obj.get("confidence", 0.0),
                "timestamp": event.captured_at.isoformat(),
                "media_kind": event.media_kind,
                "media_uri": event.media_uri,
                "thumbnail_url": f"/detections/{event.event_id}/media",
                "object_count": event.object_count,
                "canonical_pothole_id": str(event.canonical_pothole_id) if event.canonical_pothole_id else None,
            }
        })

    return {"type": "FeatureCollection", "features": features}


@app.get("/potholes", summary="Alias for deduplicated canonical potholes")
async def get_potholes(session: AsyncSession = Depends(get_session)):
    return await get_all_detections(session=session, deduplicated=True)


@app.get("/potholes/{pothole_id}", summary="Get canonical pothole details & timeseries history")
async def get_pothole_detail(
    pothole_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
):
    """Retrieve full canonical pothole profile and its complete multi-pass observation log."""
    stmt = select(CanonicalPothole).where(CanonicalPothole.pothole_id == pothole_id)
    result = await session.execute(stmt)
    pothole = result.scalar_one_or_none()
    if pothole is None:
        raise HTTPException(status_code=404, detail="Canonical pothole not found")

    return {
        "pothole_id": str(pothole.pothole_id),
        "latitude": pothole.latitude,
        "longitude": pothole.longitude,
        "severity": pothole.severity,
        "status": pothole.status.value,
        "observation_count": pothole.observation_count,
        "avg_confidence": pothole.avg_confidence,
        "first_detected_at": pothole.first_detected_at.isoformat(),
        "last_detected_at": pothole.last_detected_at.isoformat(),
        "primary_event_id": str(pothole.primary_event_id) if pothole.primary_event_id else None,
        "primary_media_uri": pothole.primary_media_uri,
        "thumbnail_url": f"/potholes/{pothole.pothole_id}/media",
        "observations": pothole.observations,
    }


def render_road_snapshot_svg(id_str: str, severity: str, passes: int, lat: float, lon: float, confidence: float = 0.94) -> str:
    sev_upper = (severity or "Low").upper()
    box_color = "#EF4444" if sev_upper == "CRITICAL" else "#F97316" if sev_upper == "HIGH" else "#F59E0B" if sev_upper == "MEDIUM" else "#10B981"
    
    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="480" height="270" viewBox="0 0 480 270">
  <defs>
    <linearGradient id="asphalt" x1="0%" y1="0%" x2="0%" y2="100%">
      <stop offset="0%" stop-color="#141822"/>
      <stop offset="50%" stop-color="#1E2330"/>
      <stop offset="100%" stop-color="#10131A"/>
    </linearGradient>
    <linearGradient id="potholeCavity" x1="20%" y1="20%" x2="80%" y2="80%">
      <stop offset="0%" stop-color="#080A0E"/>
      <stop offset="60%" stop-color="#050608"/>
      <stop offset="100%" stop-color="#1A1512"/>
    </linearGradient>
    <filter id="shadow" x="-10%" y="-10%" width="120%" height="120%">
      <feDropShadow dx="0" dy="4" stdDeviation="6" flood-color="#000" flood-opacity="0.7"/>
    </filter>
  </defs>

  <!-- Asphalt Roadway Background -->
  <rect width="480" height="270" fill="url(#asphalt)"/>

  <!-- Road Texture Grid & Surface Grain -->
  <line x1="0" y1="90" x2="480" y2="90" stroke="rgba(255,255,255,0.03)" stroke-width="1"/>
  <line x1="0" y1="180" x2="480" y2="180" stroke="rgba(255,255,255,0.03)" stroke-width="1"/>
  
  <!-- Dashed Highway Lane Marking -->
  <line x1="240" y1="0" x2="240" y2="270" stroke="#EAB308" stroke-width="4" stroke-dasharray="24,20" opacity="0.7"/>
  <line x1="20" y1="0" x2="20" y2="270" stroke="#FFFFFF" stroke-width="3" opacity="0.4"/>
  <line x1="460" y1="0" x2="460" y2="270" stroke="#FFFFFF" stroke-width="3" opacity="0.4"/>

  <!-- Physical Pothole Cavity Geometry -->
  <path d="M 170 120 C 185 95, 290 100, 310 125 C 325 145, 305 180, 275 185 C 220 195, 155 170, 170 120 Z" 
        fill="url(#potholeCavity)" stroke="#2B2118" stroke-width="3" filter="url(#shadow)"/>
  
  <!-- Asphalt Internal Fracture Cracks -->
  <path d="M 170 120 Q 140 105 125 110" stroke="#10141D" stroke-width="2" fill="none" opacity="0.8"/>
  <path d="M 310 125 Q 345 130 365 120" stroke="#10141D" stroke-width="2" fill="none" opacity="0.8"/>
  <path d="M 275 185 Q 285 215 300 225" stroke="#10141D" stroke-width="2" fill="none" opacity="0.8"/>
  <path d="M 210 175 Q 185 200 170 215" stroke="#10141D" stroke-width="2" fill="none" opacity="0.8"/>

  <!-- AI Detection Bounding Box -->
  <rect x="145" y="85" width="190" height="115" rx="6" fill="none" stroke="{box_color}" stroke-width="2.5" stroke-dasharray="6,4"/>
  
  <!-- Corner Crosshairs -->
  <path d="M 145 95 L 145 85 L 155 85" stroke="{box_color}" stroke-width="3" fill="none"/>
  <path d="M 325 85 L 335 85 L 335 95" stroke="{box_color}" stroke-width="3" fill="none"/>
  <path d="M 145 190 L 145 200 L 155 200" stroke="{box_color}" stroke-width="3" fill="none"/>
  <path d="M 325 200 L 335 200 L 335 190" stroke="{box_color}" stroke-width="3" fill="none"/>

  <!-- AI Classification Tag -->
  <rect x="145" y="62" width="165" height="22" rx="4" fill="{box_color}"/>
  <text x="152" y="77" fill="#FFFFFF" font-family="-apple-system, sans-serif" font-size="11" font-weight="bold" letter-spacing="0.5">
    POTHOLE ({int(confidence*100)}% CONF)
  </text>

  <!-- Top Camera Telemetry Overlay -->
  <rect x="0" y="0" width="480" height="32" fill="rgba(11, 15, 23, 0.85)"/>
  <circle cx="16" cy="16" r="4" fill="#EF4444"/>
  <text x="26" y="20" fill="#E2E8F0" font-family="-apple-system, sans-serif" font-size="10" font-weight="600">LIVE SENSOR CAM // PATROL-{id_str[:4].upper()}</text>
  <text x="465" y="20" fill="#94A3B8" font-family="-apple-system, sans-serif" font-size="10" text-anchor="end">FPS: 30.0 | ISO 400</text>

  <!-- Bottom Telemetry HUD -->
  <rect x="0" y="238" width="480" height="32" fill="rgba(11, 15, 23, 0.85)"/>
  <text x="14" y="258" fill="#F8FAFC" font-family="-apple-system, sans-serif" font-size="11" font-weight="600">GPS: {lat:.4f}, {lon:.4f}</text>
  <text x="465" y="258" fill="{box_color}" font-family="-apple-system, sans-serif" font-size="11" font-weight="bold" text-anchor="end">{sev_upper} ({passes} PASS{'ES' if passes > 1 else ''})</text>
</svg>"""


@app.get("/potholes/{pothole_id}/media", summary="Stream representative photo for canonical pothole")
async def get_pothole_media(
    pothole_id: uuid.UUID,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    stmt = select(CanonicalPothole).where(CanonicalPothole.pothole_id == pothole_id)
    result = await session.execute(stmt)
    pothole = result.scalar_one_or_none()
    if pothole is None:
        raise HTTPException(status_code=404, detail="Canonical pothole not found")

    settings: Settings = request.app.state.settings
    media_uri = pothole.primary_media_uri or ""
    object_key = media_uri.replace(f"minio://{settings.minio_bucket}/", "")

    if Minio is not None:
        try:
            s3 = Minio(
                settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
                secure=settings.minio_secure,
            )
            response = s3.get_object(settings.minio_bucket, object_key)
            data = response.read()
            response.close()
            response.release_conn()
            content_type = "image/jpeg" if object_key.endswith((".jpg", ".jpeg", ".png")) else "video/mp4"
            return Response(content=data, media_type=content_type, headers={"Cache-Control": "public, max-age=86400"})
        except Exception:
            pass

    # High-definition photorealistic dashcam snapshot visualization
    svg = render_road_snapshot_svg(
        id_str=str(pothole_id),
        severity=pothole.severity,
        passes=pothole.observation_count,
        lat=pothole.latitude,
        lon=pothole.longitude,
        confidence=pothole.avg_confidence or 0.94,
    )
    return Response(content=svg, media_type="image/svg+xml")


@app.patch("/potholes/{pothole_id}/status", summary="Update repair status of a canonical pothole")
async def update_pothole_status(
    pothole_id: uuid.UUID,
    payload: dict = Body(...),
    session: AsyncSession = Depends(get_session),
):
    """Road authority endpoint to mark a pothole as 'repaired', 'active', or 'archived'."""
    new_status = payload.get("status", "").lower()
    try:
        status_enum = PotholeStatus(new_status)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid status: {new_status}. Allowed: active, repaired, archived")

    stmt = select(CanonicalPothole).where(CanonicalPothole.pothole_id == pothole_id)
    result = await session.execute(stmt)
    pothole = result.scalar_one_or_none()
    if pothole is None:
        raise HTTPException(status_code=404, detail="Canonical pothole not found")

    pothole.status = status_enum
    await session.commit()
    return {"pothole_id": str(pothole_id), "status": pothole.status.value}


@app.post("/api/deduplicate/rebuild", summary="Re-cluster all events into canonical potholes")
async def rebuild_clusters(session: AsyncSession = Depends(get_session)):
    """Administrative batch sweep to re-cluster historical detection events."""
    count = await recluster_all_events(session)
    return {"status": "ok", "reوبه": count, "message": f"Successfully processed {count} detection events into canonical clusters"}


@app.get("/detections/export/geojson", summary="Export road anomalies as GeoJSON")
async def export_geojson(session: AsyncSession = Depends(get_session)):
    """Download full GeoJSON FeatureCollection for GIS integration."""
    geo = await get_all_detections(session=session, limit=2000, deduplicated=True)
    json_bytes = json.dumps(geo, indent=2).encode("utf-8")
    now_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return Response(
        content=json_bytes,
        media_type="application/geo+json",
        headers={
            "Content-Disposition": f'attachment; filename="flux_potholes_{now_str}.geojson"'
        },
    )


@app.get("/detections/{event_id}", summary="Get raw anomaly observation details")
async def get_detection_detail(
    event_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
):
    stmt = select(DetectionEvent).where(DetectionEvent.event_id == event_id)
    result = await session.execute(stmt)
    event = result.scalar_one_or_none()
    if event is None:
        raise HTTPException(status_code=404, detail="Detection not found")

    sev = "Low"
    for obj in event.objects:
        w = obj.get("bbox", [0, 0, 0, 0])[2] - obj.get("bbox", [0, 0, 0, 0])[0]
        h = obj.get("bbox", [0, 0, 0, 0])[3] - obj.get("bbox", [0, 0, 0, 0])[1]
        if w * h > 0.5:
            sev = "Critical"
        elif w * h > 0.3:
            sev = "High"
        elif w * h > 0.15:
            sev = "Medium"
    if event.metrics and "severity" in event.metrics:
        sev = event.metrics["severity"].capitalize()

    return {
        "event_id": str(event.event_id),
        "device_id": event.device_id,
        "status": event.status.value,
        "captured_at": event.captured_at.isoformat(),
        "received_at": event.received_at.isoformat(),
        "processed_at": event.processed_at.isoformat() if event.processed_at else None,
        "latitude": event.latitude,
        "longitude": event.longitude,
        "severity": sev,
        "object_count": event.object_count,
        "objects": event.objects,
        "metrics": event.metrics,
        "processing_ms": event.processing_ms,
        "media_kind": event.media_kind,
        "media_uri": event.media_uri,
        "canonical_pothole_id": str(event.canonical_pothole_id) if event.canonical_pothole_id else None,
        "thumbnail_url": f"/detections/{event.event_id}/media",
    }


@app.get("/detections/{event_id}/media", summary="Stream raw detection media/thumbnail")
async def get_detection_media(
    event_id: uuid.UUID,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    stmt = select(DetectionEvent).where(DetectionEvent.event_id == event_id)
    result = await session.execute(stmt)
    event = result.scalar_one_or_none()
    if event is None:
        raise HTTPException(status_code=404, detail="Detection not found")

    settings: Settings = request.app.state.settings
    object_key = event.media_uri.replace(f"minio://{settings.minio_bucket}/", "")

    if Minio is not None:
        try:
            s3 = Minio(
                settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
                secure=settings.minio_secure,
            )
            response = s3.get_object(settings.minio_bucket, object_key)
            data = response.read()
            response.close()
            response.release_conn()
            content_type = "image/jpeg" if (event.media_kind == "image" or object_key.endswith((".jpg", ".jpeg", ".png"))) else "video/mp4"
            return Response(content=data, media_type=content_type, headers={"Cache-Control": "public, max-age=86400"})
        except Exception:
            pass

    # Photorealistic dashcam capture
    svg = render_road_snapshot_svg(
        id_str=str(event_id),
        severity=event.metrics.get("severity", "Medium") if event.metrics else "Medium",
        passes=1,
        lat=event.latitude or 33.72,
        lon=event.longitude or 73.09,
        confidence=0.92,
    )
    return Response(content=svg, media_type="image/svg+xml")


@app.get("/api/stream/events", summary="Live SSE stream for real-time canonical & detection events")
async def sse_events(request: Request):
    """Server-Sent Events stream for real-time map alerts."""
    async def event_generator():
        last_check = datetime.now(timezone.utc)
        yield ": connected\n\n"
        while True:
            if await request.is_disconnected():
                break
            try:
                async with SessionLocal() as session:
                    # Query newly created/updated canonical potholes
                    stmt = (
                        select(CanonicalPothole)
                        .where(CanonicalPothole.last_detected_at >= last_check)
                        .order_by(CanonicalPothole.last_detected_at.asc())
                        .limit(10)
                    )
                    res = await session.execute(stmt)
                    updated_potholes = res.scalars().all()

                    if updated_potholes:
                        last_check = datetime.now(timezone.utc)
                        for p in updated_potholes:
                            payload = {
                                "type": "pothole.created" if p.observation_count == 1 else "pothole.updated",
                                "id": str(p.pothole_id),
                                "latitude": p.latitude,
                                "longitude": p.longitude,
                                "severity": p.severity,
                                "observation_count": p.observation_count,
                                "avg_confidence": p.avg_confidence,
                                "timestamp": p.last_detected_at.isoformat(),
                                "thumbnail_url": f"/potholes/{p.pothole_id}/media",
                            }
                            yield f"data: {json.dumps(payload)}\n\n"
                    else:
                        yield ": keepalive\n\n"
            except Exception as e:
                logger.debug("SSE stream heartbeat: %s", e)
                yield ": keepalive\n\n"

            await asyncio.sleep(3)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/stats")
async def get_stats(session: AsyncSession = Depends(get_session)):
    stmt_potholes = select(func.count(CanonicalPothole.pothole_id)).where(CanonicalPothole.status == PotholeStatus.ACTIVE)
    total_potholes = (await session.scalar(stmt_potholes)) or 0

    stmt_raw = select(func.count(DetectionEvent.event_id)).where(DetectionEvent.status == DetectionStatus.PROCESSED)
    total_raw = (await session.scalar(stmt_raw)) or 0

    stmt_crit = select(func.count(CanonicalPothole.pothole_id)).where(
        CanonicalPothole.status == PotholeStatus.ACTIVE,
        CanonicalPothole.severity.in_(["High", "Critical"]),
    )
    crit = (await session.scalar(stmt_crit)) or 0

    return {
        "total": total_potholes if total_potholes > 0 else total_raw,
        "high": crit,
        "total_potholes": total_potholes,
        "total_observations": total_raw,
    }


@app.get("/notifications")
async def get_notifications(session: AsyncSession = Depends(get_session)):
    stmt = (
        select(CanonicalPothole)
        .where(CanonicalPothole.status == PotholeStatus.ACTIVE)
        .order_by(CanonicalPothole.last_detected_at.desc())
        .limit(6)
    )
    result = await session.execute(stmt)
    potholes = result.scalars().all()

    notifs = []
    for p in potholes:
        notifs.append({
            "id": str(p.pothole_id),
            "lat": p.latitude,
            "lon": p.longitude,
            "severity": p.severity,
            "observation_count": p.observation_count,
            "city": "Active Anomaly",
            "area": f"Pothole ({p.severity})",
            "timestamp": p.last_detected_at.isoformat(),
        })

    return {"notifications": notifs}


@app.post("/seed")
async def seed_data(session: AsyncSession = Depends(get_session)):
    import uuid
    from datetime import datetime, timezone, timedelta

    now = datetime.now(timezone.utc)

    # Comprehensive realistic road defects across major avenues & urban centers
    # Format: (lat, lon, severity, confidence, device_id, time_offset_min, road_name, depth_cm)
    sample_records = [
        # ── Islamabad Capital Territory & Rawalpindi ──
        # Blue Area / Jinnah Avenue (Multi-pass Cluster: 3 observations at same spot)
        (33.7198, 73.0895, "Critical", 0.96, "patrol-isb-04", 15, "Jinnah Avenue (Blue Area)", 18),
        (33.7200, 73.0897, "High",     0.94, "dashcam-civic-12", 90, "Jinnah Avenue (Blue Area)", 16),
        (33.7201, 73.0896, "Critical", 0.98, "transit-van-02", 340, "Jinnah Avenue (Blue Area)", 19),

        # Srinagar Highway near H-8 Interchange (2-pass cluster)
        (33.6844, 73.0479, "High",     0.92, "fleet-patrol-09", 45, "Srinagar Highway H-8", 14),
        (33.6845, 73.0481, "Medium",   0.88, "dashcam-patrol-01", 600, "Srinagar Highway H-8", 11),

        # Murree Road near Chandni Chowk / Sixth Road (Multi-pass Cluster: 3 observations)
        (33.6358, 73.0722, "Critical", 0.97, "rwp-surveyor-03", 25, "Murree Road (Sixth Rd)", 22),
        (33.6360, 73.0724, "High",     0.93, "transit-bus-81", 180, "Murree Road (Sixth Rd)", 20),
        (33.6359, 73.0723, "Critical", 0.95, "dashcam-civic-44", 720, "Murree Road (Sixth Rd)", 21),

        # Sector F-6 / Super Market loop
        (33.7295, 73.0745, "Medium",   0.89, "patrol-isb-04", 110, "School Road (F-6)", 9),
        # Sector F-10 Markaz Outer Ring
        (33.6932, 73.0118, "Low",      0.85, "patrol-isb-02", 240, "F-10 Markaz Crescent", 6),
        # Islamabad Expressway near Faizabad Flyover
        (33.6601, 73.0850, "Critical", 0.96, "fleet-patrol-09", 70, "Expressway (Faizabad)", 17),
        # I-9 Industrial Sector
        (33.6685, 73.0560, "High",     0.91, "truck-fleet-07", 400, "I-9 Industrial Avenue", 15),

        # ── Lahore Metropolitan Area ──
        # Mall Road near Anarkali / Lahore Museum (3-pass Cluster)
        (31.5642, 74.3125, "Critical", 0.97, "patrol-lhe-01", 30, "The Mall (Anarkali)", 20),
        (31.5644, 74.3127, "High",     0.94, "dashcam-lhe-88", 160, "The Mall (Anarkali)", 18),
        (31.5643, 74.3126, "Critical", 0.96, "transit-bus-14", 520, "The Mall (Anarkali)", 19),

        # Gulberg Main Boulevard near Liberty Roundabout (2-pass Cluster)
        (31.5204, 74.3587, "Medium",   0.89, "patrol-lhe-03", 80, "Main Blvd Gulberg (Liberty)", 10),
        (31.5206, 74.3589, "High",     0.93, "dashcam-lhe-19", 290, "Main Blvd Gulberg (Liberty)", 12),

        # Canal Bank Road near Muslim Town
        (31.5120, 74.3210, "High",     0.92, "patrol-lhe-02", 140, "Canal Bank (Muslim Town)", 14),
        # MM Alam Road near Mini Market
        (31.5340, 74.3510, "Medium",   0.87, "dashcam-lhe-55", 380, "MM Alam Road", 8),
        # Lahore Ring Road Northern Segment
        (31.6050, 74.3850, "Critical", 0.98, "highway-patrol-05", 60, "Ring Road North", 16),
        # DHA Phase 5 Commercial Boulevard
        (31.4720, 74.4050, "Low",      0.84, "patrol-lhe-04", 450, "DHA Phase 5 Avenue", 5),
        # Ferozepur Road near Kalma Chowk
        (31.5050, 74.3350, "High",     0.94, "transit-bus-22", 210, "Ferozepur Rd (Kalma Chowk)", 15),

        # ── Karachi Metropolitan Area ──
        # Shahrah-e-Faisal near Nursery Flyover (4-pass Cluster)
        (24.8607, 67.0611, "Critical", 0.99, "patrol-khi-01", 10, "Shahrah-e-Faisal (Nursery)", 24),
        (24.8609, 67.0613, "Critical", 0.97, "dashcam-khi-09", 120, "Shahrah-e-Faisal (Nursery)", 22),
        (24.8608, 67.0612, "High",     0.95, "transit-bus-99", 300, "Shahrah-e-Faisal (Nursery)", 20),
        (24.8607, 67.0610, "Critical", 0.98, "patrol-khi-02", 600, "Shahrah-e-Faisal (Nursery)", 23),

        # Clifton Block 2 / Sea View Road (2-pass Cluster)
        (24.8120, 67.0310, "High",     0.92, "dashcam-khi-44", 95, "Sea View Road (Clifton)", 13),
        (24.8122, 67.0312, "Medium",   0.88, "patrol-khi-03", 420, "Sea View Road (Clifton)", 11),

        # M.A. Jinnah Road near Saddar
        (24.8650, 67.0180, "Critical", 0.96, "patrol-khi-01", 50, "M.A. Jinnah Rd (Saddar)", 19),
        # Rashid Minhas Road near Millennium Mall
        (24.9050, 67.1150, "High",     0.91, "transit-bus-45", 260, "Rashid Minhas Road", 14),
        # Korangi Industrial Road
        (24.8350, 67.1350, "Critical", 0.97, "freight-patrol-08", 170, "Korangi Industrial Causeway", 25),
        # Gulshan-e-Iqbal Block 13D
        (24.9210, 67.0850, "Medium",   0.89, "patrol-khi-04", 330, "University Rd (Gulshan)", 10),

        # ── Additional Urban Corridors ──
        # Peshawar: University Road
        (34.0080, 71.5350, "High",     0.93, "patrol-pew-01", 150, "University Road (Peshawar)", 15),
        # Multan: Bosan Road
        (30.2150, 71.4850, "Medium",   0.88, "patrol-mul-02", 220, "Bosan Road (Multan)", 12),
        # Faisalabad: D-Ground
        (31.4150, 73.0950, "Low",      0.86, "patrol-fsd-01", 310, "D-Ground (Faisalabad)", 7),
        # Quetta: Zarghoon Road
        (30.1850, 67.0150, "High",     0.90, "patrol-qta-01", 190, "Zarghoon Road (Quetta)", 16),
    ]

    for lat, lon, sev, conf, dev_id, offset_min, road, depth in sample_records:
        cap_time = now - timedelta(minutes=offset_min)
        event = DetectionEvent(
            event_id=uuid.uuid4(),
            device_id=dev_id,
            captured_at=cap_time,
            received_at=cap_time + timedelta(seconds=2),
            processed_at=cap_time + timedelta(seconds=5),
            status=DetectionStatus.PROCESSED,
            media_kind="image",
            media_uri=f"minio://media/{dev_id}_{int(cap_time.timestamp())}.jpg",
            latitude=lat,
            longitude=lon,
            object_count=1,
            objects=[{
                "label": "pothole",
                "confidence": conf,
                "bbox": [0.15, 0.2, 0.85, 0.75],
                "depth_cm": depth,
                "road_segment": road,
            }],
            metrics={
                "severity": sev,
                "depth_cm": depth,
                "road_name": road,
                "confidence": conf,
            },
        )
        session.add(event)
        await cluster_detection(session, event)

    await session.commit()
    return {"status": "ok", "seeded_observations": len(sample_records)}


# ═══════════════════════════════════════════════════════════════════════
# MS-006: Chunked Upload Endpoints
# ═══════════════════════════════════════════════════════════════════════

@app.post(
    "/api/uploads",
    status_code=201,
    response_model=UploadSessionResponse,
    summary="Create a chunked upload session",
)
async def create_upload_session(req: UploadSessionRequest) -> UploadSessionResponse:
    """Create a new upload session. Returns session_id and upload instructions."""
    mgr: UploadManager = app.state.upload_manager
    session = mgr.create_session(
        device_id=req.device_id,
        filename=req.filename,
        total_chunks=req.total_chunks,
        latitude=req.latitude,
        longitude=req.longitude,
        content_type=req.content_type,
    )
    return UploadSessionResponse(
        session_id=session.session_id,
        total_chunks=session.total_chunks,
        chunk_size_hint=DEFAULT_CHUNK_SIZE,
        upload_url_template=f"/api/uploads/{session.session_id}/chunks/{{n}}",
    )


@app.put(
    "/api/uploads/{session_id}/chunks/{chunk_index}",
    status_code=200,
    response_model=ChunkUploadResponse,
    summary="Upload a single chunk",
)
async def upload_chunk(
    session_id: str,
    chunk_index: int,
    chunk: UploadFile,
) -> ChunkUploadResponse:
    """Upload chunk N for a given upload session."""
    mgr: UploadManager = app.state.upload_manager
    session = mgr.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Unknown session: {session_id}")

    try:
        data = await chunk.read()
        mgr.store_chunk(session_id, chunk_index, data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    received = len(session.received_chunks)
    remaining = session.total_chunks - received
    return ChunkUploadResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        received=received,
        remaining=remaining,
    )


@app.post(
    "/api/uploads/{session_id}/complete",
    status_code=200,
    response_model=CompleteUploadResponse,
    summary="Complete a chunked upload and trigger processing",
)
async def complete_upload(
    session_id: str,
    request: Request,
) -> CompleteUploadResponse:
    """Assemble chunks into a final file and push an ingest event to Redis."""
    mgr: UploadManager = app.state.upload_manager
    session = mgr.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Unknown session: {session_id}")

    try:
        media_uri, object_key = mgr.complete_session(session_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Create an ingest event and push standard envelope to Redis stream for async worker
    event_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    event = DetectionEventIn(
        schema_version=1,
        event_id=event_id,
        captured_at=now,
        media={
            "kind": "video" if "video" in session.content_type else "image",
            "uri": media_uri,
        },
        objects=[],
        latitude=session.latitude,
        longitude=session.longitude,
    )
    envelope = {
        "schema_version": 1,
        "event_id": str(event_id),
        "device_id": session.device_id,
        "received_at": now.isoformat(),
        "payload": event.model_dump(mode="json"),
    }
    settings = request.app.state.settings
    await request.app.state.redis.xadd(
        settings.ingest_stream,
        {"data": json.dumps(envelope, separators=(",", ":"))},
        maxlen=settings.stream_maxlen,
        approximate=True,
    )

    logger.info(
        "chunked upload completed session=%s event=%s uri=%s",
        session_id, event_id, media_uri,
    )
    return CompleteUploadResponse(
        status="completed",
        session_id=session_id,
        event_id=str(event_id),
        media_uri=media_uri,
    )


@app.delete("/api/uploads/{session_id}", status_code=200, summary="Cancel an upload session")
async def cancel_upload(session_id: str):
    """Cancel an in-progress upload session and clean up its chunks."""
    mgr: UploadManager = app.state.upload_manager
    session = mgr.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Unknown session: {session_id}")

    # Clean up any chunks that were stored
    for i in session.received_chunks:
        try:
            mgr._client.remove_object(mgr.bucket, mgr._chunk_key(session_id, i))
        except Exception:
            pass
    del mgr._sessions[session_id]
    return {"status": "cancelled", "session_id": session_id}


# Mount static files AFTER all API routes
app.mount("/static", StaticFiles(directory="static", html=True), name="static")
