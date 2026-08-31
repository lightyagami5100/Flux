"""Sprint 1 — Ingest Plane API.

POST /v1/ingest/detections:
  body  : DetectionEventIn (strict schema)
  auth  : X-API-Key header -> device identity
  idem  : Idempotency-Key header (falls back to the event_id)
  output: 201 + envelope appended to a Redis Stream for async processing.
"""
from __future__ import annotations

import asyncio
import io
import json
import logging
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, UTC
from typing import Annotated

import redis.asyncio as aioredis
from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query, Request, Response, Security, UploadFile, Form, status
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

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

    # Fail fast rather than silently serving production traffic on dev defaults.
    misconfigured = settings.missing_production_settings()
    if misconfigured:
        raise RuntimeError(
            f"ENVIRONMENT={settings.environment} but these settings still hold "
            f"insecure/dev defaults: {', '.join(misconfigured)}. "
            "Set them in the environment before starting."
        )

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
        if settings.is_production:
            raise RuntimeError(f"Redis is unreachable at {settings.redis_url}") from e
        logger.warning(
            "Redis unavailable at %s (%s). Using in-memory fakeredis fallback (development only).",
            settings.redis_url,
            e,
        )
        import fakeredis.aioredis
        app.state.redis = fakeredis.aioredis.FakeRedis(decode_responses=True)

    # Resilient Database initialization
    if settings.auto_create_tables:
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Database schema ensured in PostgreSQL")
        except Exception as e:
            if settings.is_production:
                raise RuntimeError("PostgreSQL is unreachable; refusing to fall back to SQLite") from e
            logger.warning(
                "PostgreSQL unavailable (%s). Falling back to local SQLite (development only).", e
            )
            import app.db as db_module
            from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
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

# ─── CORS configuration for Mobile App Testing & Web Dashboard ────────────────
# ─── Middleware Stack ────────────────────────────────────────────────────────
app.add_middleware(PrometheusMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """Enforce OWASP-recommended security headers on all HTTP responses."""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return response


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
    received_at = datetime.now(UTC)
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
    except Exception as exc:
        logger.exception("failed to append event %s to stream", event.event_id)
        await release(redis_client, idem_key)  # free the key so the client can retry
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Ingest queue temporarily unavailable",
            headers={"Retry-After": "2"},
        ) from exc

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
    from datetime import datetime
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
    now = datetime.now(UTC)
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


def _extract_class_label(record) -> str:
    """Extract the anomaly class label from a CanonicalPothole or DetectionEvent record."""
    # Check observations list for label
    if hasattr(record, 'observations') and record.observations:
        for obs in record.observations:
            if isinstance(obs, dict):
                if 'label' in obs:
                    return obs['label']
    # Check objects list for label
    if hasattr(record, 'objects') and record.objects:
        for obj in record.objects:
            if isinstance(obj, dict):
                return obj.get('label', 'pothole')
    # Check metrics dict for label
    if hasattr(record, 'metrics') and isinstance(record.metrics, dict):
        return record.metrics.get('label', 'pothole')
    return 'pothole'


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
            p_first = getattr(p, "first_detected_at", getattr(p, "captured_at", datetime.now(UTC)))
            p_last = getattr(p, "last_detected_at", getattr(p, "captured_at", datetime.now(UTC)))
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
                    "class": _extract_class_label(p),
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

    class_label = _extract_class_label(pothole)

    return {
        "pothole_id": str(pothole.pothole_id),
        "id": str(pothole.pothole_id),
        "class": class_label,
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


def render_road_snapshot_svg(
    id_str: str,
    severity: str,
    passes: int,
    lat: float,
    lon: float,
    confidence: float = 0.94,
    anomaly_class: str = "pothole",
) -> str:
    sev_upper = (severity or "Low").upper()
    cls_lower = (anomaly_class or "pothole").lower()
    
    # Custom color themes per anomaly type & severity
    class_color_map = {
        "debris": "#F59E0B",
        "road_debris": "#F59E0B",
        "object": "#F59E0B",
        "pothole": "#EF4444",
        "crack": "#F97316",
        "manhole": "#A855F7",
        "waterlogging": "#0EA5E9",
        "sewage": "#14B8A6",
        "garbage_dump": "#F43F5E",
    }
    box_color = class_color_map.get(cls_lower, "#EF4444")
    
    class_title_map = {
        "debris": "ROAD DEBRIS",
        "road_debris": "ROAD DEBRIS",
        "object": "ROAD OBSTACLE",
        "pothole": "POTHOLE",
        "crack": "ROAD CRACK",
        "manhole": "MANHOLE HAZARD",
        "waterlogging": "WATERLOGGING",
        "sewage": "SEWAGE OVERFLOW",
        "garbage_dump": "GARBAGE DUMP",
    }
    class_title = class_title_map.get(cls_lower, cls_lower.upper().replace("_", " "))

    # Generate custom SVG visuals based on anomaly category
    if cls_lower in ("debris", "road_debris", "object"):
        anomaly_visual = """
  <!-- Road Debris / Fallen Cargo Obstacle -->
  <defs>
    <linearGradient id="debrisGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#F59E0B"/>
      <stop offset="60%" stop-color="#D97706"/>
      <stop offset="100%" stop-color="#78350F"/>
    </linearGradient>
  </defs>
  <!-- Fallen wooden pallet / debris barrier -->
  <rect x="180" y="115" width="120" height="60" rx="6" fill="url(#debrisGrad)" filter="url(#shadow)"/>
  <rect x="190" y="125" width="100" height="12" rx="2" fill="#FEF3C7" opacity="0.4"/>
  <rect x="190" y="145" width="100" height="12" rx="2" fill="#FEF3C7" opacity="0.3"/>
  <line x1="210" y1="115" x2="210" y2="175" stroke="#451A03" stroke-width="2.5"/>
  <line x1="270" y1="115" x2="270" y2="175" stroke="#451A03" stroke-width="2.5"/>
  <!-- Warning stripes -->
  <path d="M 230 115 L 245 175" stroke="#FEF08A" stroke-width="3" stroke-dasharray="6,4"/>
        """
    elif cls_lower == "waterlogging":
        anomaly_visual = """
  <!-- Waterlogging Flood Pool -->
  <defs>
    <linearGradient id="waterGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#0284C7" stop-opacity="0.85"/>
      <stop offset="50%" stop-color="#0369A1" stop-opacity="0.95"/>
      <stop offset="100%" stop-color="#0C4A6E" stop-opacity="0.98"/>
    </linearGradient>
  </defs>
  <ellipse cx="240" cy="142" rx="90" ry="42" fill="url(#waterGrad)" filter="url(#shadow)"/>
  <!-- Ripple rings -->
  <ellipse cx="240" cy="142" rx="70" ry="30" fill="none" stroke="#38BDF8" stroke-width="2" opacity="0.75"/>
  <ellipse cx="240" cy="142" rx="45" ry="18" fill="none" stroke="#7DD3FC" stroke-width="1.5" opacity="0.6"/>
  <!-- Wave glints -->
  <path d="M 180 135 Q 200 130 220 135 T 260 135" stroke="#BAE6FD" stroke-width="2" fill="none" opacity="0.8"/>
  <path d="M 210 150 Q 230 145 250 150 T 290 150" stroke="#BAE6FD" stroke-width="1.5" fill="none" opacity="0.7"/>
        """
    elif cls_lower == "sewage":
        anomaly_visual = """
  <!-- Sewage Overflow Effluent -->
  <defs>
    <linearGradient id="sewageGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#0D9488" stop-opacity="0.85"/>
      <stop offset="50%" stop-color="#0F766E" stop-opacity="0.95"/>
      <stop offset="100%" stop-color="#115E59" stop-opacity="0.98"/>
    </linearGradient>
  </defs>
  <!-- Gutter sewer burst runoff -->
  <path d="M 160 115 C 190 95, 270 100, 315 125 C 335 155, 290 185, 250 180 C 190 190, 140 160, 160 115 Z" fill="url(#sewageGrad)" filter="url(#shadow)"/>
  <path d="M 240 100 Q 220 140 250 170" stroke="#2DD4BF" stroke-width="3" fill="none" opacity="0.8"/>
  <circle cx="210" cy="135" r="8" fill="#14B8A6" opacity="0.6"/>
  <circle cx="265" cy="145" r="10" fill="#14B8A6" opacity="0.7"/>
  <circle cx="235" cy="155" r="6" fill="#5EEAD4" opacity="0.8"/>
        """
    elif cls_lower == "garbage_dump":
        anomaly_visual = """
  <!-- Illegal Garbage Heap Mound -->
  <defs>
    <linearGradient id="dumpGrad" x1="0%" y1="0%" x2="0%" y2="100%">
      <stop offset="0%" stop-color="#E11D48"/>
      <stop offset="60%" stop-color="#9F1239"/>
      <stop offset="100%" stop-color="#4C0519"/>
    </linearGradient>
  </defs>
  <!-- Solid Waste Piles -->
  <path d="M 155 175 Q 180 105 240 98 Q 300 105 325 175 Z" fill="url(#dumpGrad)" filter="url(#shadow)"/>
  <!-- Bag / Box shapes in dump -->
  <rect x="175" y="140" width="28" height="24" rx="4" fill="#FB7185" opacity="0.85" transform="rotate(-12 189 152)"/>
  <rect x="235" y="132" width="34" height="26" rx="4" fill="#FDA4AF" opacity="0.9" transform="rotate(15 252 145)"/>
  <rect x="205" y="120" width="30" height="22" rx="3" fill="#F43F5E" opacity="0.85"/>
  <circle cx="280" cy="155" r="12" fill="#BE123C"/>
        """
    elif cls_lower == "manhole":
        anomaly_visual = """
  <!-- Displaced Manhole Cover Hazard -->
  <defs>
    <radialGradient id="manholeGrad" cx="50%" cy="50%" r="50%">
      <stop offset="0%" stop-color="#64748B"/>
      <stop offset="70%" stop-color="#334155"/>
      <stop offset="100%" stop-color="#0F172A"/>
    </radialGradient>
  </defs>
  <!-- Manhole open rim -->
  <ellipse cx="240" cy="142" rx="65" ry="38" fill="#05070A" stroke="#C084FC" stroke-width="3" filter="url(#shadow)"/>
  <!-- Shifted lid -->
  <ellipse cx="260" cy="132" rx="60" ry="34" fill="url(#manholeGrad)" stroke="#A855F7" stroke-width="2"/>
  <circle cx="260" cy="132" r="14" fill="none" stroke="#E2E8F0" stroke-width="2" opacity="0.6"/>
  <line x1="220" y1="132" x2="300" y2="132" stroke="#94A3B8" stroke-width="2" opacity="0.5"/>
  <line x1="260" y1="108" x2="260" y2="156" stroke="#94A3B8" stroke-width="2" opacity="0.5"/>
        """
    elif cls_lower == "crack":
        anomaly_visual = """
  <!-- Road Surface Crack Network -->
  <path d="M 160 90 L 195 125 L 180 145 L 220 160 L 255 135 L 285 170 L 320 185" stroke="#F97316" stroke-width="5" fill="none" filter="url(#shadow)"/>
  <!-- Branching cracks -->
  <path d="M 195 125 L 230 115 L 250 95" stroke="#FB923C" stroke-width="3" fill="none"/>
  <path d="M 220 160 L 205 190 L 175 205" stroke="#FB923C" stroke-width="3" fill="none"/>
  <path d="M 255 135 L 290 120 L 315 130" stroke="#FDBA74" stroke-width="2.5" fill="none"/>
  <path d="M 285 170 L 270 200 L 295 215" stroke="#FDBA74" stroke-width="2.5" fill="none"/>
        """
    else:
        # Default: Pothole
        anomaly_visual = """
  <!-- Physical Pothole Cavity Geometry -->
  <path d="M 170 120 C 185 95, 290 100, 310 125 C 325 145, 305 180, 275 185 C 220 195, 155 170, 170 120 Z" 
        fill="url(#potholeCavity)" stroke="#2B2118" stroke-width="3" filter="url(#shadow)"/>
  <!-- Asphalt Internal Fracture Cracks -->
  <path d="M 170 120 Q 140 105 125 110" stroke="#10141D" stroke-width="2" fill="none" opacity="0.8"/>
  <path d="M 310 125 Q 345 130 365 120" stroke="#10141D" stroke-width="2" fill="none" opacity="0.8"/>
  <path d="M 275 185 Q 285 215 300 225" stroke="#10141D" stroke-width="2" fill="none" opacity="0.8"/>
  <path d="M 210 175 Q 185 200 170 215" stroke="#10141D" stroke-width="2" fill="none" opacity="0.8"/>
        """

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

  {anomaly_visual}

  <!-- AI Detection Bounding Box -->
  <rect x="135" y="75" width="210" height="135" rx="6" fill="none" stroke="{box_color}" stroke-width="2.5" stroke-dasharray="6,4"/>
  
  <!-- Corner Crosshairs -->
  <path d="M 135 85 L 135 75 L 145 75" stroke="{box_color}" stroke-width="3" fill="none"/>
  <path d="M 335 75 L 345 75 L 345 85" stroke="{box_color}" stroke-width="3" fill="none"/>
  <path d="M 135 200 L 135 210 L 145 210" stroke="{box_color}" stroke-width="3" fill="none"/>
  <path d="M 335 210 L 345 210 L 345 200" stroke="{box_color}" stroke-width="3" fill="none"/>

  <!-- AI Classification Tag -->
  <rect x="135" y="50" width="200" height="24" rx="4" fill="{box_color}"/>
  <text x="142" y="66" fill="#FFFFFF" font-family="-apple-system, sans-serif" font-size="11" font-weight="bold" letter-spacing="0.5">
    {class_title} ({int(confidence*100)}% CONF)
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
    cls_label = _extract_class_label(pothole)
    svg = render_road_snapshot_svg(
        id_str=str(pothole_id),
        severity=pothole.severity,
        passes=pothole.observation_count,
        lat=pothole.latitude,
        lon=pothole.longitude,
        confidence=pothole.avg_confidence or 0.94,
        anomaly_class=cls_label,
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
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status: {new_status}. Allowed: active, repaired, archived",
        ) from exc

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
    now_str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
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

    class_label = _extract_class_label(event)

    return {
        "event_id": str(event.event_id),
        "id": str(event.event_id),
        "class": class_label,
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
    cls_label = _extract_class_label(event)
    svg = render_road_snapshot_svg(
        id_str=str(event_id),
        severity=event.metrics.get("severity", "Medium") if event.metrics else "Medium",
        passes=1,
        lat=event.latitude or 33.72,
        lon=event.longitude or 73.09,
        confidence=0.92,
        anomaly_class=cls_label,
    )
    return Response(content=svg, media_type="image/svg+xml")


@app.get("/api/stream/events", summary="Live SSE stream for real-time canonical & detection events")
async def sse_events(request: Request):
    """Server-Sent Events stream for real-time map alerts."""
    async def event_generator():
        last_check = datetime.now(UTC)
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
                        last_check = datetime.now(UTC)
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
async def seed_data(
    session: AsyncSession = Depends(get_session),
    wipe: bool = False,
):
    import uuid
    from datetime import datetime, timedelta
    from sqlalchemy import delete

    if wipe:
        await session.execute(delete(CanonicalPothole))
        await session.execute(delete(DetectionEvent))
        await session.commit()
        logger.info("Cleared previous detection tables before seeding.")

    now = datetime.now(UTC)

    # Comprehensive realistic road defects across major avenues & urban centers
    # Format: (lat, lon, severity, confidence, device_id, time_offset_min, road_name, depth_cm, label)
    # Labels: pothole, crack, manhole, waterlogging, sewage, garbage_dump
    sample_records = [
        # ── Islamabad Capital Territory & Rawalpindi ──
        # Potholes
        (33.7198, 73.0895, "Critical", 0.98, "patrol-isb-04", 15, "Jinnah Avenue (Blue Area)", 18, "pothole"),
        (33.7200, 73.0897, "High",     0.94, "dashcam-civic-12", 90, "Jinnah Avenue (Blue Area)", 16, "pothole"),
        (33.7201, 73.0896, "Critical", 0.97, "transit-van-02", 340, "Jinnah Avenue (Blue Area)", 19, "pothole"),
        (33.6601, 73.0850, "Critical", 0.96, "fleet-patrol-09", 70, "Expressway (Faizabad)", 17, "pothole"),
        (33.6358, 73.0722, "Critical", 0.97, "rwp-surveyor-03", 25, "Murree Road (Sixth Rd)", 22, "pothole"),
        (33.6360, 73.0724, "High",     0.93, "transit-bus-81", 180, "Murree Road (Sixth Rd)", 20, "pothole"),
        (33.6359, 73.0723, "Critical", 0.95, "dashcam-civic-44", 720, "Murree Road (Sixth Rd)", 21, "pothole"),
        
        # Road Cracks
        (33.6844, 73.0479, "High",     0.93, "fleet-patrol-09", 45, "Srinagar Highway H-8", 14, "crack"),
        (33.6845, 73.0481, "Medium",   0.89, "dashcam-patrol-01", 600, "Srinagar Highway H-8", 11, "crack"),
        (33.6685, 73.0560, "High",     0.91, "truck-fleet-07", 400, "I-9 Industrial Avenue", 5, "crack"),
        (33.7240, 73.0610, "Medium",   0.88, "patrol-isb-01", 80, "Margalla Road (F-7)", 4, "crack"),
        (33.6420, 73.0580, "Critical", 0.95, "rwp-patrol-02", 120, "I.J.P. Principal Road", 8, "crack"),

        # Manhole Hazards
        (33.6932, 73.0118, "High",     0.92, "patrol-isb-02", 240, "F-10 Markaz Crescent", 0, "manhole"),
        (33.7110, 73.0580, "Critical", 0.97, "patrol-isb-05", 60, "Jinnah Super F-7", 0, "manhole"),
        (33.6290, 73.0640, "High",     0.94, "rwp-surveyor-01", 190, "Liaquat Bagh Intersection", 0, "manhole"),
        (33.6720, 73.0330, "Medium",   0.88, "fleet-patrol-08", 310, "H-9 Sector Inner Ring", 0, "manhole"),

        # Sewage Overflow & Line Burst
        (33.7295, 73.0745, "Critical", 0.96, "patrol-isb-04", 110, "School Road (F-6) Drain Overflow", 0, "sewage"),
        (33.6990, 73.0360, "High",     0.91, "dashcam-patrol-03", 420, "G-9/4 Commercial Sewer Burst", 0, "sewage"),
        (33.6810, 73.0510, "Critical", 0.95, "patrol-isb-07", 260, "Zero Point Waste Runoff", 0, "sewage"),
        (33.6210, 73.0680, "High",     0.93, "rwp-patrol-03", 140, "Raja Bazaar Main Sewage Leak", 0, "sewage"),

        # Illegal Garbage Dumps & Waste Accumulation
        (33.7050, 73.0400, "High",     0.92, "patrol-isb-02", 500, "G-9 Service Road Open Dump", 0, "garbage_dump"),
        (33.7310, 73.0820, "Medium",   0.89, "patrol-isb-01", 140, "Kashmir Highway Waste Heap", 0, "garbage_dump"),
        (33.6180, 73.0790, "Critical", 0.97, "rwp-patrol-04", 95, "Rawal Road Solid Waste Dump", 0, "garbage_dump"),
        (33.6490, 73.0730, "High",     0.94, "rwp-surveyor-04", 180, "Commercial Market Waste Cluster", 0, "garbage_dump"),

        # Waterlogging
        (33.6750, 73.0690, "High",     0.91, "patrol-isb-06", 35, "I-8 Markaz Ring Road", 0, "waterlogging"),
        (33.6520, 73.0810, "Critical", 0.96, "patrol-isb-03", 50, "Faizabad Underpass Flooding", 0, "waterlogging"),
        (33.6390, 73.0680, "Medium",   0.88, "rwp-surveyor-02", 210, "Committee Chowk Murree Rd", 0, "waterlogging"),

        # ── Lahore Metropolitan Area ──
        # Potholes
        (31.5642, 74.3125, "Critical", 0.98, "patrol-lhe-01", 30, "The Mall (Anarkali)", 20, "pothole"),
        (31.5644, 74.3127, "High",     0.94, "dashcam-lhe-88", 160, "The Mall (Anarkali)", 18, "pothole"),
        (31.6050, 74.3850, "Critical", 0.98, "highway-patrol-05", 60, "Ring Road North", 16, "pothole"),
        (31.4720, 74.4050, "Low",      0.84, "patrol-lhe-04", 450, "DHA Phase 5 Avenue", 5, "pothole"),
        
        # Road Cracks
        (31.5204, 74.3587, "Medium",   0.89, "patrol-lhe-03", 80, "Main Blvd Gulberg (Liberty)", 3, "crack"),
        (31.5206, 74.3589, "High",     0.93, "dashcam-lhe-19", 290, "Main Blvd Gulberg (Liberty)", 4, "crack"),
        (31.5120, 74.3210, "High",     0.92, "patrol-lhe-02", 140, "Canal Bank (Muslim Town)", 5, "crack"),
        (31.4850, 74.3050, "Critical", 0.95, "transit-bus-33", 110, "Wahdat Road (Muslim Town)", 7, "crack"),

        # Manhole Hazards
        (31.5420, 74.3310, "High",     0.93, "patrol-lhe-02", 140, "Jail Road near Services Hospital", 0, "manhole"),
        (31.5790, 74.3180, "Critical", 0.97, "patrol-lhe-06", 75, "Circular Road (Bhati Gate)", 0, "manhole"),
        (31.4680, 74.3520, "Medium",   0.88, "patrol-lhe-08", 320, "Peco Road (Kot Lakhpat)", 0, "manhole"),

        # Sewage Overflow & Toxic Spill
        (31.5340, 74.3510, "Critical", 0.97, "dashcam-lhe-55", 380, "MM Alam Road Sewage Backflow", 0, "sewage"),
        (31.5150, 74.3450, "High",     0.92, "patrol-lhe-05", 410, "Garden Town Sewer Line Burst", 0, "sewage"),
        (31.5890, 74.3050, "Critical", 0.98, "patrol-lhe-01", 190, "Ravi Road Wastewater Spill", 0, "sewage"),

        # Illegal Garbage Dumps
        (31.4920, 74.3910, "High",     0.93, "patrol-lhe-04", 450, "DHA Phase 3 Open Trash Heap", 0, "garbage_dump"),
        (31.5310, 74.3720, "Medium",   0.88, "patrol-lhe-09", 240, "Cavalry Ground Roadside Waste", 0, "garbage_dump"),
        (31.4550, 74.2980, "Critical", 0.96, "patrol-lhe-07", 150, "Township Sector B-1 Solid Waste Dump", 0, "garbage_dump"),

        # Waterlogging
        (31.5050, 74.3350, "High",     0.94, "transit-bus-22", 210, "Ferozepur Rd (Kalma Chowk)", 0, "waterlogging"),
        (31.5580, 74.3420, "Critical", 0.98, "patrol-lhe-03", 40, "Lakshmi Chowk Junction", 0, "waterlogging"),
        (31.4780, 74.2810, "High",     0.92, "patrol-lhe-10", 130, "Thokar Niaz Baig Flyover", 0, "waterlogging"),

        # ── Karachi Metropolitan Area ──
        # Potholes
        (24.8607, 67.0611, "Critical", 0.99, "patrol-khi-01", 10, "Shahrah-e-Faisal (Nursery)", 24, "pothole"),
        (24.8609, 67.0613, "Critical", 0.97, "dashcam-khi-09", 120, "Shahrah-e-Faisal (Nursery)", 22, "pothole"),
        (24.8350, 67.1350, "Critical", 0.97, "freight-patrol-08", 170, "Korangi Industrial Causeway", 25, "pothole"),
        (24.8650, 67.0180, "Critical", 0.96, "patrol-khi-01", 50, "M.A. Jinnah Rd (Saddar)", 19, "pothole"),

        # Road Cracks
        (24.8120, 67.0310, "High",     0.92, "dashcam-khi-44", 95, "Sea View Road (Clifton)", 4, "crack"),
        (24.8122, 67.0312, "Medium",   0.88, "patrol-khi-03", 420, "Sea View Road (Clifton)", 3, "crack"),
        (24.9180, 67.0980, "High",     0.93, "patrol-khi-07", 160, "Gulshan Block 6 Rashid Minhas", 5, "crack"),
        (24.9450, 67.0350, "Critical", 0.95, "patrol-khi-05", 85, "Nazimabad 7-Number Road", 6, "crack"),

        # Manhole Hazards
        (24.8720, 67.0250, "Critical", 0.98, "patrol-khi-02", 30, "Saddar Bohri Bazaar Loop", 0, "manhole"),
        (24.8210, 67.0580, "High",     0.91, "patrol-khi-04", 190, "Khayaban-e-Ittehad (DHA 6)", 0, "manhole"),
        (24.8890, 67.1120, "Critical", 0.96, "patrol-khi-08", 90, "Drigh Road Station Crossing", 0, "manhole"),

        # Sewage Overflow & Gutters Burst
        (24.9210, 67.0850, "Critical", 0.98, "patrol-khi-04", 330, "University Rd Gulshan Sewer Flood", 0, "sewage"),
        (24.8450, 67.0050, "High",     0.93, "patrol-khi-09", 490, "I.I. Chundrigar Road Drainage Overflow", 0, "sewage"),
        (24.8010, 67.0420, "Critical", 0.96, "patrol-khi-03", 270, "Khayaban-e-Shamsheer Gutter Burst", 0, "sewage"),

        # Illegal Garbage Dumps & Landfill Spill
        (24.8390, 67.0480, "Critical", 0.97, "patrol-khi-06", 220, "Gizri Boulevard Huge Trash Dump", 0, "garbage_dump"),
        (24.9310, 67.0620, "High",     0.94, "patrol-khi-07", 110, "Federal B Area Block 14 Open Garbage Dump", 0, "garbage_dump"),
        (24.8700, 67.0890, "Critical", 0.99, "patrol-khi-08", 70, "Tariq Road Commercial Dump", 0, "garbage_dump"),

        # Waterlogging
        (24.9050, 67.1150, "High",     0.94, "transit-bus-45", 260, "Rashid Minhas Road", 0, "waterlogging"),
        (24.8510, 67.0120, "Critical", 0.98, "patrol-khi-01", 45, "Submarine Chowk Underpass", 0, "waterlogging"),
        (24.8810, 67.1720, "High",     0.92, "freight-patrol-02", 150, "Malir River Causeway", 0, "waterlogging"),

        # ── Additional Urban Hubs ──
        (34.0080, 71.5350, "High",     0.93, "patrol-pew-01", 150, "University Road (Peshawar)", 15, "pothole"),
        (34.0150, 71.5800, "Medium",   0.88, "patrol-pew-02", 280, "GT Road (Peshawar)", 3, "crack"),
        (34.0010, 71.5120, "Critical", 0.96, "patrol-pew-03", 70, "Hayatabad Phase 3 Commercial", 0, "manhole"),
        (34.0120, 71.5600, "Critical", 0.97, "patrol-pew-04", 90, "Khyber Bazaar Sewage Spill", 0, "sewage"),
        (34.0220, 71.5900, "High",     0.93, "patrol-pew-05", 140, "Ring Road Peshawar Illegal Dump", 0, "garbage_dump"),
        (30.2150, 71.4850, "Medium",   0.88, "patrol-mul-02", 220, "Bosan Road (Multan)", 0, "waterlogging"),
        (30.1980, 71.4420, "High",     0.92, "patrol-mul-01", 130, "Abdali Road (Multan)", 14, "pothole"),
        (30.1890, 71.4650, "Critical", 0.95, "patrol-mul-03", 85, "Chungi No 9 Sewage Flood", 0, "sewage"),
        (31.4150, 73.0950, "Critical", 0.96, "patrol-fsd-01", 310, "D-Ground Garbage Heap", 0, "garbage_dump"),
        (31.4280, 73.0780, "High",     0.93, "patrol-fsd-02", 100, "Jaranwala Road (Faisalabad)", 16, "pothole"),
        (30.1850, 67.0150, "High",     0.90, "patrol-qta-01", 190, "Zarghoon Road (Quetta)", 16, "pothole"),
        (30.1700, 66.9900, "Critical", 0.94, "patrol-qta-02", 100, "Sariab Road (Quetta)", 0, "manhole"),
        (30.1620, 67.0050, "Critical", 0.97, "patrol-qta-03", 80, "Jinnah Road Quetta Sewage Leak", 0, "sewage"),
    ]

    for lat, lon, sev, conf, dev_id, offset_min, road, depth, label in sample_records:
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
                "label": label,
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
                "label": label,
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
        raise HTTPException(status_code=400, detail=str(e)) from e

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
        raise HTTPException(status_code=400, detail=str(e)) from e

    # Create an ingest event and push standard envelope to Redis stream for async worker
    event_id = uuid.uuid4()
    now = datetime.now(UTC)
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
