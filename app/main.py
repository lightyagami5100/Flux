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
import os
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
from .deps import DeviceIdentity, get_current_device, get_redis, get_upload_device
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
from .seed import seed_database
from .severity import compute_severity, extract_class_label
from .visualization import render_road_snapshot_svg
from .db import get_session
from sqlalchemy import select, func
try:
    from minio import Minio
except ImportError:
    Minio = None  # type: ignore
from .upload_manager import UploadManager, DEFAULT_CHUNK_SIZE
from .deduplication import recluster_all_events
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

    # Embedded Worker for Local Dev & Hackathon Demos:
    # Guarantees that any upload from phone or web is processed by Roboflow and appears
    # on the dashboard immediately, even without Docker/standalone worker containers.
    dev_worker = None
    if not settings.is_production and settings.environment != "test" and os.environ.get("FLUX_DISABLE_EMBEDDED_WORKER", "0") != "1":
        try:
            from app.worker import DetectionWorker
            import app.db as db_module
            dev_worker = DetectionWorker(
                redis_client=app.state.redis,
                session_factory=db_module.SessionLocal,
                settings=settings,
            )
            app.state.dev_worker = dev_worker
            app.state.dev_worker_task = asyncio.create_task(dev_worker.run())
            logger.info("Embedded DetectionWorker started (seamless dev/demo mode)")
        except Exception as e:
            logger.warning("Could not start embedded DetectionWorker: %s", e)

    async def _cleanup_loop():
        while True:
            try:
                await asyncio.sleep(3600)
                app.state.upload_manager.cleanup_stale()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    cleanup_task = asyncio.create_task(_cleanup_loop())

    try:
        yield
    finally:
        cleanup_task.cancel()
        if dev_worker:
            dev_worker.request_stop()
            if hasattr(app.state, "dev_worker_task"):
                app.state.dev_worker_task.cancel()
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

# ─── CORS: config-driven origins (wildcard blocked in production) ──────────
# ─── Middleware Stack ────────────────────────────────────────────────────────
app.add_middleware(PrometheusMiddleware)
_settings_cors = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=_settings_cors.cors_origins,
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


# Media upload routes carry raw photo/video chunks (~5 MiB); the JSON body cap
# would 413 every real capture.
BODY_SIZE_EXEMPT_PREFIXES = ("/v1/ingest/upload", "/api/uploads")


@app.middleware("http")
async def enforce_body_size(request: Request, call_next):
    """Cheap DoS guard: reject oversized declared bodies before parsing."""
    if request.url.path.startswith(BODY_SIZE_EXEMPT_PREFIXES):
        return await call_next(request)
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
    device: DeviceIdentity = Security(get_current_device),
    event: DetectionEventIn = Body(...),
    request: Request = None,
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
    device: DeviceIdentity = Depends(get_upload_device),
):
    import os
    import uuid
    from datetime import datetime
    event_id = uuid.uuid4()
    
    # Upload to MinIO (using settings)
    settings = app.state.settings
    object_name = f"mobile/{event_id}_{video.filename or 'frame.jpg'}"
    file_content = await video.read()
    uri = f"minio://{settings.minio_bucket}/{object_name}"
    
    try:
        mgr = getattr(app.state, "upload_manager", None)
        s3 = getattr(mgr, "_client", None)
        if s3 is None:
            s3 = Minio(
                settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
                secure=settings.minio_secure
            )
        bucket = settings.minio_bucket
        if not s3.bucket_exists(bucket):
            s3.make_bucket(bucket)
        s3.put_object(
            bucket,
            object_name,
            io.BytesIO(file_content),
            length=len(file_content),
            content_type=video.content_type
        )
    except Exception as exc:
        if settings.is_production:
            raise HTTPException(status_code=500, detail=f"MinIO storage error: {exc}") from exc
        logger.warning("MinIO unavailable (%s); saving upload locally (dev mode)", exc)
        local_dir = os.path.join("data", "media", "mobile")
        os.makedirs(local_dir, exist_ok=True)
        local_file = os.path.join(local_dir, f"{event_id}_{video.filename or 'frame.jpg'}")
        with open(local_file, "wb") as f:
            f.write(file_content)
        uri = f"file://{os.path.abspath(local_file)}"
    
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
        "device_id": device.device_id,
        "received_at": now.isoformat(),
        "payload": event.model_dump(mode="json"),
    }
    redis_client = request.app.state.redis if request else app.state.redis
    await redis_client.xadd(
        settings.ingest_stream,
        {"data": json.dumps(envelope, separators=(",", ":"))},
        maxlen=settings.stream_maxlen,
        approximate=True,
    )
    
    return {"status": "accepted", "event_id": str(event_id)}


_extract_class_label = extract_class_label


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
                p_sev = compute_severity(getattr(p, "objects", []), getattr(p, "metrics", None))

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
        sev = compute_severity(event.objects, event.metrics)

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

    # Check local filesystem fallback (for dev/demo uploads saved without MinIO)
    if media_uri.startswith("file://"):
        local_path = media_uri.replace("file://", "")
        if os.path.exists(local_path):
            with open(local_path, "rb") as f:
                return Response(content=f.read(), media_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})
    local_candidate = os.path.join("data", "media", "mobile", os.path.basename(object_key))
    if os.path.exists(local_candidate):
        with open(local_candidate, "rb") as f:
            return Response(content=f.read(), media_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})
    if "sample_pothole.jpg" in media_uri and os.path.exists(os.path.join("static", "sample_pothole.jpg")):
        with open(os.path.join("static", "sample_pothole.jpg"), "rb") as f:
            return Response(content=f.read(), media_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})

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
    return {"status": "ok", "reclustered": count, "message": f"Successfully processed {count} detection events into canonical clusters"}


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

    sev = compute_severity(event.objects, event.metrics)

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

    # Check local filesystem fallback (for dev/demo uploads saved without MinIO)
    if event.media_uri.startswith("file://"):
        local_path = event.media_uri.replace("file://", "")
        if os.path.exists(local_path):
            with open(local_path, "rb") as f:
                return Response(content=f.read(), media_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})
    local_candidate = os.path.join("data", "media", "mobile", os.path.basename(object_key))
    if os.path.exists(local_candidate):
        with open(local_candidate, "rb") as f:
            return Response(content=f.read(), media_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})
    if "sample_pothole.jpg" in event.media_uri and os.path.exists(os.path.join("static", "sample_pothole.jpg")):
        with open(os.path.join("static", "sample_pothole.jpg"), "rb") as f:
            return Response(content=f.read(), media_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})

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
    """Server-Sent Events stream for real-time map alerts.

    Primary path: Redis Pub/Sub subscription (instant delivery from the worker).
    Fallback: 3-second DB poll (for dev/fakeredis where pub/sub isn't wired).
    """
    LIVE_CHANNEL = "flux:events:live"

    async def _pubsub_generator():
        """Subscribe to Redis Pub/Sub and yield SSE events instantly."""
        redis: Redis = request.app.state.redis
        pubsub = redis.pubsub()
        await pubsub.subscribe(LIVE_CHANNEL)
        try:
            yield ": connected (pubsub)\n\n"
            keepalive_interval = 15  # seconds between keepalive pings
            while True:
                if await request.is_disconnected():
                    break
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=keepalive_interval,
                )
                if msg and msg["type"] == "message":
                    yield f"data: {msg['data']}\n\n"
                else:
                    yield ": keepalive\n\n"
        finally:
            await pubsub.unsubscribe(LIVE_CHANNEL)
            await pubsub.aclose()

    async def _db_poll_generator():
        """Fallback: poll the DB every 3 seconds for updated canonical potholes."""
        last_check = datetime.now(UTC)
        yield ": connected (poll)\n\n"
        while True:
            if await request.is_disconnected():
                break
            try:
                async with SessionLocal() as session:
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

    # Choose strategy: try pub/sub first, fall back to polling
    async def event_generator():
        redis: Redis = request.app.state.redis
        try:
            # Probe whether pub/sub is functional (fakeredis doesn't support it)
            pubsub = redis.pubsub()
            await pubsub.subscribe(LIVE_CHANNEL)
            await pubsub.unsubscribe(LIVE_CHANNEL)
            await pubsub.aclose()
            use_pubsub = True
        except Exception:
            use_pubsub = False

        gen = _pubsub_generator() if use_pubsub else _db_poll_generator()
        async for chunk in gen:
            yield chunk

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


def _check_not_production() -> None:
    if get_settings().is_production:
        raise HTTPException(status_code=404, detail="Endpoint not available in production")


@app.post("/seed", dependencies=[Depends(_check_not_production)])
async def seed_data(
    session: AsyncSession = Depends(get_session),
    wipe: bool = False,
):
    count = await seed_database(session, wipe=wipe)
    return {"status": "ok", "seeded_observations": count}


# ═══════════════════════════════════════════════════════════════════════
# MS-006: Chunked Upload Endpoints
# ═══════════════════════════════════════════════════════════════════════

@app.post(
    "/api/uploads",
    status_code=201,
    response_model=UploadSessionResponse,
    summary="Create a chunked upload session",
)
async def create_upload_session(
    req: UploadSessionRequest,
    device: DeviceIdentity = Depends(get_upload_device),
) -> UploadSessionResponse:
    """Create a new upload session. Returns session_id and upload instructions."""
    mgr: UploadManager = app.state.upload_manager
    dev_id = device.device_id if device.device_id != "mobile-anonymous" else (req.device_id or "mobile-anonymous")
    session = mgr.create_session(
        device_id=dev_id,
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
