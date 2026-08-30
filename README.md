# Flux — Road Intelligence Platform

Video/image ingest → external CV inference → spatial pothole deduplication → live map.

The API is a FastAPI service. Detections arrive from patrol devices (a React Native
app in `mobile/`) or fixed cameras, are queued on a Redis Stream, and are processed
by a worker that calls an **external** inference API. Results are clustered into
canonical potholes and rendered on a Leaflet map served from `static/`.

**Inference is remote only.** No local weights, no local GPU. See
[`.qoder/rules/Flux-sys.md`](.qoder/rules/Flux-sys.md).

---

## Layout

| Path | What lives there |
| --- | --- |
| `app/main.py` | FastAPI app: ingest, chunked upload, query/visualisation endpoints, SSE |
| `app/worker.py` | Redis Stream consumer: pulls media, calls the processor, persists detections |
| `app/processors/` | Pluggable inference backends. `roboflow.py` is the only built-in |
| `app/deduplication.py` | Haversine clustering of raw detections into canonical potholes |
| `app/config.py` | All settings, env-driven. Includes the production-readiness guard |
| `app/models.py` | SQLAlchemy 2.x async ORM models |
| `static/index.html` | Leaflet dashboard |
| `mobile/` | Expo / React Native patrol app |
| `infra/` | Compose stacks and the nginx reverse proxy |
| `tests/` | pytest suite |

`app/database.py` is a leftover duplicate of `app/db.py` + `app/models.py` and is
imported by nothing. It is scheduled for deletion.

---

## Local development

Requires Python 3.11+ (the container image is 3.11) and, optionally, Docker for
Postgres/Redis/MinIO.

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt

cp .env.example .env      # fill in ROBOFLOW_API_KEY and API_KEYS
./start.sh                # or: run.sh
```

`start.sh` detects your LAN IP, writes `mobile/.env` so Expo Go can reach the API,
brings up the Docker dependencies if available, and runs uvicorn with `--reload`.

Then:

- Map dashboard — <http://localhost:8000/>
- Live event stream (SSE) — <http://localhost:8000/api/stream/events>
- Metrics — <http://localhost:8000/metrics>

### Testing with the phone (dashboard on laptop, app on phone)

1. Same Wi-Fi on both devices. Start the stack: `./start.sh` — it starts
   Postgres/Redis/MinIO **and the worker** in Docker, writes `mobile/.env` with
   your LAN IP, and serves the dashboard on `0.0.0.0:8000`.
2. In another terminal: `cd mobile && npm run start`, scan the QR code with
   Expo Go.
3. The phone needs **two** ports reachable on the laptop: `8081` (Expo dev
   bundle) and `8000` (API). On EndeavourOS:
   `sudo firewall-cmd --add-port=8000/tcp --add-port=8081/tcp`
4. Smart Patrol (bump snapshots) and Video Patrol (chunked clips) land in
   MinIO → Redis → worker → Roboflow → map. Expect ~10–30 s end-to-end.

Docker must be running for this to work: `start.sh` warns loudly if it isn't,
 because without a shared Redis the worker cannot see the uploads.

### Graceful degradation

`ENVIRONMENT=development` (the default) keeps two local fallbacks alive so the
stack runs with nothing else installed:

| Dependency missing | Fallback |
| --- | --- |
| Redis | in-process `fakeredis` |
| PostgreSQL | `sqlite+aiosqlite:///flux_dev.db` |

Both fallbacks are **disabled** when `ENVIRONMENT` is anything else — production
refuses to boot on a degraded backing store rather than silently losing data.

`inference-sdk` has no wheels for Python ≥ 3.13. On a newer interpreter the
Roboflow processor raises `ProcessorUnavailable` with an explanation instead of
crashing on import; everything except live inference still works.

What is deliberately **not** degraded: media that cannot be fetched or decoded
never becomes a synthetic frame. It raises, the message keeps its place in the
retry budget (or goes straight to the DLQ if the input is permanently broken),
and the row is persisted as `FAILED`. A fabricated blank frame would be billed by
the inference API and then stored as a successful "no potholes here" reading.

---

## Video handling and multi-model inference

A clip is subsampled rather than decoded whole, because inference is billed per
frame:

| Variable | Default | Effect |
| --- | --- | --- |
| `VIDEO_SAMPLE_EVERY_N_FRAMES` | `15` | ~2 frames/sec on 30 fps footage |
| `VIDEO_MAX_FRAMES` | `20` | Hard ceiling on sampled frames per clip |

`ROBOFLOW_MODEL_IDS` takes a comma-separated list of Roboflow `project/version`
ids. Every model runs on every sampled frame and the detections are merged,
each keeping its model's own class name — so one deployment can detect potholes,
cracks, and other road damage at once. The cost formula is:

> calls per clip = sampled frames × number of models (default 20 × 2 = 40)

Trim the bill by lowering `VIDEO_MAX_FRAMES` or running fewer models.
Detections carry `frame_index` and `timestamp_ms` so a hit can be traced back to
its moment in the clip.

---

## Configuration

Every setting is an environment variable. `.env.example` is the complete list.

The ones with no safe default:

| Variable | Why |
| --- | --- |
| `API_KEYS` | JSON map `api_key -> device_id`. Empty means every ingest returns 401 |
| `ROBOFLOW_API_KEY` | External inference credential |
| `ROBOFLOW_MODEL_IDS` | Comma-separated model list. The default `coco/3` has **no road-damage classes**. Verified public options: `pothole-detection-03iso/1` (Potholes), `damage-road/1` (pothole + crack, mAP@50 71.9) |
| `DATABASE_URL` | Postgres DSN |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | Object storage credentials |

When `ENVIRONMENT` is not one of `development` / `dev` / `test` / `local`,
`Settings.missing_production_settings()` runs at startup and the app **refuses to
start** if any of the above still holds a development default. Compose's
production stack enforces the same contract with `${VAR:?...}`, so a missing
value fails before a container starts.

---

## Tests and lint

```bash
.venv/bin/pytest -q
.venv/bin/ruff check app tests
```

Both gates run in CI (`.github/workflows/ci.yml`), which also builds the Docker
image, validates both compose files, and rejects a committed `.env` or committed
model weights.

---

## Deployment

Target: a Docker host (Alibaba Cloud ECS).

```bash
cp .env.example .env      # fill in real production values
./deploy.sh
```

`deploy.sh` runs a preflight (required vars present, no `minioadmin` defaults,
compose file validates), records the currently running image, builds, starts, then
polls `/healthz` for up to `HEALTH_TIMEOUT` seconds. If the health gate never
passes it dumps the last 80 log lines and **rolls back** to the previous image.

nginx terminates :80, serves `static/`, and proxies the API routes. Only nginx is
published; `/metrics` is restricted to private ranges.

### Known gaps

- No migration tool. The schema is created from ORM metadata on boot
  (`AUTO_CREATE_TABLES=true`), in production too.
- The compose stack uses the `postgis/postgis` image but no PostGIS types are
  used — clustering is Haversine in Python. Plain `postgres` would do.
- Video frames are sampled at a fixed stride. There is no motion or
  bump-signal gating, so a clip of smooth road still costs
  `VIDEO_MAX_FRAMES × len(ROBOFLOW_MODEL_IDS)` inference calls.
- Clustering is purely spatial (label-agnostic): a crack and a pothole at the
  same spot merge into one canonical marker; both labels survive in its
  observation history.
