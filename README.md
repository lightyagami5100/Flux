<div align="center">

<img src="static/flux-logo.png" alt="Flux" width="120" />

# Flux — Road Intelligence Platform

**Turn dashcam video into a live, deduplicated map of road damage.**

Patrol phones record the road. A worker samples frames and calls an external CV API.
Detections are clustered by geography into canonical potholes and streamed to a live
Leaflet dashboard.

[![CI](https://github.com/lightyagami5100/Flux/actions/workflows/ci.yml/badge.svg)](https://github.com/lightyagami5100/Flux/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11+-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-async-009688?logo=fastapi&logoColor=white)
![Expo](https://img.shields.io/badge/Expo-SDK%2054-000020?logo=expo&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-compose-2496ED?logo=docker&logoColor=white)

</div>

---

## What it does

A city has thousands of kilometres of road and no cheap way to know which parts are
broken. Flux makes any phone on a windscreen mount into a survey device.

1. **Capture** — the Expo app in `mobile/` records video and fires bump-triggered
   snapshots, each geo-tagged from GPS.
2. **Upload** — 5 MB chunked uploads survive a flaky mobile connection; sessions
   resume instead of restarting.
3. **Infer** — a worker pulls from a Redis Stream, subsamples the clip, and calls
   Roboflow. Multiple models run per frame and their detections merge.
4. **Deduplicate** — the same pothole seen on Monday and Thursday is *one* map
   marker with two observations, not two markers. Haversine clustering, 10 m radius.
5. **Act** — the dashboard shows severity, confidence, observation history, and a
   repair lifecycle (`reported → acknowledged → in_progress → repaired`).

> **Inference is remote only.** No local weights, no local GPU, no `torch` in the
> image. This is an architectural constraint, not a preference — see
> [`.qoder/rules/Flux-sys.md`](.qoder/rules/Flux-sys.md).

---

## Architecture

```
   ┌─────────────────┐
   │  Flux Patrol    │  Expo / React Native
   │  (iOS·Android)  │  camera · GPS · accelerometer
   └────────┬────────┘
            │  chunked upload (5 MB)
            ▼
   ┌─────────────────┐        ┌──────────────┐
   │  FastAPI  :8000 │───────▶│    MinIO     │  raw video + frames
   │  ingest · query │        └──────┬───────┘
   └────────┬────────┘               │
            │  XADD                  │ presigned GET
            ▼                        ▼
   ┌─────────────────┐        ┌──────────────┐
   │  Redis Stream   │───────▶│    Worker    │
   │ stream:detections│  XREAD │ frame sample │
   └─────────────────┘        └──────┬───────┘
                                     │ HTTPS
                                     ▼
                              ┌──────────────┐
                              │  Roboflow    │  external inference
                              └──────┬───────┘
                                     │ detections
                                     ▼
   ┌─────────────────┐        ┌──────────────┐
   │ Leaflet Dashboard│◀──────│  PostgreSQL  │  canonical potholes
   │  SSE live radar │  SSE   │  + events    │  ← Haversine clustering
   └─────────────────┘        └──────────────┘
```

| Path | What lives there |
| --- | --- |
| `app/main.py` | FastAPI app: ingest, chunked upload, query endpoints, SSE, SVG snapshots |
| `app/worker.py` | Redis Stream consumer → media fetch → processor → persist |
| `app/processors/` | Pluggable inference backends. `roboflow.py` is the only built-in |
| `app/deduplication.py` | Haversine clustering of raw events into canonical potholes |
| `app/upload_manager.py` | Chunked upload sessions, stale-session reaping |
| `app/config.py` | Every setting, env-driven, plus the production-readiness guard |
| `app/models.py` | SQLAlchemy 2.x async ORM |
| `static/index.html` | Leaflet dashboard (single file, no build step) |
| `mobile/` | Expo patrol app — [own AGENTS.md](mobile/AGENTS.md) |
| `infra/` | Compose stacks + nginx reverse proxy |
| `tests/` | pytest suite, 81 tests, all external deps stubbed |

---

## Quick start

**One command starts everything** — infrastructure, backend, mobile dev server, and
it opens the dashboard in your browser:

```bash
# first time only
python -m venv .venv
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
cp .env.example .env          # fill in ROBOFLOW_API_KEY + API_KEYS

# every time after
./launch.sh
```

`launch.sh` will:

- detect your LAN IP and write `mobile/.env` so the phone can reach the API
- bring up Postgres, Redis, MinIO **and the detection worker** in Docker
- start uvicorn with `--reload` on `0.0.0.0:8000`
- `npm install` the mobile app if `node_modules` is missing, then start Expo on `:8081`
- seed the map with demo data and open <http://localhost:8000/>
- tear all of it down on <kbd>Ctrl</kbd>+<kbd>C</kbd>

| | |
| --- | --- |
| Map dashboard | <http://localhost:8000/> |
| Live event stream (SSE) | <http://localhost:8000/api/stream/events> |
| Prometheus metrics | <http://localhost:8000/metrics> |
| MinIO console | <http://localhost:9001> |
| API docs (Swagger) | <http://localhost:8000/docs> |

**Backend only**, skipping the mobile dev server:

```bash
SKIP_MOBILE=1 ./launch.sh
```

`./start.sh` (and its alias `./run.sh`) still exist and do the backend half only —
no Expo, no browser, no teardown. `launch.sh` supersedes them for normal use.

---

## The mobile app

The patrol app is **Flux Patrol** (`com.flux.patrol`). Two modes:

- **Smart Patrol** — leave the phone mounted. The accelerometer detects a bump and
  captures a geo-tagged frame at that instant.
- **Video Patrol** — record a continuous clip; it uploads in 5 MB chunks and the
  worker samples frames from it.

### Run it in Expo Go (no build needed)

Fastest path for development — this is what `./launch.sh` sets up for you.

1. Install **Expo Go** from the App Store / Play Store.
2. Phone and laptop on the **same Wi-Fi**.
3. `./launch.sh`, then scan the QR code with Expo Go.
4. The phone needs **two** ports open on the laptop: `8081` (JS bundle) and `8000`
   (API). On EndeavourOS / Fedora:
   ```bash
   sudo firewall-cmd --add-port=8000/tcp --add-port=8081/tcp
   ```
5. Grant camera + location when prompted. Expect 10–30 s from capture to map marker.

### Build real installable apps

Native builds go through [Expo EAS Build](https://docs.expo.dev/build/introduction/) —
it compiles in the cloud, so no Android Studio or Xcode is needed locally (and no
macOS requirement for iOS).

```bash
cd mobile

# one-time
npx eas login
npx eas init          # registers the project, fills in extra.eas.projectId
```

Then:

| Command | Output | Needs |
| --- | --- | --- |
| `npm run build:android` | **APK** — sideload directly onto any device | Free Expo account |
| `npm run build:ios` | **IPA** — internal distribution | Apple Developer Program |
| `npm run build:all` | Both of the above | Both |
| `npm run build:android:prod` | **AAB** for Google Play | Play Console account |
| `npm run build:ios:prod` | **IPA** for the App Store | Apple Developer Program |
| `npm run submit:android` / `submit:ios` | Uploads to the store | Store credentials |

Build profiles live in [`mobile/eas.json`](mobile/eas.json):

| Profile | Distribution | Android artifact | API target |
| --- | --- | --- | --- |
| `development` | internal, dev client | APK | your LAN IP |
| `preview` | internal | APK | your LAN IP |
| `production` | store | AAB | `EXPO_PUBLIC_API_BASE` from the profile |

> **Before a production build**, edit `mobile/eas.json`: replace the
> `your-production-domain.com` placeholders with your real host, and fill in
> `appleId` / `ascAppId` / `appleTeamId` under `submit.production.ios`. The
> `preview` profile works immediately with no edits.

The APK from `preview` is the easiest thing to hand to a tester — one file, no
store, no provisioning profile.

---

## Video handling and multi-model inference

A clip is subsampled, never decoded whole, because inference is billed per frame:

| Variable | Default | Effect |
| --- | --- | --- |
| `VIDEO_SAMPLE_EVERY_N_FRAMES` | `15` | ~2 frames/sec on 30 fps footage |
| `VIDEO_MAX_FRAMES` | `20` | Hard ceiling on sampled frames per clip |

`ROBOFLOW_MODEL_IDS` takes a comma-separated list of `project/version` ids. Every
model runs on every sampled frame and detections merge, each keeping its own class
name — so one deployment detects potholes, cracks, and debris at once.

> **calls per clip = sampled frames × number of models** (default 20 × 2 = 40)

Trim the bill by lowering `VIDEO_MAX_FRAMES` or running fewer models. Every
detection carries `frame_index` and `timestamp_ms`, so a hit traces back to its
exact moment in the clip.

Two public Roboflow models are live-verified against this pipeline:

| Model id | Classes | mAP@50 |
| --- | --- | --- |
| `pothole-detection-03iso/1` | pothole | 0.556 |
| `damage-road/1` | pothole, crack | 0.719 |

---

## API

Ingest requires the `X-API-Key` header, matched against the `API_KEYS` JSON map.

| Method | Route | Purpose |
| --- | --- | --- |
| `POST` | `/v1/ingest` | Ingest a single detection event (idempotent) |
| `POST` | `/v1/ingest/upload` | One-shot media upload + ingest |
| `POST` | `/api/uploads` | Open a chunked upload session |
| `PUT` | `/api/uploads/{id}/chunks/{n}` | Upload one chunk |
| `POST` | `/api/uploads/{id}/complete` | Finalise → queue for inference |
| `DELETE` | `/api/uploads/{id}` | Cancel a session |
| `GET` | `/detections` | Query raw observations as GeoJSON (bbox-filtered) |
| `GET` | `/detections/{id}` · `/media` | Observation detail · snapshot |
| `GET` | `/detections/export/geojson` | Full GeoJSON export |
| `GET` | `/potholes` | Deduplicated canonical potholes |
| `GET` | `/potholes/{id}` · `/media` | Detail + observation timeseries · photo |
| `PATCH` | `/potholes/{id}/status` | Advance the repair lifecycle |
| `POST` | `/api/deduplicate/rebuild` | Re-cluster every event from scratch |
| `GET` | `/api/stream/events` | SSE live radar |
| `GET` | `/stats` · `/notifications` | Dashboard aggregates |
| `GET` | `/healthz` · `/livez` · `/readyz` | Liveness · liveness · deep readiness |
| `GET` | `/metrics` | Prometheus exposition |

Interactive docs at `/docs` when the server is running.

---

## Configuration

Every setting is an environment variable; [`.env.example`](.env.example) is the
complete list. The ones with no safe default:

| Variable | Why |
| --- | --- |
| `API_KEYS` | JSON map `api_key -> device_id`. Empty means every ingest returns 401 |
| `ROBOFLOW_API_KEY` | External inference credential |
| `ROBOFLOW_MODEL_IDS` | Comma-separated model list. The built-in default `coco/3` has **no road-damage classes** and is rejected in production |
| `DATABASE_URL` | Postgres DSN |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | Object storage credentials |

When `ENVIRONMENT` is **not** one of `development` / `dev` / `test` / `local`,
`Settings.missing_production_settings()` runs at startup and the app **refuses to
boot** if any of the above still holds a development default. The production
compose stack enforces the same contract with `${VAR:?...}`, so a missing value
fails before a container even starts.

Secrets go in `.env` (gitignored) or the deploy environment. Never committed, never
written into `STATE.yaml`.

---

## Graceful degradation

`ENVIRONMENT=development` keeps two local fallbacks alive so the stack runs with
nothing else installed:

| Dependency missing | Fallback |
| --- | --- |
| Redis | in-process `fakeredis` |
| PostgreSQL | `sqlite+aiosqlite:///flux_dev.db` |

Both are **disabled** outside development — production refuses to boot on a
degraded backing store rather than silently losing data.

`inference-sdk` has no wheels for Python ≥ 3.13. On a newer interpreter the
Roboflow processor raises `ProcessorUnavailable` with an explanation instead of
crashing on import; everything except live inference still works.

**What is deliberately not degraded:** media that cannot be fetched or decoded
never becomes a synthetic frame. It raises — the message keeps its place in the
retry budget, or goes straight to the DLQ if the input is permanently broken, and
the row persists as `FAILED`. A fabricated blank frame would be billed by the
inference API and then stored as a successful "no potholes here" reading. That is
worse than an error.

---

## Tests and lint

```bash
.venv/bin/pytest -q
.venv/bin/ruff check app tests
```

Both gates run in CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)),
which also builds the Docker image, validates both compose files, and rejects a
committed `.env` or committed model weights.

`tests/conftest.py` installs `sys.modules` stubs for `cv2` and `inference_sdk` and
force-assigns a fake `ROBOFLOW_API_KEY` — the suite is green without a `.env` and
can never bill the real account.

---

## Deployment

Target: any Docker host (Alibaba Cloud ECS in our case).

```bash
cp .env.example .env      # fill in real production values
./deploy.sh
```

`deploy.sh` runs a preflight (required vars present, no `minioadmin` defaults,
compose validates), records the running image, builds, starts, then polls
`/healthz` for up to `HEALTH_TIMEOUT` seconds. If the health gate never passes it
dumps the last 80 log lines and **rolls back** to the previous image.

nginx terminates `:80`, serves `static/`, and proxies the API routes. Only nginx is
published; `/metrics` is restricted to private ranges.

> ⚠️ The nginx route regex in `infra/nginx.conf` must stay in sync with the route
> prefixes in `app/main.py`. A new top-level prefix that isn't listed there gets
> served as a static file and 404s in production.

---

## Known gaps

Honest list, not a roadmap.

- **No migration tool.** The schema is created from ORM metadata on boot
  (`AUTO_CREATE_TABLES=true`), in production too.
- **Upload endpoints are unauthenticated.** `/v1/ingest/upload` and `/api/uploads`
  accept requests without `X-API-Key` so the mobile app works as-is. Tighten this
  before any public deployment.
- **`postgis/postgis` image is unnecessary.** Clustering is Haversine in Python; no
  PostGIS types are used. Plain `postgres` would do.
- **Fixed-stride frame sampling.** No motion or bump-signal gating, so a clip of
  smooth road still costs `VIDEO_MAX_FRAMES × len(ROBOFLOW_MODEL_IDS)` calls.
- **Clustering is label-agnostic.** A crack and a pothole at the same spot merge
  into one canonical marker; both labels survive in the observation history.
- **`app/database.py` is dead code** — a duplicate `Base` + models that nothing
  imports. Scheduled for deletion, don't add to it.

---

<div align="center">

Built for a hackathon. Kept honest by `pytest`, `ruff`, and
[`STATE.yaml`](STATE.yaml).

</div>
