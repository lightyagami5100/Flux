<div align="center">

<img src="static/flux-logo.png" alt="Flux" width="120" />

# Flux — Road Intelligence Platform

**Turn municipal dashcam video & smartphone telemetry into a live, deduplicated map of road damage.**

Patrol phones record road conditions and detect surface impacts. An asynchronous worker pipeline samples frames and queries cloud computer vision APIs. Detections are clustered by geography into canonical hazards and streamed to a real-time command dashboard.

[![CI](https://github.com/lightyagami5100/Flux/actions/workflows/ci.yml/badge.svg)](https://github.com/lightyagami5100/Flux/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11+-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-async-009688?logo=fastapi&logoColor=white)
![Expo](https://img.shields.io/badge/Expo-SDK%2054-000020?logo=expo&logoColor=white)
![Redis Streams](https://img.shields.io/badge/Redis-Streams%20%26%20PubSub-DC382D?logo=redis&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-PostGIS-4169E1?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-compose-2496ED?logo=docker&logoColor=white)
![Tests](https://img.shields.io/badge/Tests-98%20passed-success?logo=pytest&logoColor=white)

</div>

---

## What it does

Cities contain thousands of kilometers of roadway with no continuous, cost-effective way to monitor surface decay. Flux converts standard smartphones mounted to municipal or volunteer vehicle windshields into automated surveying stations:

1. **Edge Telemetry & Sensor Capture** — The Expo app (`mobile/`) samples the accelerometer at **10 Hz (100 ms intervals)**. G-force impacts exceeding **2.0g** trigger instant geo-tagged camera captures (with a 3-second debounce). In video mode, it records continuous clips for chunked background transfer.
2. **Resilient Chunked Ingest** — 5 MB binary slices survive intermittent 4G/5G connections; upload sessions resume seamlessly across network drops without restarting.
3. **Remote Perception Engine** — An asynchronous worker pulls detection envelopes from a Redis Stream, subsamples video frames (configurable frame stride and ceilings), and calls external computer vision APIs (Roboflow Universe). Multi-model ensembles detect potholes, road cracks, and obstacles in parallel.
4. **Spatial Deduplication & Centroid Math** — The same defect observed across multiple passes is clustered into a single **Canonical Pothole** entity within a **10-meter Haversine Great-Circle radius**. GPS centroids dynamically converge via weighted moving average, and severity escalates monotonically.
5. **Real-Time Surveillance & Municipal Dispatch** — Detections broadcast instantly via Redis Pub/Sub to a Server-Sent Events (SSE) stream. The visionOS-styled dashboard updates in real time with animated radar pulses, severity-ranked toasts, multi-pass defect timelines, and full RFC 7946 GeoJSON export for municipal GIS systems (ArcGIS, QGIS).

> **Architectural Constraint:** CV inference is strictly remote-only. No local model weights, no local GPU dependencies, and no `torch` or `ultralytics` in the application container. See [`.qoder/rules/Flux-sys.md`](.qoder/rules/Flux-sys.md).

---

## End-to-End Pipeline Architecture

```
   ┌──────────────────────┐
   │  Flux Patrol (Expo)  │  iOS · Android · Web (SDK 54)
   │  10 Hz Accel · GPS   │  2.0g impact trigger · Camera Viewfinder
   └──────────┬───────────┘
              │  POST /v1/ingest/upload (one-shot) or PUT /api/uploads/{id}/chunks (5 MB)
              ▼
   ┌──────────────────────┐        ┌──────────────┐
   │  FastAPI  :8000      │───────▶│    MinIO     │  media store (S3 API)
   │  Gateway & Ingest    │        └──────┬───────┘  (local disk fallback in dev)
   └──────────┬───────────┘               │
              │  XADD (at-least-once)     │ presigned / local stream
              ▼                           ▼
   ┌──────────────────────┐        ┌──────────────┐
   │  Redis Stream        │───────▶│  CV Worker   │  async worker (XREADGROUP)
   │  flux:stream:ingest  │        │  Subsampling │  XAUTOCLAIM dead consumer recovery
   └──────────────────────┘        └──────┬───────┘
                                          │ HTTPS (tenacity retry + circuit breaker)
                                          ▼
                                   ┌──────────────┐
                                   │   Roboflow   │  external multi-model perception
                                   │  Universe    │  pothole-detection-03iso/1 & damage-road/1
                                   └──────┬───────┘
                                          │ structured detections
                                          ▼
   ┌──────────────────────┐        ┌──────────────┐
   │  Leaflet Command HUD │◀───────│  PostgreSQL  │  spatial deduplication & clustering
   │  Apple Liquid Glass  │  SSE   │  + PostGIS   │  10m Haversine centroid convergence
   │  Pub/Sub Live Radar  │        └──────────────┘  monotonic severity escalation
   └──────────────────────┘
```

---

## Repository Map

| Path | Purpose & Key Components |
|---|---|
| [`app/main.py`](app/main.py) | FastAPI application: media ingest, chunked upload manager, GeoJSON endpoints, SSE live radar stream, photorealistic SVG rendering |
| [`app/worker.py`](app/worker.py) | Consumer-group stream worker: `XREADGROUP`, at-least-once ACK, `XAUTOCLAIM` stale-consumer reclaim, DLQ routing |
| [`app/deduplication.py`](app/deduplication.py) | Spatial engine: Haversine distance calculations, 10m clustering, weighted centroid convergence, batch reclustering |
| [`app/severity.py`](app/severity.py) | Unified bounding-box area-to-severity formulas, rank-based monotonic escalation (`Low` $\rightarrow$ `Medium` $\rightarrow$ `High` $\rightarrow$ `Critical`) |
| [`app/processors/`](app/processors/) | Pluggable perception engine: `BaseProcessor` abstract contract, `RoboflowProcessor` with circuit breaker and REST fallback |
| [`app/upload_manager.py`](app/upload_manager.py) | Resilient chunked uploads: byte-offset assembly, disk/MinIO storage, stale session reaper |
| [`app/visualization.py`](app/visualization.py) | Dynamic SVG dashcam snapshot generator with HUD targeting brackets and anomaly graphics |
| [`app/seed.py`](app/seed.py) | Realistic multi-city road defect dataset across Islamabad, Rawalpindi, Lahore, Karachi, Peshawar, Multan, Quetta |
| [`app/config.py`](app/config.py) | Typed Pydantic configuration with strict startup production guards (`missing_production_settings`) |
| [`app/models.py`](app/models.py) | SQLAlchemy 2.0 async ORM: `CanonicalPothole` and `DetectionEvent` tables with spatial composite indexes |
| [`static/index.html`](static/index.html) | High-performance Leaflet command center: visionOS Liquid Quartz & Obsidian Pro dark theme, Defect Profile Drawer, Live AI Ingestion Lab |
| [`mobile/`](mobile/) | React Native / Expo patrol client: Smart Patrol (accelerometer-triggered) and Video Patrol (chunked transfer) |
| [`infra/`](infra/) | Docker Compose topologies (dev and production) + hardened Nginx reverse proxy |
| [`tests/`](tests/) | Comprehensive pytest suite (98 tests), fully mocked external APIs, zero cost to test |

---

## Quick Start

### 1. One-Command Launch (Recommended)

The automated launcher configures environment variables, boots required infrastructure containers, launches the FastAPI backend, starts the Expo mobile development server, and opens the dashboard:

```bash
# Setup virtual environment (first time only)
python -m venv .venv
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
cp .env.example .env          # provide ROBOFLOW_API_KEY if testing live external perception

# Launch entire platform
./launch.sh
```

`launch.sh` will:
- Detect your LAN IP and write `mobile/.env` so client devices on Wi-Fi reach the API automatically.
- Launch Postgres, Redis, MinIO, and the background detection worker via Docker (with embedded SQLite and in-memory fakeredis fallbacks if Docker is absent).
- Launch Uvicorn with hot-reloading on `0.0.0.0:8000`.
- Initialize the Expo development server on port `8081`.
- Seed the spatial engine with realistic multi-city road defect telemetry and open [`http://localhost:8000`](http://localhost:8000).
- Gracefully shut down all processes on <kbd>Ctrl</kbd>+<kbd>C</kbd>.

### Available Local Endpoints

| Service | URL | Notes |
|---|---|---|
| **Command Center (Map)** | <http://localhost:8000/> | Vision-OS Liquid Glass dashboard & live radar |
| **Live Radar SSE** | <http://localhost:8000/api/stream/events> | Real-time event streaming via Server-Sent Events |
| **Interactive API Docs** | <http://localhost:8000/docs> | OpenAPI / Swagger UI |
| **Prometheus Telemetry** | <http://localhost:8000/metrics> | System throughput, latency histograms, error counters |
| **MinIO Console** | <http://localhost:9001> | Object storage UI (`minioadmin` / `minioadmin`) |
| **Expo Dev Server** | <http://localhost:8081> | Mobile web runner and Metro bundler |

### 2. Backend-Only Mode

To run only the backend services without starting Expo:

```bash
SKIP_MOBILE=1 ./launch.sh
# or
./start.sh
```

### 3. Worldwide Public Tunnel (Testing over Cellular 4G/5G)

To connect physical phones over 4G/5G mobile data or without sharing the same local Wi-Fi:

```bash
./tunnel.sh
```
This spawns an on-demand Cloudflare Tunnel pointing directly to `localhost:8000` with a secure public `*.trycloudflare.com` URL.

---

## Live Demo & Inspection Features

### ⚡ Live Edge AI Ingestion Lab
When presenting to judges or testing without a vehicle, the dashboard includes a built-in **Live Edge AI Ingestion Lab**:
1. Click **"Live AI Test"** in the top island HUD or action dock.
2. Select an instant test scenario (e.g. *Severe Pothole — Jinnah Ave, Islamabad*, *Road Surface Crack — Srinagar Highway*) or drag-and-drop any road photo from your computer.
3. Click **"Execute End-to-End AI Ingestion"**.
4. Watch the live console execute the entire edge-to-cloud round-trip:
   - `POST /v1/ingest/upload` $\rightarrow$ Media Store
   - Envelope enqueued to Redis Stream `flux:stream:ingest`
   - Worker runs Roboflow CV perception
   - Haversine deduplication recalculates GPS centroid
   - Redis Pub/Sub fires Server-Sent Event to dashboard
5. The map automatically **flies smoothly** to the coordinates, triggers a sonar radar pulse, and opens the **Defect Profile Drawer** showing class label, severity, confidence, and multi-pass history.

### 🔄 Multi-Pass Spatial Deduplication
- **The Problem:** In a real city, multiple patrol cars or transit buses drive over the same road hazard, creating duplicate complaints.
- **The Solution:** Flux correlates detections within 10 meters into a single canonical entity.
- **Visual Demo:** Click the **"Canonical (ON)"** toggle in the top bar to toggle between aggregated canonical work orders and raw scattered detections.

---

## Perception Engine & Severity Heuristics

### Computer Vision Models
Video and still frames are processed by verified public Roboflow Universe models:

| Model ID | Target Classes | Benchmark mAP@50 |
|---|---|---|
| `pothole-detection-03iso/1` | Potholes | 0.556 |
| `damage-road/1` | Potholes, Cracks | 0.719 |

### Bounding Box Area-to-Severity Formula
Defined in [`app/severity.py`](app/severity.py):
$$\text{area} = (x_2 - x_1) \times (y_2 - y_1)$$
- **Critical:** $\text{area} > 0.50$ (takes up over 50% of the lane perspective)
- **High:** $\text{area} > 0.30$
- **Medium:** $\text{area} > 0.15$
- **Low:** $\text{area} \le 0.15$

Severity escalates monotonically across observations:
$$\text{severity}_{\text{canonical}} = \max(\text{rank}(\text{current}), \text{rank}(\text{incoming}))$$

---

## Mobile Patrol App (`mobile/`)

Built with React Native and Expo SDK 54 (`com.flux.patrol`):

- **Smart Patrol (`index.tsx`):** Real-time accelerometer listener (10 Hz). When $G = \sqrt{x^2 + y^2 + z^2} > 2.0g$, the app grabs current GPS location, triggers an instant snapshot (`takePictureAsync`), and dispatches to `/v1/ingest/upload`.
- **Video Patrol (`explore.tsx`):** High-definition video recording with post-capture 5 MB chunking and exponential backoff retry.

### Native Builds via Expo EAS

```bash
cd mobile
npx eas login
npx eas init

npm run build:android        # Generates standalone installable APK
npm run build:ios            # Generates IPA for internal distribution
```

---

## Cloud Infrastructure & Enterprise Deployment

The containerized stack is fully cloud-portable and maps directly to enterprise cloud infrastructure (e.g. Alibaba Cloud):

| Flux Component | Local / OSS Container | Alibaba Cloud Enterprise Service | Role |
|---|---|---|---|
| **API & Worker** | `Dockerfile` (Python 3.11-slim) | **ECS** or **Container Service for Kubernetes (ACK)** | Ingestion gateway, auto-scaling worker nodes |
| **Spatial Store** | `postgis/postgis:16-3.4` | **ApsaraDB RDS for PostgreSQL** (PostGIS enabled) | Relational store, spatial indexing |
| **Stream Broker** | `redis:7-alpine` | **ApsaraDB for Redis (Tair)** | Ingest streaming, consumer groups, Pub/Sub |
| **Object Store** | `minio/minio:latest` | **Object Storage Service (OSS)** (S3-compatible) | Raw dashcam video and high-res stills |
| **Ingress** | `nginx:alpine` | **Server Load Balancer (SLB)** / **ALB** | SSL termination, rate limiting, static asset cache |

### Production Deployment

```bash
cp .env.example .env         # configure production credentials
./deploy.sh                  # preflight check, builds images, zero-downtime healthcheck & rollback
```

---

## Verification & Quality Gates

The repository strictly enforces two automated quality gates before any change is accepted:

```bash
# 1. Run complete pytest test suite (95 tests, all external deps mocked)
.venv/bin/pytest -q

# 2. Run Ruff linter and import validation
.venv/bin/ruff check app tests
```

---

## Known Gaps & Roadmap

- **Database Migrations:** Schema is initialized from SQLAlchemy ORM metadata on boot (`AUTO_CREATE_TABLES=true`). Alembic migrations are planned for future versions.
- **Upload Authentication:** `/v1/ingest/upload` accepts unauthenticated requests when `REQUIRE_UPLOAD_AUTH=false` (development/hackathon mode). Production enforces `REQUIRE_UPLOAD_AUTH=true` requiring a registered `X-API-Key`.
- **Fixed-Stride Video Sampling:** Video frames are sampled at fixed intervals (`VIDEO_SAMPLE_EVERY_N_FRAMES=15`). Motion-compensated keyframe sampling is on the roadmap.

---

<div align="center">

Built with precision for smart cities and road maintenance authorities.  
Kept honest by `pytest`, `ruff`, and [`STATE.yaml`](STATE.yaml).

</div>
