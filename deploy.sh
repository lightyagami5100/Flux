#!/usr/bin/env bash
# Production deployment for Alibaba Cloud ECS (or any Docker host).
# Preflight -> build -> start -> health gate -> rollback on failure.
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${REPO_ROOT}/infra/docker-compose.prod.yml"
HEALTH_URL="${HEALTH_URL:-http://localhost/healthz}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-90}"

# Prefer the v2 plugin, fall back to the standalone binary.
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "FATAL: neither 'docker compose' nor 'docker-compose' is available." >&2
  exit 1
fi
COMPOSE+=(--env-file "${REPO_ROOT}/.env" -f "${COMPOSE_FILE}")

log() { printf '==> %s\n' "$*"; }
fail() { printf 'FATAL: %s\n' "$*" >&2; exit 1; }

# --- Preflight ---------------------------------------------------------------
log "Preflight"

[[ -f "${REPO_ROOT}/.env" ]] || fail ".env not found. Copy .env.example to .env and fill in real values."

# Export .env without mangling values that contain spaces or JSON.
set -a
# shellcheck disable=SC1091
source "${REPO_ROOT}/.env"
set +a

REQUIRED=(POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB MINIO_ACCESS_KEY MINIO_SECRET_KEY API_KEYS ROBOFLOW_API_KEY ROBOFLOW_MODEL_IDS CORS_ORIGINS)
MISSING=()
for var in "${REQUIRED[@]}"; do
  [[ -n "${!var:-}" ]] || MISSING+=("${var}")
done
if ((${#MISSING[@]})); then
  fail "these variables are empty in .env: ${MISSING[*]}"
fi

if [[ "${CORS_ORIGINS}" == *"*"* ]]; then
  fail "CORS_ORIGINS contains wildcard '*'. Production requires explicit allowed origin(s)."
fi

if [[ "${MINIO_ACCESS_KEY}" == "minioadmin" || "${MINIO_SECRET_KEY}" == "minioadmin" ]]; then
  fail "MINIO_ACCESS_KEY/MINIO_SECRET_KEY are still the default 'minioadmin'."
fi

if [[ "${ROBOFLOW_MODEL_IDS}" == *"coco/3"* ]]; then
  fail "ROBOFLOW_MODEL_IDS contains coco/3, which has no road-damage classes. Point it at real models."
fi

# Validate the rendered compose file before touching running containers.
"${COMPOSE[@]}" config >/dev/null || fail "compose file did not validate."

# --- Record the current state so we can roll back ----------------------------
PREV_CONTAINER="$("${COMPOSE[@]}" ps -q api 2>/dev/null | head -n1 || true)"
PREV_IMAGE_ID=""
PREV_IMAGE_TAG=""
if [[ -n "${PREV_CONTAINER}" ]]; then
  PREV_IMAGE_ID="$(docker inspect --format '{{.Image}}' "${PREV_CONTAINER}")"
  PREV_IMAGE_TAG="$(docker inspect --format '{{.Config.Image}}' "${PREV_CONTAINER}")"
  log "Rollback target: ${PREV_IMAGE_TAG} (${PREV_IMAGE_ID:0:19})"
else
  log "No running api container: this is a first deploy (no rollback target)."
fi

rollback() {
  if [[ -z "${PREV_IMAGE_ID}" || -z "${PREV_IMAGE_TAG}" ]]; then
    log "Nothing to roll back to. Stopping the stack so it does not serve broken traffic."
    "${COMPOSE[@]}" down || true
    return
  fi
  log "Rolling back to ${PREV_IMAGE_TAG} (${PREV_IMAGE_ID:0:19})"
  # The new build took over the tag; point it back at the previous image id.
  docker tag "${PREV_IMAGE_ID}" "${PREV_IMAGE_TAG}" || {
    log "Could not re-tag the previous image. Inspect 'docker images' manually."
    return
  }
  "${COMPOSE[@]}" up -d --no-build api worker || \
    log "Rollback command failed; inspect '${COMPOSE[*]} logs' manually."
}

# --- Build and start ---------------------------------------------------------
log "Building images"
"${COMPOSE[@]}" build

log "Starting stack"
"${COMPOSE[@]}" up -d --remove-orphans

# --- Health gate -------------------------------------------------------------
log "Waiting up to ${HEALTH_TIMEOUT}s for ${HEALTH_URL}"
deadline=$(( SECONDS + HEALTH_TIMEOUT ))
healthy=0
while (( SECONDS < deadline )); do
  if curl -fsS --max-time 5 "${HEALTH_URL}" >/dev/null 2>&1; then
    healthy=1
    break
  fi
  sleep 3
done

if (( healthy == 0 )); then
  log "Health check never passed. Last 80 log lines:"
  "${COMPOSE[@]}" logs --tail=80 api worker || true
  rollback
  exit 1
fi

log "Healthy. Deployment complete."
log "Map/API: http://<server-ip>/   Logs: ${COMPOSE[*]} logs -f"
