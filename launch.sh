#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# Flux — One-command launcher for Backend + Dashboard + Mobile
# ─────────────────────────────────────────────────────────────
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

export ENVIRONMENT="${ENVIRONMENT:-development}"

# ── Colours ──
R='\033[0;31m'; G='\033[0;32m'; Y='\033[1;33m'; C='\033[0;36m'; B='\033[1m'; N='\033[0m'

banner() {
  echo ""
  echo -e "${B}${C}══════════════════════════════════════════════════════════════${N}"
  echo -e "${B}${C}          🚀  FLUX ROAD INTELLIGENCE PLATFORM  🚀           ${N}"
  echo -e "${B}${C}══════════════════════════════════════════════════════════════${N}"
  echo ""
}

cleanup() {
  echo ""
  echo -e "${Y}Shutting down all services...${N}"
  [ -n "$UVICORN_PID" ] && kill "$UVICORN_PID" 2>/dev/null && echo -e "  ${G}✓${N} Backend stopped"
  [ -n "$EXPO_PID" ]    && kill "$EXPO_PID"    2>/dev/null && echo -e "  ${G}✓${N} Mobile dev server stopped"
  if [ -n "$DOCKER_STARTED" ]; then
    docker compose -f infra/docker-compose.yml down --remove-orphans 2>/dev/null || true
    echo -e "  ${G}✓${N} Docker containers stopped"
  fi
  exit 0
}
trap cleanup SIGINT SIGTERM

# ── Preflight ──
if [ ! -x "$DIR/.venv/bin/python" ]; then
  echo -e "${R}❌ .venv not found.${N} Create it first:"
  echo "   python -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt"
  exit 1
fi

banner

# ── 1. Detect LAN IP ──
IP_ADDR=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7}' || hostname -I 2>/dev/null | awk '{print $1}' || echo "127.0.0.1")
echo -e "${G}🌐  LAN IP:${N}  $IP_ADDR"

# ── 2. Configure mobile .env ──
cat <<EOF > "$DIR/mobile/.env"
EXPO_PUBLIC_API_URL=http://$IP_ADDR:8000/v1/ingest/upload
EXPO_PUBLIC_API_BASE=http://$IP_ADDR:8000
EOF
echo -e "${G}📱  Mobile .env updated${N} → http://$IP_ADDR:8000"

# ── 3. Docker infrastructure (Postgres, Redis, MinIO, Worker) ──
COMPOSE_ARGS=(-f infra/docker-compose.yml)
[ -f "$DIR/.env" ] && COMPOSE_ARGS=(--env-file "$DIR/.env" "${COMPOSE_ARGS[@]}")

if command -v docker >/dev/null 2>&1; then
  echo ""
  echo -e "${B}🐳  Starting infrastructure containers...${N}"
  echo -e "    Postgres · Redis · MinIO · Detection Worker"
  echo ""
  if docker compose "${COMPOSE_ARGS[@]}" up -d db redis minio worker 2>/dev/null || \
     sudo -n docker compose "${COMPOSE_ARGS[@]}" up -d db redis minio worker 2>/dev/null; then
    DOCKER_STARTED=1
    echo -e "${G}  ✓ All containers running${N}"
  else
    echo -e "${Y}  ⚠ Could not start containers.${N}"
    echo -e "    The dashboard runs with seed data only; phone uploads won't be processed."
  fi
else
  echo -e "${Y}⚠  Docker not available.${N} Dashboard runs with seed data only."
fi

# ── 4. Background seeder ──
(
  sleep 3
  curl -s -X POST http://127.0.0.1:8000/seed >/dev/null 2>&1 || true
)&

# ── 5. Start FastAPI backend (foreground-process, backgrounded) ──
echo ""
echo -e "${B}🔧  Starting FastAPI backend on :8000...${N}"
"$DIR/.venv/bin/python" -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload &
UVICORN_PID=$!
echo -e "${G}  ✓ Backend PID $UVICORN_PID${N}"

# ── 6. Install mobile deps if needed, then start Expo ──
if [ -d "$DIR/mobile" ] && [ "$SKIP_MOBILE" != "1" ]; then
  echo ""
  echo -e "${B}📱  Starting mobile dev server...${N}"
  cd "$DIR/mobile"

  if [ ! -d "node_modules" ]; then
    echo -e "    Installing npm dependencies (first run)..."
    npm install --silent 2>/dev/null || echo -e "${Y}    npm install had warnings (continuing)${N}"
  fi

  npx expo start --port 8081 --lan &
  EXPO_PID=$!
  cd "$DIR"
  echo -e "${G}  ✓ Expo dev server PID $EXPO_PID${N}"
fi

# ── 7. Wait for backend to be ready, then open dashboard ──
(
  for i in $(seq 1 20); do
    sleep 0.5
    if curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8000/healthz 2>/dev/null | grep -q 200; then
      if command -v xdg-open >/dev/null 2>&1; then
        xdg-open "http://localhost:8000/" 2>/dev/null &
      elif command -v open >/dev/null 2>&1; then
        open "http://localhost:8000/" 2>/dev/null &
      fi
      break
    fi
  done
)&

# ── 8. Summary ──
sleep 1
echo ""
echo -e "${B}${C}══════════════════════════════════════════════════════════════${N}"
echo -e "${B}  ✨  ALL SERVICES RUNNING${N}"
echo -e "${B}${C}══════════════════════════════════════════════════════════════${N}"
echo ""
echo -e "  ${G}📍${N} ${B}Dashboard (Map)${N}     http://localhost:8000/"
echo -e "  ${G}📍${N} ${B}Dashboard (LAN)${N}     http://$IP_ADDR:8000/"
echo -e "  ${G}📡${N} ${B}Live Radar SSE${N}      http://localhost:8000/api/stream/events"
echo -e "  ${G}📊${N} ${B}Prometheus${N}          http://localhost:8000/metrics"
echo ""
if [ -n "$EXPO_PID" ]; then
echo -e "  ${G}📱${N} ${B}Mobile App${N}          Scan the QR code below with Expo Go"
echo -e "                         (phone must be on the same Wi-Fi network)"
echo ""
fi
echo -e "  ${Y}Press Ctrl+C to stop everything${N}"
echo ""
echo -e "${B}${C}══════════════════════════════════════════════════════════════${N}"
echo ""

# ── 9. Keep alive ──
wait $UVICORN_PID 2>/dev/null
