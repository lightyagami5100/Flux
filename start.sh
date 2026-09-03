#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# Local runs are always development: keeps the Redis/SQLite fallbacks enabled
# and stops app/config.py from demanding production credentials.
export ENVIRONMENT="${ENVIRONMENT:-development}"

if [ ! -x "$DIR/.venv/bin/python" ]; then
    echo "❌ .venv not found. Create it first:"
    echo "   python -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt"
    exit 1
fi

echo "============================================================"
echo "           🚀 FLUX ROAD INTELLIGENCE PLATFORM               "
echo "============================================================"

# 1. Detect LAN IP for Mobile/Expo Go
IP_ADDR=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7}' || hostname -I 2>/dev/null | awk '{print $1}' || echo "127.0.0.1")
echo "🌐 Detected Local IP: $IP_ADDR"

# 2. Automatically configure Mobile .env
cat <<EOF > "$DIR/mobile/.env"
EXPO_PUBLIC_API_URL=http://$IP_ADDR:8000/v1/ingest/upload
EXPO_PUBLIC_API_BASE=http://$IP_ADDR:8000
EOF
echo "📱 Mobile configuration updated -> http://$IP_ADDR:8000"

# 3. Start Docker Containers if Docker is available
# The worker is what turns uploads into detections: without it, events queue
# in Redis forever and the map never updates.
COMPOSE_ARGS=(-f infra/docker-compose.yml)
# Compose resolves .env relative to the compose file, so point it at the repo root.
[ -f "$DIR/.env" ] && COMPOSE_ARGS=(--env-file "$DIR/.env" "${COMPOSE_ARGS[@]}")
if command -v docker >/dev/null 2>&1; then
    echo "🐳 Starting Postgres, Redis, MinIO + the detection worker..."
    echo "   (first run builds the worker image — a few minutes)"
    docker compose "${COMPOSE_ARGS[@]}" up -d db redis minio worker 2>/dev/null || \
    sudo -n docker compose "${COMPOSE_ARGS[@]}" up -d db redis minio worker 2>/dev/null || \
    echo "⚡ External Docker not detected: running embedded AI worker & storage (fully functional for mobile demo)."
else
    echo "⚡ Docker not installed: running embedded AI worker & storage (fully functional for mobile demo)."
fi

# 4. Background Seeder to populate map if empty after server starts
(
    sleep 2
    curl -s -X POST http://127.0.0.1:8000/seed >/dev/null 2>&1 || true
)&

echo "============================================================"
echo "📍 Map Dashboard : http://localhost:8000/ (or http://$IP_ADDR:8000/)"
echo "📡 Live Radar SSE : http://localhost:8000/api/stream/events"
echo "📊 Metrics & Stats: http://localhost:8000/metrics"
echo "============================================================"
echo "✨ Server is LIVE and ready for iOS Expo Go & Web Browser!"
echo "============================================================"
echo "📱 On the phone (same Wi-Fi):"
echo "   1. cd mobile && npm run start   # Expo dev server (laptop, port 8081)"
echo "   2. Scan the QR code with Expo Go"
echo "   3. Phone needs BOTH ports reachable: 8081 (app bundle) and 8000 (API)"
echo "   4. If the phone can't connect, open the firewall:"
echo "      sudo firewall-cmd --add-port=8000/tcp --add-port=8081/tcp"
echo "============================================================"

exec "$DIR/.venv/bin/python" -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
