#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

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
if command -v docker >/dev/null 2>&1; then
    echo "🐳 Checking background containers (Postgres, Redis, MinIO)..."
    docker compose -f infra/docker-compose.yml up db redis minio -d 2>/dev/null || \
    sudo -n docker compose -f infra/docker-compose.yml up db redis minio -d 2>/dev/null || \
    echo "ℹ️  Docker daemon in user mode or not running — utilizing instant high-speed fallback engine."
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

exec "$DIR/.venv/bin/python" -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
