#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# Flux — Clean Shutdown Script: Reclaims Memory and Stops Services
# ─────────────────────────────────────────────────────────────

echo "🛑 Shutting down all Flux services and reclaiming memory..."

# 1. Terminate uvicorn and child worker processes
pkill -9 -f "uvicorn app.main:app" 2>/dev/null || true
pkill -9 -f "spawn_main.*multiprocessing" 2>/dev/null || true

# 2. Terminate Expo / Metro packagers if running
pkill -9 -f "expo start" 2>/dev/null || true

# 3. Stop Docker containers if docker is running
if command -v docker >/dev/null 2>&1; then
    docker compose -f infra/docker-compose.yml down 2>/dev/null || true
fi

echo "✅ All Flux processes stopped. RAM and CPU fully restored!"
