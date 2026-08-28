#!/bin/bash
set -e

# Production deployment script for Alibaba Cloud ECS (or local docker setup)

echo "🔄 Pulling latest changes (if any)..."
# git pull origin main

echo "🔧 Checking environment variables..."
if [ ! -f .env ]; then
    echo "⚠️  .env file not found. Creating from .env.example..."
    cp .env.example .env
    echo "❌ Please edit .env with your actual API keys and run deploy.sh again."
    exit 1
fi

# Load variables to pass them to docker-compose
export $(grep -v '^#' .env | xargs)

echo "🚀 Building and starting production containers..."
cd infra
docker-compose -f docker-compose.prod.yml up -d --build

echo "✅ Deployment successful!"
echo "📍 API and Map are now available at http://localhost (or your server's public IP)"
echo "Use 'docker-compose -f infra/docker-compose.prod.yml logs -f' to view logs."
