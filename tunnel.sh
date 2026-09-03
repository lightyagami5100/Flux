#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$DIR/bin/cloudflared"

if [ ! -x "$BIN" ]; then
    echo "Downloading cloudflared..."
    mkdir -p "$DIR/bin"
    curl -sL "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64" -o "$BIN"
    chmod +x "$BIN"
fi

echo "============================================================"
echo "      🌐 FLUX WORLDWIDE PUBLIC TUNNEL (CLOUDFLARE)          "
echo "============================================================"
echo "Forwarding to http://localhost:8000 ..."
echo "Your phone can connect from 4G/5G mobile data or ANY Wi-Fi!"
echo "Look for the *.trycloudflare.com URL below:"
echo "============================================================"
exec "$BIN" tunnel --url http://localhost:8000
