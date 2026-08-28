#!/usr/bin/env bash
# Runs the root Flux launcher from inside the mobile directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec bash "$DIR/start.sh" "$@"
