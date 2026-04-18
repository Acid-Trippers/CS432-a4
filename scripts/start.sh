#!/usr/bin/env bash
set -euo pipefail

INITIALISE_COUNT="${1:-0}"

echo "[1/4] Checking Docker availability..."
docker --version >/dev/null
docker-compose version >/dev/null

echo "[2/4] Starting containers..."
docker-compose up -d

echo "[3/4] Verifying service status..."
docker-compose ps

if [ "$INITIALISE_COUNT" -gt 0 ] 2>/dev/null; then
  echo "[4/4] Triggering pipeline initialise with count=$INITIALISE_COUNT ..."
  URL="http://127.0.0.1:8080/api/pipeline/initialise?count=${INITIALISE_COUNT}"
  if command -v curl >/dev/null 2>&1; then
    if ! curl -sS -X POST "$URL" >/dev/null; then
      echo "Dashboard may not be running yet. Start it with: python dashboard/run.py"
      echo "Then rerun initialise manually using: POST $URL"
    fi
  else
    echo "curl not found. Start dashboard and call: POST $URL"
  fi
fi

echo "Done. Start dashboard with: python dashboard/run.py"
echo "Open: http://127.0.0.1:8080/"
