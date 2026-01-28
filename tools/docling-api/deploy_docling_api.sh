#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${1:-5010}"
IMAGE_NAME="docling-api"
CONTAINER_NAME="docling-api"

cd "$SCRIPT_DIR"

echo "Building Docker image: $IMAGE_NAME"
docker build -t "$IMAGE_NAME" .

echo "Stopping existing container (if any)"
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

echo "Starting container on port $PORT"
docker run -d --name "$CONTAINER_NAME" -p "$PORT":5010 "$IMAGE_NAME"

echo "Docling API running at http://<server-ip>:$PORT/extract-generic"
