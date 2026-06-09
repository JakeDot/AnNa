#!/usr/bin/env bash
# Smoke-tests the AnNa Docker image: builds it, runs a container, and
# exercises the core HTTP API against the live server.
#
# Usage: scripts/smoke-test.sh
set -euo pipefail

IMAGE="anna-sync-smoke-test"
CONTAINER="anna-sync-smoke-test-$$"
HOST_PORT=38080
BASE_URL="http://127.0.0.1:${HOST_PORT}"
WORKDIR=$(mktemp -d)

cleanup() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

echo "==> Building image ($IMAGE)"
docker build -t "$IMAGE" .

echo "==> Starting container"
docker run -d --name "$CONTAINER" -p "${HOST_PORT}:3000" "$IMAGE" >/dev/null

echo "==> Waiting for server to become reachable"
server_ready=false
for _ in $(seq 1 30); do
    if curl -sf "${BASE_URL}/api/status" >/dev/null; then
        server_ready=true
        break
    fi
    sleep 1
done
if [ "$server_ready" = false ]; then
    echo "FAIL: server did not become reachable within 30 seconds"
    docker logs "$CONTAINER"
    exit 1
fi

echo "==> GET /api/status"
status_json=$(curl -sf "${BASE_URL}/api/status")
echo "$status_json" | jq -e '.version != null' >/dev/null \
    || { echo "FAIL: /api/status missing 'version'"; exit 1; }

echo "==> Upload gate: POST /api/upload without ?backup=true must be rejected"
echo "smoke" > "${WORKDIR}/payload.txt"
gate_code=$(curl -s -o /dev/null -w '%{http_code}' -X POST -F "file=@${WORKDIR}/payload.txt" "${BASE_URL}/api/upload")
[ "$gate_code" = "400" ] || { echo "FAIL: expected 400 without backup=true, got $gate_code"; exit 1; }

echo "==> Upload round-trip: POST /api/upload?backup=true"
printf 'AnNa smoke test payload %s\n' "$(date -u +%s)" > "${WORKDIR}/payload.txt"
upload_json=$(curl -sf -X POST -F "file=@${WORKDIR}/payload.txt" "${BASE_URL}/api/upload?backup=true")
hash=$(echo "$upload_json" | jq -r '.hash // empty')
[ -n "$hash" ] || { echo "FAIL: upload response missing hash: $upload_json"; exit 1; }
echo "    stored as $hash"

echo "==> GET /api/files/check/:hash"
curl -sf "${BASE_URL}/api/files/check/${hash}" | jq -e '.exists == true' >/dev/null \
    || { echo "FAIL: file not reported as existing by hash"; exit 1; }

echo "==> GET /api/download/:hash (content must round-trip)"
curl -sf "${BASE_URL}/api/download/${hash}" -o "${WORKDIR}/downloaded.txt"
cmp -s "${WORKDIR}/payload.txt" "${WORKDIR}/downloaded.txt" \
    || { echo "FAIL: downloaded content does not match uploaded content"; exit 1; }

echo "==> All smoke checks passed"
