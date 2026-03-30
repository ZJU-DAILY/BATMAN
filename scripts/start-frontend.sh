#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FRONTEND_DIR="${ROOT_DIR}/src/frontend"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/start-frontend.sh \
    --api-base-url URL \
    --host HOST \
    --port PORT

Required parameters:
  --api-base-url   Frontend API base URL (NEXT_PUBLIC_API_BASE_URL)
  --host           Frontend bind host
  --port           Frontend bind port (integer)
EOF
}

if [[ ! -d "${FRONTEND_DIR}" ]]; then
  echo "[ERROR] Frontend directory not found: ${FRONTEND_DIR}" >&2
  exit 1
fi

NEXT_PUBLIC_API_BASE_URL=""
ADP_FRONTEND_HOST=""
ADP_FRONTEND_PORT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --api-base-url)
      NEXT_PUBLIC_API_BASE_URL="${2:-}"
      shift 2
      ;;
    --host)
      ADP_FRONTEND_HOST="${2:-}"
      shift 2
      ;;
    --port)
      ADP_FRONTEND_PORT="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_non_empty() {
  local name="$1"
  local value="$2"
  if [[ -z "${value}" ]]; then
    echo "[ERROR] Missing required argument: ${name}" >&2
    usage
    exit 1
  fi
}

require_integer() {
  local name="$1"
  local value="$2"
  if ! [[ "${value}" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] ${name} must be an integer. Got: ${value}" >&2
    exit 1
  fi
}

require_non_empty "--api-base-url" "${NEXT_PUBLIC_API_BASE_URL}"
require_non_empty "--host" "${ADP_FRONTEND_HOST}"
require_non_empty "--port" "${ADP_FRONTEND_PORT}"

require_integer "--port" "${ADP_FRONTEND_PORT}"

export NEXT_PUBLIC_API_BASE_URL

echo "[INFO] Installing frontend dependencies..."
cd "${FRONTEND_DIR}"
npm install

echo "[INFO] Starting frontend at http://${ADP_FRONTEND_HOST}:${ADP_FRONTEND_PORT}"
exec npm run dev -- --hostname "${ADP_FRONTEND_HOST}" --port "${ADP_FRONTEND_PORT}"
