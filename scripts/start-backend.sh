#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND_DIR="${ROOT_DIR}/src/backend"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/start-backend.sh \
    --api-base-url URL \
    --api-key KEY \
    --generation-model MODEL \
    --explanation-model MODEL \
    --timeout-seconds SECONDS \
    --session-ttl-seconds SECONDS \
    --host HOST \
    --port PORT

Required parameters:
  --api-base-url          LLM API base URL
  --api-key               LLM API key
  --generation-model      Model for pipeline generation
  --explanation-model     Model for explanation/review
  --timeout-seconds       Request timeout in seconds (integer)
  --session-ttl-seconds   Session TTL in seconds (integer)
  --host                  Backend bind host
  --port                  Backend bind port (integer)
EOF
}

if [[ ! -d "${BACKEND_DIR}" ]]; then
  echo "[ERROR] Backend directory not found: ${BACKEND_DIR}" >&2
  exit 1
fi

ADP_API_BASE_URL=""
ADP_API_KEY=""
ADP_GENERATION_MODEL=""
ADP_EXPLANATION_MODEL=""
ADP_TIMEOUT_SECONDS=""
ADP_SESSION_TTL_SECONDS=""
ADP_BACKEND_HOST=""
ADP_BACKEND_PORT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --api-base-url)
      ADP_API_BASE_URL="${2:-}"
      shift 2
      ;;
    --api-key)
      ADP_API_KEY="${2:-}"
      shift 2
      ;;
    --generation-model)
      ADP_GENERATION_MODEL="${2:-}"
      shift 2
      ;;
    --explanation-model)
      ADP_EXPLANATION_MODEL="${2:-}"
      shift 2
      ;;
    --timeout-seconds)
      ADP_TIMEOUT_SECONDS="${2:-}"
      shift 2
      ;;
    --session-ttl-seconds)
      ADP_SESSION_TTL_SECONDS="${2:-}"
      shift 2
      ;;
    --host)
      ADP_BACKEND_HOST="${2:-}"
      shift 2
      ;;
    --port)
      ADP_BACKEND_PORT="${2:-}"
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

require_non_empty "--api-base-url" "${ADP_API_BASE_URL}"
require_non_empty "--api-key" "${ADP_API_KEY}"
require_non_empty "--generation-model" "${ADP_GENERATION_MODEL}"
require_non_empty "--explanation-model" "${ADP_EXPLANATION_MODEL}"
require_non_empty "--timeout-seconds" "${ADP_TIMEOUT_SECONDS}"
require_non_empty "--session-ttl-seconds" "${ADP_SESSION_TTL_SECONDS}"
require_non_empty "--host" "${ADP_BACKEND_HOST}"
require_non_empty "--port" "${ADP_BACKEND_PORT}"

require_integer "--timeout-seconds" "${ADP_TIMEOUT_SECONDS}"
require_integer "--session-ttl-seconds" "${ADP_SESSION_TTL_SECONDS}"
require_integer "--port" "${ADP_BACKEND_PORT}"

export ADP_API_BASE_URL
export ADP_API_KEY
export ADP_GENERATION_MODEL
export ADP_EXPLANATION_MODEL
export ADP_TIMEOUT_SECONDS
export ADP_SESSION_TTL_SECONDS
export PYTHONPATH="${BACKEND_DIR}"

echo "[INFO] Installing backend dependencies..."
python3 -m pip install -r "${BACKEND_DIR}/requirements.txt"

echo "[INFO] Starting backend at http://${ADP_BACKEND_HOST}:${ADP_BACKEND_PORT}"
cd "${BACKEND_DIR}"
exec python3 -m uvicorn app.main:app --reload --host "${ADP_BACKEND_HOST}" --port "${ADP_BACKEND_PORT}"
