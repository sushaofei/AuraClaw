#!/usr/bin/env bash
# One-shot DEV_SERVICE deploy: rsync → docker build → compose up.
# Usage (from repo root):
#   ./scripts/dev_service_deploy.sh
#   ./scripts/dev_service_deploy.sh --migrate
#   ./scripts/dev_service_deploy.sh --skip-sync
#   ./scripts/dev_service_deploy.sh --skip-build
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOST_ENV="${ROOT}/.host.env"
IMAGE_TAG="${AURACLAW_DEV_IMAGE:-auraclaw:dev}"
DO_SYNC=1
DO_BUILD=1
DO_UP=1
DO_MIGRATE=0
DO_HEALTH=1

usage() {
  cat <<'EOF'
Usage: ./scripts/dev_service_deploy.sh [options]

  (default)     rsync code → docker build → compose up -d --wait
  --migrate     also run DB migrate before up
  --skip-sync   do not rsync (build/up remote tree only)
  --skip-build  do not docker build
  --skip-up     stop after sync/build
  --skip-health skip curl health check
  -h, --help    show this help

Reads host/user from .host.env (DEV_SERVICE_* preferred, else AURACLAW_HOST*).
Does not overwrite remote .env.production or .runtime/compose-secrets.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --migrate) DO_MIGRATE=1 ;;
    --skip-sync) DO_SYNC=0 ;;
    --skip-build) DO_BUILD=0 ;;
    --skip-up) DO_UP=0 ;;
    --skip-health) DO_HEALTH=0 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [[ ! -f "${HOST_ENV}" ]]; then
  echo "missing ${HOST_ENV}" >&2
  exit 1
fi

host_var() {
  local key="$1"
  grep -E "^${key}=" "${HOST_ENV}" | head -n1 | cut -d= -f2- || true
}

HOST="$(host_var DEV_SERVICE_HOST)"
USER_NAME="$(host_var DEV_SERVICE_USER)"
if [[ -z "${HOST}" ]]; then
  HOST="$(host_var AURACLAW_HOST)"
fi
if [[ -z "${USER_NAME}" ]]; then
  USER_NAME="$(host_var AURACLAW_HOST_USR)"
fi
USER_NAME="${USER_NAME:-jcroot}"
REMOTE_DIR="${AURACLAW_REMOTE_DIR:-/home/${USER_NAME}/workspace/AuraClaw}"

if [[ -z "${HOST}" ]]; then
  echo "DEV_SERVICE_HOST / AURACLAW_HOST missing in .host.env" >&2
  exit 1
fi

REMOTE="${USER_NAME}@${HOST}"

remote() {
  ssh -o BatchMode=yes "${REMOTE}" "cd $(printf '%q' "${REMOTE_DIR}") && $*"
}

echo "==> target ${REMOTE}:${REMOTE_DIR}"
echo "==> image  ${IMAGE_TAG}"

if [[ "${DO_SYNC}" -eq 1 ]]; then
  echo "==> rsync (excluding secrets / local env)"
  rsync -az --delete \
    --exclude '.git/' \
    --exclude '.venv/' \
    --exclude 'node_modules/' \
    --exclude 'frontend/node_modules/' \
    --exclude '.runtime/' \
    --exclude '.env' \
    --exclude '.env.production' \
    --exclude '.host.env' \
    --exclude '.chaintower' \
    --exclude '.DS_Store' \
    --exclude '__pycache__/' \
    --exclude '.pytest_cache/' \
    --exclude '.mypy_cache/' \
    --exclude '.ruff_cache/' \
    --exclude 'Users/' \
    "${ROOT}/" "${REMOTE}:${REMOTE_DIR}/"
else
  echo "==> skip rsync"
fi

remote "test -f .env.production" || {
  echo "remote missing .env.production — create it on ${HOST} first" >&2
  exit 1
}

if ! remote "grep -E '^AURACLAW_IMAGE=${IMAGE_TAG}\$' .env.production >/dev/null || grep -E '^AURACLAW_IMAGE=\"${IMAGE_TAG}\"\$' .env.production >/dev/null"; then
  echo "warning: .env.production AURACLAW_IMAGE is not ${IMAGE_TAG}" >&2
  echo "         set AURACLAW_IMAGE=${IMAGE_TAG} on the server or this deploy may run an old image" >&2
fi

remote "docker network inspect auraclaw-platform >/dev/null 2>&1 || docker network create auraclaw-platform"

if [[ "${DO_BUILD}" -eq 1 ]]; then
  echo "==> docker build -t ${IMAGE_TAG}"
  remote "docker build -t $(printf '%q' "${IMAGE_TAG}") ."
else
  echo "==> skip build"
fi

# Optional overlay files stay on the server if present.
# Keep $(...) literal so the remote shell expands them (same as remote_compose.sh).
COMPOSE_CMD='docker compose --env-file .env.production -f compose.production.yml'
COMPOSE_CMD+=' $(test -f compose.kafka-fix.yml && echo -f compose.kafka-fix.yml)'
COMPOSE_CMD+=' $(test -f compose.hotfix-errors.yml && echo -f compose.hotfix-errors.yml)'

if [[ "${DO_MIGRATE}" -eq 1 ]]; then
  echo "==> migrate"
  remote "${COMPOSE_CMD} --profile migrate run --rm migrate"
fi

if [[ "${DO_UP}" -eq 1 ]]; then
  echo "==> compose up"
  remote "${COMPOSE_CMD} up -d --wait --remove-orphans"
  echo "==> compose ps"
  remote "${COMPOSE_CMD} ps"
else
  echo "==> skip up"
fi

if [[ "${DO_HEALTH}" -eq 1 && "${DO_UP}" -eq 1 ]]; then
  echo "==> health"
  if NO_PROXY="${HOST},127.0.0.1,localhost${NO_PROXY:+,${NO_PROXY}}" \
    no_proxy="${HOST},127.0.0.1,localhost${no_proxy:+,${no_proxy}}" \
    curl --fail --silent --show-error "http://${HOST}:8080/health/ready" >/dev/null; then
    echo "ready: http://${HOST}:8080/health/ready"
  else
    echo "health check failed: http://${HOST}:8080/health/ready" >&2
    echo "hint: if you use a local HTTP proxy, ensure ${HOST} is in NO_PROXY" >&2
    exit 1
  fi
fi

echo "==> done"
