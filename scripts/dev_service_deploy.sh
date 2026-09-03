#!/usr/bin/env bash
# Maintenance deployment: build → stop → migrate/check → recreate all services.
# Usage (from repo root):
#   ./scripts/dev_service_deploy.sh
#   ./scripts/dev_service_deploy.sh --migrate
#   ./scripts/dev_service_deploy.sh --skip-sync
#   ./scripts/dev_service_deploy.sh --skip-build
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOST_ENV="${ROOT}/.host.env"
IMAGE_TAG="${AURACLAW_DEV_IMAGE:-auraclaw:dev}"
COMPOSE_ENV_FILE="${AURACLAW_COMPOSE_ENV_FILE:-.env.test}"
DO_SYNC=1
DO_BUILD=1
DO_UP=1
DO_HEALTH=1
MIGRATE_TARGET="${AURACLAW_MIGRATE_TARGET:-0063}"

usage() {
  cat <<'EOF'
Usage: ./scripts/dev_service_deploy.sh [options]

  (default)     sync/build → stop services → migrate/check → recreate/start
  --migrate     compatibility alias; migrations are now mandatory before up
  --skip-sync   do not rsync (build/up remote tree only)
  --skip-build  do not docker build
  --skip-up     stop after sync/build
  --skip-health skip curl health check
  -h, --help    show this help

Reads host/user from .host.env (DEV_SERVICE_* preferred, else AURACLAW_HOST*).
Uses remote .env.test (server test). Does not overwrite .env.test or .runtime/compose-secrets.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --migrate) : ;;
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
    --exclude '.runtime/' \
    --exclude '.env.dev' \
    --exclude '.env.test' \
    --exclude '.env.prod' \
    --exclude '.host.env' \
    --exclude 'compose.kafka-fix.yml' \
    --exclude 'compose.hotfix-errors.yml' \
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

echo "==> compose env ${COMPOSE_ENV_FILE}"

remote "test -f ${COMPOSE_ENV_FILE}" || {
  echo "remote missing ${COMPOSE_ENV_FILE} — cp .env.test.example ${COMPOSE_ENV_FILE} on ${HOST} first" >&2
  exit 1
}

remote "docker network inspect auraclaw-platform >/dev/null 2>&1 || docker network create auraclaw-platform"

if [[ "${DO_BUILD}" -eq 1 ]]; then
  echo "==> docker build -t ${IMAGE_TAG}"
  remote "docker build -t $(printf '%q' "${IMAGE_TAG}") ."
else
  echo "==> skip build"
fi

COMPOSE_FILE="compose.prod.yml"
if [[ "${COMPOSE_ENV_FILE}" == ".env.test" ]]; then
  COMPOSE_FILE="compose.test.yml"
fi
# Optional hotfix overlay files stay on the server if present.
# Keep $(...) literal so the remote shell expands them (same as remote_compose.sh).
COMPOSE_CMD="AURACLAW_IMAGE=$(printf '%q' "${IMAGE_TAG}") AURACLAW_MIGRATE_TARGET=$(printf '%q' "${MIGRATE_TARGET}") docker compose --env-file ${COMPOSE_ENV_FILE} -f ${COMPOSE_FILE}"
COMPOSE_CMD+=' $(test -f compose.kafka-fix.yml && echo -f compose.kafka-fix.yml)'
COMPOSE_CMD+=' $(test -f compose.hotfix-errors.yml && echo -f compose.hotfix-errors.yml)'

if [[ "${DO_UP}" -eq 1 ]]; then
  # Read the required version from the selected image before stopping anything.
  REQUIRED_TARGET="$(remote "${COMPOSE_CMD} --profile migrate run --rm --no-deps -T migrate migrate latest --directory /app/migrations")"
  if [[ "${MIGRATE_TARGET}" != "${REQUIRED_TARGET}" ]]; then
    echo "migration target ${MIGRATE_TARGET} does not match image schema ${REQUIRED_TARGET}; deploy aborted before stop" >&2
    exit 1
  fi
  echo "==> maintenance stop (rebuild all database connection pools after DDL)"
  remote "${COMPOSE_CMD} stop"
  echo "==> migrate target ${MIGRATE_TARGET}"
  remote "${COMPOSE_CMD} --profile migrate run --rm --no-deps -T migrate"
  remote "${COMPOSE_CMD} --profile migrate run --rm --no-deps -T migrate migrate check --target ${MIGRATE_TARGET} --directory /app/migrations"
  echo "==> recreate/start all services"
  remote "${COMPOSE_CMD} up -d --force-recreate --wait --remove-orphans"
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
