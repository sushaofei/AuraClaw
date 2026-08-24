#!/usr/bin/env bash
# Run docker compose on the remote AuraClaw host from .host.env.
# Default env file: .env.test (server test). Override: AURACLAW_COMPOSE_ENV_FILE=.env.prod
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOST_ENV="${ROOT}/.host.env"
COMPOSE_ENV_FILE="${AURACLAW_COMPOSE_ENV_FILE:-.env.test}"
if [[ ! -f "${HOST_ENV}" ]]; then
  echo "missing ${HOST_ENV}" >&2
  exit 1
fi

HOST="$(grep -E '^AURACLAW_HOST=' "${HOST_ENV}" | cut -d= -f2-)"
USER_NAME="$(grep -E '^AURACLAW_HOST_USR=' "${HOST_ENV}" | cut -d= -f2-)"
USER_NAME="${USER_NAME:-jcroot}"
REMOTE_DIR="${AURACLAW_REMOTE_DIR:-/home/${USER_NAME}/workspace/AuraClaw}"

if [[ -z "${HOST}" ]]; then
  echo "AURACLAW_HOST missing in .host.env" >&2
  exit 1
fi

ACTION="${1:-ps}"
shift || true

COMPOSE_PREFIX="docker compose --env-file ${COMPOSE_ENV_FILE} -f compose.production.yml"
COMPOSE_PREFIX+=' $(test -f compose.kafka-fix.yml && echo -f compose.kafka-fix.yml)'
COMPOSE_PREFIX+=' $(test -f compose.hotfix-errors.yml && echo -f compose.hotfix-errors.yml)'

remote_cmd() {
  local inner="$1"
  ssh -o BatchMode=yes "${USER_NAME}@${HOST}" "cd ${REMOTE_DIR} && ${inner}"
}

case "${ACTION}" in
  ps)
    remote_cmd "${COMPOSE_PREFIX} ps $*"
    ;;
  logs)
    remote_cmd "${COMPOSE_PREFIX} logs --since ${REMOTE_LOG_SINCE:-30m} --tail ${REMOTE_LOG_TAIL:-200} $*"
    ;;
  restart)
    remote_cmd "${COMPOSE_PREFIX} up -d --no-deps --force-recreate $*"
    ;;
  exec)
    SERVICE="${1:?service required}"
    shift
    remote_cmd "${COMPOSE_PREFIX} exec -T ${SERVICE} $*"
    ;;
  shell)
    SERVICE="${1:?service required}"
    ssh -t -o BatchMode=yes "${USER_NAME}@${HOST}" \
      "cd ${REMOTE_DIR} && ${COMPOSE_PREFIX} exec -it ${SERVICE} sh"
    ;;
  *)
    remote_cmd "${COMPOSE_PREFIX} ${ACTION} $*"
    ;;
esac
