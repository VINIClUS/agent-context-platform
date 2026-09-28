#!/usr/bin/env bash
# Manage the ephemeral integration-test service stack (PostgreSQL, Neo4j,
# Garage) defined in compose.test.yml.
#
# Usage:
#   scripts/test-services.sh up      # start services, wait for health, bootstrap Garage
#   scripts/test-services.sh env     # print `export AGENT_CONTEXT_TEST_...` lines
#   scripts/test-services.sh down    # stop services and remove all state
#   scripts/test-services.sh status  # show compose service status
#
# The Compose project name is derived from this repository's absolute path
# so that concurrent worktrees never collide. Override it explicitly with
# AGENT_CONTEXT_TEST_PROJECT.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly REPO_ROOT
COMPOSE_FILE="${REPO_ROOT}/compose.test.yml"
readonly COMPOSE_FILE

if [[ -n "${AGENT_CONTEXT_TEST_PROJECT:-}" ]]; then
  PROJECT="${AGENT_CONTEXT_TEST_PROJECT}"
else
  PROJECT="agent-context-test-$(printf '%s' "${REPO_ROOT}" | sha256sum | cut -c1-12)"
fi
readonly PROJECT

# Fixed, non-sensitive, test-only credentials. Services are single-node,
# ephemeral, and bound to 127.0.0.1 only -- these are not real secrets.
readonly POSTGRES_USER="agent_context_test"
readonly POSTGRES_PASSWORD="agent_context_test"
readonly POSTGRES_DB="postgres"

readonly NEO4J_USERNAME="neo4j"
readonly NEO4J_PASSWORD="agent-context-test"

readonly S3_BUCKET="agent-context-test"
readonly S3_KEY_NAME="agent-context-test"
# Deterministic per-project Garage key so `up` and `env` agree without
# persisting any state file. `garage key import` requires a GK-prefixed
# 24 hex character key id and a 64 hex character secret.
S3_ACCESS_KEY_ID="GK$(printf '%s' "${PROJECT}:access-key" | sha256sum | cut -c1-24)"
readonly S3_ACCESS_KEY_ID
S3_SECRET_ACCESS_KEY="$(printf '%s' "${PROJECT}:secret-key" | sha256sum | cut -c1-64)"
readonly S3_SECRET_ACCESS_KEY

compose() {
  docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" "$@"
}

garage_exec() {
  timeout 60 docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" exec -T garage "$@"
}

host_port() {
  # host_port <service> <container-port>
  compose port "$1" "$2" | sed -E 's/.*:([0-9]+)[[:space:]]*$/\1/'
}

bootstrap_garage() {
  local status_output node_id
  status_output="$(garage_exec /garage status)"
  node_id="$(printf '%s\n' "${status_output}" | grep -oE '^[0-9a-f]{16}' | head -n1)"
  if [[ -z "${node_id}" ]]; then
    echo "test-services: could not determine garage node id" >&2
    printf '%s\n' "${status_output}" >&2
    exit 1
  fi

  if printf '%s\n' "${status_output}" | grep -q 'NO ROLE ASSIGNED'; then
    garage_exec /garage layout assign -z test -c 1GB "${node_id}"
    garage_exec /garage layout apply --version 1
  fi

  if ! garage_exec /garage bucket list | grep -qF "${S3_BUCKET}"; then
    garage_exec /garage bucket create "${S3_BUCKET}"
  fi

  if ! garage_exec /garage key list | grep -qF "${S3_ACCESS_KEY_ID}"; then
    garage_exec /garage key import "${S3_ACCESS_KEY_ID}" "${S3_SECRET_ACCESS_KEY}" -n "${S3_KEY_NAME}" --yes
  fi

  garage_exec /garage bucket allow --read --write --owner "${S3_BUCKET}" --key "${S3_ACCESS_KEY_ID}"
}

cmd_up() {
  compose up -d --wait --wait-timeout 180
  bootstrap_garage
  echo "test-services: stack '${PROJECT}' is up" >&2
}

cmd_env() {
  local postgres_port neo4j_bolt_port s3_port
  postgres_port="$(host_port postgres 5432)"
  neo4j_bolt_port="$(host_port neo4j 7687)"
  s3_port="$(host_port garage 3900)"

  cat <<EOF
export AGENT_CONTEXT_TEST_POSTGRES_DSN="postgresql+psycopg://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:${postgres_port}/${POSTGRES_DB}"
export AGENT_CONTEXT_TEST_NEO4J_URI="bolt://127.0.0.1:${neo4j_bolt_port}"
export AGENT_CONTEXT_TEST_NEO4J_USERNAME="${NEO4J_USERNAME}"
export AGENT_CONTEXT_TEST_NEO4J_PASSWORD="${NEO4J_PASSWORD}"
export AGENT_CONTEXT_TEST_S3_ENDPOINT_URL="http://127.0.0.1:${s3_port}"
export AGENT_CONTEXT_TEST_S3_REGION_NAME="garage"
export AGENT_CONTEXT_TEST_S3_BUCKET_NAME="${S3_BUCKET}"
export AGENT_CONTEXT_TEST_S3_ACCESS_KEY_ID="${S3_ACCESS_KEY_ID}"
export AGENT_CONTEXT_TEST_S3_SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY}"
EOF
}

cmd_down() {
  compose down --volumes --remove-orphans
}

cmd_status() {
  compose ps
}

main() {
  local sub="${1:-}"
  case "${sub}" in
    up) cmd_up ;;
    env) cmd_env ;;
    down) cmd_down ;;
    status) cmd_status ;;
    *)
      echo "usage: $(basename "${BASH_SOURCE[0]}") up|env|down|status" >&2
      exit 2
      ;;
  esac
}

main "$@"
