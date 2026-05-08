#!/usr/bin/env bash
# server/scripts/start.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

export APP_ENV="prod"

# # Mandatory fields in local settings
# # set this in local settings.env, example: 
# export GEOMAP_DB_DIR=/proj/boston/boston/stage/db
# export GEOMAP_LOG_DIR=/proj/boston/boston/stage/logs
# export GEOMAP_LISTS_DIR=/proj/boston/boston/stage/lists
# # Primary key (SOS)
# export ARTDATABANKEN_SUBSCRIPTION_KEY="<your secret here>"


source "${REPO_ROOT}/../settings.env"

: "${ARTDATABANKEN_SUBSCRIPTION_KEY:?ARTDATABANKEN_SUBSCRIPTION_KEY missing}"

export ARTDATABANKEN_SUBSCRIPTION_KEY

echo "ARTDATABANKEN_SUBSCRIPTION_KEY is set, length=${#ARTDATABANKEN_SUBSCRIPTION_KEY}" >&2

mkdir -p \
  "${GEOMAP_DB_DIR}" \
  "${GEOMAP_LOG_DIR}" \
  "${GEOMAP_LISTS_DIR}"

cd "${REPO_ROOT}"

exec "${REPO_ROOT}/.venv/bin/python3" \
  "${REPO_ROOT}/server/app.py" \
  --db-dir "${GEOMAP_DB_DIR}" \
  --logs-dir "${GEOMAP_LOG_DIR}" \
  --lists-dir "${GEOMAP_LISTS_DIR}"
