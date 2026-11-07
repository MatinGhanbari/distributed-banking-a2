#!/usr/bin/env bash
# Fresh platform: remove containers (incl. healer-made replicas) + volumes, rebuild, start, wait.
set -uo pipefail
cd "$(dirname "$0")/.."
docker compose --profile healing kill healer >/dev/null 2>&1   # stop the controller first (no races)
ids=$(docker ps -aq --filter label=dsbank.managed-by=healer)
if [[ -n "$ids" ]]; then docker rm -f $ids >/dev/null; fi
docker compose --profile shardc --profile healing down -v --remove-orphans >/dev/null 2>&1
docker compose up -d --build --wait || exit 1
docker compose ps --format 'table {{.Service}}\t{{.Status}}'
