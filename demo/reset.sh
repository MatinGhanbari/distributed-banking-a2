#!/usr/bin/env bash
# Fresh platform: remove containers + volumes, rebuild, start and wait until healthy.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose --profile shardc down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build --wait
docker compose ps --format 'table {{.Service}}\t{{.Status}}'
