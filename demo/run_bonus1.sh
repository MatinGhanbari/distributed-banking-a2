#!/usr/bin/env bash
# Bonus I demo: fresh deployment, then live re-sharding B -> C under load with 4 controller crashes.
#   demo/run_bonus1.sh  |  PAUSE=1 demo/run_bonus1.sh  |  demo/run_bonus1.sh --no-reset
cd "$(dirname "$0")"
C=$'\033[36m'; B=$'\033[1m'; X=$'\033[0m'
start=$(date +%s)
if [[ "${1:-}" != "--no-reset" ]]; then
  echo "${C}${B}▶ Fresh deployment (core topology: shards A and B)${X}"
  ./reset.sh || exit 1
fi
[[ "${PAUSE:-0}" == 1 ]] && read -rp "${C}[Enter] start live re-sharding demo${X}"
PAUSE="${PAUSE:-0}" python3 bonus1_migration.py; rc=$?
echo "  total time: $(( $(date +%s) - start ))s"
exit $rc
