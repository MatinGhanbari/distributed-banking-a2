#!/usr/bin/env bash
# Bonus II demo: fresh deployment, self-healing controller under load with injected crashes.
#   demo/run_bonus2.sh | PAUSE=1 demo/run_bonus2.sh | SLOW=1 demo/run_bonus2.sh | --no-reset
cd "$(dirname "$0")"
C=$'\033[36m'; B=$'\033[1m'; X=$'\033[0m'
start=$(date +%s)
[[ "${SLOW:-0}" == 1 ]] && export SLOW_STEP=8   # video pacing
if [[ "${1:-}" != "--no-reset" ]]; then
  echo "${C}${B}▶ Fresh deployment (core topology)${X}"
  ./reset.sh || exit 1
fi
[[ "${PAUSE:-0}" == 1 ]] && read -rp "${C}[Enter] start self-healing demo${X}"
python3 bonus2_self_healing.py; rc=$?
echo "  total time: $(( $(date +%s) - start ))s"
exit $rc
