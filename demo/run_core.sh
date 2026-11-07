#!/usr/bin/env bash
# Runs the 7 required experiments in order (fresh deployment first) and prints a summary.
#   demo/run_core.sh            reset + experiments 1-7
#   demo/run_core.sh --no-reset   use the running deployment
#   PAUSE=1 demo/run_core.sh      wait for Enter before each experiment (video recording)
cd "$(dirname "$0")"
B=$'\033[1m'; G=$'\033[32m'; R=$'\033[31m'; C=$'\033[36m'; X=$'\033[0m'
start=$(date +%s)
if [[ "${1:-}" != "--no-reset" ]]; then
  echo "${C}${B}▶ Fresh deployment: docker compose down -v && docker compose up -d --build --wait${X}"
  ./reset.sh || { echo "${R}deployment failed${X}"; exit 1; }
fi
declare -a names=("Shard distribution" "Replica load distribution" "Concurrent replica operations"
                  "Replica interruption" "Local and cross-shard transfers"
                  "Interrupted distributed transaction" "Concurrent cross-shard transfers")
files=(exp1_shard_distribution.py exp2_replica_load.py exp3_concurrent_replicas.py
       exp4_replica_interruption.py exp5_transfers.py exp6_coordinator_crash.py
       exp7_concurrent_cross_shard.py)
results=()
for i in "${!files[@]}"; do
  [[ "${PAUSE:-0}" == 1 ]] && read -rp "${C}[Enter] experiment $((i+1)): ${names[$i]}${X}"
  if python3 "${files[$i]}"; then results+=("PASS"); else results+=("FAIL"); fi
  sleep 1
done
echo
echo "${B}════════════════════ SUMMARY ════════════════════${X}"
fail=0
for i in "${!files[@]}"; do
  if [[ ${results[$i]} == PASS ]]; then col=$G; else col=$R; fail=1; fi
  printf "  %d. %-38s %s%s%s\n" $((i+1)) "${names[$i]}" "$col$B" "${results[$i]}" "$X"
done
echo "  total time: $(( $(date +%s) - start ))s"
exit $fail
