#!/usr/bin/env bash
# Registered order: R6/0, R6/30, R5/0, R5/30, 3 reps each (2026-10-26 registration).
set -uo pipefail
cd "$(dirname "$0")/../../.."
for cell in "R6 0" "R6 30" "R5 0" "R5 30"; do
  read -r arm pmr <<< "$cell"
  for rep in 1 2 3; do
    out=results/caerus/restart-arm/${arm}_pmr${pmr}_rep${rep}
    echo "$(date -Is) start $out"
    external/caerus/run/run_restart.sh "$arm" "$pmr" "$rep" "$out" || echo "$(date -Is) FAILED $out rc=$?"
  done
done
echo "$(date -Is) campaign done"
