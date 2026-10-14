#!/usr/bin/env bash
# Registered campaign: 3 reps x percent_mr {0,10,30}, interleaved.
# Pre-registration + amendments: docs/superpowers/notes/2026-10-13-caerus-artifact-preregistration.md
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${1:-$HERE/../../../results/caerus/artifact}
load=$(cut -d' ' -f1 /proc/loadavg)
avail=$(awk '/MemAvailable/ {print int($2/1048576)}' /proc/meminfo)
echo "preflight: load1=$load avail_gib=$avail"
awk -v l="$load" 'BEGIN {exit !(l <= 8)}' || { echo "abort: host load $load > 8"; exit 2; }
[ "$avail" -ge 60 ] || { echo "abort: available memory ${avail} GiB < 60"; exit 2; }
mkdir -p "$OUT"
for rep in 1 2 3; do
  for pmr in 0 10 30; do
    d="$OUT/pmr${pmr}_rep${rep}"
    [ -f "$d/done" ] && { echo "skip $d"; continue; }
    echo "$(date -Is) start pmr=$pmr rep=$rep"
    IMG=${IMG:-caerus-run:aaf0eb6-nopin} "$HERE/run_failure.sh" "$pmr" "$rep" "$d" 120 600
  done
done
echo "$(date -Is) campaign done"
