#!/usr/bin/env bash
# DIAGNOSTIC run (not the registered campaign): as run_failure.sh, plus optional ZMQ send HWM.
# Pre-registration: docs/superpowers/notes/2026-10-13-caerus-artifact-preregistration.md
# Usage: run_failure.sh <percent_mr> <rep> <outdir> [warm_s=120] [after_s=600]
set -euo pipefail
PMR=$1; REP=$2; OUT=$3; WARM=${4:-120}; AFTER=${5:-600}
IMG=${IMG:-caerus-run:aaf0eb6-hwm}
ENVARG=(); [ -n "${SNDHWM:-}" ] && ENVARG=(-e "CAERUS_SNDHWM=$SNDHWM")
NET=caerus-net
SUBNET=172.30.0.0/24
DEAD=(2 5)  # france-c, east-asia
CONT=(NA NA EU EU AP AP)
ip_of() { echo "172.30.0.$((10 + $1))"; }
one_way_ms() {
  local a=${CONT[$1]} b=${CONT[$2]}
  case "$a-$b" in
    NA-NA) echo 3 ;; EU-EU) echo 6 ;; AP-AP) echo 17 ;;
    NA-EU|EU-NA) echo 41 ;; EU-AP|AP-EU) echo 80 ;; NA-AP|AP-NA) echo 100 ;;
  esac
}

mkdir -p "$OUT"
docker network inspect $NET >/dev/null 2>&1 || docker network create --subnet $SUBNET $NET >/dev/null
for i in 0 1 2 3 4 5; do docker rm -f r$i >/dev/null 2>&1 || true; done

CONF="$OUT/six.conf"
: > "$CONF"
for i in 0 1 2 3 4 5; do echo "$i:$i:$(ip_of $i):10001" >> "$CONF"; done

for i in 0 1 2 3 4 5; do
  # Per-destination egress delay: htb class + netem per peer, u32 match on dst IP.
  TC="tc qdisc add dev eth0 root handle 1: htb default 99 && tc class add dev eth0 parent 1: classid 1:99 htb rate 10gbit"
  for j in 0 1 2 3 4 5; do
    [ "$i" = "$j" ] && continue
    c=$((10 + j))
    TC="$TC && tc class add dev eth0 parent 1: classid 1:$c htb rate 10gbit"
    TC="$TC && tc qdisc add dev eth0 parent 1:$c handle $c: netem delay $(one_way_ms $i $j)ms"
    TC="$TC && tc filter add dev eth0 parent 1: protocol ip u32 match ip dst $(ip_of $j)/32 flowid 1:$c"
  done
  docker run -d --name r$i --hostname r$i --network $NET --ip "$(ip_of $i)" --cap-add NET_ADMIN --memory=8g \
    "${ENVARG[@]}" -v "$(realpath "$CONF")":/caerus/six.conf:ro $IMG \
    bash -c "$TC && exec ./bin/scripts/caerus_server --machine_id=$i --experiment=MovR --config=six.conf \
      --max_batch_size=10 --percent_mr=$PMR --percent_mp=0 --hot_records=240" >/dev/null
done

date +%s.%N > "$OUT/t_start"
# Warm-up begins once every region has logged throughput at least once.
for _ in $(seq 1 120); do
  ready=0
  for i in 0 1 2 3 4 5; do docker logs r$i 2>&1 | grep -q "SEQUENCER THROUGHPUT" && ready=$((ready + 1)); done
  [ $ready -eq 6 ] && break
  sleep 1
done
echo "$ready" > "$OUT/ready_regions"
date +%s.%N > "$OUT/t_ready"
sleep "$WARM"
date +%s.%N > "$OUT/t_fail"
[ "${NOKILL:-0}" = 1 ] || for d in "${DEAD[@]}"; do docker kill r$d >/dev/null; done
sleep "$AFTER"
date +%s.%N > "$OUT/t_end"
for i in 0 1 2 3 4 5; do docker logs -t r$i > "$OUT/r$i.log" 2>&1 || true; docker inspect -f "r$i oom={{.State.OOMKilled}} exit={{.State.ExitCode}}" r$i >> "$OUT/state" 2>&1 || true; done
[ "${KEEP:-0}" = 1 ] || for i in 0 1 2 3 4 5; do docker rm -f r$i >/dev/null 2>&1 || true; done
echo "pmr=$PMR rep=$REP ready=$ready" > "$OUT/done"
