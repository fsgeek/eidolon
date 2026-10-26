#!/usr/bin/env bash
# One registered restart-arm trial: six Caerus regions, Table-1 delays; at the
# fault instant stop all six and start a fresh cluster (Caerus has no load path).
# Pre-registration: docs/superpowers/notes/2026-10-26-caerus-restart-arm-preregistration.md
# Usage: run_restart.sh <arm R6|R5> <percent_mr> <rep> <outdir> [warm_s=120] [after_s=300]
#   R6: restart all six regions (the fault treated as transient; idealized).
#   R5: restart without region 2 (france-c stays dead): five regions, renumbered 0-4.
set -euo pipefail
ARM=$1; PMR=$2; REP=$3; OUT=$4; WARM=${5:-120}; AFTER=${6:-300}
IMG=${IMG:-caerus-run:aaf0eb6-nopin-rt1200}
NET=caerus-net
SUBNET=172.30.0.0/24
P=rr  # experiment prefix; never touch the historical r0-r5 names
CONT6=(NA NA EU EU AP AP)
CONT5=(NA NA EU AP AP)   # physical regions 0,1,3,4,5
one_way_ms() {
  case "$1-$2" in
    NA-NA) echo 3 ;; EU-EU) echo 6 ;; AP-AP) echo 17 ;;
    NA-EU|EU-NA) echo 41 ;; EU-AP|AP-EU) echo 80 ;; NA-AP|AP-NA) echo 100 ;;
  esac
}

# start_cluster <gen> <n> <cont-array-name> <ip-base>
start_cluster() {
  local gen=$1 n=$2 base=$4
  local -n cont=$3
  local conf="$OUT/conf.$gen"
  : > "$conf"
  for ((i = 0; i < n; i++)); do echo "$i:$i:172.30.0.$((base + i)):10001" >> "$conf"; done
  for ((i = 0; i < n; i++)); do
    local tc="tc qdisc add dev eth0 root handle 1: htb default 99 && tc class add dev eth0 parent 1: classid 1:99 htb rate 10gbit"
    for ((j = 0; j < n; j++)); do
      [ "$i" = "$j" ] && continue
      local c=$((10 + j))
      tc="$tc && tc class add dev eth0 parent 1: classid 1:$c htb rate 10gbit"
      tc="$tc && tc qdisc add dev eth0 parent 1:$c handle $c: netem delay $(one_way_ms "${cont[$i]}" "${cont[$j]}")ms"
      tc="$tc && tc filter add dev eth0 parent 1: protocol ip u32 match ip dst 172.30.0.$((base + j))/32 flowid 1:$c"
    done
    docker run -d --name $P$gen-$i --hostname $P$gen-$i --network $NET --ip "172.30.0.$((base + i))" \
      --cap-add NET_ADMIN --memory=8g -v "$(realpath "$conf")":/caerus/six.conf:ro $IMG \
      bash -c "$tc && exec ./bin/scripts/caerus_server --machine_id=$i --experiment=MovR --config=six.conf \
        --max_batch_size=10 --percent_mr=$PMR --percent_mp=0 --hot_records=240" >/dev/null
  done
}
dump_cluster() {
  local gen=$1 n=$2
  for ((i = 0; i < n; i++)); do
    docker logs -t $P$gen-$i > "$OUT/$gen.r$i.log" 2>&1 || true
    docker inspect -f "$gen.r$i oom={{.State.OOMKilled}} exit={{.State.ExitCode}}" $P$gen-$i >> "$OUT/state" 2>&1 || true
  done
}
cleanup() { docker ps -a --format '{{.Names}}' | grep -E "^${P}[ab]-[0-9]$" | xargs -r docker rm -f >/dev/null; }

mkdir -p "$OUT"
docker network inspect $NET >/dev/null 2>&1 || docker network create --subnet $SUBNET $NET >/dev/null
cleanup

# Generation a: the original six regions (IPs .10-.15), as in run_failure.sh.
start_cluster a 6 CONT6 10
date +%s.%N > "$OUT/t_start"
for _ in $(seq 1 120); do
  ready=0
  for i in 0 1 2 3 4 5; do [ "$(docker logs $P"a"-$i 2>&1 | grep -c "SEQUENCER THROUGHPUT" || true)" -gt 0 ] && ready=$((ready + 1)); done
  [ $ready -eq 6 ] && break
  sleep 1
done
echo "$ready" > "$OUT/ready_regions"
sleep "$WARM"

# Fault instant = restart instant (idealized: detection delay 0).
date +%s.%N > "$OUT/t_fail"
docker kill $(for i in 0 1 2 3 4 5; do echo $P"a"-$i; done) >/dev/null
date +%s.%N > "$OUT/t_killed"
dump_cluster a 6
for i in 0 1 2 3 4 5; do docker rm -f $P"a"-$i >/dev/null 2>&1 || true; done

# Generation b: the restarted cluster (IPs .30-), fresh processes, same image.
if [ "$ARM" = R6 ]; then N=6; start_cluster b 6 CONT6 30
else N=5; start_cluster b 5 CONT5 30; fi
date +%s.%N > "$OUT/t_restarted"
sleep "$AFTER"
date +%s.%N > "$OUT/t_end"
dump_cluster b $N
[ "${KEEP:-0}" = 1 ] || cleanup
echo "arm=$ARM pmr=$PMR rep=$REP ready=$ready n_after=$N" > "$OUT/done"
