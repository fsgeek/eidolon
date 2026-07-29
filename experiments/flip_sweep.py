"""Sweep the mid-round capability flip across arms, retry budgets and seeds.

Pre-registration:
  docs/superpowers/notes/2026-07-29-midround-flip-preregistration.md

Two outputs, deliberately separated (premortem A4 — deterministic primary
map, stochastic sweep second):

  --map-output    jitter_scale=0, one row per (arm, incumbent_max_rounds).
                  Deterministic; this is the primary result.
  --output        jitter_scale>0, N seeds per cell. Wilson intervals only;
                  the map is what the claims rest on.

Usage:
  uv run python experiments/flip_sweep.py \
      --seeds 40..89 --output results/flip/flip_sweep.csv \
      --map-output results/flip/flip_map.csv
"""
import argparse
import csv
import os
import sys
from dataclasses import asdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flip import ARMS, run_flip_trial  # noqa: E402

RETRY_BUDGETS = (1, 2, 4, 8)
FIELDS = ("arm", "seed", "k", "incumbent_max_rounds", "jitter_scale",
          "flip_applied", "flip_time", "cap_before", "cap_after",
          "healthy_committed", "decided_by", "healthy_ttfc", "healthy_rounds",
          "incumbent_p1_quorums", "incumbent_committed", "decided_value",
          "nacks_seen_by_healthy")


def _row(res, jitter_scale):
    d = asdict(res)
    d["jitter_scale"] = jitter_scale
    d["cap_before"] = "" if d["cap_before"] is None else (
        f"({int(d['cap_before'][0])},{int(d['cap_before'][1])})")
    d["cap_after"] = "" if d["cap_after"] is None else (
        f"({int(d['cap_after'][0])},{int(d['cap_after'][1])})")
    return {f: ("" if d.get(f) is None else d[f]) for f in FIELDS}


def parse_seeds(spec):
    if ".." in spec:
        lo, hi = spec.split("..")
        return list(range(int(lo), int(hi) + 1))
    return [int(s) for s in spec.split(",") if s.strip()]


def write(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", default="40..89")
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--jitter-scale", type=float, default=1.0)
    ap.add_argument("--output", default="results/flip/flip_sweep.csv")
    ap.add_argument("--map-output", default="results/flip/flip_map.csv")
    args = ap.parse_args()

    # Deterministic primary map.
    map_rows = []
    for arm in ARMS:
        for mr in RETRY_BUDGETS:
            res = run_flip_trial(arm=arm, seed=0, k=args.k,
                                 incumbent_max_rounds=mr, jitter_scale=0.0)
            map_rows.append(_row(res, 0.0))
    write(args.map_output, map_rows)
    print(f"map: {len(map_rows)} rows -> {args.map_output}")

    # Stochastic sweep.
    seeds = parse_seeds(args.seeds)
    rows = []
    for arm in ARMS:
        for mr in RETRY_BUDGETS:
            for seed in seeds:
                res = run_flip_trial(arm=arm, seed=seed, k=args.k,
                                     incumbent_max_rounds=mr,
                                     jitter_scale=args.jitter_scale)
                rows.append(_row(res, args.jitter_scale))
    write(args.output, rows)
    print(f"sweep: {len(rows)} rows ({len(ARMS)} arms x "
          f"{len(RETRY_BUDGETS)} budgets x {len(seeds)} seeds) -> {args.output}")


if __name__ == "__main__":
    main()
