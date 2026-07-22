"""Dueling-proposer sweep: offset -> outcome map plus jitter robustness.

Primary (--mode map): fully deterministic (jitter 0, nothing random ever
drawn). One trial per (condition, offset). Presented as a MAP — never as
mean±CI: 50 identical deterministic replicas would manufacture ±0.0%
precision (premortem A4).

Secondary (--mode jitter): per-link-RNG stochastic sweep over seeds, on a
reduced offset set, aggregated with Wilson intervals and degenerate-cell
flags.

Result language (binding, from the spec's prohibitions): this measures
contention COST vs offset. No FLP claims, no Multi-Paxos authority
claims, no backoff-policy study.
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from duel import LIVELOCK_MIN_PREEMPTED_ROUNDS, DuelTrialResult, run_duel_trial

POLARITIES = ("leo_high", "earth_high")
KS = (5, 3)                 # k=4 omitted: capability-identical to k=5
EARTH_RETRIES = (1, 5)      # single-shot vs bounded-retry (premortem A6)
LEO_MAX_ROUNDS = 8

# Reduced offsets for the jitter sweep: full fine coverage is the
# deterministic map's job; here we sample the collision band and a few
# far points to bound jitter sensitivity.
JITTER_OFFSETS = [-10.0, -5.0, -2.0, -1.0, -0.5, -0.2, -0.05,
                  0.05, 0.2, 0.5, 1.0, 2.0, 30.0, 90.0]


def offset_grid(fine_lo: float = -12.0, fine_hi: float = 6.0,
                fine_step: float = 0.05, coarse_step: float = 5.0,
                coarse_hi: float = 118.0) -> list[float]:
    """Two-stage grid (premortem A9): fine band at sub-round resolution
    around offset 0 (exact 0 excluded, A10), coarse tail across the
    reconcile cadence to confirm the flat no-interaction region."""
    n = int(round((fine_hi - fine_lo) / fine_step))
    fine = [round(fine_lo + i * fine_step, 3) for i in range(n + 1)]
    fine = [o for o in fine if abs(o) >= fine_step / 2]
    start = fine_hi + coarse_step
    coarse = []
    o = start
    while o <= coarse_hi:
        coarse.append(round(o, 3))
        o += coarse_step
    return fine + coarse


def wilson_ci(successes: int, n: int, z: float = 1.96):
    """Wilson score interval — no ±0.0% certainty at the 0/1 boundary."""
    if n == 0:
        return (None, None)
    p = successes / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def trial_row(t: DuelTrialResult) -> dict:
    e, l = t.earth_result, t.leo_result
    return {
        "offset": t.offset,
        "polarity": t.polarity,
        "k": t.k,
        "earth_max_rounds": t.earth_max_rounds,
        "leo_max_rounds": t.leo_max_rounds,
        "jitter_scale": t.jitter_scale,
        "seed": t.seed,
        "leo_enabled": int(t.leo_enabled),
        "livelock_min_preempted_rounds": LIVELOCK_MIN_PREEMPTED_ROUNDS,
        "outcome": t.outcome,
        "decided_by": t.decided_by or "",
        "decided_ballot": t.decided_ballot if t.decided_ballot is not None else "",
        "rounds_overlapped": int(t.rounds_overlapped),
        "preempted_earth_rounds": t.preempted_earth_rounds,
        "preempted_leo_rounds": t.preempted_leo_rounds,
        "earth_success": int(bool(e and e.success)),
        "earth_rounds": e.rounds if e else "",
        "earth_p1_nacks": e.phase1_nacks if e else "",
        "earth_p2_nacks": e.phase2_nacks if e else "",
        "earth_late_nacks": t.earth_late_nacks,
        "earth_commit_latency_s": (f"{t.earth_commit_latency:.6f}"
                                   if t.earth_commit_latency is not None else ""),
        "leo_success": int(bool(l and l.success)),
        "leo_p1_quorums": l.phase1_quorums if l else "",
        "leo_p2_failures": l.phase2_failures if l else "",
        "leo_p1_nacks": l.phase1_nacks if l else "",
        "leo_p2_nacks": l.phase2_nacks if l else "",
        "leo_late_nacks": t.leo_late_nacks,
    }


FIELDNAMES = [
    "offset", "polarity", "k", "earth_max_rounds", "leo_max_rounds",
    "jitter_scale", "seed", "leo_enabled", "livelock_min_preempted_rounds",
    "outcome", "decided_by", "decided_ballot", "rounds_overlapped",
    "preempted_earth_rounds", "preempted_leo_rounds", "earth_success", "earth_rounds",
    "earth_p1_nacks", "earth_p2_nacks", "earth_late_nacks",
    "earth_commit_latency_s", "leo_success", "leo_p1_quorums",
    "leo_p2_failures", "leo_p1_nacks", "leo_p2_nacks", "leo_late_nacks",
]


def _write_rows(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(rows)


def run_map(output: Path):
    grid = offset_grid()
    rows = []
    total = len(POLARITIES) * len(KS) * len(EARTH_RETRIES)
    done = 0
    for polarity in POLARITIES:
        for k in KS:
            for retries in EARTH_RETRIES:
                for off in grid:
                    rows.append(trial_row(run_duel_trial(
                        offset=off, polarity=polarity, k=k,
                        earth_max_rounds=retries,
                        leo_max_rounds=LEO_MAX_ROUNDS)))
                done += 1
                print(f"  map {done}/{total}: {polarity} k={k} "
                      f"retries={retries} ({len(grid)} offsets)")
    # Baselines: offset-invariant under determinism -> one per condition.
    for k in KS:
        for retries in EARTH_RETRIES:
            rows.append(trial_row(run_duel_trial(
                offset=30.0, polarity="leo_high", k=k,
                earth_max_rounds=retries, leo_max_rounds=LEO_MAX_ROUNDS,
                leo_enabled=False)))
    _write_rows(output, rows)
    print(f"Wrote map: {output} ({len(rows)} rows)")


def run_jitter(output: Path, aggregate_output: Path, seeds: list[int]):
    rows = []
    for polarity in POLARITIES:
        for k in KS:
            for retries in EARTH_RETRIES:
                for off in JITTER_OFFSETS:
                    for seed in seeds:
                        rows.append(trial_row(run_duel_trial(
                            offset=off, polarity=polarity, k=k,
                            earth_max_rounds=retries,
                            leo_max_rounds=LEO_MAX_ROUNDS,
                            jitter_scale=1.0, seed=seed)))
    for k in KS:
        for retries in EARTH_RETRIES:
            for seed in seeds:
                rows.append(trial_row(run_duel_trial(
                    offset=30.0, polarity="leo_high", k=k,
                    earth_max_rounds=retries, leo_max_rounds=LEO_MAX_ROUNDS,
                    jitter_scale=1.0, seed=seed, leo_enabled=False)))
    _write_rows(output, rows)
    print(f"Wrote raw: {output} ({len(rows)} rows)")

    # Aggregate: earth_success rate per cell with Wilson interval and an
    # explicit degenerate flag instead of fake ±0.0 (premortem A4).
    cells: dict[tuple, list[dict]] = {}
    for r in rows:
        key = (r["polarity"], r["k"], r["earth_max_rounds"],
               r["leo_enabled"], r["offset"])
        cells.setdefault(key, []).append(r)
    agg_rows = []
    for (polarity, k, retries, enabled, off), cell in sorted(cells.items()):
        n = len(cell)
        s = sum(r["earth_success"] for r in cell)
        lo, hi = wilson_ci(s, n)
        lats = [float(r["earth_commit_latency_s"]) for r in cell
                if r["earth_commit_latency_s"] != ""]
        agg_rows.append({
            "polarity": polarity, "k": k, "earth_max_rounds": retries,
            "leo_enabled": enabled, "offset": off, "n": n,
            "earth_success_rate": f"{s / n:.6f}",
            "wilson_lo": f"{lo:.6f}", "wilson_hi": f"{hi:.6f}",
            "degenerate": int(s == 0 or s == n),
            "commit_latency_mean_s": (f"{sum(lats) / len(lats):.6f}"
                                      if lats else ""),
            "commit_latency_n": len(lats),
            "livelock_count": sum(r["outcome"] == "livelock" for r in cell),
            "censored_count": sum(r["outcome"] == "censored" for r in cell),
        })
    aggregate_output.parent.mkdir(parents=True, exist_ok=True)
    with aggregate_output.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(agg_rows[0].keys()))
        w.writeheader()
        w.writerows(agg_rows)
    print(f"Wrote aggregate: {aggregate_output} ({len(agg_rows)} cells)")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mode", choices=["map", "jitter"], required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--aggregate-output", type=Path, default=None)
    ap.add_argument("--seeds", type=str, default="")
    args = ap.parse_args()
    if args.mode == "map":
        run_map(args.output)
    else:
        seeds = [int(s) for s in args.seeds.split(",") if s.strip()]
        assert seeds, "--seeds required for jitter mode"
        assert args.aggregate_output is not None
        run_jitter(args.output, args.aggregate_output, seeds)


if __name__ == "__main__":
    main()
