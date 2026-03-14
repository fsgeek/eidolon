"""Liveness envelope sweep for Step 9.

Explores success under varying global timeout and blackout duration.
Outputs:
- raw per-seed CSV
- aggregated mean/95% CI CSV
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from statistics import mean, stdev

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from demo_step_9 import ExperimentConfig, run_conjunction_experiment


def _parse_float_list(values: str) -> list[float]:
    return [float(v.strip()) for v in values.split(",") if v.strip()]


def _parse_int_list(values: str) -> list[int]:
    return [int(v.strip()) for v in values.split(",") if v.strip()]


def _mean_ci95(values: list[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    if len(values) == 1:
        return values[0], 0.0
    mu = mean(values)
    sigma = stdev(values)
    ci95 = 1.96 * sigma / (len(values) ** 0.5)
    return mu, ci95


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mars-latency-s", type=float, default=186.0)
    parser.add_argument("--timeout-s", type=str, default="120,240,360,500,720")
    parser.add_argument("--blackout-durations-s", type=str, default="300,900,1800")
    parser.add_argument("--blackout-start-s", type=float, default=600.0)
    parser.add_argument("--sim-end-s", type=float, default=4000.0)
    parser.add_argument("--reconcile-interval-s", type=float, default=120.0)
    parser.add_argument("--global-max-rounds", type=int, default=1)
    parser.add_argument(
        "--seeds",
        type=str,
        default="40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89",
    )
    parser.add_argument("--output", type=str, default="results/step9/step9_liveness.csv")
    parser.add_argument(
        "--aggregate-output",
        type=str,
        default="results/step9/step9_liveness_ci.csv",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    timeouts = _parse_float_list(args.timeout_s)
    blackout_durations = _parse_float_list(args.blackout_durations_s)
    seeds = _parse_int_list(args.seeds)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    agg_out = Path(args.aggregate_output)
    agg_out.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        "scenario",
        "mars_base_latency_s",
        "global_timeout_s",
        "blackout_duration_s",
        "seed",
        "global_pre_success",
        "global_pre_total",
        "global_during_success",
        "global_during_total",
        "global_post_success",
        "global_post_total",
        "global_during_rate",
        "global_post_rate",
        "first_success_after_blackout_s",
        "avg_global_latency_s",
    ]
    rows = []

    for timeout in timeouts:
        for blackout_duration in blackout_durations:
            for seed in seeds:
                cfg = ExperimentConfig(
                    mars_base_latency_s=args.mars_latency_s,
                    blackout_start_s=args.blackout_start_s,
                    blackout_duration_s=blackout_duration,
                    sim_end_s=args.sim_end_s,
                    reconcile_interval_s=args.reconcile_interval_s,
                    global_timeout_s=timeout,
                    global_max_rounds=args.global_max_rounds,
                    seed=seed,
                )
                for with_repeater in (False, True):
                    result = run_conjunction_experiment(
                        with_repeater=with_repeater,
                        cfg=cfg,
                        verbose=False,
                    )
                    during_rate = (
                        result.during_blackout.success / result.during_blackout.total
                        if result.during_blackout.total > 0
                        else 0.0
                    )
                    post_rate = (
                        result.post_blackout.success / result.post_blackout.total
                        if result.post_blackout.total > 0
                        else 0.0
                    )
                    rows.append(
                        [
                            result.name,
                            cfg.mars_base_latency_s,
                            cfg.global_timeout_s,
                            cfg.blackout_duration_s,
                            cfg.seed,
                            result.pre_blackout.success,
                            result.pre_blackout.total,
                            result.during_blackout.success,
                            result.during_blackout.total,
                            result.post_blackout.success,
                            result.post_blackout.total,
                            f"{during_rate:.6f}",
                            f"{post_rate:.6f}",
                            (
                                f"{result.first_success_after_blackout_s:.6f}"
                                if result.first_success_after_blackout_s is not None
                                else ""
                            ),
                            (
                                f"{result.avg_global_latency_s:.6f}"
                                if result.avg_global_latency_s is not None
                                else ""
                            ),
                        ]
                    )

    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    grouped = {}
    for row in rows:
        key = (row[0], float(row[2]), float(row[3]))
        grouped.setdefault(key, []).append(row)

    agg_headers = [
        "scenario",
        "global_timeout_s",
        "blackout_duration_s",
        "n_seeds",
        "global_during_rate_mean",
        "global_during_rate_ci95",
        "global_post_rate_mean",
        "global_post_rate_ci95",
        "first_success_after_blackout_s_mean",
        "first_success_after_blackout_s_ci95",
        "first_success_after_blackout_s_n",
        "avg_global_latency_s_mean",
        "avg_global_latency_s_ci95",
        "avg_global_latency_s_n",
    ]
    agg_rows = []
    for (scenario, timeout, blackout_duration), bucket in sorted(grouped.items()):
        during = [float(r[11]) for r in bucket]
        post = [float(r[12]) for r in bucket]
        recovery = [float(r[13]) for r in bucket if r[13] != ""]
        lat = [float(r[14]) for r in bucket if r[14] != ""]
        during_mu, during_ci = _mean_ci95(during)
        post_mu, post_ci = _mean_ci95(post)
        rec_mu, rec_ci = _mean_ci95(recovery)
        lat_mu, lat_ci = _mean_ci95(lat)
        agg_rows.append(
            [
                scenario,
                timeout,
                blackout_duration,
                len(bucket),
                f"{during_mu:.6f}" if during_mu is not None else "",
                f"{during_ci:.6f}" if during_ci is not None else "",
                f"{post_mu:.6f}" if post_mu is not None else "",
                f"{post_ci:.6f}" if post_ci is not None else "",
                f"{rec_mu:.6f}" if rec_mu is not None else "",
                f"{rec_ci:.6f}" if rec_ci is not None else "",
                len(recovery),
                f"{lat_mu:.6f}" if lat_mu is not None else "",
                f"{lat_ci:.6f}" if lat_ci is not None else "",
                len(lat),
            ]
        )

    with agg_out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(agg_headers)
        writer.writerows(agg_rows)

    print(f"Wrote liveness raw CSV: {out}")
    print(f"Wrote liveness aggregate CSV: {agg_out}")
    print()
    print(
        f"{'Scenario':>14} {'Timeout(s)':>12} {'Blackout(s)':>12} "
        f"{'During rate':>14} {'Post rate':>12}"
    )
    for row in agg_rows:
        during = f"{100*float(row[4]):.1f}±{100*float(row[5]):.1f}%" if row[4] else "n/a"
        post = f"{100*float(row[6]):.1f}±{100*float(row[7]):.1f}%" if row[6] else "n/a"
        print(f"{row[0]:>14} {float(row[1]):>12.1f} {float(row[2]):>12.1f} {during:>14} {post:>12}")


if __name__ == "__main__":
    main()
