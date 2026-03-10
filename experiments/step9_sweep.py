"""Parameter sweep for Step 9 conjunction experiments.

Outputs:
- Raw CSV (one row per scenario/parameter/seed run)
- Aggregate CSV with mean and 95% CI over seeds
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

from demo_step_9 import ExperimentConfig, compare_blackout_vs_repeater


def _parse_list(values: str) -> list[float]:
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
    parser.add_argument("--mars-latencies-s", type=str, default="186,750,1342")
    parser.add_argument("--blackout-durations-s", type=str, default="300,900,1800")
    parser.add_argument("--blackout-start-s", type=float, default=600.0)
    parser.add_argument("--sim-end-s", type=float, default=4000.0)
    parser.add_argument("--reconcile-interval-s", type=float, default=120.0)
    parser.add_argument("--seeds", type=str, default="40,41,42,43,44")
    parser.add_argument(
        "--output",
        type=str,
        default="results/step9/step9_sweep.csv",
    )
    parser.add_argument(
        "--aggregate-output",
        type=str,
        default="results/step9/step9_sweep_ci.csv",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    mars_latencies = _parse_list(args.mars_latencies_s)
    blackout_durations = _parse_list(args.blackout_durations_s)
    seeds = _parse_int_list(args.seeds)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    aggregate_output = Path(args.aggregate_output)
    aggregate_output.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        "scenario",
        "mars_base_latency_s",
        "blackout_start_s",
        "blackout_duration_s",
        "sim_end_s",
        "reconcile_interval_s",
        "seed",
        "earth_success",
        "earth_total",
        "mars_success",
        "mars_total",
        "global_pre_success",
        "global_pre_total",
        "global_during_success",
        "global_during_total",
        "global_post_success",
        "global_post_total",
        "first_success_after_blackout_s",
        "avg_global_latency_s",
    ]

    rows = []

    for mars_latency in mars_latencies:
        for blackout_duration in blackout_durations:
            for seed in seeds:
                cfg = ExperimentConfig(
                    mars_base_latency_s=mars_latency,
                    blackout_start_s=args.blackout_start_s,
                    blackout_duration_s=blackout_duration,
                    sim_end_s=args.sim_end_s,
                    reconcile_interval_s=args.reconcile_interval_s,
                    seed=seed,
                )
                baseline, repeater = compare_blackout_vs_repeater(cfg, verbose=False)
                for r in (baseline, repeater):
                    rows.append(
                        [
                            r.name,
                            cfg.mars_base_latency_s,
                            cfg.blackout_start_s,
                            cfg.blackout_duration_s,
                            cfg.sim_end_s,
                            cfg.reconcile_interval_s,
                            cfg.seed,
                            r.earth_success,
                            r.earth_total,
                            r.mars_success,
                            r.mars_total,
                            r.pre_blackout.success,
                            r.pre_blackout.total,
                            r.during_blackout.success,
                            r.during_blackout.total,
                            r.post_blackout.success,
                            r.post_blackout.total,
                            (
                                f"{r.first_success_after_blackout_s:.6f}"
                                if r.first_success_after_blackout_s is not None
                                else ""
                            ),
                            f"{r.avg_global_latency_s:.6f}" if r.avg_global_latency_s is not None else "",
                        ]
                    )

    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"Wrote raw sweep results: {output}")
    print()

    # Aggregate by (scenario, mars_latency, blackout_duration)
    grouped = {}
    for row in rows:
        key = (row[0], float(row[1]), float(row[3]))
        grouped.setdefault(key, []).append(row)

    agg_headers = [
        "scenario",
        "mars_base_latency_s",
        "blackout_duration_s",
        "n_seeds",
        "earth_success_rate_mean",
        "earth_success_rate_ci95",
        "mars_success_rate_mean",
        "mars_success_rate_ci95",
        "global_pre_rate_mean",
        "global_pre_rate_ci95",
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

    aggregate_rows = []
    for (scenario, mars_latency, blackout_duration), bucket in sorted(grouped.items()):
        earth_rates = [int(r[7]) / max(1, int(r[8])) for r in bucket]
        mars_rates = [int(r[9]) / max(1, int(r[10])) for r in bucket]
        pre_rates = [int(r[11]) / max(1, int(r[12])) for r in bucket]
        during_rates = [int(r[13]) / max(1, int(r[14])) for r in bucket]
        post_rates = [int(r[15]) / max(1, int(r[16])) for r in bucket]

        recovery_vals = [float(r[17]) for r in bucket if r[17] != ""]
        latency_vals = [float(r[18]) for r in bucket if r[18] != ""]

        earth_mu, earth_ci = _mean_ci95(earth_rates)
        mars_mu, mars_ci = _mean_ci95(mars_rates)
        pre_mu, pre_ci = _mean_ci95(pre_rates)
        during_mu, during_ci = _mean_ci95(during_rates)
        post_mu, post_ci = _mean_ci95(post_rates)
        recovery_mu, recovery_ci = _mean_ci95(recovery_vals)
        latency_mu, latency_ci = _mean_ci95(latency_vals)

        aggregate_rows.append(
            [
                scenario,
                mars_latency,
                blackout_duration,
                len(bucket),
                f"{earth_mu:.6f}" if earth_mu is not None else "",
                f"{earth_ci:.6f}" if earth_ci is not None else "",
                f"{mars_mu:.6f}" if mars_mu is not None else "",
                f"{mars_ci:.6f}" if mars_ci is not None else "",
                f"{pre_mu:.6f}" if pre_mu is not None else "",
                f"{pre_ci:.6f}" if pre_ci is not None else "",
                f"{during_mu:.6f}" if during_mu is not None else "",
                f"{during_ci:.6f}" if during_ci is not None else "",
                f"{post_mu:.6f}" if post_mu is not None else "",
                f"{post_ci:.6f}" if post_ci is not None else "",
                f"{recovery_mu:.6f}" if recovery_mu is not None else "",
                f"{recovery_ci:.6f}" if recovery_ci is not None else "",
                len(recovery_vals),
                f"{latency_mu:.6f}" if latency_mu is not None else "",
                f"{latency_ci:.6f}" if latency_ci is not None else "",
                len(latency_vals),
            ]
        )

    with aggregate_output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(agg_headers)
        writer.writerows(aggregate_rows)

    print(f"Wrote aggregate CI results: {aggregate_output}")
    print()
    print(
        f"{'Mars latency(s)':>15} {'Blackout(s)':>12} {'Scenario':>14} "
        f"{'During rate':>14} {'Recovery mean±CI(s)':>24}"
    )
    for row in aggregate_rows:
        during = (
            f"{100 * float(row[10]):.1f}±{100 * float(row[11]):.1f}%"
            if row[10] and row[11]
            else "n/a"
        )
        recovery = (
            f"{float(row[14]):.1f}±{float(row[15]):.1f}"
            if row[14] and row[15]
            else "n/a"
        )
        print(
            f"{float(row[1]):>15.1f} {float(row[2]):>12.1f} {row[0]:>14} "
            f"{during:>14} {recovery:>24}"
        )


if __name__ == "__main__":
    main()
