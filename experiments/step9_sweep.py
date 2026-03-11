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
    parser.add_argument("--global-timeout-s", type=float, default=500.0)
    parser.add_argument("--global-max-rounds", type=int, default=1)
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
        "global_timeout_s",
        "global_max_rounds",
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
        "earth_local_avg_latency_s",
        "earth_local_p95_latency_s",
        "mars_local_avg_latency_s",
        "mars_local_p95_latency_s",
        "global_phase1_responses_earth",
        "global_phase1_responses_leo",
        "global_phase1_responses_moon",
        "global_phase1_responses_mars",
        "global_phase2_responses_earth",
        "global_phase2_responses_leo",
        "global_phase2_responses_moon",
        "global_phase2_responses_mars",
        "network_src_sent_earth",
        "network_src_sent_leo",
        "network_src_sent_moon",
        "network_src_sent_mars",
        "network_src_avg_oneway_s_earth",
        "network_src_avg_oneway_s_leo",
        "network_src_avg_oneway_s_moon",
        "network_src_avg_oneway_s_mars",
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
                    global_timeout_s=args.global_timeout_s,
                    global_max_rounds=args.global_max_rounds,
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
                            cfg.global_timeout_s,
                            cfg.global_max_rounds,
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
                            f"{r.earth_local_avg_latency_s:.6f}" if r.earth_local_avg_latency_s is not None else "",
                            f"{r.earth_local_p95_latency_s:.6f}" if r.earth_local_p95_latency_s is not None else "",
                            f"{r.mars_local_avg_latency_s:.6f}" if r.mars_local_avg_latency_s is not None else "",
                            f"{r.mars_local_p95_latency_s:.6f}" if r.mars_local_p95_latency_s is not None else "",
                            r.global_phase1_responses_earth,
                            r.global_phase1_responses_leo,
                            r.global_phase1_responses_moon,
                            r.global_phase1_responses_mars,
                            r.global_phase2_responses_earth,
                            r.global_phase2_responses_leo,
                            r.global_phase2_responses_moon,
                            r.global_phase2_responses_mars,
                            r.network_src_sent_earth,
                            r.network_src_sent_leo,
                            r.network_src_sent_moon,
                            r.network_src_sent_mars,
                            f"{r.network_src_avg_oneway_s_earth:.6f}" if r.network_src_avg_oneway_s_earth is not None else "",
                            f"{r.network_src_avg_oneway_s_leo:.6f}" if r.network_src_avg_oneway_s_leo is not None else "",
                            f"{r.network_src_avg_oneway_s_moon:.6f}" if r.network_src_avg_oneway_s_moon is not None else "",
                            f"{r.network_src_avg_oneway_s_mars:.6f}" if r.network_src_avg_oneway_s_mars is not None else "",
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
        "earth_local_avg_latency_s_mean",
        "earth_local_avg_latency_s_ci95",
        "earth_local_p95_latency_s_mean",
        "earth_local_p95_latency_s_ci95",
        "mars_local_avg_latency_s_mean",
        "mars_local_avg_latency_s_ci95",
        "mars_local_p95_latency_s_mean",
        "mars_local_p95_latency_s_ci95",
        "global_phase1_earth_share_mean",
        "global_phase1_earth_share_ci95",
        "global_phase2_earth_share_mean",
        "global_phase2_earth_share_ci95",
        "network_src_sent_earth_share_mean",
        "network_src_sent_earth_share_ci95",
    ]

    aggregate_rows = []
    for (scenario, mars_latency, blackout_duration), bucket in sorted(grouped.items()):
        earth_rates = [int(r[9]) / max(1, int(r[10])) for r in bucket]
        mars_rates = [int(r[11]) / max(1, int(r[12])) for r in bucket]
        pre_rates = [int(r[13]) / max(1, int(r[14])) for r in bucket]
        during_rates = [int(r[15]) / max(1, int(r[16])) for r in bucket]
        post_rates = [int(r[17]) / max(1, int(r[18])) for r in bucket]

        recovery_vals = [float(r[19]) for r in bucket if r[19] != ""]
        latency_vals = [float(r[20]) for r in bucket if r[20] != ""]
        earth_avg_lat_vals = [float(r[21]) for r in bucket if r[21] != ""]
        earth_p95_lat_vals = [float(r[22]) for r in bucket if r[22] != ""]
        mars_avg_lat_vals = [float(r[23]) for r in bucket if r[23] != ""]
        mars_p95_lat_vals = [float(r[24]) for r in bucket if r[24] != ""]

        p1_earth_share_vals = []
        p2_earth_share_vals = []
        src_earth_share_vals = []
        for r in bucket:
            p1_earth = int(r[25]); p1_leo = int(r[26]); p1_moon = int(r[27]); p1_mars = int(r[28])
            p2_earth = int(r[29]); p2_leo = int(r[30]); p2_moon = int(r[31]); p2_mars = int(r[32])
            src_earth = int(r[33]); src_leo = int(r[34]); src_moon = int(r[35]); src_mars = int(r[36])
            p1_total = p1_earth + p1_leo + p1_moon + p1_mars
            p2_total = p2_earth + p2_leo + p2_moon + p2_mars
            src_total = src_earth + src_leo + src_moon + src_mars
            if p1_total > 0:
                p1_earth_share_vals.append(p1_earth / p1_total)
            if p2_total > 0:
                p2_earth_share_vals.append(p2_earth / p2_total)
            if src_total > 0:
                src_earth_share_vals.append(src_earth / src_total)

        earth_mu, earth_ci = _mean_ci95(earth_rates)
        mars_mu, mars_ci = _mean_ci95(mars_rates)
        pre_mu, pre_ci = _mean_ci95(pre_rates)
        during_mu, during_ci = _mean_ci95(during_rates)
        post_mu, post_ci = _mean_ci95(post_rates)
        recovery_mu, recovery_ci = _mean_ci95(recovery_vals)
        latency_mu, latency_ci = _mean_ci95(latency_vals)
        earth_avg_mu, earth_avg_ci = _mean_ci95(earth_avg_lat_vals)
        earth_p95_mu, earth_p95_ci = _mean_ci95(earth_p95_lat_vals)
        mars_avg_mu, mars_avg_ci = _mean_ci95(mars_avg_lat_vals)
        mars_p95_mu, mars_p95_ci = _mean_ci95(mars_p95_lat_vals)
        p1_share_mu, p1_share_ci = _mean_ci95(p1_earth_share_vals)
        p2_share_mu, p2_share_ci = _mean_ci95(p2_earth_share_vals)
        src_share_mu, src_share_ci = _mean_ci95(src_earth_share_vals)

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
                f"{earth_avg_mu:.6f}" if earth_avg_mu is not None else "",
                f"{earth_avg_ci:.6f}" if earth_avg_ci is not None else "",
                f"{earth_p95_mu:.6f}" if earth_p95_mu is not None else "",
                f"{earth_p95_ci:.6f}" if earth_p95_ci is not None else "",
                f"{mars_avg_mu:.6f}" if mars_avg_mu is not None else "",
                f"{mars_avg_ci:.6f}" if mars_avg_ci is not None else "",
                f"{mars_p95_mu:.6f}" if mars_p95_mu is not None else "",
                f"{mars_p95_ci:.6f}" if mars_p95_ci is not None else "",
                f"{p1_share_mu:.6f}" if p1_share_mu is not None else "",
                f"{p1_share_ci:.6f}" if p1_share_ci is not None else "",
                f"{p2_share_mu:.6f}" if p2_share_mu is not None else "",
                f"{p2_share_ci:.6f}" if p2_share_ci is not None else "",
                f"{src_share_mu:.6f}" if src_share_mu is not None else "",
                f"{src_share_ci:.6f}" if src_share_ci is not None else "",
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
