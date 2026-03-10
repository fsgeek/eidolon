"""Parameter sweep for Step 9 conjunction experiments.

Outputs a CSV with baseline (hard blackout) vs repeater scenarios across:
- Mars one-way latency values
- Blackout duration values
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from demo_step_9 import ExperimentConfig, compare_blackout_vs_repeater


def _parse_list(values: str) -> list[float]:
    return [float(v.strip()) for v in values.split(",") if v.strip()]


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mars-latencies-s", type=str, default="186,750,1342")
    parser.add_argument("--blackout-durations-s", type=str, default="300,900,1800")
    parser.add_argument("--blackout-start-s", type=float, default=600.0)
    parser.add_argument("--sim-end-s", type=float, default=4000.0)
    parser.add_argument("--reconcile-interval-s", type=float, default=120.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--output",
        type=str,
        default="results/step9/step9_sweep.csv",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    mars_latencies = _parse_list(args.mars_latencies_s)
    blackout_durations = _parse_list(args.blackout_durations_s)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        "scenario",
        "mars_base_latency_s",
        "blackout_start_s",
        "blackout_duration_s",
        "sim_end_s",
        "reconcile_interval_s",
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
            cfg = ExperimentConfig(
                mars_base_latency_s=mars_latency,
                blackout_start_s=args.blackout_start_s,
                blackout_duration_s=blackout_duration,
                sim_end_s=args.sim_end_s,
                reconcile_interval_s=args.reconcile_interval_s,
                seed=args.seed,
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

    print(f"Wrote sweep results: {output}")
    print()
    print(f"{'Mars latency(s)':>15} {'Blackout(s)':>12} {'Scenario':>14} {'During':>12} {'Recovery(s)':>12}")
    for row in rows:
        during = f"{row[12]}/{row[13]}"
        recovery = row[16] if row[16] else "n/a"
        print(f"{float(row[1]):>15.1f} {float(row[3]):>12.1f} {row[0]:>14} {during:>12} {recovery:>12}")


if __name__ == "__main__":
    main()
