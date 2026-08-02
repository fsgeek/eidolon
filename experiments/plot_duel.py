"""Working plot for the duel map: outcome and cost vs offset, per condition.

Reading aid for review — NOT the paper figure. Follows the house style of
experiments/plot_step9.py (matplotlib, SVG out).
"""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUTCOME_Y = {"earth_commit": 3, "leo_commit": 2, "leo_blocked": 1,
             "no_decision": 1, "livelock": 0.5, "censored": 0}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path,
                    default=Path("results/duel/duel_map.csv"))
    ap.add_argument("--output", type=Path,
                    default=Path("results/duel/plots/duel_map.svg"))
    ap.add_argument("--zoom", type=float, nargs=2, default=(-12.0, 6.0),
                    help="offset window to display")
    args = ap.parse_args()

    rows = [r for r in csv.DictReader(args.input.open())
            if r["leo_enabled"] == "1"]
    conds = sorted({(r["polarity"], r["k"], r["earth_max_rounds"])
                    for r in rows})
    fig, axes = plt.subplots(len(conds), 1, figsize=(9, 2.2 * len(conds)),
                             sharex=True)
    if len(conds) == 1:
        axes = [axes]
    lo, hi = args.zoom
    for ax, cond in zip(axes, conds):
        pol, k, retries = cond
        sel = sorted((float(r["offset"]), r) for r in rows
                     if (r["polarity"], r["k"], r["earth_max_rounds"]) == cond
                     and lo <= float(r["offset"]) <= hi)
        xs = [o for o, _ in sel]
        ys = [OUTCOME_Y.get(r["outcome"], 0) for _, r in sel]
        ax.step(xs, ys, where="mid", lw=0.8)
        lat_x = [o for o, r in sel if r["earth_commit_latency_s"]]
        lat_y = [float(r["earth_commit_latency_s"]) for _, r in sel
                 if r["earth_commit_latency_s"]]
        ax2 = ax.twinx()
        ax2.plot(lat_x, lat_y, ".", ms=2, alpha=0.5)
        ax2.set_ylabel("commit s", fontsize=7)
        ax.set_yticks(list(set(OUTCOME_Y.values())))
        ax.set_ylim(-0.3, 3.3)
        ax.set_ylabel(f"{pol}\nk={k} r={retries}", fontsize=7)
    axes[-1].set_xlabel("offset = leo_start - earth_start (s)")
    fig.suptitle("Duel map: outcome (step) and Earth commit latency (dots)")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(args.output)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
