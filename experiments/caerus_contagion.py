"""Sweep for the pre-registered Caerus (0,1) contagion predictions.

Pre-registration: docs/superpowers/notes/2026-10-11-caerus-contagion-preregistration.md

    uv run python experiments/caerus_contagion.py --output results/caerus/contagion.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from caerus_model import Config, run, unaffected_goodput  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--p-mr", default="0,0.01,0.05,0.1,0.3")
    ap.add_argument("--policies", default="submit,reject")
    ap.add_argument("--seeds", default="0,1,2,3,4")
    ap.add_argument("--output", default="results/caerus/contagion.csv")
    args = ap.parse_args()

    rows = []
    for policy in args.policies.split(","):
        for p in (float(x) for x in args.p_mr.split(",")):
            for seed in (int(s) for s in args.seeds.split(",")):
                cfg = Config(p_mr=p, policy=policy, seed=seed)
                txns = run(cfg)
                for start, g in unaffected_goodput(txns, cfg):
                    rows.append(dict(policy=policy, p_mr=p, seed=seed,
                                     window_start_s=start, unaffected_goodput=round(g, 4)))
                print(f"{policy} p_mr={p} seed={seed} done", file=sys.stderr)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main()
