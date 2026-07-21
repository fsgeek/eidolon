"""Capability map over the spec's minimum scenario basis.

Runs the static classifier over each scenario in the design spec's
Scenario Admission Rule table and emits one row per (scenario,
initiating tier): R1, R2, hazard flags, authority dependence, and the
inputs (threshold, reachable set) plus witnesses needed to
independently reconstruct every row from the CSV alone.

Usage:
    uv run python experiments/capability_map.py \
        --output results/capability/capability_map.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from capability import classify, format_missing
from quorums import CrumblingWallQuorum

MARS = [100, 101, 102]
MOON = [200]
LEO = [300]
EARTH = [1, 2, 3, 4, 5]
ALL_NODES = set(MARS + MOON + LEO + EARTH)
TIER_NAMES = ["mars", "moon", "leo", "earth"]

# LEO reaches its satellite plus three of five Earth ground stations
# (sparse topology of experiments/tier_liveness_sweep.py).
SPARSE_LEO_REACH = {300, 1, 2, 3}


def _ids(nodes) -> str:
    return ";".join(str(n) for n in sorted(nodes)) if nodes else ""


def build_scenarios() -> list[tuple[str, int, dict[int, set[int]]]]:
    """(name, phase2_threshold, reachable set per initiator tier)."""
    scenarios: list[tuple[str, int, dict[int, set[int]]]] = []
    for k in (5, 4, 3):
        scenarios.append((f"full_reachability_k{k}", k,
                          {t: set(ALL_NODES) for t in range(4)}))
        scenarios.append((f"sparse_leo_k{k}", k, {2: SPARSE_LEO_REACH}))
    # Broken INTERMEDIATE Phase 1 row with the Earth anchor reachable:
    # Moon keeps its own row and Earth; only the LEO row is missing.
    scenarios.append(("moon_row_broken_k5", 5, {1: set(MOON) | set(EARTH)}))
    # Hard upper-tier cut: Mars conjunction blackout.
    scenarios.append(("mars_conjunction_k5", 5,
                      {t: (set(MARS) if t == 0 else ALL_NODES - set(MARS))
                       for t in range(4)}))
    return scenarios


def run() -> list[dict]:
    rows = []
    for name, k, reach_by_tier in build_scenarios():
        wall = CrumblingWallQuorum([MARS, MOON, LEO, EARTH],
                                   phase2_threshold=k)
        for tier, reachable in sorted(reach_by_tier.items()):
            report = classify(wall, tier, reachable)
            rows.append({
                "scenario": name,
                "initiator_tier": TIER_NAMES[tier],
                "phase2_threshold": k,
                "reachable": _ids(reachable),
                "r1": int(report.r1),
                "r2": int(report.r2),
                "r1_witness": _ids(report.r1_witness),
                "r2_witness": _ids(report.r2_witness),
                "requires_preexisting_authority":
                    int(report.requires_preexisting_authority),
                "hazards": ";".join(h.value for h in report.hazards),
                "missing": " | ".join(format_missing(report)),
            })
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Static capability map over the scenario basis")
    parser.add_argument(
        "--output", type=Path,
        default=Path("results/capability/capability_map.csv"))
    args = parser.parse_args()

    rows = run()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    for row in rows:
        print(f"{row['scenario']:24} {row['initiator_tier']:6} "
              f"R1={row['r1']} R2={row['r2']}  {row['hazards']}")
    print(f"\n{len(rows)} rows -> {args.output}")


if __name__ == "__main__":
    main()
