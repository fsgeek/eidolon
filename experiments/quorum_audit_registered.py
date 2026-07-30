"""Regenerate and score the preregistered crumbling-wall audits."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from itertools import combinations, product
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from experiments.quorum_audit import report_to_dict
from quorum_audit import audit_quorum_families
from quorums import CrumblingWallQuorum


TIERS = (
    ("0", "1", "2"),
    ("3",),
    ("4",),
    ("5", "6", "7", "8", "9"),
)
TIER_NAMES = ("Mars", "Moon", "LEO", "Earth")
UNIVERSE = tuple(node for tier in TIERS for node in tier)


def build_wall_families(
    phase2_threshold: int, initiator_tier: int
) -> tuple[list[list[str]], list[list[str]]]:
    """Construct the wall's explicit minimal families for one initiator."""
    earth_floor = len(TIERS[-1]) - phase2_threshold + 1
    phase1 = [
        list(non_anchor + earth)
        for non_anchor in product(*TIERS[initiator_tier:-1])
        for earth in combinations(TIERS[-1], earth_floor)
    ]
    phase2 = [list(quorum) for quorum in combinations(
        TIERS[-1], phase2_threshold
    )]
    return phase1, phase2


def _verify_live_predicates(
    phase2_threshold: int,
    initiator_tier: int,
    phase1: list[list[str]],
    phase2: list[list[str]],
) -> None:
    """Require the explicit construction to equal the simulator predicates."""
    numeric_tiers = [[int(node) for node in tier] for tier in TIERS]
    wall = CrumblingWallQuorum(
        numeric_tiers, phase2_threshold=phase2_threshold
    )
    phase1_sets = [frozenset(quorum) for quorum in phase1]
    phase2_sets = [frozenset(quorum) for quorum in phase2]
    for size in range(len(UNIVERSE) + 1):
        for members in combinations(UNIVERSE, size):
            connected = frozenset(members)
            explicit_r1 = any(quorum <= connected for quorum in phase1_sets)
            explicit_r2 = any(quorum <= connected for quorum in phase2_sets)
            numeric = {int(node) for node in connected}
            live_r1 = wall.is_phase1_quorum(numeric, initiator_tier)
            live_r2 = wall.is_phase2_quorum(numeric)
            if (explicit_r1, explicit_r2) != (live_r1, live_r2):
                raise AssertionError(
                    "explicit wall families disagree with live predicates: "
                    f"k={phase2_threshold}, tier={TIER_NAMES[initiator_tier]}, "
                    f"connected={sorted(connected)}"
                )


def _load_gradient_rows(path: Path) -> dict[tuple[int, str, str], dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        selected = [
            row for row in csv.DictReader(stream)
            if row["construction"] == "crumbling_wall_5_1_1_3"
            and int(row["k"]) in (4, 5)
            and row["state"] in ("(1,0)", "(0,1)")
        ]
    rows: dict[tuple[int, str, str], dict[str, str]] = {}
    for row in selected:
        key = (int(row["k"]), row["tier_name"], row["state"])
        if key in rows:
            raise ValueError(f"duplicate gradient row: {key!r}")
        rows[key] = row
    return rows


def run_registered_audits(gradient_csv: Path) -> dict[str, Any]:
    """Run all registered wall cases and reject any same-reading mismatch."""
    gradient = _load_gradient_rows(gradient_csv)
    registrations: list[dict[str, Any]] = []
    disagreements = 0

    for k in (4, 5):
        for tier_index, tier_name in enumerate(TIER_NAMES):
            phase1, phase2 = build_wall_families(k, tier_index)
            _verify_live_predicates(k, tier_index, phase1, phase2)
            for reading, pinned, csv_column in (
                ("unconstrained", [], "reachable_unconstrained"),
                ("self-reachable", [TIERS[tier_index][0]],
                 "reachable_self_reachable"),
            ):
                report = audit_quorum_families(
                    UNIVERSE,
                    phase1,
                    phase2,
                    pinned=pinned,
                    exhaustive=True,
                )
                audit = report_to_dict(report)
                csv_gaps: dict[str, bool] = {}
                for state in ("(1,0)", "(0,1)"):
                    key = (k, tier_name, state)
                    if key not in gradient:
                        raise ValueError(f"missing gradient row: {key!r}")
                    expected = gradient[key][csv_column] == "1"
                    observed = audit["gaps"][state] is not None
                    csv_gaps[state] = expected
                    if observed != expected:
                        disagreements += 1
                        raise AssertionError(
                            "registered wall/CSV disagreement: "
                            f"k={k}, tier={tier_name}, reading={reading}, "
                            f"state={state}, auditor={observed}, csv={expected}"
                        )
                registrations.append(
                    {
                        "k": k,
                        "initiator_tier": tier_name,
                        "reading": reading,
                        "csv_column": csv_column,
                        "csv_gaps": csv_gaps,
                        "audit": audit,
                    }
                )

    return {
        "artifact": "registered generic quorum-family audits",
        "gradient_source": gradient_csv.as_posix(),
        "model": {
            "construction": "crumbling_wall_5_1_1_3",
            "tier_order": list(TIER_NAMES),
            "tiers": [list(tier) for tier in TIERS],
            "self_reachable_rule": "pin first node of initiating tier",
        },
        "registrations": registrations,
        "summary": {
            "exhaustive_self_checks": sum(
                case["audit"]["self_check_passed"] is True
                for case in registrations
            ),
            "registered_cases": len(registrations),
            "wall_csv_disagreements": disagreements,
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Regenerate preregistered wall audits and score the CSV"
    )
    parser.add_argument(
        "--gradient-csv",
        type=Path,
        default=Path("results/capability/dual_gradient_map.csv"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/capability/quorum_audit_registered.json"),
    )
    args = parser.parse_args(argv)

    try:
        artifact = run_registered_audits(args.gradient_csv)
    except (OSError, ValueError, AssertionError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(artifact, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        f"{artifact['summary']['registered_cases']} registered cases; "
        f"{artifact['summary']['wall_csv_disagreements']} wall/CSV disagreements "
        f"-> {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
