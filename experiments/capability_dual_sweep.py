#!/usr/bin/env python3
"""Enumerate the (0,1) dual and the tier-gradient.

Pre-registration: docs/superpowers/notes/2026-07-30-dual-and-gradient-
preregistration.md, committed at bf45d2a before this file existed.

Scores P1 (dual characterization), P2 (uniform corollaries) and P3 (the
tier-gradient) mechanically. Calls the repository's own predicates --
quorums.CrumblingWallQuorum, paxos.FlexibleQuorum, capability.classify --
never a reimplementation, per the pre-registration's invalidation clause.

Deterministic: no RNG, iteration over sorted structures only.
"""

from __future__ import annotations

import csv
import itertools
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from capability import classify                      # noqa: E402
from paxos import FlexibleQuorum                     # noqa: E402
from quorums import CrumblingWallQuorum              # noqa: E402

# 5/1/1/3 deployed topology. Code convention: tier 0 = Mars (top of wall),
# last tier = Earth (anchor / fast tier).
MARS = [0, 1, 2]
MOON = [3]
LEO = [4]
EARTH = [5, 6, 7, 8, 9]
TIERS = [MARS, MOON, LEO, EARTH]
TIER_NAMES = ["Mars", "Moon", "LEO", "Earth"]
ALL_NODES = MARS + MOON + LEO + EARTH

STATES = ["(1,1)", "(1,0)", "(0,1)", "(0,0)"]


def _subsets(nodes):
    """All subsets, in a deterministic order."""
    nodes = sorted(nodes)
    for r in range(len(nodes) + 1):
        for combo in itertools.combinations(nodes, r):
            yield frozenset(combo)


def _state(r1: bool, r2: bool) -> str:
    return f"({int(r1)},{int(r2)})"


def sweep_wall():
    """P3 + the cross-check. Returns (rows, mismatches, monotonicity_ok)."""
    rows = []
    mismatches = []
    monotonicity_ok = True

    for k in range(1, len(EARTH) + 1):
        wall = CrumblingWallQuorum(TIERS, phase2_threshold=k)
        for i in range(len(TIERS)):
            own_tier = set(TIERS[i])
            # state -> reachable under (unconstrained, self-reachable)
            seen_any = set()
            seen_self = set()
            for S in _subsets(ALL_NODES):
                r1 = wall.is_phase1_quorum(set(S), i)
                r2 = wall.is_phase2_quorum(set(S))

                # Independent in-repo recomputation (obligation-based).
                rep = classify(wall, i, set(S))
                if (rep.r1, rep.r2) != (r1, r2):
                    mismatches.append((k, i, sorted(S), (r1, r2),
                                       (rep.r1, rep.r2)))

                st = _state(r1, r2)
                seen_any.add(st)
                if S & own_tier:
                    seen_self.add(st)

            for st in STATES:
                rows.append({
                    "construction": "crumbling_wall_5_1_1_3",
                    "k": k,
                    "initiator_tier": i,
                    "tier_name": TIER_NAMES[i],
                    "state": st,
                    "reachable_unconstrained": int(st in seen_any),
                    "reachable_self_reachable": int(st in seen_self),
                    "min_earth_in_q1": wall.min_earth_in_q1,
                    "phase2_threshold": wall.phase2_threshold,
                })

        # Monotonicity: adding a node never revokes a capability.
        for i in range(len(TIERS)):
            for S in _subsets(ALL_NODES):
                for extra in sorted(set(ALL_NODES) - S):
                    T = S | {extra}
                    if (wall.is_phase1_quorum(set(S), i)
                            and not wall.is_phase1_quorum(set(T), i)):
                        monotonicity_ok = False
                    if (wall.is_phase2_quorum(set(S))
                            and not wall.is_phase2_quorum(set(T))):
                        monotonicity_ok = False

    return rows, mismatches, monotonicity_ok


def containment_check():
    """P1, computed as set containment, not as the reachability tautology.

    'Every Q2 contains some Q1' is evaluated over minimal Q2 sets by
    searching their subsets for a Q1 -- a different computation from
    'no connectivity state yields (0,1)'. Reporting both tests whether
    the characterization is an equivalence or merely a restatement.
    """
    rows = []
    for k in range(1, len(EARTH) + 1):
        wall = CrumblingWallQuorum(TIERS, phase2_threshold=k)
        minimal_q2 = [frozenset(c) for c in itertools.combinations(sorted(EARTH), k)]
        for i in range(len(TIERS)):
            every_q2_contains_a_q1 = True
            for q2 in minimal_q2:
                if not any(wall.is_phase1_quorum(set(T), i)
                           for T in _subsets(q2)):
                    every_q2_contains_a_q1 = False
                    break
            dual_empty = not any(
                wall.is_phase2_quorum(set(S))
                and not wall.is_phase1_quorum(set(S), i)
                for S in _subsets(ALL_NODES)
            )
            rows.append({
                "k": k,
                "initiator_tier": i,
                "tier_name": TIER_NAMES[i],
                "every_q2_contains_a_q1": int(every_q2_contains_a_q1),
                "dual_01_empty": int(dual_empty),
                "agree": int(every_q2_contains_a_q1 == dual_empty),
            })
    return rows


def uniform_corollaries():
    """P2 over pure threshold families, via the repo's FlexibleQuorum."""
    rows = []
    for n in range(3, 8):
        nodes = list(range(n))
        for q1 in range(1, n + 1):
            for q2 in range(1, n + 1):
                if q1 + q2 <= n:
                    continue  # FlexibleQuorum rejects: no intersection
                qs = FlexibleQuorum(nodes, q1, q2)
                seen = set()
                for S in _subsets(nodes):
                    seen.add(_state(qs.is_phase1_quorum(set(S)),
                                    qs.is_phase2_quorum(set(S))))
                rows.append({
                    "n": n, "q1": q1, "q2": q2,
                    "hazard_10_reachable": int("(1,0)" in seen),
                    "hazard_01_reachable": int("(0,1)" in seen),
                    "pred_10_reachable": int(q1 < q2),
                    "pred_01_reachable": int(q2 < q1),
                    "p2_holds": int(("(1,0)" in seen) == (q1 < q2)
                                    and ("(0,1)" in seen) == (q2 < q1)),
                })
    return rows


def main():
    out = Path(__file__).resolve().parent.parent / "results" / "capability"
    out.mkdir(parents=True, exist_ok=True)

    wall_rows, mismatches, mono_ok = sweep_wall()
    cont_rows = containment_check()
    unif_rows = uniform_corollaries()

    for name, rows in (("dual_gradient_map.csv", wall_rows),
                       ("dual_containment.csv", cont_rows),
                       ("dual_uniform.csv", unif_rows)):
        path = out / name
        with path.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"wrote {path.relative_to(path.parents[2])}  ({len(rows)} rows)")

    print("\n=== VALIDITY GATES (pre-registration) ===")
    print(f"predicate cross-check mismatches : {len(mismatches)}"
          f"  {'OK' if not mismatches else 'INVALID'}")
    for m in mismatches[:5]:
        print(f"    k={m[0]} tier={m[1]} S={m[2]} direct={m[3]} classify={m[4]}")
    print(f"monotonicity of both predicates  : "
          f"{'OK' if mono_ok else 'VIOLATED'}")

    print("\n=== P2: uniform corollaries ===")
    bad = [r for r in unif_rows if not r["p2_holds"]]
    print(f"threshold configs tested: {len(unif_rows)}, violations: {len(bad)}")
    for r in bad[:5]:
        print(f"    n={r['n']} q1={r['q1']} q2={r['q2']}")

    print("\n=== P1: dual characterization ===")
    dis = [r for r in cont_rows if not r["agree"]]
    print(f"(k, tier) cells: {len(cont_rows)}, disagreements: {len(dis)}")
    for r in dis[:5]:
        print(f"    k={r['k']} {r['tier_name']}: containment="
              f"{r['every_q2_contains_a_q1']} dual_empty={r['dual_01_empty']}")

    print("\n=== P3: the tier-gradient — (0,1) reachability ===")
    print(f"{'k':>2}  " + "  ".join(f"{n:>6}" for n in TIER_NAMES)
          + "     (self-reachable variant)")
    for k in range(1, len(EARTH) + 1):
        cells_any, cells_self = [], []
        for i in range(len(TIERS)):
            r = next(x for x in wall_rows if x["k"] == k
                     and x["initiator_tier"] == i and x["state"] == "(0,1)")
            cells_any.append("YES" if r["reachable_unconstrained"] else ".")
            cells_self.append("YES" if r["reachable_self_reachable"] else ".")
        print(f"{k:>2}  " + "  ".join(f"{c:>6}" for c in cells_any)
              + "   |  " + "  ".join(f"{c:>6}" for c in cells_self))

    print("\n=== (1,0) reachability, for contrast (boundary theorem) ===")
    print(f"{'k':>2}  " + "  ".join(f"{n:>6}" for n in TIER_NAMES))
    for k in range(1, len(EARTH) + 1):
        cells = []
        for i in range(len(TIERS)):
            r = next(x for x in wall_rows if x["k"] == k
                     and x["initiator_tier"] == i and x["state"] == "(1,0)")
            cells.append("YES" if r["reachable_unconstrained"] else ".")
        print(f"{k:>2}  " + "  ".join(f"{c:>6}" for c in cells))


if __name__ == "__main__":
    main()
