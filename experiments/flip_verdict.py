"""Score the flip sweep against the pre-registered predictions.

Applies the falsification criteria stated in
docs/superpowers/notes/2026-07-29-midround-flip-preregistration.md
mechanically, and prints a verdict per prediction. It does not decide what
the verdict means.
"""
import argparse
import csv
import math
import statistics
from collections import defaultdict


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    m = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((c - m) / d, (c + m) / d)


def overlap(a, b):
    return not (a[1] < b[0] or b[1] < a[0])


def load(path):
    with open(path) as fh:
        return list(csv.DictReader(fh))


def cell(rows, arm, budget):
    return [r for r in rows if r["arm"] == arm
            and int(r["incumbent_max_rounds"]) == budget]


def summarise(rows):
    n = len(rows)
    committed = sum(1 for r in rows if r["healthy_committed"] == "True")
    own = sum(1 for r in rows if r["decided_by"] == "healthy")
    ttfcs = [float(r["healthy_ttfc"]) for r in rows if r["healthy_ttfc"]]
    return {
        "n": n, "committed": committed, "own_value": own,
        "never": n - committed,
        "p_committed": committed / n if n else 0.0,
        "ci_committed": wilson(committed, n),
        "p_own": own / n if n else 0.0,
        "median_ttfc": statistics.median(ttfcs) if ttfcs else None,
        "max_ttfc": max(ttfcs) if ttfcs else None,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sweep", default="results/flip/flip_sweep.csv")
    ap.add_argument("--map", default="results/flip/flip_map.csv")
    args = ap.parse_args()

    sweep = load(args.sweep)
    mp = load(args.map)
    budgets = sorted({int(r["incumbent_max_rounds"]) for r in sweep})
    arms = sorted({r["arm"] for r in sweep})

    print("=" * 78)
    print("DETERMINISTIC PRIMARY MAP (jitter_scale=0)")
    print("=" * 78)
    print(f"{'arm':<22}{'budget':>7}{'cap_after':>13}{'attempt':>9}"
          f"{'value':>11}{'ttfc':>9}{'h_rnds':>7}{'nacks':>7}")
    for r in mp:
        t = f"{float(r['healthy_ttfc']):.3f}" if r["healthy_ttfc"] else "NEVER"
        print(f"{r['arm']:<22}{r['incumbent_max_rounds']:>7}"
              f"{r['cap_after'] or '-':>13}{r['healthy_committed']:>9}"
              f"{r['decided_by'] or '-':>11}{t:>9}"
              f"{r['healthy_rounds']:>7}{r['nacks_seen_by_healthy']:>7}")

    print()
    print("=" * 78)
    print(f"STOCHASTIC SWEEP  ({len(sweep)} rows, jitter on)")
    print("=" * 78)
    stats = {}
    print(f"{'arm':<22}{'budget':>7}{'P(attempt ok)':>15}"
          f"{'Wilson 95%':>20}{'P(own value)':>14}{'med ttfc':>10}")
    for arm in arms:
        for b in budgets:
            s = summarise(cell(sweep, arm, b))
            stats[(arm, b)] = s
            ci = f"[{s['ci_committed'][0]:.3f},{s['ci_committed'][1]:.3f}]"
            mt = f"{s['median_ttfc']:.3f}" if s["median_ttfc"] else "NEVER"
            print(f"{arm:<22}{b:>7}{s['p_committed']:>15.3f}{ci:>20}"
                  f"{s['p_own']:>14.3f}{mt:>10}")
        print()

    print("=" * 78)
    print("VERDICTS against the pre-registered criteria")
    print("=" * 78)

    # P1: treatment degrades relative to C2. Falsified if the Wilson
    # intervals overlap; refuted-in-reverse if treatment is strictly better.
    print("\nP1  treatment (1,0) degrades healthy commit relative to C2:")
    p1_any_degrade = False
    for b in budgets:
        t = stats[("treatment", b)]
        c = stats[("c2_phase1_fails", b)]
        ov = overlap(t["ci_committed"], c["ci_committed"])
        worse = t["p_committed"] < c["p_committed"]
        p1_any_degrade = p1_any_degrade or (worse and not ov)
        rel = ("treatment WORSE" if worse else
               "treatment BETTER" if t["p_committed"] > c["p_committed"]
               else "equal")
        print(f"    budget {b}: treatment {t['p_committed']:.3f} vs "
              f"C2 {c['p_committed']:.3f} | CIs "
              f"{'overlap' if ov else 'disjoint'} | {rel}")
    print(f"  -> P1 {'CONFIRMED' if p1_any_degrade else 'FALSIFIED'}"
          f" (no budget shows treatment significantly worse than C2)"
          if not p1_any_degrade else "  -> P1 CONFIRMED")

    # P2: C2 disruption is transient - healthy commits in the large majority.
    print("\nP2  C2 disruption is transient (healthy commits in most seeds):")
    p2_holds = True
    for b in budgets:
        c = stats[("c2_phase1_fails", b)]
        ok = c["p_committed"] >= 0.9
        p2_holds = p2_holds and ok
        print(f"    budget {b}: P(attempt ok)={c['p_committed']:.3f} "
              f"{'>=0.9' if ok else '<0.9  <-- NOT transient'}")
    print(f"  -> P2 {'CONFIRMED' if p2_holds else 'FALSIFIED'}")

    # P3: treatment with retries=1 does not degrade; discriminator is
    # whether treatment is budget-SENSITIVE at all.
    print("\nP3  treatment is retry-insensitive (harm is not the retry loop):")
    tvals = [stats[("treatment", b)]["p_committed"] for b in budgets]
    tttfc = [stats[("treatment", b)]["median_ttfc"] for b in budgets]
    flat = len(set(f"{v:.3f}" for v in tvals)) == 1
    print(f"    P(attempt ok) across budgets {budgets}: "
          f"{[f'{v:.3f}' for v in tvals]}")
    print(f"    median ttfc  across budgets {budgets}: "
          f"{[f'{v:.3f}' if v else 'NEVER' for v in tttfc]}")
    print(f"  -> P3 {'CONFIRMED' if flat else 'FALSIFIED'} "
          f"(treatment {'is' if flat else 'is NOT'} flat in retry budget)")

    # P4: timing of the Phase-1-failing state is irrelevant.
    print("\nP4  late (0,1) onset is indistinguishable from early (0,1):")
    p4_holds = True
    for b in budgets:
        a = stats[("c2_phase1_fails", b)]
        d = stats[("p4_late_phase1_fail", b)]
        ov = overlap(a["ci_committed"], d["ci_committed"])
        p4_holds = p4_holds and ov
        print(f"    budget {b}: early {a['p_committed']:.3f} vs "
              f"late {d['p_committed']:.3f} | CIs "
              f"{'overlap' if ov else 'DISJOINT'}")
    print(f"  -> P4 {'CONFIRMED' if p4_holds else 'FALSIFIED'}")

    # Direction check: which arm is actually the harmful one?
    print("\n" + "=" * 78)
    print("DIRECTION OF EFFECT (not pre-registered; reported as observed)")
    print("=" * 78)
    for b in budgets:
        ranked = sorted(arms, key=lambda a: stats[(a, b)]["p_committed"])
        print(f"  budget {b}, worst->best: " +
              " < ".join(f"{a}({stats[(a, b)]['p_committed']:.2f})"
                         for a in ranked))


if __name__ == "__main__":
    main()
