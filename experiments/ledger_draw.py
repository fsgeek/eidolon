"""Fifth and sixth systems for the restart ledger, continuing cloudflare's stamped draw.

Rule: docs/superpowers/notes/2026-10-26-restart-ledger-plan.md ("Two strata").
Source rule: cloudflare docs/hotos/measure/FOURTH-SYSTEM-SELECTION.md (seed 20261016).
Run once, in the commit after the rule.
"""

import random

POOL = [4, 7, 8, 10, 11, 14, 18, 19]  # survey sample order
NAMES = {4: "Morph", 7: "Stripeless", 8: "FDBKeeper", 10: "LeaseGuard",
         11: "Styx", 14: "Sonata", 18: "libDSE", 19: "OceanBase 2PC"}

r = random.Random(20261016)
fourth = r.choice(POOL)
assert fourth == 8, fourth
rest = [p for p in POOL if p != fourth]
fifth = r.choice(rest)
rest = [p for p in rest if p != fifth]
sixth = r.choice(rest)
for label, p in (("fourth", fourth), ("fifth", fifth), ("sixth", sixth)):
    print(f"{label}: {p} {NAMES[p]}")
