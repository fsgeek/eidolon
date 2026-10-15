"""Analysis for the Caerus artifact region-failure campaign, fixed before data.

Pre-registration (incl. Amendment 2, which defines this statistic):
docs/superpowers/notes/2026-10-13-caerus-artifact-preregistration.md

    uv run python experiments/caerus_artifact_analyze.py results/caerus/artifact
"""

from __future__ import annotations

import datetime as dt
import json
import re
import statistics as st
import sys
from pathlib import Path

LIVE = tuple(int(x) for x in __import__("os").environ.get("LIVE_REGIONS", "0 1 3 4").split())
WINDOW = 30.0
_LINE = re.compile(r"^(\S+) .*SEQUENCER THROUGHPUT -- Sent ([0-9.]+)")


def _series(log: Path) -> list[tuple[float, float]]:
    out = []
    for line in log.read_text(errors="replace").splitlines():
        m = _LINE.match(line)
        if m:
            ts = dt.datetime.fromisoformat(m.group(1)[:26].rstrip("Z")).replace(tzinfo=dt.UTC)
            out.append((ts.timestamp(), float(m.group(2))))
    return out


def analyze_run(run: Path) -> dict:
    t_fail = float((run / "t_fail").read_text())
    t_end = float((run / "t_end").read_text())
    pmr = int(run.name.split("_")[0].removeprefix("pmr"))
    regions = {}
    for r in LIVE:
        pts = _series(run / f"r{r}.log")
        base_pts = [v for t, v in pts if t_fail - 60 <= t < t_fail]
        base = st.mean(base_pts) if base_pts else float("nan")
        n_win = int((t_end - t_fail) // WINDOW)
        windows, flagged = [], []
        for k in range(n_win):
            lo, hi = t_fail + k * WINDOW, t_fail + (k + 1) * WINDOW
            vals = [v for t, v in pts if lo <= t < hi]
            if not vals:
                windows.append(0.0)
                flagged.append(k)
            else:
                windows.append(st.mean(vals) / base)
        regions[r] = dict(baseline=base, windows=[round(w, 3) for w in windows], flagged=flagged)
    return dict(run=run.name, pmr=pmr, regions=regions,
                state=(run / "state").read_text().split("\n") if (run / "state").exists() else [])


def verdicts(res: dict) -> dict:
    pmr, out = res["pmr"], {}
    ws = {r: v["windows"] for r, v in res["regions"].items()}
    if pmr == 0:
        out["A1"] = all(all(w > 0.8 for w in w_) for w_ in ws.values())
    else:
        start_limit = 90 if pmr == 10 else 30
        kmax = int(start_limit // WINDOW)
        ok, persist = True, True
        for w_ in ws.values():
            hit = next((k for k, w in enumerate(w_) if w < 0.05), None)
            if hit is None or hit > kmax:
                ok = False
            if hit is None or any(w >= 0.05 for w in w_[hit:]):
                persist = False
        out["A2" if pmr == 10 else "A3"] = ok
        out["A4"] = persist
        k300 = int(300 // WINDOW)
        out["falsifier_t300_gt_0.2"] = any(len(w_) > k300 and w_[k300] > 0.2 for w_ in ws.values())
    return out


def main() -> None:
    root = Path(sys.argv[1])
    rows = []
    for run in sorted(p for p in root.iterdir() if (p / "done").exists()):
        res = analyze_run(run)
        res["verdicts"] = verdicts(res)
        rows.append(res)
        print(run.name, res["verdicts"], {r: v["windows"][:6] for r, v in res["regions"].items()})
    (root / "analysis.json").write_text(json.dumps(rows, indent=1))


if __name__ == "__main__":
    main()
