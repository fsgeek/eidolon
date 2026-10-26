"""Score the Caerus restart-arm trials (R6, R5).

Pre-registration: docs/superpowers/notes/2026-10-26-caerus-restart-arm-preregistration.md
Fixed before any trial ran. Usage: uv run python experiments/caerus_restart_analyze.py <campaign dir>

Per trial directory (written by external/caerus/run/run_restart.sh):
  a.r<i>.log  original cluster, physical region i
  b.r<k>.log  restarted cluster; for R5, k -> physical PHYS5[k]
  t_fail, t_end, done
"""

import datetime as dt
import json
import re
import statistics
import sys
from pathlib import Path

PHYS5 = [0, 1, 3, 4, 5]
LINE = re.compile(r"^(\S+) .*SEQUENCER THROUGHPUT -- Sent ([\d.]+) txns/sec")


def ts(s: str) -> float:
    s = s.rstrip("Z")
    head, _, frac = s.partition(".")
    return dt.datetime.fromisoformat(head + "." + (frac + "000000")[:6] + "+00:00").timestamp()


def samples(path: Path) -> list[tuple[float, float]]:
    out = []
    for line in path.read_text(errors="replace").splitlines():
        m = LINE.search(line)
        if m:
            out.append((ts(m.group(1)), float(m.group(2))))
    return out


def score(trial: Path) -> dict:
    done = dict(kv.split("=") for kv in (trial / "done").read_text().split())
    arm = done["arm"]
    t_fail = float((trial / "t_fail").read_text())
    t_end = float((trial / "t_end").read_text())
    phys = list(range(6)) if arm == "R6" else PHYS5
    regions = {}
    for k, p in enumerate(phys):
        pre = [v for t, v in samples(trial / f"a.r{p}.log") if t_fail - 60 <= t < t_fail]
        med = statistics.median(pre) if pre else float("nan")
        post = samples(trial / f"b.r{k}.log")
        first = next((t for t, v in post if med > 0 and v >= 0.9 * med), None)
        # 30 s windows from t_fail + 60 to t_end: mean normalized throughput.
        windows = []
        w = t_fail + 60
        while w + 30 <= t_end:
            vs = [v for t, v in post if w <= t < w + 30]
            windows.append(round(statistics.mean(vs) / med, 3) if vs and med > 0 else 0.0)
            w += 30
        regions[p] = {
            "prefault_median": med,
            "t_regain": round(first - t_fail, 2) if first else None,
            "windows": windows,
            "n_post_samples": len(post),
        }
    regains = [r["t_regain"] for r in regions.values()]
    return {
        "trial": trial.name,
        "arm": arm,
        "pmr": int(done["pmr"]),
        "cluster_t_regain": None if None in regains else max(regains),
        "min_window_after_60s": min((min(r["windows"]) for r in regions.values() if r["windows"]), default=None),
        "regions": regions,
    }


def main() -> None:
    root = Path(sys.argv[1])
    trials = sorted(p for p in root.iterdir() if (p / "done").exists() and not p.name.startswith("check"))
    rows = [score(t) for t in trials]
    for r in rows:
        print(f"{r['trial']:<16} {r['arm']} pmr={r['pmr']:<3} cluster T_regain={r['cluster_t_regain']} "
              f"min 30s-window (>= +60 s)={r['min_window_after_60s']}")
    (root / "analysis.json").write_text(json.dumps(rows, indent=1))


if __name__ == "__main__":
    main()
