"""Gate check for no-kill controls (prereg Amendment 3). Exit 0 iff all pass.

    uv run python experiments/caerus_control_gate.py results/caerus/artifact-rt1200-control
"""

from __future__ import annotations

import statistics as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from caerus_artifact_analyze import WINDOW, _series  # noqa: E402


def gate(run: Path) -> list[str]:
    problems = []
    state = (run / "state").read_text().split()
    for tok in state:
        if tok.startswith("exit=") and tok != "exit=0":
            problems.append(f"nonzero {tok}")
    t_fail = float((run / "t_fail").read_text())
    t_end = float((run / "t_end").read_text())
    for r in range(6):
        log = run / f"r{r}.log"
        if "STARTING STOP PROCESS" in log.read_text(errors="replace"):
            problems.append(f"r{r} logged STARTING STOP PROCESS")
        pts = _series(log)
        base = [v for t, v in pts if t_fail - 60 <= t < t_fail]
        if not base:
            problems.append(f"r{r} no baseline samples")
            continue
        b = st.mean(base)
        for k in range(int((t_end - t_fail) // WINDOW)):
            lo = t_fail + k * WINDOW
            vals = [v for t, v in pts if lo <= t < lo + WINDOW]
            m = st.mean(vals) / b if vals else 0.0
            if m <= 0.8:
                problems.append(f"r{r} window {k} mean {m:.3f}")
    return problems


def main() -> None:
    ok = True
    for run in sorted(p for p in Path(sys.argv[1]).iterdir() if (p / "done").exists()):
        problems = gate(run)
        ok &= not problems
        print(run.name, "PASS" if not problems else "FAIL", problems[:10])
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
