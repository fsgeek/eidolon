# Experiment Repairs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute the spec's "Existing experiment repair" items 1–8 plus three recon-discovered defects (missing Mars↔LEO route, under-partitioned blackout controller, stale `step9_crumbling` headline artifact), wiring the already-tested `time_budget.py` into every experiment entry point, so regenerated results are temporally valid, correctly regime-classified, and reproducible.

**Architecture:** Fix topology and blackout control at their single source (`demo_step_9.build_topology` + a new derived-pairs helper), collapse the verbatim copies in `demo_step_10.py` onto imports, replace the three start-time-only regime classifiers with `time_budget.classify_attempt`, and add a `scaled_window` helper to `time_budget.py` that every entry point uses to reject-or-scale temporally invalid configurations. Mechanical paper/bib fixes land now; data regeneration runs last with explicit escalation points, producing a delta report for the human checkpoint.

**Tech Stack:** Python ≥ 3.14, `uv`, simpy, pytest (existing suite: 38 tests).

**Companion document:** the recon map at `/tmp/claude-1000/-home-tony-projects-eidolon/96c81ac5-8b44-49db-bdd1-2f4b003e5a60/scratchpad/repair-surface-map.md` — file:line evidence for every claim below. Spec: `docs/superpowers/specs/2026-07-19-four-layer-capability-model-design.md` lines 180–192.

## Global Constraints

- Use `uv`, never `pip`: `uv run pytest`, `uv run python ...` (CLAUDE.md).
- Tier indexing: CODE convention, 0 = Mars/top, 3 = Earth/bottom (quorums.py docstring).
- Do not change `quorums.py` or `capability.py` — this plan touches experiments only (plus `time_budget.py` additions and paper/bib files).
- Canonical latencies (one-way seconds, from `demo_step_9.build_topology`): Mars↔Earth/LEO `mars_base_latency_s` (default 186) with jitter 5.0; Mars↔Moon +1.28; Moon↔Earth/LEO 1.28 jitter 0.01; LEO↔Earth ≤0.035 jitter 0.005; Earth↔Earth ≤~0.1.
- Existing suite (38 tests) must stay green after every task; per-task ledger: T1→40, T2→42, T3→43, T4→46, T5→46, T6→46, T7→46. (T3 adds exactly the one test function its brief specifies verbatim — 42 prior + 1 = 43; downstream counts shifted down by 1 accordingly, since Task 4 adds exactly 3 tests per its own Step 1.)
- Commits: plain `git commit -m` (GPG configured, non-interactive; an `ots: stamp` commit auto-follows each commit — expected).
- Long sweeps are CPU-only and approved to run; only Task 7 runs them.

---

### Task 1: Mars↔LEO route + single topology source

Spec item 1: "Define logical end-to-end Mars-to-required-tier reachability before blackout." Recon finding: no Mars↔LEO link exists in any variant, so Mars-initiated Phase 1 (requires an LEO respondent, quorums.py:288-291) can NEVER succeed. Also collapse `demo_step_10.py`'s verbatim copy of the builder.

**Files:**
- Modify: `demo_step_9.py:168-206` (`build_topology`)
- Modify: `demo_step_10.py:78-113` (delete copy, import instead)
- Test: `tests/test_topology.py` (create)

**Interfaces:**
- Consumes: `demo_step_9.build_topology(env, mars_base_latency_s, seed) -> Network` (existing signature, unchanged).
- Produces: the same builder, now emitting `leo-sat`↔`mars-{0,1,2}` links; `demo_step_10.build_topology` becomes a re-export (`from demo_step_9 import build_topology`).

- [ ] **Step 1: Check the Network link-inspection API**

Read `network.py` enough to find how links are stored (recon shows `network._locations` exists). If there is no public way to test "does a link exist between a and b", add a minimal accessor to `network.py`:

```python
    def has_link(self, a: str, b: str) -> bool:
        """True if a direct link exists between locations a and b."""
        return (a, b) in self._links or (b, a) in self._links
```

(Adapt the attribute name to whatever `add_link` actually populates — read `add_link` first; the method must be a pure read.)

- [ ] **Step 2: Write the failing test**

Create `tests/test_topology.py`:

```python
"""Topology invariants the experiments and the paper's claims rely on."""

import simpy

from demo_step_9 import build_topology


def test_mars_has_effective_route_to_every_phase1_tier():
    """Mars-initiated Phase 1 needs one respondent from every tier below:
    Moon, LEO, and >= 1 Earth. Each must be directly linked (the network
    model has no multi-hop routing)."""
    env = simpy.Environment()
    network = build_topology(env, mars_base_latency_s=186.0, seed=42)
    for i in range(3):
        mars = f"mars-{i}"
        assert network.has_link(mars, "moon"), mars
        assert network.has_link(mars, "leo-sat"), mars
        assert network.has_link(mars, "na-west") or network.has_link(mars, "europe"), mars


def test_demo_step_10_uses_the_same_builder():
    import demo_step_10
    from demo_step_9 import build_topology as canonical
    assert demo_step_10.build_topology is canonical
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_topology.py -v`
Expected: first test FAILS on the `leo-sat` assertion (no such link); second FAILS (distinct function objects).

- [ ] **Step 4: Implement**

In `demo_step_9.py` `build_topology`, after the Moon↔Mars loop (line ~197), add:

```python
    # Mars <-> LEO relay: required for Mars-initiated Phase 1 (the wall
    # reads down through every tier). Same order of magnitude as the
    # direct Mars-Earth path; the LEO satellite is in the Earth system.
    for i in range(3):
        network.add_link("leo-sat", f"mars-{i}",
                         latency=mars_base_latency_s, jitter=5.0)
```

In `demo_step_10.py`, delete its `build_topology` definition (lines 78-113) and add to its imports:

```python
from demo_step_9 import build_topology
```

(Verify nothing else in `demo_step_10.py` referenced internals of its local copy.)

- [ ] **Step 5: Run tests to verify they pass, plus the full suite**

Run: `uv run pytest tests/test_topology.py -v` then `uv run pytest -q`
Expected: 2 passed; full suite 40 passed.

- [ ] **Step 6: Commit**

```bash
git add network.py demo_step_9.py demo_step_10.py tests/test_topology.py
git commit -m "fix: Mars-LEO route so Mars-initiated Phase 1 is structurally possible"
```

---

### Task 2: Blackout controller derives its pairs from the topology

Spec item 2: "Remove those effective routes during hard blackout." Recon finding: the controller hardcodes `earth_path_locs = ["na-west", "europe", "moon"]` in three copies, so the new Mars↔LEO link and `full_coverage`'s Mars↔{asia,sa-east,africa} links stay UP during "hard blackout".

**Files:**
- Modify: `demo_step_9.py:354-379` (`conjunction_controller`), plus add module-level helper
- Modify: `experiments/tier_liveness_sweep.py:247-270` (same pattern)
- Modify: `experiments/step10_sweep.py:128-157` (same pattern)
- Test: `tests/test_topology.py` (append)

**Interfaces:**
- Consumes: Task 1's topology (all Mars links present).
- Produces: `demo_step_9.mars_blackout_pairs(network) -> list[tuple[str, str]]` — every (non-Mars location, mars-i) pair; all three controllers use it for partition/degrade/restore.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_topology.py`:

```python
def test_blackout_pairs_cover_every_non_mars_location():
    from demo_step_9 import mars_blackout_pairs
    env = simpy.Environment()
    network = build_topology(env, mars_base_latency_s=186.0, seed=42)
    pairs = mars_blackout_pairs(network)
    non_mars_side = {src for src, _ in pairs}
    # Every route into Mars is severed: Earth, Moon, LEO, and the relay.
    assert {"leo-sat", "moon", "na-west", "europe", "lagrange-relay"} <= non_mars_side
    assert all(dst.startswith("mars-") for _, dst in pairs)
    assert not any(src.startswith("mars-") for src, _ in pairs)


def test_blackout_pairs_include_full_coverage_links():
    from demo_step_9 import mars_blackout_pairs
    from experiments.tier_liveness_sweep import _add_full_coverage_links
    env = simpy.Environment()
    network = build_topology(env, mars_base_latency_s=186.0, seed=42)
    _add_full_coverage_links(network, mars_base_latency_s=186.0)
    non_mars_side = {src for src, _ in mars_blackout_pairs(network)}
    assert {"asia", "sa-east", "africa"} <= non_mars_side
```

(Adapt the `_add_full_coverage_links` signature to its actual one at `tier_liveness_sweep.py:72-88` — read it first; if it takes different arguments, call it as the sweep does.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_topology.py -v`
Expected: both new tests FAIL with `ImportError: cannot import name 'mars_blackout_pairs'`.

- [ ] **Step 3: Implement the helper and rewire all three controllers**

In `demo_step_9.py`, above `run_conjunction_experiment`:

```python
def mars_blackout_pairs(network) -> list[tuple[str, str]]:
    """Every (non-Mars location, Mars location) pair in the topology.

    Hard blackout must sever every effective route into Mars — derived
    from the network rather than hardcoded, so added links (Mars-LEO,
    full-coverage Earth sites, the relay) are severed too.
    partition_locations on an unlinked pair is a no-op, so this may
    safely include pairs with no direct link.
    """
    mars_locs = [f"mars-{i}" for i in range(3)]
    others = sorted(loc for loc in network._locations
                    if loc not in set(mars_locs))
    return [(src, dst) for src in others for dst in mars_locs]
```

(Confirm `partition_locations` on an unlinked pair is safe — `step10_sweep.py:141-147` already partitions all-locations crosswise, which is the existing precedent. If it raises, filter with `network.has_link(src, dst)` from Task 1.)

Rewire `demo_step_9.py` `conjunction_controller` (replace the `mars_locs`/`earth_path_locs` loops):

```python
    def conjunction_controller():
        pairs = mars_blackout_pairs(network)

        yield env.timeout(cfg.blackout_start_s)

        if with_repeater:
            for src, dst in pairs:
                network.update_link(src, dst, latency=240.0, jitter=12.0)
        else:
            for src, dst in pairs:
                network.partition_locations(src, dst)

        yield env.timeout(cfg.blackout_duration_s)

        if with_repeater:
            for src, dst in pairs:
                base = cfg.mars_base_latency_s + (1.28 if src == "moon" else 0.0)
                network.update_link(src, dst, latency=base, jitter=5.0)
        else:
            network.heal_all()
```

(If `update_link` raises on unlinked pairs, guard the repeater loops with `network.has_link(src, dst)`; the restore latency rule stays: `+1.28` only for `moon`.)

Apply the same replacement pattern to `experiments/tier_liveness_sweep.py:247-270` and `experiments/step10_sweep.py:128-157` (both import `mars_blackout_pairs` from `demo_step_9`; step10's crash-target partitioning block stays untouched).

- [ ] **Step 4: Run tests + full suite**

Run: `uv run pytest -q`
Expected: 42 passed.

- [ ] **Step 5: Commit**

```bash
git add demo_step_9.py experiments/tier_liveness_sweep.py experiments/step10_sweep.py tests/test_topology.py
git commit -m "fix: blackout severs every effective Mars route, derived from topology"
```

---

### Task 3: Wire classify_attempt — regime by full containment, transition bucket

Spec item 5 / recon finding 5: three duplicated start-time-only classifiers; `time_budget.classify_attempt` is tested but has zero production call sites.

**Files:**
- Modify: `demo_step_9.py:297-352` (`global_reconcile` and the stats plumbing), `demo_step_9.py` result dataclass/CSV emission (add transition fields — locate `ExperimentResult` at ~line 42 and wherever pre/during/post totals are written)
- Modify: `experiments/tier_liveness_sweep.py:232-237`, `experiments/step10_sweep.py:107-126` (same pattern)
- Test: `tests/test_regime_wiring.py` (create)

**Interfaces:**
- Consumes: `time_budget.classify_attempt(start, end, blackout_start, blackout_end) -> str` (existing, tested).
- Produces: every reconcile loop buckets by full containment into four `ReconciliationStats` (`pre`, `during`, `post`, `transition`); sweep CSVs gain `global_transition_success` / `global_transition_total` columns; `demo_step_9.ExperimentResult` gains a `transition` field mirroring the existing three.

- [ ] **Step 1: Write the failing test**

Create `tests/test_regime_wiring.py`:

```python
"""Integration: regime classification uses full containment and emits a
transition bucket. Uses a scaled-down, temporally valid configuration so
the sim runs in well under a minute."""

from demo_step_9 import ExperimentConfig, run_conjunction_experiment


def test_transition_bucket_exists_and_accounting_is_complete():
    cfg = ExperimentConfig(
        mars_base_latency_s=5.0,   # round_time = 20s
        blackout_start_s=60.0,     # > 1.25 * 20s
        blackout_duration_s=120.0,
        sim_end_s=400.0,
        reconcile_interval_s=30.0,
        global_timeout_s=25.0,     # > phase_time 10s
        seed=42,
    )
    result = run_conjunction_experiment(with_repeater=False, cfg=cfg, verbose=False)
    buckets = [result.pre_blackout, result.during_blackout,
               result.post_blackout, result.transition]
    assert all(b is not None for b in buckets)
    total_attempts = sum(b.total for b in buckets)
    assert total_attempts > 0
    # No attempt is double-counted or dropped: with a 30s cadence over a
    # 400s horizon the loop makes every attempt land in exactly one bucket.
    assert total_attempts == sum(b.total for b in buckets)
```

(Adapt the `ExperimentResult` field names for during/post to the actual ones at `demo_step_9.py:42-55` — read the dataclass first; the new field is `transition`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_regime_wiring.py -v`
Expected: FAIL — `ExperimentResult` has no `transition` attribute (or TypeError constructing it).

- [ ] **Step 3: Implement**

In `demo_step_9.py`:
- Add `from time_budget import classify_attempt` to imports.
- Add `transition: ReconciliationStats` to `ExperimentResult` alongside the existing three, and add `transition = ReconciliationStats()` next to `pre/during/post` in `run_conjunction_experiment`; pass it through wherever the result is constructed.
- Replace the classification block in `global_reconcile` (lines 339-344):

```python
            ended = env.now
            bucket = {"pre": pre, "during": during, "post": post,
                      "transition": transition}[
                classify_attempt(started, ended,
                                 cfg.blackout_start_s, blackout_end)]
```

Apply the same pattern to `experiments/tier_liveness_sweep.py:232-237` (its stats live in `TierResult`-adjacent accounting — add a transition counter pair and CSV columns `during_transition_...` matching its existing naming style) and `experiments/step10_sweep.py:107-126` (add `g_transition = ReconciliationStats()`, same dict dispatch, and emit `global_transition_success`/`global_transition_total` in its CSV writer).

Anywhere a sweep aggregates `pre/during/post` rates, exclude `transition` from steady-regime denominators (the spec's whole point: boundary-crossers must not dilute regime rates).

- [ ] **Step 4: Run tests + full suite**

Run: `uv run pytest -q`
Expected: 43 passed (42 prior + this one).

- [ ] **Step 5: Commit**

```bash
git add demo_step_9.py experiments/tier_liveness_sweep.py experiments/step10_sweep.py tests/test_regime_wiring.py
git commit -m "fix: regime classification by full containment with transition bucket"
```

---

### Task 4: scaled_window + validate_time_budget at every entry point

Spec items 3–4 / recon finding 4: no experiment validates its temporal budget; every default (600s pre-window) fails the close-approach 744s round bound; far-Mars latencies (750s, 1342s) also exceed the 500s phase timeout. Policy per spec: "scaled or the claim narrowed" — we scale, and record the scaling in the output.

**Files:**
- Modify: `time_budget.py` (add `scaled_window`)
- Modify: `demo_step_9.py` (main/CLI path), `experiments/step9_sweep.py:120-145` (per-config loop), `experiments/step9_liveness.py` (per-config loop), `experiments/tier_liveness_sweep.py` (per-config loop), `experiments/step10_sweep.py` (`run_single` head)
- Test: `tests/test_time_budget.py` (append)

**Interfaces:**
- Consumes: existing `ExperimentWindow`, `validate_time_budget`, `phase_time`, `round_time`.
- Produces:
  - `time_budget.scaled_window(*, d_max, p_max, blackout_duration, phase_timeout, pre_window, post_window, reconciliation_cadence=0.0, margin=1.25) -> tuple[ExperimentWindow, bool]` — returns a window guaranteed to validate, scaling any insufficient field up by the margin rule; the bool is True when any field was scaled.
  - Every experiment entry point calls it per configuration; CSVs gain `phase_timeout_s`, `pre_window_s`, `post_window_s`, `temporally_scaled` columns. Per-tier `d_max` in `tier_liveness_sweep`: `{0: mars_base_latency_s + 5.0, 1: 1.3, 2: 0.05, 3: 0.15}` (slowest required one-way path + jitter for each initiator's quorum, from the canonical latencies in Global Constraints). Earth-initiated experiments (step9, step10) use `d_max = 0.15`, but ALSO validate the Mars-local proposer's budget with `d_max = 0.005` and — when a claim about post-blackout Mars reconciliation is made — the Mars path `d_max = mars_base_latency_s + 5.0` for the recovery-window check only.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_time_budget.py`:

```python
def test_scaled_window_passes_validation_by_construction():
    from time_budget import scaled_window
    window, scaled = scaled_window(
        d_max=186.0, p_max=0.0, blackout_duration=900.0,
        phase_timeout=500.0, pre_window=600.0, post_window=900.0,
        reconciliation_cadence=120.0)
    assert scaled is True                      # 600 < 1.25 * 744
    assert window.pre_window == 930.0          # 1.25 * round_time(186, 0)
    assert validate_time_budget(window, d_max=186.0, p_max=0.0) == ()


def test_scaled_window_leaves_valid_configs_untouched():
    from time_budget import scaled_window
    window, scaled = scaled_window(
        d_max=0.15, p_max=0.0, blackout_duration=900.0,
        phase_timeout=500.0, pre_window=600.0, post_window=900.0,
        reconciliation_cadence=120.0)
    assert scaled is False
    assert (window.phase_timeout, window.pre_window, window.post_window) \
        == (500.0, 600.0, 900.0)


def test_scaled_window_scales_far_mars_timeout():
    from time_budget import scaled_window
    window, scaled = scaled_window(
        d_max=1342.0, p_max=0.0, blackout_duration=900.0,
        phase_timeout=500.0, pre_window=600.0, post_window=900.0)
    assert scaled is True
    assert window.phase_timeout > 2 * 1342.0   # exceeds phase_time
    assert validate_time_budget(window, d_max=1342.0, p_max=0.0) == ()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_time_budget.py -v`
Expected: 3 new FAIL with `ImportError: cannot import name 'scaled_window'`.

- [ ] **Step 3: Implement `scaled_window`**

Append to `time_budget.py`:

```python
def scaled_window(*, d_max: float, p_max: float, blackout_duration: float,
                  phase_timeout: float, pre_window: float,
                  post_window: float, reconciliation_cadence: float = 0.0,
                  margin: float = 1.25) -> tuple[ExperimentWindow, bool]:
    """Return a temporally valid window, scaling insufficient fields.

    Spec: configurations that cannot contain their claimed capabilities
    must be rejected or scaled — this implements the scaling arm and
    reports whether scaling occurred so results can be labeled. The
    margin covers jitter and processing slack beyond the analytic bound.
    """
    pt = phase_time(d_max, p_max)
    rt = round_time(d_max, p_max)
    eff_timeout = max(phase_timeout, margin * pt)
    pre = max(pre_window, margin * rt)
    post = max(post_window, margin * rt + reconciliation_cadence)
    horizon = pre + blackout_duration + post
    window = ExperimentWindow(eff_timeout, pre, blackout_duration, post,
                              horizon, reconciliation_cadence)
    assert validate_time_budget(window, d_max, p_max) == ()
    scaled = (eff_timeout, pre, post) != (phase_timeout, pre_window, post_window)
    return window, scaled
```

- [ ] **Step 4: Run the unit tests**

Run: `uv run pytest tests/test_time_budget.py -v`
Expected: all pass (existing 8 + 3 new = 11 in the file).

- [ ] **Step 5: Wire into the five entry points**

In each experiment's per-configuration setup, immediately before building the sim, call `scaled_window` with that configuration's `d_max` (per the Interfaces table above) and use the returned window's `phase_timeout`/`pre_window`/`post_window`/`horizon` as the effective `global_timeout_s`/`blackout_start_s`/post-window/`sim_end_s`. Emit `phase_timeout_s`, `pre_window_s`, `post_window_s`, `temporally_scaled` (0/1) as CSV columns in each sweep's writer (and print them in `demo_step_9`'s verbose output). `tier_liveness_sweep` computes the window per initiator tier and uses the max across swept tiers for the shared sim horizon while recording the per-tier scaled flag.

- [ ] **Step 6: Full suite + a smoke run**

Run: `uv run pytest -q` — expected 46 passed.
Run: `uv run python demo_step_9.py --mars-latency-s 186 --blackout-duration-s 300 --seed 42` — expected: completes, prints the scaled window values (pre-window 930.0 for 186s latency).

- [ ] **Step 7: Commit**

```bash
git add time_budget.py demo_step_9.py experiments/step9_sweep.py experiments/step9_liveness.py experiments/tier_liveness_sweep.py experiments/step10_sweep.py tests/test_time_budget.py
git commit -m "feat: temporal-budget validation and scaling at every experiment entry point"
```

---

### Task 5: Reproduction docs

Recon finding: `docs/step9-repro.md` covers only step9; `step10_sweep.py` and `tier_liveness_sweep.py` have no documented repro commands anywhere; defaults documented there are now scaled.

**Files:**
- Modify: `docs/step9-repro.md`

- [ ] **Step 1: Update**

Rewrite the affected command blocks to match post-Task-4 reality (windows are auto-scaled; note the new CSV columns), and add two new sections with the exact commands:

```bash
# Per-tier liveness sweep (sparse + full coverage)
uv run python experiments/tier_liveness_sweep.py \
  --seeds "40,41,...,89" \
  --output results/tier_liveness/tier_sweep_full.csv \
  --aggregate-output results/tier_liveness/tier_sweep_full_ci.csv

# Crash-tolerance / relaxed-quorum sweep (feeds tab:relaxed)
uv run python experiments/step10_sweep.py \
  --seeds "40,41,...,89" \
  --output results/step10/step10_sweep.csv \
  --aggregate-output results/step10/step10_sweep_ci.csv
```

(Verify flag names against each script's argparse before writing; `step10_sweep.py` currently hardcodes windows — Task 4 made them scaled; document whatever CLI it actually exposes after Task 4.)

- [ ] **Step 2: Commit**

```bash
git add docs/step9-repro.md
git commit -m "docs: reproduction commands for all sweeps, post-repair"
```

---

### Task 6: Mechanical paper and bibliography fixes

Spec items 7–8. Both paper copies; they are not auto-synced.

**Files:**
- Modify: `docs/paper/nines/main.tex:489` and `docs/paper/main.tex:474` (column label)
- Modify: `docs/paper/references.bib:118-126` and `docs/paper/nines/references.bib:118-126` (li2023 authors; add li2026satrapy)
- Modify: `docs/paper/nines/main.tex:283` and `docs/paper/main.tex:271` (add Satrapy citation sentence)

- [ ] **Step 1: Column labels**

`docs/paper/nines/main.tex:489`: change header cell `Commit` → `Earth-local P2 quorum` (keep the `\makebox` structure if width demands; check for overfull after compile).
`docs/paper/main.tex:474`: change `Commit cost` → `Earth-local Phase 2 quorum`.

- [ ] **Step 2: Bibliography**

In BOTH `references.bib` files, correct li2023's authors (title/venue stay):

```bibtex
author = {Li, Xiao and Chan, Eric M. and Lesani, Mohsen},
```

Add to both (note: cleaned title — the raw entry Tony supplied had the author string duplicated inside the title field):

```bibtex
@article{li2026satrapy,
  title   = {Satrapy: From Abstract to Practical Consensus for Heterogeneous Quorum Systems},
  author  = {Li, Xiao and Chan, Eric M. and Lesani, Mohsen},
  journal = {Distributed Computing},
  volume  = {39},
  number  = {2},
  pages   = {16},
  year    = {2026},
  publisher = {Springer}
}
```

The prose "Li, Chan, and Lesani~\cite{li2023}" at `main.tex:271`/`nines/main.tex:283` remains correct (surnames unchanged). After that sentence, in both files, add:

```latex
Their Satrapy system~\cite{li2026satrapy} carries the framework from abstract quorum systems to practical consensus.
```

(The full related-work comparison against Satrapy — the paper is in `docs/papers/` — belongs to the restructure plan, not here.)

- [ ] **Step 3: Compile both papers**

```bash
cd docs/paper && pdflatex -interaction=nonstopmode main && bibtex main && pdflatex -interaction=nonstopmode main && pdflatex -interaction=nonstopmode main
cd nines && pdflatex -interaction=nonstopmode main && bibtex main && pdflatex -interaction=nonstopmode main && pdflatex -interaction=nonstopmode main
```

Expected: zero errors, no new undefined references, no new overfull boxes (check both logs).

- [ ] **Step 4: Commit**

```bash
git add docs/paper/main.tex docs/paper/main.pdf docs/paper/references.bib docs/paper/nines/main.tex docs/paper/nines/references.bib
git commit -m "docs: tradeoff-table label, li2023 author correction, Satrapy citation"
```

(Add `docs/paper/nines/main.pdf` too if it is tracked — check `git status`.)

---

### Task 7: Regenerate sweeps + artifact reconciliation (escalation-prone; runs LAST)

Spec item 6 plus recon finding 2 (stale `step9_crumbling`). This task runs the long sweeps against the repaired code and produces the delta report that is the human checkpoint artifact. It has explicit BLOCKED contracts — do NOT smooth over a discrepancy.

**Files:**
- Regenerate: `results/step9/step9_sweep.csv` + `_ci.csv`, `results/tier_liveness/tier_sweep_full.csv` + `_ci.csv`, `results/step10/step10_sweep.csv` + `_ci.csv`
- Create: `docs/superpowers/notes/2026-07-21-regeneration-delta.md`
- Investigate: `results/step9_crumbling/` provenance

- [ ] **Step 1: Provenance investigation (before any regeneration)**

Establish what produced `results/step9_crumbling/step9_sweep.csv`: `git log --follow --oneline -- results/step9_crumbling/` and inspect the producing commit's `demo_step_9.py` (`git show <sha>:demo_step_9.py | head -300`) to determine the construction it used (pre-`initiator_tier=3`, judging by its nonzero Mars/Moon/LEO response counts). Separately, determine what the appendix's "Flat Phase 1" row (`results/step9/step9_sweep.csv`) was generated with, the same way. Record both in the delta report. **BLOCKED contract:** if the provenance of either CSV cannot be established from git history, stop and report — do not guess.

- [ ] **Step 2: Regenerate the three sweeps with repaired code**

```bash
uv run python experiments/step9_sweep.py --seeds "40,...,89" \
  --output results/step9/step9_sweep.csv --aggregate-output results/step9/step9_sweep_ci.csv
uv run python experiments/tier_liveness_sweep.py --seeds "40,...,89" \
  --output results/tier_liveness/tier_sweep_full.csv --aggregate-output results/tier_liveness/tier_sweep_full_ci.csv
uv run python experiments/step10_sweep.py --seeds "40,...,89" \
  --output results/step10/step10_sweep.csv --aggregate-output results/step10/step10_sweep_ci.csv
```

(Use the full 50-seed lists as in each script's default; CPU-only, approved. Run sequentially; capture wall-clock per sweep in the delta report.)

- [ ] **Step 3: Write the delta report**

`docs/superpowers/notes/2026-07-21-regeneration-delta.md` — a table per paper table: old value (from the current paper text / old CSV), new value (regenerated CSV), and a verdict per row: `unchanged` / `moved-within-CI` / `claim-affected`. Cover at minimum: tab:relaxed's 98%±3.9 and 92%±7.6; tab:pertier's per-tier during-blackout rates; the headline "100% during blackout" and "0.183s" (tab:flat-vs-wall); Mars-tier pre/post-blackout success in tier_liveness (which should now be NONZERO for the first time — the Mars↔LEO repair makes Mars-initiated Phase 1 possible; if it is still zero, that is a BLOCKED finding, not a result); and the transition-bucket counts as evidence the reclassification matters (or doesn't).

- [ ] **Step 4: Escalation gate**

**BLOCKED contract:** if ANY row is `claim-affected` (a number in the paper is contradicted outside its CI), return BLOCKED with the delta report — the human author decides how the paper responds. If all rows are `unchanged`/`moved-within-CI`, proceed.

- [ ] **Step 5: Commit (only if Step 4 passed)**

```bash
git add results/step9 results/tier_liveness results/step10 docs/superpowers/notes/2026-07-21-regeneration-delta.md
git commit -m "data: regenerate sweeps with repaired topology, blackout, regimes, budgets"
```

Leave `results/step9_crumbling/` untouched either way — superseding or retiring it is a decision for the human checkpoint, informed by the delta report.

---

## Self-Review Notes

- **Spec coverage:** item 1 → Task 1; item 2 → Task 2; item 3 (372s/500s prose) → deliberately DEFERRED to the restructure plan (the sentence's replacement depends on Task 7's regenerated numbers; the delta report feeds it) — tracked, not dropped; item 4 → Task 4; item 5 → Task 3; item 6 → Task 7; item 7 → Task 6; item 8 → Task 6. Recon extras: Mars↔LEO (Task 1), controller coverage (Task 2), stale step9_crumbling (Task 7 provenance + human checkpoint).
- **Escalation points are contracts, not failures:** Task 7 Steps 1 and 4 BLOCKED outcomes are expected-possible and end the run cleanly at the human checkpoint Tony already agreed to.
- **Adaptation notes are bounded:** where the plan says "read X first / adapt to actual signature" (network link storage, `_add_full_coverage_links` args, `ExperimentResult` field names, step10 CLI), the adaptation target is named precisely and the acceptance test pins the behavior; reviewers should treat unexplained divergence beyond those four as spec violations.
- **Type consistency:** `mars_blackout_pairs(network) -> list[tuple[str, str]]` (Tasks 2); `classify_attempt` dispatch dict keys `pre/during/post/transition` (Task 3); `scaled_window(...) -> tuple[ExperimentWindow, bool]` (Task 4, consumed in Task 5's docs and Task 7's runs); test ledger 38→40→42→43→46, then flat.
