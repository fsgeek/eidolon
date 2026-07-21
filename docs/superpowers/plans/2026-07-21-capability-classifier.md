# Crumbling-Wall Capability Classifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Revision note (2026-07-21):** revised after external review. Changes: broken-intermediate-row scenario corrected to `MOON | EARTH`; `authority_required` renamed to `requires_preexisting_authority` with capability predicates; provenance is now sets of jointly-required evidence channels covering every report field; missing obligations are typed records with formatting at the CLI boundary; the evaluation CSV carries the inputs needed to reconstruct each row; new Task 2 fixes the pre-existing `phase1_quorum_size` inconsistency under relaxed thresholds; scope honestly narrowed to the crumbling wall; test/row counts corrected.

**Goal:** Implement the static capability classifier from the four-layer capability model spec (`docs/superpowers/specs/2026-07-19-four-layer-capability-model-design.md`) for the crumbling-wall quorum construction: a pure function from (crumbling-wall quorum system, initiating tier, effective connectivity) to the structural capability envelope, plus the time-budget validator and the scenario capability map that become the paper's evaluation evidence.

**Architecture:** A new pure module `capability.py` computes R1/R2, typed witness/missing obligation records, hazard flags, authority dependence, and evidence provenance from a `CrumblingWallQuorum` plus a reachable-node set — no simulation involved. A sibling pure module `time_budget.py` implements the spec's temporal validity checks and pre/during/post/transition regime classification. `experiments/capability_map.py` runs the classifier over the spec's minimum scenario basis and emits a self-auditable evaluation table. An exhaustive test cross-validates the classifier against `is_phase1_quorum`/`is_phase2_quorum` over all 1024 connectivity states.

**Tech Stack:** Python ≥ 3.14, `uv` for environment management, `pytest` (added as dev dependency in Task 1), stdlib only otherwise.

**Out of scope (later plans):** the dueling-proposer hazard experiment, the eight existing-experiment repairs, TLA+ alignment, the paper restructure, and a generic quorum-family interface (YAGNI until a second construction needs classifying). **Note:** spec acceptance criterion 9 (every experiment validates its temporal budget) is NOT realized by this plan; Task 8 builds the checked validator, and the experiment-repair plan wires it in at configuration boundaries.

## Global Constraints

- Use `uv`, never `pip` directly: `uv sync`, `uv run pytest`, `uv add --dev <pkg>` (CLAUDE.md).
- `requires-python = ">=3.14"` (pyproject.toml).
- Tier indexing follows the CODE convention throughout: index 0 = Mars/top of wall, last index = Earth/bottom/fast tier (`quorums.py` class docstring). Paper tier `i` = code tier `num_tiers-1-i`. Never mix conventions.
- Do not change the semantics of `is_phase1_quorum`/`is_phase2_quorum` — the classifier must agree with them, not redefine them. (Task 2 changes only the informational `phase1_quorum_size`/`describe`, bringing them into agreement with `is_phase1_quorum`.)
- Canonical 5/1/1/3 test topology, matching `quorums.py.__main__`: `MARS = [100, 101, 102]`, `MOON = [200]`, `LEO = [300]`, `EARTH = [1, 2, 3, 4, 5]`.
- Commits are GPG-signed and trigger a pinentry prompt; the human operator must be at the keyboard. Batch commit steps rather than skipping them.
- Follow existing code style: module docstrings explaining the distributed-systems concept, type hints, no external deps.

---

### Task 1: Public accessors on CrumblingWallQuorum + test infrastructure

The classifier needs the Phase 2 threshold `k` and the hitting-set bound `|E|-k+1`, currently private (`_phase2_threshold`, `_min_earth_in_q1`). Expose them as read-only properties, and bootstrap pytest while we're at it (the repo has no tests yet).

**Files:**
- Modify: `pyproject.toml` (pytest dev dependency + pytest config)
- Modify: `quorums.py` (two properties after `tier_of`, ~line 236)
- Test: `tests/test_quorum_properties.py` (create; also creates `tests/`)

**Interfaces:**
- Consumes: `CrumblingWallQuorum(tiers, phase2_threshold)` from `quorums.py` (existing).
- Produces: `wall.phase2_threshold -> int` and `wall.min_earth_in_q1 -> int`, used by every later task.

- [ ] **Step 1: Add pytest and configure test discovery**

```bash
cd /home/tony/projects/eidolon && uv add --dev pytest
```

Then append to `pyproject.toml`:

```toml
[tool.pytest.ini_options]
pythonpath = ["."]
testpaths = ["tests"]
```

(`pythonpath = ["."]` lets tests import root-level modules like `quorums` without packaging changes.)

- [ ] **Step 2: Write the failing test**

Create `tests/test_quorum_properties.py`:

```python
"""Public accessors and size reporting on CrumblingWallQuorum."""

from quorums import CrumblingWallQuorum

MARS = [100, 101, 102]
MOON = [200]
LEO = [300]
EARTH = [1, 2, 3, 4, 5]


def make_wall(k=None):
    return CrumblingWallQuorum([MARS, MOON, LEO, EARTH], phase2_threshold=k)


def test_strict_wall_exposes_threshold_and_hitting_set():
    wall = make_wall()
    assert wall.phase2_threshold == 5
    assert wall.min_earth_in_q1 == 1


def test_relaxed_wall_exposes_threshold_and_hitting_set():
    # k=3: hitting set |E|-k+1 = 3 (comment near quorums.py:229)
    wall = make_wall(3)
    assert wall.phase2_threshold == 3
    assert wall.min_earth_in_q1 == 3
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_quorum_properties.py -v`
Expected: FAIL with `AttributeError: 'CrumblingWallQuorum' object has no attribute 'phase2_threshold'`

- [ ] **Step 4: Add the properties**

In `quorums.py`, after the `tier_of` method (~line 236), insert:

```python
    @property
    def phase2_threshold(self) -> int:
        """The k in k-of-|fast tier| required for Phase 2."""
        return self._phase2_threshold

    @property
    def min_earth_in_q1(self) -> int:
        """Minimum fast-tier nodes any Q1 needs: |E| - k + 1 (hitting set)."""
        return self._min_earth_in_q1
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_quorum_properties.py -v`
Expected: 2 passed

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock quorums.py tests/test_quorum_properties.py
git commit -m "feat: expose quorum threshold accessors, bootstrap pytest"
```

---

### Task 2: Fix phase1_quorum_size and describe() under relaxed thresholds

Pre-existing inconsistency: `is_phase1_quorum` correctly requires `|E|-k+1` Earth nodes, but `phase1_quorum_size()` (quorums.py:237) counts one node per tier, and `describe()`/`describe_tiers()` repeat that number. For k=3 the Mars minimum is reported as 4 when the real minimum is 6. Callers are display-only for the wall (the base-class `is_phase1_quorum` that uses the size is overridden by `CrumblingWallQuorum`; demo scripts only print it), and for strict k the corrected formula yields identical values — so this is a pure bug fix with no protocol-behavior change.

**Files:**
- Modify: `quorums.py` (`phase1_quorum_size`, `describe`, `describe_tiers`)
- Test: `tests/test_quorum_properties.py` (append)

**Interfaces:**
- Consumes: Task 1's `min_earth_in_q1`.
- Produces: `wall.phase1_quorum_size(initiator_tier) -> int` returning the TRUE minimum Q1 size: `(num_tiers - 1 - initiator_tier) + min_earth_in_q1`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_quorum_properties.py`:

```python
def test_phase1_quorum_size_strict_matches_tier_count():
    # Strict k=5: hitting set is 1, so sizes equal the tier count.
    wall = make_wall()
    assert [wall.phase1_quorum_size(t) for t in range(4)] == [4, 3, 2, 1]


def test_phase1_quorum_size_relaxed_includes_hitting_set():
    # k=3: every Q1 needs |E|-k+1 = 3 Earth nodes, so the true minima
    # are Mars 6, Moon 5, LEO 4, Earth 3 (paper tradeoff table: 3/6).
    wall = make_wall(3)
    assert [wall.phase1_quorum_size(t) for t in range(4)] == [6, 5, 4, 3]


def test_describe_reports_relaxed_phase1_minima():
    text = make_wall(3).describe()
    assert "top needs 6" in text
    assert "bottom needs 3" in text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_quorum_properties.py -v`
Expected: `test_phase1_quorum_size_relaxed_includes_hitting_set` and `test_describe_reports_relaxed_phase1_minima` FAIL (relaxed sizes come back `[4, 3, 2, 1]`); the strict test passes (values unchanged by design).

- [ ] **Step 3: Fix the implementation**

In `quorums.py`, replace the body of `phase1_quorum_size` (keep the docstring, extend it):

```python
    def phase1_quorum_size(self, initiator_tier: int | None = None) -> int:
        """Minimum Phase 1 quorum size for the initiating tier.

        A proposer at tier i needs one node from each intermediate tier
        j in [i, num_tiers-1) plus min_earth_in_q1 fast-tier nodes (the
        |E|-k+1 hitting set that guarantees Q1/Q2 intersection). For
        strict Phase 2 the hitting set is 1 and this equals the tier
        count; under relaxed k it is strictly larger.
        """
        if initiator_tier is None:
            # Top of wall (Mars) — worst case, for backwards compatibility
            initiator_tier = 0
        intermediate = self.num_tiers - 1 - initiator_tier
        return intermediate + self._min_earth_in_q1
```

In `describe`, replace the Phase 1 fragment:

```python
        return (f"CrumblingWall(tiers=[{tier_desc}]): "
                f"Phase1=read-down (top needs {self.phase1_quorum_size(0)}, "
                f"bottom needs {self.phase1_quorum_size(self.num_tiers - 1)}), "
                f"Phase2={self._phase2_size} ({p2_desc})")
```

In `describe_tiers`, replace `q1_needs = self.num_tiers - i` with:

```python
            q1_needs = self.phase1_quorum_size(i)
```

and its line with `f"Phase 1 minimum {q1_needs} (reads downward)"` so the label no longer implies tier count alone.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_quorum_properties.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add quorums.py tests/test_quorum_properties.py
git commit -m "fix: phase1_quorum_size includes the |E|-k+1 hitting set"
```

---

### Task 3: Core classifier — R1/R2 envelope and per-tier obligations

**Files:**
- Create: `capability.py`
- Test: `tests/test_capability.py`

**Interfaces:**
- Consumes: `CrumblingWallQuorum` fields `tiers`, `fast_tier`, `num_tiers`, and Task 1's `phase2_threshold`, `min_earth_in_q1`.
- Produces:
  - `TierObligation(phase: int, tier_index: int, required: int, witnesses: frozenset[int], unreachable: frozenset[int])` with property `satisfied -> bool` (`phase` is 1 or 2)
  - `CapabilityReport(initiator_tier: int, r1: bool, r2: bool, r1_obligations: tuple[TierObligation, ...], r2_obligation: TierObligation)` (frozen dataclass; later tasks add fields)
  - `classify(wall: CrumblingWallQuorum, initiator_tier: int, reachable: set[int]) -> CapabilityReport` (raises `ValueError` on out-of-range tier)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_capability.py`:

```python
"""Core R1/R2 envelope of the crumbling-wall capability classifier."""

import pytest

from capability import classify
from quorums import CrumblingWallQuorum

MARS = [100, 101, 102]
MOON = [200]
LEO = [300]
EARTH = [1, 2, 3, 4, 5]
ALL_NODES = set(MARS + MOON + LEO + EARTH)

MARS_TIER, MOON_TIER, LEO_TIER, EARTH_TIER = 0, 1, 2, 3


def make_wall(k=None):
    return CrumblingWallQuorum([MARS, MOON, LEO, EARTH], phase2_threshold=k)


def test_full_reachability_gives_11_from_every_tier():
    wall = make_wall()
    for tier in (MARS_TIER, MOON_TIER, LEO_TIER, EARTH_TIER):
        report = classify(wall, tier, ALL_NODES)
        assert (report.r1, report.r2) == (True, True)


def test_mars_blackout_gives_00_from_mars():
    wall = make_wall()
    report = classify(wall, MARS_TIER, set(MARS))
    assert (report.r1, report.r2) == (False, False)


def test_mars_blackout_leaves_earth_at_11():
    wall = make_wall()
    report = classify(wall, EARTH_TIER, ALL_NODES - set(MARS))
    assert (report.r1, report.r2) == (True, True)


def test_sparse_leo_strict_phase2_gives_10():
    # LEO reaches its own satellite and three of five Earth nodes.
    wall = make_wall()
    report = classify(wall, LEO_TIER, {300, 1, 2, 3})
    assert (report.r1, report.r2) == (True, False)


def test_obligations_report_each_wall_row():
    wall = make_wall()
    report = classify(wall, MARS_TIER, set(MARS))
    assert [o.tier_index for o in report.r1_obligations] == [0, 1, 2, 3]
    assert [o.satisfied for o in report.r1_obligations] == [True, False, False, False]
    assert all(o.phase == 1 for o in report.r1_obligations)
    assert report.r2_obligation.phase == 2
    # Unreachable candidates are carried as data, not prose.
    assert report.r1_obligations[1].unreachable == frozenset({200})


def test_out_of_range_tier_raises():
    with pytest.raises(ValueError):
        classify(make_wall(), 4, ALL_NODES)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_capability.py -v`
Expected: FAIL at collection with `ModuleNotFoundError: No module named 'capability'`

- [ ] **Step 3: Write the implementation**

Create `capability.py`:

```python
"""Static capability classifier for the crumbling-wall construction.

Implements the structural half of the four-layer capability model for
`CrumblingWallQuorum` specifically (a generic quorum-family interface
is deliberately not attempted). Layers 1 and 2 of the model (quorum
obligation, effective reachability) are decidable from static
configuration plus a connectivity summary; that is what `classify`
computes. Layer 3 (protocol authority) is runtime state — the
classifier reports only whether structurally available progress
*depends* on it. Layer 4 (service contract) is a policy declaration,
never a classifier output.

The connectivity input is Reach(i, C): the set of acceptors the
initiating tier can exchange protocol traffic with inside the
experiment's liveness assumptions, including its own colocated
acceptors when they are up. The classifier does not decide *why* a
node is absent (scheduled disconnection, failed relay, short timeout);
the caller must report the cause.

Spec: docs/superpowers/specs/2026-07-19-four-layer-capability-model-design.md
"""

from __future__ import annotations

from dataclasses import dataclass

from quorums import CrumblingWallQuorum


@dataclass(frozen=True)
class TierObligation:
    """One obligation row: what a phase requires from one tier.

    phase is 1 or 2. required is 1 for intermediate Phase 1 rows,
    min_earth_in_q1 for the Phase 1 fast-tier row (the |E|-k+1
    hitting-set bound), and the phase2 threshold k for the Phase 2
    obligation. witnesses are the reachable candidates; unreachable
    are the candidates connectivity has removed.
    """

    phase: int
    tier_index: int
    required: int
    witnesses: frozenset[int]
    unreachable: frozenset[int]

    @property
    def satisfied(self) -> bool:
        return len(self.witnesses) >= self.required


@dataclass(frozen=True)
class CapabilityReport:
    """Structural capability envelope for one initiating tier."""

    initiator_tier: int
    r1: bool
    r2: bool
    r1_obligations: tuple[TierObligation, ...]
    r2_obligation: TierObligation


def classify(wall: CrumblingWallQuorum, initiator_tier: int,
             reachable: set[int]) -> CapabilityReport:
    """Compute R1(i, C) and R2(i, C) with per-obligation evidence."""
    if not 0 <= initiator_tier < wall.num_tiers:
        raise ValueError(
            f"initiator_tier must be in [0, {wall.num_tiers - 1}], "
            f"got {initiator_tier}")
    reachable = set(reachable)
    fast_index = wall.num_tiers - 1

    obligations = []
    for j in range(initiator_tier, wall.num_tiers):
        tier_set = set(wall.tiers[j])
        required = wall.min_earth_in_q1 if j == fast_index else 1
        obligations.append(TierObligation(
            phase=1,
            tier_index=j,
            required=required,
            witnesses=frozenset(reachable & tier_set),
            unreachable=frozenset(tier_set - reachable),
        ))
    r1 = all(o.satisfied for o in obligations)

    fast_set = set(wall.fast_tier)
    r2_obligation = TierObligation(
        phase=2,
        tier_index=fast_index,
        required=wall.phase2_threshold,
        witnesses=frozenset(reachable & fast_set),
        unreachable=frozenset(fast_set - reachable),
    )
    r2 = r2_obligation.satisfied

    return CapabilityReport(
        initiator_tier=initiator_tier,
        r1=r1,
        r2=r2,
        r1_obligations=tuple(obligations),
        r2_obligation=r2_obligation,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_capability.py -v`
Expected: 6 passed (full suite: 11)

- [ ] **Step 5: Commit**

```bash
git add capability.py tests/test_capability.py
git commit -m "feat: crumbling-wall capability classifier core (R1/R2 envelope)"
```

---

### Task 4: Witness sets, typed missing obligations, CLI formatting

Spec requirement: "A failed classification must identify the missing obligation rather than merely report timeout." Missing obligations are typed records (phase, tier, required, witnesses, unreachable candidates); human-readable text is produced only by `format_missing` at the CLI boundary. A successful classification produces a checkable witness quorum.

**Files:**
- Modify: `capability.py`
- Test: `tests/test_capability.py` (append)

**Interfaces:**
- Consumes: Task 3's `classify`, `CapabilityReport`, `TierObligation`; `wall.is_phase1_quorum(set, initiator_tier)`, `wall.is_phase2_quorum(set)`.
- Produces:
  - New `CapabilityReport` fields: `r1_witness: frozenset[int] | None`, `r2_witness: frozenset[int] | None` (minimal satisfying quorums, `None` when unreachable)
  - Property `missing -> tuple[TierObligation, ...]` (the unsatisfied obligations, Phase 1 rows first, then Phase 2 if unsatisfied; empty when both phases reachable)
  - `format_missing(report: CapabilityReport) -> tuple[str, ...]` (one human-readable line per missing obligation)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_capability.py` (and add `format_missing` to the `from capability import ...` line at the top):

```python
def test_witnesses_are_valid_quorums():
    wall = make_wall(3)
    report = classify(wall, MARS_TIER, ALL_NODES)
    assert wall.is_phase1_quorum(set(report.r1_witness), MARS_TIER)
    assert wall.is_phase2_quorum(set(report.r2_witness))


def test_witness_sizes_match_paper_tradeoff_table():
    # arXiv/NINeS tradeoff table Phase 1 minima (Earth-init / Mars-init):
    # k=5 -> 1/4, k=4 -> 2/5, k=3 -> 3/6
    for k, earth_min, mars_min in [(5, 1, 4), (4, 2, 5), (3, 3, 6)]:
        wall = make_wall(k)
        assert len(classify(wall, EARTH_TIER, ALL_NODES).r1_witness) == earth_min
        assert len(classify(wall, MARS_TIER, ALL_NODES).r1_witness) == mars_min


def test_unreachable_phases_have_no_witness():
    report = classify(make_wall(), MARS_TIER, set(MARS))
    assert report.r1_witness is None
    assert report.r2_witness is None


def test_missing_is_typed_and_identifies_each_obligation():
    report = classify(make_wall(), MARS_TIER, set(MARS))
    # Blocked at Moon (tier 1), LEO (tier 2), Earth (tier 3) for
    # Phase 1, plus the Phase 2 obligation.
    assert [(o.phase, o.tier_index) for o in report.missing] \
        == [(1, 1), (1, 2), (1, 3), (2, 3)]
    moon_row = report.missing[0]
    assert moon_row.required == 1
    assert moon_row.witnesses == frozenset()
    assert moon_row.unreachable == frozenset({200})


def test_format_missing_renders_cli_text():
    report = classify(make_wall(), MARS_TIER, set(MARS))
    lines = format_missing(report)
    assert len(lines) == 4
    assert any("tier 1" in line and "200" in line for line in lines)
    assert any(line.startswith("Phase 2") for line in lines)


def test_fully_capable_report_has_empty_missing():
    report = classify(make_wall(), EARTH_TIER, ALL_NODES)
    assert report.missing == ()
    assert format_missing(report) == ()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_capability.py -v`
Expected: FAIL at collection with `ImportError: cannot import name 'format_missing' from 'capability'`

- [ ] **Step 3: Extend the implementation**

In `capability.py`, add the witness fields and `missing` property to `CapabilityReport`:

```python
@dataclass(frozen=True)
class CapabilityReport:
    """Structural capability envelope for one initiating tier."""

    initiator_tier: int
    r1: bool
    r2: bool
    r1_obligations: tuple[TierObligation, ...]
    r2_obligation: TierObligation
    r1_witness: frozenset[int] | None
    r2_witness: frozenset[int] | None

    @property
    def missing(self) -> tuple[TierObligation, ...]:
        """The unsatisfied obligations, as typed records."""
        unmet = [o for o in self.r1_obligations if not o.satisfied]
        if not self.r2_obligation.satisfied:
            unmet.append(self.r2_obligation)
        return tuple(unmet)
```

Add above `classify`:

```python
def _minimal_witness(obligations: list[TierObligation]) -> frozenset[int]:
    """Deterministic minimal quorum: lowest node IDs satisfying each row."""
    picked: set[int] = set()
    for o in obligations:
        picked.update(sorted(o.witnesses)[:o.required])
    return frozenset(picked)
```

Extend the body of `classify` (after `r2 = r2_obligation.satisfied`, before the return):

```python
    r1_witness = _minimal_witness(obligations) if r1 else None
    if r1_witness is not None:
        assert wall.is_phase1_quorum(set(r1_witness), initiator_tier)
    r2_witness = (frozenset(sorted(r2_obligation.witnesses)[:wall.phase2_threshold])
                  if r2 else None)
    if r2_witness is not None:
        assert wall.is_phase2_quorum(set(r2_witness))
```

Update the return statement to pass `r1_witness=r1_witness, r2_witness=r2_witness`.

Add at module level (after `classify`):

```python
def format_missing(report: CapabilityReport) -> tuple[str, ...]:
    """Human-readable rendering of missing obligations (CLI boundary)."""
    return tuple(
        f"Phase {o.phase} obligation at tier {o.tier_index}: require "
        f"{o.required}, reachable {len(o.witnesses)}; "
        f"unreachable candidates {sorted(o.unreachable)}"
        for o in report.missing)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_capability.py -v`
Expected: 12 passed (full suite: 17)

- [ ] **Step 5: Commit**

```bash
git add capability.py tests/test_capability.py
git commit -m "feat: classifier witnesses and typed missing-obligation records"
```

---

### Task 5: Hazard flags, authority predicates, evidence provenance

The R1×R2 matrix's operational interpretation: `(1,0)` is the futile/harmful election hazard, `(0,1)` is incumbent-only progress. Authority semantics follow the spec's Layer 3 split — acquire/recover requires R1; exercising requires R2 *and* a valid incumbent (runtime evidence). Provenance maps every report conclusion to the SET of evidence channels it jointly requires, plus two boundary markers the spec demands ("which conclusions ... require fresh authority state, and which are service declarations").

**Files:**
- Modify: `capability.py`
- Test: `tests/test_capability.py` (append)

**Interfaces:**
- Consumes: Task 4's `classify` and `CapabilityReport`.
- Produces:
  - `EvidenceChannel` enum: `CONFIGURATION`, `CONNECTIVITY`, `RUNTIME`, `POLICY`
  - `Hazard` enum: `DISRUPTIVE_ELECTION` (value `"disruptive-election"`), `INCUMBENT_ONLY` (value `"incumbent-only"`) — docstrings state the runtime preconditions the structural flag does not itself establish
  - `PROVENANCE: dict[str, frozenset[EvidenceChannel]]` module constant
  - New `CapabilityReport` fields: `hazards: tuple[Hazard, ...]`, `requires_preexisting_authority: bool`
  - Properties: `can_acquire_or_recover_authority -> bool` (= r1), `can_exercise_existing_authority -> bool` (= r2, structural precondition only), `provenance -> dict[str, frozenset[EvidenceChannel]]`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_capability.py` (add `EvidenceChannel, Hazard` to the import at the top):

```python
def test_10_state_flags_disruptive_election():
    report = classify(make_wall(), LEO_TIER, {300, 1, 2, 3})
    assert report.hazards == (Hazard.DISRUPTIVE_ELECTION,)
    assert report.requires_preexisting_authority is False


def test_01_state_isolates_broken_intermediate_row():
    # Moon and the Earth anchor reachable; ONLY the LEO row is broken
    # (spec: "broken intermediate Phase-1 obligation with anchor
    # reachable"). Reaching only Earth would break two rows at once
    # and not isolate the intermediate obligation.
    report = classify(make_wall(), MOON_TIER, set(MOON) | set(EARTH))
    assert (report.r1, report.r2) == (False, True)
    assert report.hazards == (Hazard.INCUMBENT_ONLY,)
    assert report.requires_preexisting_authority is True
    assert report.can_acquire_or_recover_authority is False
    assert report.can_exercise_existing_authority is True
    assert [(o.phase, o.tier_index) for o in report.missing] == [(1, 2)]


def test_11_and_00_states_have_no_hazards():
    wall = make_wall()
    full = classify(wall, EARTH_TIER, ALL_NODES)
    cut = classify(wall, MARS_TIER, set(MARS))
    assert full.hazards == () and cut.hazards == ()
    assert full.requires_preexisting_authority is False
    assert cut.requires_preexisting_authority is False


def test_provenance_gives_joint_evidence_channels():
    prov = classify(make_wall(), EARTH_TIER, ALL_NODES).provenance
    assert prov["quorum_families"] == {EvidenceChannel.CONFIGURATION}
    structural = {EvidenceChannel.CONFIGURATION, EvidenceChannel.CONNECTIVITY}
    for key in ("r1", "r2", "r1_witness", "r2_witness", "missing",
                "hazards", "requires_preexisting_authority"):
        assert prov[key] == structural, key
    assert prov["operational_progress"] == {
        EvidenceChannel.CONFIGURATION, EvidenceChannel.CONNECTIVITY,
        EvidenceChannel.RUNTIME}
    assert prov["service_contract"] == {EvidenceChannel.POLICY}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_capability.py -v`
Expected: FAIL at collection with `ImportError: cannot import name 'EvidenceChannel' from 'capability'`

- [ ] **Step 3: Extend the implementation**

In `capability.py`, add `from enum import Enum` to the imports, then after them:

```python
class EvidenceChannel(Enum):
    """Kinds of evidence a conclusion can rest on (stratified legibility).

    RUNTIME denotes FRESH runtime evidence — e.g. that an incumbent's
    ballot is still un-preempted now, not merely that it once was.
    """

    CONFIGURATION = "configuration"
    CONNECTIVITY = "connectivity"
    RUNTIME = "runtime"
    POLICY = "policy"


class Hazard(Enum):
    """Hazardous-action labels from the R1 x R2 matrix.

    DISRUPTIVE_ELECTION marks where the disruptive action is
    structurally REACHABLE: actually disrupting an incumbent further
    requires completing a higher-ballot Phase 1 whose slot/epoch scope
    covers the incumbent's — runtime facts the classifier cannot see.
    INCUMBENT_ONLY marks that progress continues only while some
    incumbent's authority remains valid — likewise runtime state.
    """

    DISRUPTIVE_ELECTION = "disruptive-election"
    INCUMBENT_ONLY = "incumbent-only"


#: Evidence channels each conclusion JOINTLY requires. Structural
#: conclusions need the quorum configuration AND a connectivity
#: summary. Two boundary markers extend beyond computed report fields,
#: as the spec requires provenance to mark where structural legibility
#: ends: operational progress additionally needs fresh runtime
#: authority evidence, and the client-visible contract is a service
#: policy declaration, never a classifier output.
_STRUCTURAL = frozenset({EvidenceChannel.CONFIGURATION,
                         EvidenceChannel.CONNECTIVITY})
PROVENANCE: dict[str, frozenset[EvidenceChannel]] = {
    "quorum_families": frozenset({EvidenceChannel.CONFIGURATION}),
    "r1": _STRUCTURAL,
    "r2": _STRUCTURAL,
    "r1_witness": _STRUCTURAL,
    "r2_witness": _STRUCTURAL,
    "missing": _STRUCTURAL,
    "hazards": _STRUCTURAL,
    "requires_preexisting_authority": _STRUCTURAL,
    "operational_progress": _STRUCTURAL | {EvidenceChannel.RUNTIME},
    "service_contract": frozenset({EvidenceChannel.POLICY}),
}
```

Add the fields and properties to `CapabilityReport`:

```python
    hazards: tuple[Hazard, ...]
    requires_preexisting_authority: bool

    @property
    def can_acquire_or_recover_authority(self) -> bool:
        """Structural: a Phase 1 quorum is reachable (spec Layer 3)."""
        return self.r1

    @property
    def can_exercise_existing_authority(self) -> bool:
        """Structural precondition ONLY: a Phase 2 quorum is reachable.
        Actually exercising it also requires a valid incumbent
        authority, which is runtime evidence the classifier never sees.
        """
        return self.r2

    @property
    def provenance(self) -> dict[str, frozenset[EvidenceChannel]]:
        return dict(PROVENANCE)
```

In `classify`, after the witness block:

```python
    hazards = []
    if r1 and not r2:
        hazards.append(Hazard.DISRUPTIVE_ELECTION)
    if r2 and not r1:
        hazards.append(Hazard.INCUMBENT_ONLY)
    requires_preexisting_authority = r2 and not r1
```

And pass them in the return:

```python
        hazards=tuple(hazards),
        requires_preexisting_authority=requires_preexisting_authority,
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_capability.py -v`
Expected: 16 passed (full suite: 21)

- [ ] **Step 5: Commit**

```bash
git add capability.py tests/test_capability.py
git commit -m "feat: hazard flags, authority predicates, joint evidence provenance"
```

---

### Task 6: Exhaustive cross-validation against the quorum implementation

Spec ("Formal evidence"): "The capability classifier and its hazard flags can be checked exhaustively over the finite topology and threshold states used in the paper." Every connectivity subset × initiator tier × threshold must agree with `is_phase1_quorum`/`is_phase2_quorum`, and minimal witnesses must match the (Task 2-corrected) `phase1_quorum_size`.

**Files:**
- Test: `tests/test_capability_exhaustive.py` (create)

**Interfaces:**
- Consumes: Task 5's `classify` (full report), `CrumblingWallQuorum.is_phase1_quorum(subset, tier)`, `is_phase2_quorum(subset)`, `phase1_quorum_size(tier)`.
- Produces: nothing new — a pure verification deliverable.

- [ ] **Step 1: Write the test (expected to pass immediately — this is verification, not TDD of new behavior)**

Create `tests/test_capability_exhaustive.py`:

```python
"""Exhaustive agreement between the classifier and the quorum predicates.

2^10 connectivity states x 4 initiator tiers x 6 thresholds. This is
the paper's 'checked exhaustively over the finite topology' claim for
the classifier, distinct from Paxos safety verification.
"""

from itertools import combinations

from capability import Hazard, classify
from quorums import CrumblingWallQuorum

MARS = [100, 101, 102]
MOON = [200]
LEO = [300]
EARTH = [1, 2, 3, 4, 5]
ALL_NODES = sorted(MARS + MOON + LEO + EARTH)


def all_subsets(nodes):
    for r in range(len(nodes) + 1):
        for combo in combinations(nodes, r):
            yield set(combo)


def test_classifier_agrees_with_quorum_predicates_exhaustively():
    for k in (None, 5, 4, 3, 2, 1):
        wall = CrumblingWallQuorum([MARS, MOON, LEO, EARTH],
                                   phase2_threshold=k)
        for subset in all_subsets(ALL_NODES):
            for tier in range(wall.num_tiers):
                report = classify(wall, tier, subset)
                assert report.r1 == wall.is_phase1_quorum(subset, tier), \
                    f"R1 disagrees: k={k} tier={tier} reach={sorted(subset)}"
                assert report.r2 == wall.is_phase2_quorum(subset), \
                    f"R2 disagrees: k={k} tier={tier} reach={sorted(subset)}"
                if report.r1:
                    # Minimal witness matches the corrected size formula.
                    assert len(report.r1_witness) \
                        == wall.phase1_quorum_size(tier)


def test_hazard_flags_follow_matrix_exhaustively():
    for k in (None, 5, 4, 3):
        wall = CrumblingWallQuorum([MARS, MOON, LEO, EARTH],
                                   phase2_threshold=k)
        for subset in all_subsets(ALL_NODES):
            for tier in range(wall.num_tiers):
                report = classify(wall, tier, subset)
                expects_disruptive = report.r1 and not report.r2
                expects_incumbent = report.r2 and not report.r1
                assert (Hazard.DISRUPTIVE_ELECTION in report.hazards) \
                    == expects_disruptive
                assert (Hazard.INCUMBENT_ONLY in report.hazards) \
                    == expects_incumbent
                assert report.requires_preexisting_authority \
                    == expects_incumbent
```

- [ ] **Step 2: Run the test**

Run: `uv run pytest tests/test_capability_exhaustive.py -v`
Expected: 2 passed, in well under 10 seconds. If either fails, the classifier (or an internal witness assert) has diverged from the quorum predicates — fix `capability.py`, never the predicates.

- [ ] **Step 3: Run the whole suite**

Run: `uv run pytest`
Expected: 23 passed

- [ ] **Step 4: Commit**

```bash
git add tests/test_capability_exhaustive.py
git commit -m "test: exhaustive classifier/quorum-predicate agreement"
```

---

### Task 7: Scenario acceptance tests — relaxation sequence and full matrix

Acceptance criteria 2 and 6 from the spec: the sparse-LEO k∈{5,4,3} sequence is `(1,0), (1,0), (1,1)`, and the minimum scenario basis realizes all four matrix states.

**Files:**
- Test: `tests/test_capability_scenarios.py` (create)

**Interfaces:**
- Consumes: Task 5's `classify`, `Hazard`.
- Produces: nothing new — the paper's Evaluation-section claims as executable tests.

- [ ] **Step 1: Write the tests (expected to pass — acceptance verification)**

Create `tests/test_capability_scenarios.py`:

```python
"""Spec scenario basis as executable acceptance tests.

These encode the paper's Evaluation claims: the relaxation transition
10 -> 11 and coverage of the complete R1 x R2 matrix.
"""

from capability import Hazard, classify
from quorums import CrumblingWallQuorum

MARS = [100, 101, 102]
MOON = [200]
LEO = [300]
EARTH = [1, 2, 3, 4, 5]
ALL_NODES = set(MARS + MOON + LEO + EARTH)

MARS_TIER, MOON_TIER, LEO_TIER, EARTH_TIER = 0, 1, 2, 3

# LEO reaches its own satellite plus three of five Earth ground
# stations (the sparse topology of experiments/tier_liveness_sweep.py).
SPARSE_LEO_REACH = {300, 1, 2, 3}


def make_wall(k=None):
    return CrumblingWallQuorum([MARS, MOON, LEO, EARTH], phase2_threshold=k)


def test_sparse_leo_relaxation_sequence():
    """k=5,4,3 -> (1,0), (1,0), (1,1): relaxation converts futile
    disruption into ordinary failover capability."""
    expected = {5: (True, False), 4: (True, False), 3: (True, True)}
    for k, state in expected.items():
        report = classify(make_wall(k), LEO_TIER, SPARSE_LEO_REACH)
        assert (report.r1, report.r2) == state, f"k={k}"


def test_relaxation_clears_the_disruption_hazard():
    assert classify(make_wall(5), LEO_TIER, SPARSE_LEO_REACH).hazards \
        == (Hazard.DISRUPTIVE_ELECTION,)
    assert classify(make_wall(3), LEO_TIER, SPARSE_LEO_REACH).hazards == ()


def test_scenario_basis_realizes_all_four_matrix_states():
    wall = make_wall()
    reports = [
        classify(wall, EARTH_TIER, ALL_NODES),               # full wall
        classify(wall, LEO_TIER, SPARSE_LEO_REACH),          # sparse LEO
        classify(wall, MOON_TIER, set(MOON) | set(EARTH)),   # broken LEO row
        classify(wall, MARS_TIER, set(MARS)),                # hard cut
    ]
    assert {(r.r1, r.r2) for r in reports} == {
        (True, True), (True, False), (False, True), (False, False)}
```

- [ ] **Step 2: Run the tests**

Run: `uv run pytest tests/test_capability_scenarios.py -v`
Expected: 3 passed (full suite: 26). A failure here is a real design/spec disagreement — stop and surface it rather than adjusting the expected values.

- [ ] **Step 3: Commit**

```bash
git add tests/test_capability_scenarios.py
git commit -m "test: spec scenario basis and relaxation-transition acceptance"
```

---

### Task 8: Time-budget validator and regime classification

Spec ("Time-budget validity"): configurations that cannot temporally contain their claimed capabilities "must be rejected or explicitly classified as temporally unavailable. They must not silently become evidence of a topological capability loss." Plus the pre/during/post/transition attempt classification.

**Scope note:** this task builds and verifies the pure module only. No experiment calls it yet; spec acceptance criterion 9 is realized when the experiment-repair plan wires `validate_time_budget` into every configuration boundary.

**Files:**
- Create: `time_budget.py`
- Test: `tests/test_time_budget.py`

**Interfaces:**
- Consumes: nothing from earlier tasks (pure stdlib module).
- Produces (consumed by the later experiment-repair plan):
  - `phase_time(d_max: float, p_max: float) -> float` = `2*d_max + p_max`
  - `round_time(d_max: float, p_max: float) -> float` = `4*d_max + 2*p_max`
  - `ExperimentWindow(phase_timeout, pre_window, blackout_duration, post_window, horizon, reconciliation_cadence=0.0)` (frozen dataclass, all floats, seconds)
  - `validate_time_budget(window: ExperimentWindow, d_max: float, p_max: float) -> tuple[str, ...]` (empty tuple = valid)
  - `classify_attempt(start: float, end: float, blackout_start: float, blackout_end: float) -> str` returning `"pre" | "during" | "post" | "transition"`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_time_budget.py`:

```python
"""Time-budget validity and regime classification (spec section
'Time-budget validity'). Numeric cases come from the spec's Mars
examples: close approach ~186 s one-way, so a fresh two-phase round is
~744 s before margin and a 600 s pre-window cannot contain one."""

from time_budget import (
    ExperimentWindow,
    classify_attempt,
    phase_time,
    round_time,
    validate_time_budget,
)


def test_phase_and_round_bounds():
    assert phase_time(186, 0) == 372
    assert round_time(186, 0) == 744


def test_close_approach_mars_with_short_pre_window_is_rejected():
    window = ExperimentWindow(phase_timeout=500, pre_window=600,
                              blackout_duration=900, post_window=900,
                              horizon=2400)
    violations = validate_time_budget(window, d_max=186, p_max=0)
    assert any("pre-blackout window" in v for v in violations)
    # The 500 s per-phase timeout DOES exceed the 372 s round trip:
    # the old paper claim to the contrary was wrong.
    assert not any("phase timeout" in v for v in violations)


def test_far_mars_delay_exceeds_phase_timeout():
    window = ExperimentWindow(phase_timeout=500, pre_window=6000,
                              blackout_duration=900, post_window=6000,
                              horizon=12900)
    violations = validate_time_budget(window, d_max=1342, p_max=0)
    assert any("phase timeout" in v for v in violations)


def test_valid_configuration_returns_no_violations():
    window = ExperimentWindow(phase_timeout=500, pre_window=900,
                              blackout_duration=900, post_window=900,
                              horizon=2700)
    assert validate_time_budget(window, d_max=186, p_max=1) == ()


def test_horizon_must_contain_all_windows():
    window = ExperimentWindow(phase_timeout=500, pre_window=900,
                              blackout_duration=900, post_window=900,
                              horizon=2000)
    violations = validate_time_budget(window, d_max=186, p_max=1)
    assert any("horizon" in v for v in violations)


def test_post_window_accounts_for_reconciliation_cadence():
    window = ExperimentWindow(phase_timeout=500, pre_window=900,
                              blackout_duration=900, post_window=900,
                              horizon=2700, reconciliation_cadence=300)
    violations = validate_time_budget(window, d_max=186, p_max=1)
    assert any("post-blackout window" in v for v in violations)


def test_attempt_regimes_by_full_containment():
    # Blackout spans [1000, 1900].
    assert classify_attempt(0, 999, 1000, 1900) == "pre"
    assert classify_attempt(1100, 1500, 1000, 1900) == "during"
    assert classify_attempt(1950, 2400, 1000, 1900) == "post"
    # A packet sent before the boundary may arrive after it.
    assert classify_attempt(900, 1100, 1000, 1900) == "transition"
    assert classify_attempt(1800, 2000, 1000, 1900) == "transition"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_time_budget.py -v`
Expected: FAIL at collection with `ModuleNotFoundError: No module named 'time_budget'`

- [ ] **Step 3: Write the implementation**

Create `time_budget.py`:

```python
"""Time-budget validity checks for experiment configurations.

A capability claim is only evidence if the experiment gave it enough
time: the per-phase timeout must exceed the slowest required
request-response path, and each observation window must be able to
contain at least one completed round. Configurations that fail these
checks must be rejected or labeled temporally unavailable — they must
not silently become evidence of a topological capability loss.

Spec: docs/superpowers/specs/2026-07-19-four-layer-capability-model-design.md
(section "Time-budget validity")
"""

from __future__ import annotations

from dataclasses import dataclass


def phase_time(d_max: float, p_max: float) -> float:
    """Worst-case two-message (request-response) Paxos phase."""
    return 2 * d_max + p_max


def round_time(d_max: float, p_max: float) -> float:
    """Worst-case fresh two-phase round."""
    return 4 * d_max + 2 * p_max


@dataclass(frozen=True)
class ExperimentWindow:
    """Temporal layout of a blackout experiment, in seconds."""

    phase_timeout: float
    pre_window: float
    blackout_duration: float
    post_window: float
    horizon: float
    reconciliation_cadence: float = 0.0


def validate_time_budget(window: ExperimentWindow, d_max: float,
                         p_max: float) -> tuple[str, ...]:
    """Return violations; an empty tuple means temporally valid."""
    violations = []
    if window.phase_timeout <= phase_time(d_max, p_max):
        violations.append(
            f"phase timeout {window.phase_timeout}s does not exceed the "
            f"worst request-response path {phase_time(d_max, p_max)}s")
    if window.pre_window < round_time(d_max, p_max):
        violations.append(
            f"pre-blackout window {window.pre_window}s cannot contain one "
            f"full two-phase round {round_time(d_max, p_max)}s")
    needed_post = round_time(d_max, p_max) + window.reconciliation_cadence
    if window.post_window < needed_post:
        violations.append(
            f"post-blackout window {window.post_window}s cannot contain one "
            f"full round plus reconciliation cadence ({needed_post}s)")
    needed_horizon = (window.pre_window + window.blackout_duration
                      + window.post_window)
    if window.horizon < needed_horizon:
        violations.append(
            f"horizon {window.horizon}s shorter than pre + blackout + post "
            f"({needed_horizon}s)")
    return tuple(violations)


def classify_attempt(start: float, end: float, blackout_start: float,
                     blackout_end: float) -> str:
    """Regime by FULL containment; boundary-crossers are 'transition'.

    The network tests a partition when a packet is sent, so a packet
    sent before a boundary may arrive after it; attempts that cross a
    boundary must be excluded from steady-regime success rates.
    """
    if end <= blackout_start:
        return "pre"
    if start >= blackout_end:
        return "post"
    if start >= blackout_start and end <= blackout_end:
        return "during"
    return "transition"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_time_budget.py -v`
Expected: 7 passed (full suite: 33)

- [ ] **Step 5: Commit**

```bash
git add time_budget.py tests/test_time_budget.py
git commit -m "feat: time-budget validator and attempt-regime classification"
```

---

### Task 9: Capability map over the scenario basis

The evaluation artifact: run the classifier over the spec's minimum scenario basis and emit the capability table (stdout + CSV) that feeds the paper's Evaluation section. Every row carries the inputs (threshold, reachable set) and evidence (witnesses, typed-missing rendering, authority dependence) needed to independently reconstruct it.

**Files:**
- Create: `experiments/capability_map.py`
- Test: `tests/test_capability_map.py`

**Interfaces:**
- Consumes: Task 5's `classify`, Task 4's `format_missing`; `CrumblingWallQuorum`.
- Produces:
  - `build_scenarios() -> list[tuple[str, int, dict[int, set[int]]]]` — (name, phase2_threshold, reachable-set per initiator tier)
  - `run() -> list[dict]` — rows with keys `scenario`, `initiator_tier`, `phase2_threshold`, `reachable`, `r1`, `r2`, `r1_witness`, `r2_witness`, `requires_preexisting_authority`, `hazards`, `missing`
  - CLI writing `results/capability/capability_map.csv`

- [ ] **Step 1: Write the failing test**

Create `tests/test_capability_map.py`:

```python
"""The capability-map experiment reproduces the spec's expected states."""

from experiments.capability_map import run


def _index(rows):
    return {(r["scenario"], r["initiator_tier"]): r for r in rows}


def test_map_contains_relaxation_sequence():
    rows = _index(run())
    assert (rows[("sparse_leo_k5", "leo")]["r1"],
            rows[("sparse_leo_k5", "leo")]["r2"]) == (1, 0)
    assert (rows[("sparse_leo_k4", "leo")]["r1"],
            rows[("sparse_leo_k4", "leo")]["r2"]) == (1, 0)
    assert (rows[("sparse_leo_k3", "leo")]["r1"],
            rows[("sparse_leo_k3", "leo")]["r2"]) == (1, 1)


def test_map_labels_hazards_and_authority():
    rows = _index(run())
    assert rows[("sparse_leo_k5", "leo")]["hazards"] == "disruptive-election"
    moon = rows[("moon_row_broken_k5", "moon")]
    assert moon["hazards"] == "incumbent-only"
    assert moon["requires_preexisting_authority"] == 1
    assert rows[("full_reachability_k5", "earth")]["hazards"] == ""


def test_rows_are_reconstructible_from_recorded_inputs():
    rows = _index(run())
    mars_row = rows[("mars_conjunction_k5", "mars")]
    assert (mars_row["r1"], mars_row["r2"]) == (0, 0)
    # The input reachable set is recorded, so the row can be recomputed.
    assert mars_row["reachable"] == "100;101;102"
    assert "tier 1" in mars_row["missing"]
    earth_row = rows[("mars_conjunction_k5", "earth")]
    assert (earth_row["r1"], earth_row["r2"]) == (1, 1)
    assert earth_row["r2_witness"] == "1;2;3;4;5"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_capability_map.py -v`
Expected: FAIL at collection with `ModuleNotFoundError: No module named 'experiments.capability_map'`

- [ ] **Step 3: Write the implementation**

Create `experiments/capability_map.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_capability_map.py -v`
Expected: 3 passed

- [ ] **Step 5: Run the experiment and inspect the artifact**

Run: `uv run python experiments/capability_map.py`
Expected: 20 rows on stdout (12 full-reachability: 4 tiers × 3 thresholds; 3 sparse-LEO; 1 moon-row-broken; 4 mars-conjunction), and `results/capability/capability_map.csv` created. Spot-check: `sparse_leo_k5 leo R1=1 R2=0 disruptive-election`.

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest`
Expected: 36 passed

- [ ] **Step 7: Commit**

```bash
git add experiments/capability_map.py tests/test_capability_map.py results/capability/capability_map.csv
git commit -m "feat: capability map experiment over the scenario basis"
```

---

## Self-Review Notes

- **Spec coverage:** classifier output fields (envelope ✓ Task 3, typed witnesses/missing ✓ Task 4, authority predicates + hazard flags + joint provenance ✓ Task 5), exhaustive finite-topology check incl. witness-size agreement ✓ Task 6, relaxation transition + matrix coverage with the isolated broken-intermediate-row scenario ✓ Task 7, time-budget validity + `transition` regime (module only; criterion 9 deferred to the experiment-repair plan) ✓ Task 8, reconstructible scenario-basis evaluation artifact ✓ Task 9. Deliberately deferred: dueling-proposer experiment, existing-experiment repairs (consume `time_budget.py`), TLA+ alignment, paper text, generic quorum-family API.
- **Test-count ledger:** T1: 2 → T2: 5 → T3: 11 → T4: 17 → T5: 21 → T6: 23 → T7: 26 → T8: 33 → T9: 36.
- **Row count (Task 9, Step 5):** 12 + 3 + 1 + 4 = 20.
- **Type consistency:** `TierObligation(phase, tier_index, required, witnesses, unreachable)` and `classify(wall, initiator_tier, reachable)` identical across Tasks 3–9; `requires_preexisting_authority` (never `authority_required`) in Tasks 5, 6, 9; `Hazard` values `"disruptive-election"`/`"incumbent-only"` consistent in Tasks 5, 7, 9; broken-intermediate scenario is `set(MOON) | set(EARTH)` in Tasks 5, 7, 9.
