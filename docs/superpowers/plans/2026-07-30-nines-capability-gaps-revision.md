# NINeS Capability-Gaps Revision Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver an audited NINeS revision whose formal claim is the exact correspondence between quorum-predicate ordering and reachable capability gaps, supported by a preregistered generic auditor, evidence-calibrated experiments, a persuasive planetary-to-edge narrative, and an anonymous artifact.

**Architecture:** `quorum_audit.py` is a construction-independent, pure library over explicit finite quorum families; `experiments/quorum_audit.py` is its JSON/text command boundary; and `experiments/quorum_audit_registered.py` deterministically regenerates the registered cases and reconciles the wall cases with existing CSV evidence. The paper consumes that result but keeps proof primary. The optional wall-specific readout remains a thin wrapper around `capability.py`, and anonymous packaging uses an allowlist because the tracked repository contains older identifying files.

**Tech Stack:** Python >=3.14, stdlib, `uv`, pytest, LaTeX (`pdflatex` + `bibtex`), Git/GPG and OpenTimestamps through WSL.

## Global Constraints

- Active manuscript: `docs/paper/nines/main.tex`; do not edit `docs/paper/main.tex`.
- Canonical topology notation: 5/1/1/3 (Earth/LEO/Moon/Mars in prose). Code tier order is Mars, Moon, LEO, Earth; paper tier order is Earth, LEO, Moon, Mars. Name tiers at boundaries instead of passing unexplained indices.
- Use `uv`, never `pip`. On Windows prefix commands with `$env:UV_PROJECT_ENVIRONMENT='.venv-windows'`. WSL uses `/mnt/c/Users/TonyMason/source/repos/eidolon` and `.venv-linux`.
- All semantic commits are made from WSL with `git commit -S`, the `research@wamason.com` key, and a manually created OpenTimestamps follow-up commit. Do not commit from Windows.
- Artifact A's expected outputs must be committed, signed, and timestamped before implementation code or tests are written.
- Tests are written and observed failing before their implementation. A test that unexpectedly passes is investigated before proceeding.
- The four report classes are mutually exclusive: `equal`, `r1-strictly-implies-r2`, `r2-strictly-implies-r1`, `incomparable`. Equality is checked before implication.
- Manuscript ban list: no arbitrary-family “exactly one” claim; no anchor claim without its tier quantifier; no “recovery” synonym for acquisition; no unconditional livelock or “blocks liveness completely” claim; no “only available mitigation” claim.
- `N_e` is fixed within a decision window; volatile effective participation is `C_t subseteq N_e`. Reconfiguration is a separate, epoch-changing protocol.
- Mars is evidence and magnifying glass. Terrestrial edge is a structural applicability argument until a separately registered experiment is run after the audited upload.
- Artifact B is droppable. Its absence must not weaken any headline claim.
- The dual-dashboard rendering is not scheduled in this plan. Reconsider it
  only after Task 9 is green; it is communication slack below Artifact B and
  supplies no new evidence.
- Do not start the terrestrial experiment before the author confirms the audited PDF has been uploaded.

## File Map

- Create `docs/superpowers/notes/2026-07-30-quorum-auditor-preregistration.md`: immutable expected outputs and model readings.
- Create `quorum_audit.py`: validation, canonical antichains, safety audit, predicate classification, witnesses, and independent exhaustive self-check.
- Create `experiments/quorum_audit.py`: deterministic JSON/text CLI.
- Create `experiments/quorum_audit_registered.py`: registered-case and wall/CSV reconciliation driver.
- Create `tests/test_quorum_audit.py`: pure-library TDD and exhaustive checks.
- Create `tests/test_quorum_audit_cli.py`: input validation, deterministic serialization, and exit codes.
- Create `results/capability/quorum_audit_registered.json`: deterministic registered output.
- Modify `docs/paper/nines/main.tex`: title, abstract, introduction, theorem, scope, wall purpose, edge recurrence, claims, discussion, conclusion, and traceability.
- Modify `docs/paper/nines/references.bib`: add Refined Quorum Systems primary citation.
- Create `tests/test_nines_claim_language.py`: narrow regression checks for falsified or overbroad wording.
- Optionally create `experiments/capability_readout.py`, `tests/test_capability_readout.py`, and two JSON examples under `examples/capability/`.
- Create `artifact-manifest.txt`, `scripts/build_anonymous_artifact.py`, and `tests/test_anonymous_artifact.py`: explicit anonymous bundle construction and scanning.
- Modify `docs/ai-provenance.md`: record this drafting/review loop and the registration/audit chain without identifying the author in the anonymous bundle.

## Signed Commit Procedure

Every task's “Commit” step uses this procedure with the exact message stated in
that task:

1. From WSL, stage only the task files and run `git commit -S -m "$TASK_MESSAGE"`,
   where `TASK_MESSAGE` has first been assigned the task's exact message.
2. Verify with `git verify-commit HEAD`; stop if the signature is not from fingerprint `1D7C4A68252F6EC1ACD2FC8E934778A0EB5EABB1`.
3. Obtain `HASH=$(git rev-parse HEAD)`. Using `apply_patch`, create `timestamps/$HASH` containing exactly `$HASH` plus a newline.
4. Run `.venv-linux/bin/ots stamp timestamps/$HASH`.
5. Run `git add timestamps/$HASH timestamps/$HASH.ots && git commit -S -m "ots: stamp $HASH"`.
6. Verify the timestamp commit with `git verify-commit HEAD` and require a clean worktree before the next task.

---

### Task 1: Register Artifact A Expected Outputs

**Files:**
- Create: `docs/superpowers/notes/2026-07-30-quorum-auditor-preregistration.md`

**Interfaces:**
- Produces: the immutable cases that Tasks 2–5 must score without adjustment.
- Consumes: no implementation code.

- [ ] **Step 1: Write the registration note with these exact model rules**

State that `Form(Q) = {C subseteq N | exists q in Q: q subseteq C}`; `P` restricts the domain to states containing every pinned node; safety is evaluated on the original families; gap classification is evaluated on the restricted domain. State that wall “self-reachable” rows pin the first node of the initiating tier and are equivalent to “some own-tier node” only by within-tier symmetry.

- [ ] **Step 2: Register the generic cases**

Use this table verbatim; witnesses are sorted string sequences:

| Case | `N`; `Q1`; `Q2`; `P` | Safe | Relation | `(1,0)` | `(0,1)` |
|---|---|---:|---|---|---|
| semantic equality | `abc`; `{{a,b},{a,b,c}}`; `{{a,b}}`; `{}` | yes | `equal` | none | none |
| R1 strict implies R2 | `abc`; `{{a,b}}`; `{{a}}`; `{}` | yes | `r1-strictly-implies-r2` | none | `{a}` |
| R2 strict implies R1 | `abc`; `{{a}}`; `{{a,b}}`; `{}` | yes | `r2-strictly-implies-r1` | `{a}` | none |
| incomparable | `abc`; `{{a,b}}`; `{{a,c}}`; `{}` | yes | `incomparable` | `{a,b}` | `{a,c}` |
| threshold `1/3` | `abc`; all 1-sets; `{{a,b,c}}`; `{}` | yes | `r2-strictly-implies-r1` | `{a}` | none |
| threshold `2/2` | `abc`; all 2-sets; all 2-sets; `{}` | yes | `equal` | none | none |
| threshold `3/1` | `abc`; `{{a,b,c}}`; all 1-sets; `{}` | yes | `r1-strictly-implies-r2` | none | `{a}` |
| unsafe control | `abc`; `{{a}}`; `{{b}}`; `{}` | no, witness `({a},{b})` | `incomparable` | `{a}` | `{b}` |

- [ ] **Step 3: Register the wall cases**

Use code IDs `Mars={0,1,2}`, `Moon={3}`, `LEO={4}`, `Earth={5,6,7,8,9}`. For both `k=4` and `k=5`:

| Initiator | `P={}` | Pinned self | Expected profile |
|---|---|---|---|
| Mars | incomparable | incomparable, pin `{0}` | both gaps |
| Moon | incomparable | incomparable, pin `{3}` | both gaps |
| LEO | incomparable | `r2-strictly-implies-r1`, pin `{4}` | unpinned both; pinned `(1,0)` only |
| Earth | `r2-strictly-implies-r1` | same, pin `{5}` | `(1,0)` only |

Register deterministic witnesses:

- `k=4`: Mars `(1,0)={0,3,4,5,6}`, Moon `{3,4,5,6}`, LEO `{4,5,6}`, Earth `{5,6}`; unpinned `(0,1)={5,6,7,8}` and pinned Mars/Moon add their pinned node.
- `k=5`: Mars `(1,0)={0,3,4,5}`, Moon `{3,4,5}`, LEO `{4,5}`, Earth `{5}`; unpinned `(0,1)={5,6,7,8,9}` and pinned Mars/Moon add their pinned node.

Explicitly fence off the scarcity lemma and every aggregate conclusion not listed above.

- [ ] **Step 4: Inspect and commit before any Artifact A code exists**

Run:

```powershell
rg -n "quorum_audit" quorum_audit.py experiments tests 2>$null
```

Expected: no implementation or test file exists. Commit using message `docs: preregister generic quorum auditor outputs`, then perform the signed timestamp procedure.

---

### Task 2: Validate Inputs and Canonicalize Quorum Families

**Files:**
- Create: `quorum_audit.py`
- Create: `tests/test_quorum_audit.py`

**Interfaces:**
- Produces `PredicateRelation(StrEnum)` with the four registered string values.
- Produces `QuorumAudit[NodeT]` frozen dataclass with `universe`, `pinned`, `phase1_minimal`, `phase2_minimal`, `phase1_effective`, `phase2_effective`, `safe`, `unsafe_witness`, `relation`, `gap_10_witness`, `gap_01_witness`, and `self_check_passed`.
- Produces `_normalize_inputs(universe, phase1, phase2, pinned)` and `_minimal_antichain(family, rank)`.

- [ ] **Step 1: Write failing validation and canonicalization tests**

```python
import pytest

from quorum_audit import audit_quorum_families


@pytest.mark.parametrize("kwargs", [
    {"universe": [], "phase1": [["a"]], "phase2": [["a"]]},
    {"universe": ["a"], "phase1": [], "phase2": [["a"]]},
    {"universe": ["a"], "phase1": [[]], "phase2": [["a"]]},
    {"universe": ["a"], "phase1": [["b"]], "phase2": [["a"]]},
    {"universe": ["a"], "phase1": [["a"]], "phase2": [["a"]],
     "pinned": ["b"]},
])
def test_invalid_inputs_raise_value_error(kwargs):
    with pytest.raises(ValueError):
        audit_quorum_families(**kwargs)


def test_supersets_are_removed_without_changing_semantics():
    report = audit_quorum_families(
        ["a", "b", "c"], [["a", "b"], ["a", "b", "c"]],
        [["a", "b"]])
    assert report.phase1_minimal == (frozenset({"a", "b"}),)
    assert report.phase2_minimal == report.phase1_minimal
```

- [ ] **Step 2: Run and observe collection failure**

Run: `$env:UV_PROJECT_ENVIRONMENT='.venv-windows'; uv run pytest tests/test_quorum_audit.py -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'quorum_audit'`.

- [ ] **Step 3: Implement the types, validation, and minimizer**

Use universe sequence order as the library's deterministic rank, allowing arbitrary hashable identifiers without requiring mixed types to compare. Reject duplicate universe entries. Canonical families sort by `(cardinality, tuple(node ranks))`.

```python
from dataclasses import dataclass
from enum import StrEnum
from typing import Generic, Hashable, Iterable, Sequence, TypeVar

NodeT = TypeVar("NodeT", bound=Hashable)
Quorum = frozenset[NodeT]


class PredicateRelation(StrEnum):
    EQUAL = "equal"
    R1_STRICTLY_IMPLIES_R2 = "r1-strictly-implies-r2"
    R2_STRICTLY_IMPLIES_R1 = "r2-strictly-implies-r1"
    INCOMPARABLE = "incomparable"


@dataclass(frozen=True)
class QuorumAudit(Generic[NodeT]):
    universe: tuple[NodeT, ...]
    pinned: frozenset[NodeT]
    phase1_minimal: tuple[Quorum, ...]
    phase2_minimal: tuple[Quorum, ...]
    phase1_effective: tuple[Quorum, ...]
    phase2_effective: tuple[Quorum, ...]
    safe: bool
    unsafe_witness: tuple[Quorum, Quorum] | None
    relation: PredicateRelation
    gap_10_witness: Quorum | None
    gap_01_witness: Quorum | None
    self_check_passed: bool | None = None
```

Add a temporary `audit_quorum_families` that validates, canonicalizes, and returns an `equal` report only when the two canonical antichains match; other relations can raise `NotImplementedError` until Task 3.

- [ ] **Step 4: Run the narrow tests**

Expected: validation and canonicalization tests PASS; no other Artifact A tests exist yet.

- [ ] **Step 5: Commit**

Commit with `feat: validate and canonicalize explicit quorum families`, then perform the signed timestamp procedure.

---

### Task 3: Classify Safety, Predicate Ordering, Gaps, and Witnesses

**Files:**
- Modify: `quorum_audit.py`
- Modify: `tests/test_quorum_audit.py`

**Interfaces:**
- Completes `audit_quorum_families(..., pinned=(), exhaustive=False) -> QuorumAudit`.
- Relation is derived from gap existence, not family syntax.
- Safety remains available for unsafe configurations and reports the first deterministic disjoint pair.

- [ ] **Step 1: Add the registered unpinned tests**

```python
from quorum_audit import PredicateRelation


@pytest.mark.parametrize("q1,q2,relation,w10,w01", [
    ([["a", "b"], ["a", "b", "c"]], [["a", "b"]],
     PredicateRelation.EQUAL, None, None),
    ([["a", "b"]], [["a"]],
     PredicateRelation.R1_STRICTLY_IMPLIES_R2, None, frozenset({"a"})),
    ([["a"]], [["a", "b"]],
     PredicateRelation.R2_STRICTLY_IMPLIES_R1, frozenset({"a"}), None),
    ([["a", "b"]], [["a", "c"]],
     PredicateRelation.INCOMPARABLE, frozenset({"a", "b"}),
     frozenset({"a", "c"})),
])
def test_registered_predicate_classes(q1, q2, relation, w10, w01):
    report = audit_quorum_families(["a", "b", "c"], q1, q2)
    assert report.relation is relation
    assert report.gap_10_witness == w10
    assert report.gap_01_witness == w01


def test_unsafe_configuration_is_classified_and_flagged():
    report = audit_quorum_families(
        ["a", "b", "c"], [["a"]], [["b"]])
    assert report.safe is False
    assert report.unsafe_witness == (
        frozenset({"a"}), frozenset({"b"}))
    assert report.relation is PredicateRelation.INCOMPARABLE
```

- [ ] **Step 2: Run and observe failures**

Expected: directional/incomparable cases FAIL at the temporary `NotImplementedError` or wrong relation.

- [ ] **Step 3: Implement containment classification**

For each effective Phase 1 quorum, a `(1,0)` witness exists iff it contains no effective Phase 2 quorum; apply the dual for `(0,1)`. Choose the smallest candidate, then its rank tuple. Classify in this order:

```python
if not gap_10 and not gap_01:
    relation = PredicateRelation.EQUAL
elif not gap_10:
    relation = PredicateRelation.R1_STRICTLY_IMPLIES_R2
elif not gap_01:
    relation = PredicateRelation.R2_STRICTLY_IMPLIES_R1
else:
    relation = PredicateRelation.INCOMPARABLE
```

Compute `unsafe_witness` over the original minimal antichains, sorting pairs by the same deterministic key. Do not reject unsafe input after validation.

Document classification complexity as
`O(|min(Q1)| * |min(Q2)| * |N|)`, excluding parsing, deterministic
sorting, and the separate quadratic-in-family-size antichain minimization
pass.

- [ ] **Step 4: Add and run uniform-threshold tests**

Generate all `r`-subsets with `itertools.combinations` and assert the registered `1/3`, `2/2`, and `3/1` results. Expected: all Task 2–3 tests PASS.

- [ ] **Step 5: Commit**

Commit with `feat: classify quorum predicate order and capability gaps`, then perform the signed timestamp procedure.

---

### Task 4: Add Pinned Domains and an Independent Exhaustive Self-Check

**Files:**
- Modify: `quorum_audit.py`
- Modify: `tests/test_quorum_audit.py`

**Interfaces:**
- `pinned` lifts every minimal quorum to `q union P`, then minimizes again.
- `verify_report_exhaustively(universe, phase1, phase2, pinned, report) -> None` enumerates connectivity states from original input and raises `AssertionError` on disagreement.
- `exhaustive=True` sets `report.self_check_passed` to `True` only after that independent check.

- [ ] **Step 1: Write pinned-domain tests**

```python
def test_pinning_can_close_only_the_01_gap():
    unpinned = audit_quorum_families(
        ["a", "b", "c"], [["a"]], [["b"]])
    pinned = audit_quorum_families(
        ["a", "b", "c"], [["a"]], [["b"]], pinned=["a"])
    assert unpinned.relation is PredicateRelation.INCOMPARABLE
    assert pinned.relation is PredicateRelation.R2_STRICTLY_IMPLIES_R1
    assert pinned.gap_10_witness == frozenset({"a"})
    assert pinned.gap_01_witness is None


def test_lifted_family_is_minimized_again():
    report = audit_quorum_families(
        ["a", "b", "c"], [["a"], ["b"]], [["c"]], pinned=["a"])
    assert report.phase1_effective == (frozenset({"a"}),)
```

- [ ] **Step 2: Run and observe the pinned tests fail**

Expected: at least the lifted-family assertion FAILS until lifting is implemented.

- [ ] **Step 3: Implement pinned lifting**

After canonicalizing the original families:

```python
phase1_effective = _minimal_antichain(
    (q | pinned_set for q in phase1_minimal), rank)
phase2_effective = _minimal_antichain(
    (q | pinned_set for q in phase2_minimal), rank)
```

Use effective families only for predicate relation and gap witnesses; use original families for safety.

- [ ] **Step 4: Write the independent self-check and mutation control**

```python
from dataclasses import replace

from quorum_audit import verify_report_exhaustively


def test_exhaustive_self_check_passes():
    report = audit_quorum_families(
        ["a", "b", "c"], [["a", "b"]], [["a", "c"]],
        pinned=["a"], exhaustive=True)
    assert report.self_check_passed is True


def test_mutated_report_is_rejected_by_independent_check():
    args = (["a", "b", "c"], [["a", "b"]], [["a", "c"]], [])
    report = audit_quorum_families(*args[:3])
    bad = replace(report, relation=PredicateRelation.EQUAL,
                  gap_10_witness=None, gap_01_witness=None)
    with pytest.raises(AssertionError):
        verify_report_exhaustively(*args, bad)
```

The independent function must enumerate all `C subseteq N` satisfying `P subseteq C`, evaluate `any(q subseteq C for q in original_family)`, and compare the resulting set differences and relation. It must not call `_minimal_antichain` or reuse containment classification.

- [ ] **Step 5: Exhaustively validate all three-node family pairs**

Add a test iterating every nonempty family over the seven nonempty subsets of `{a,b,c}` and all eight pinned sets. Expected: `129032` classifications agree, matching the design-review verification.

- [ ] **Step 6: Commit**

Commit with `feat: audit pinned connectivity domains exhaustively`, then perform the signed timestamp procedure.

---

### Task 5: Build the CLI and Reconcile Registered Wall Outputs

**Files:**
- Create: `experiments/quorum_audit.py`
- Create: `experiments/quorum_audit_registered.py`
- Create: `tests/test_quorum_audit_cli.py`
- Modify: `tests/test_quorum_audit.py`
- Create: `results/capability/quorum_audit_registered.json`

**Interfaces:**
- JSON input: `{"universe": [str], "phase1": [[str]], "phase2": [[str]], "pinned": [str]}`.
- CLI: `uv run python experiments/quorum_audit.py --input PATH|- --format json|text --exhaustive`.
- Exit `0` on a completed audit, including unsafe configurations; exit `2` on schema/input errors.
- JSON output keys: `universe`, `pinned`, `safe`, `unsafe_witness`, `phase1_minimal`, `phase2_minimal`, `phase1_effective`, `phase2_effective`, `relation`, `gaps`, `self_check_passed`.

- [ ] **Step 1: Write failing CLI tests**

Use `subprocess.run` with the active interpreter. Assert a valid incomparable case returns code 0, canonical sorted JSON, `safe: true`, relation `incomparable`, and both witnesses. Assert an unknown node returns code 2 with a one-line error on stderr and no traceback. Run the same input twice and assert stdout bytes are identical.

- [ ] **Step 2: Implement the thin CLI**

The CLI validates JSON container types before calling the library, sorts the string universe lexicographically, writes JSON with `sort_keys=True, indent=2`, and renders unsafe status first in text mode. It contains no quorum logic.

- [ ] **Step 3: Add wall-family construction to the registered driver**

For each code-tier initiator, build minimal Phase 1 families as the Cartesian product of one node from each required non-anchor tier and `combinations(Earth, |E|-k+1)`. Build Phase 2 as `combinations(Earth, k)`. Audit `k in {4,5}` under `P={}` and the first node of the initiating tier.

- [ ] **Step 4: Reconcile against the existing CSV**

Load `results/capability/dual_gradient_map.csv`. For each registered wall row, compare auditor gap existence with the matching `(1,0)` and `(0,1)` columns under the explicitly corresponding `reachable_unconstrained` or `reachable_self_reachable` reading. A mismatch raises and prevents output. Stop and write a discrepancy note identifying registration, implementation, and model-reading possibilities before changing anything; never adjust the expected output to obtain a pass. Do not compare a pinned result to the unconstrained column.

- [ ] **Step 5: Run and score the registration**

Run:

```powershell
$env:UV_PROJECT_ENVIRONMENT='.venv-windows'
uv run python experiments/quorum_audit_registered.py --output results/capability/quorum_audit_registered.json
uv run pytest tests/test_quorum_audit.py tests/test_quorum_audit_cli.py -v
```

Expected: every registered case matches; wall/CSV disagreements are `0`; the output includes no scarcity-lemma verdict.

- [ ] **Step 6: Verify deterministic regeneration**

Copy the first JSON to a temporary path, regenerate, and use `Compare-Object` or `Get-FileHash` to require byte identity.

- [ ] **Step 7: Commit**

Commit with `feat: add registered generic quorum auditor`, then perform the signed timestamp procedure.

---

### Task 6: Rewrite the Title, Abstract, Introduction, and Scope

**Files:**
- Modify: `docs/paper/nines/main.tex`
- Create: `tests/test_nines_claim_language.py`

**Interfaces:**
- Consumes: Task 5's exact four-way classification and registered output.
- Produces: the author-review draft of page 1 and the fixed-membership applicability boundary.

- [ ] **Step 1: Add failing manuscript regression checks**

```python
from pathlib import Path

PAPER = Path("docs/paper/nines/main.tex")


def test_approved_title_and_disallowed_claims():
    text = PAPER.read_text(encoding="utf-8")
    assert "Legible Consensus: Capability Gaps in Flexible Quorums" in text
    for banned in (
        "every departure from phase symmetry opens exactly one",
        "blocks liveness completely",
        "only available mitigation",
    ):
        assert banned not in text


def test_scope_and_edge_boundary_are_explicit():
    text = PAPER.read_text(encoding="utf-8")
    assert "N_e" in text and "C_t" in text
    assert "magnifying glass" in text
    assert "not an evaluated result" in text
```

Run the test and require failure on the title and banned abstract language before editing.

- [ ] **Step 2: Replace the title and abstract exactly**

Use title `Legible Consensus: Capability Gaps in Flexible Quorums` and this
HotCRP-registered abstract (convert punctuation to LaTeX without changing the
claim language):

> Distributed-systems monitoring tells operators which nodes are healthy, but
> not whether a quorum-based service can acquire proposal authority or complete
> a commit. Phase symmetry makes these capabilities identical; with majority
> quorums, a reachable-node count therefore answers both questions. Flexible
> quorum designs can spend that coincidence to make the commit path fast. We
> characterize the resulting capability gaps exactly. An
> acquisition-but-not-commit state is impossible if and only if every Phase 1
> quorum contains a Phase 2 quorum; the dual gap is impossible under the
> mirrored containment. Thus, when the induced phase predicates differ, at
> least one gap exists: directionally ordered predicates admit one direction,
> while incomparable predicates admit both. In pre-registered single-decree
> simulations, the two gaps have sharply different consequences. An
> acquisition-only incumbent was indistinguishable from a healthy contender on
> every recorded metric and injected accepted values that subsequent
> acquisition must preserve. Its commit-capable but acquisition-incapable dual
> prevented a healthy proposer from deciding in all 50 seeds at retry budget
> eight under the modeled retry policy. We apply the characterization to a
> wall-shaped quorum construction for physically tiered networks. The wall
> anchors commits to the fast tier and encodes per-tier participation policy in
> acquisition. No Phase 2 threshold closes both gaps at every tier, but an
> O(tiers) readout exposes each tier's capability state and failed obligation
> from a connectivity summary. A 10-node Earth/LEO/Moon/Mars topology makes the
> distinction physically undeniable. The same characterization applies to edge
> and control-plane systems whose effective participation changes faster than
> logical membership: Mars does not create the gap; it stretches it until node
> counts, timeouts, and green dashboards can no longer hide it. Quorum
> properties are verified exhaustively in TLA+; all results are design-level.

- [ ] **Step 3: Rewrite Section 1 to this seven-paragraph contract**

1. Start with the familiar operator problem: node health does not answer whether authority can be acquired or a commit completed.
2. Introduce Mars as the magnifier and terrestrial edge as the recognition moment on page 1. Include: “Mars does not create the capability gap; it stretches the gap until familiar operational approximations—timeouts, node counts, and green health dashboards—can no longer hide it.”
3. Explain phase symmetry as the reason one reachability count historically answered both predicates; majority is an instance, not the cause.
4. State the exact four-way correspondence, including incomparable predicates admitting both gaps.
5. Report the two measured consequences with the registered model bounds; do not call `(1,0)` inherently benign or `(0,1)` unconditional livelock.
6. Introduce the wall as a participation-policy case study: upper-tier witnesses are valuable only when local representation, administrative, sovereignty, or fencing policy requires them; otherwise the wall is analytical, not recommended deployment geometry.
7. End with three contributions only: formal characterization plus auditor; measured valence difference; wall case study plus legible state readout and structural edge applicability.

- [ ] **Step 4: Add the participation/membership paragraph to System Model**

Define logical acceptor universe `N_e`, effective reachable set `C_t subseteq N_e`, and the decision window. State that the analysis applies epoch by epoch; changing `N_e` requires a reconfiguration/state-transfer protocol outside scope. Do not imply Paxos has no membership discipline.

- [ ] **Step 5: Run prose tests and compile**

Run the manuscript regression test, then from `docs/paper/nines` run `pdflatex main && bibtex main && pdflatex main && pdflatex main`. Require exit 0 and inspect the log for undefined references/citations and overfull boxes that affect readability.

- [ ] **Step 6: Author review gate**

Provide the compiled PDF and a concise list of changed claims to the author. Do not proceed with additional prose restructuring until the author has reviewed the new Section 1.

- [ ] **Step 7: Commit after author acceptance**

Commit with `docs: reframe NINeS paper around capability gaps`, then perform the signed timestamp procedure.

---

### Task 7: Correct the Theorem, Related Work, Consequences, and Payoff

**Files:**
- Modify: `docs/paper/nines/main.tex`
- Modify: `docs/paper/nines/references.bib`
- Modify: `tests/test_nines_claim_language.py`

**Interfaces:**
- Consumes: Refined Quorum Systems, Section 2.1 Definition 2; Li–Chan–Lesani, Section 2 Definition 6; Task 5 auditor results; existing registered flip data.
- Produces: the exact theorem and defensible novelty boundary throughout the paper.

- [ ] **Step 1: Verify the two primary sources before drafting**

Read [Guerraoui and Vukolić, Refined Quorum Systems](https://vukolic.com/RQS-DC.pdf), especially Section 2.1: its classes satisfy `QC1 subseteq QC2 subseteq RQS`, while its Properties 1–3 impose adversary-aware intersection conditions. Read [Li, Chan, and Lesani, Quorum Subsumption](https://drops.dagstuhl.de/storage/00lipics/lipics-vol281-disc2023/LIPIcs.DISC.2023.28/LIPIcs.DISC.2023.28.pdf), Definition 6: for every member `p` of quorum `q`, some personal quorum of `p` is contained in `q`. Record page/definition notes before writing.

- [ ] **Step 2: Add the RQS bibliography entry**

```bibtex
@article{guerraoui2010rqs,
  author = {Guerraoui, Rachid and Vukoli\'{c}, Marko},
  title = {Refined Quorum Systems},
  journal = {Distributed Computing},
  volume = {23},
  number = {1},
  pages = {1--42},
  year = {2010},
  doi = {10.1007/s00446-010-0103-7}
}
```

- [ ] **Step 3: Replace Proposition 2 with predicate-level definitions and the exact correspondence**

Define `Form(Q)` explicitly. State semantic equality rather than syntactic family equality. Prove the two containment emptiness lemmas, then state the corollary:

- equal predicates iff neither gap is reachable;
- `R1` strictly implies `R2` iff only `(0,1)` is reachable;
- `R2` strictly implies `R1` iff only `(1,0)` is reachable;
- incomparable predicates iff both gaps are reachable.

Formally introduce the term—“we call the mixed states capability gaps”—and
call the reachable gap profile a complete invariant of the predicate ordering.
State the auditor's pairwise-containment complexity with the same exclusions as
Task 3. Keep the wall threshold result as a construction-specific proposition
after the general result.

- [ ] **Step 4: Repair related-work positioning**

State that RQS already uses nested quorum classes to connect resilience and optimistic complexity; its class inclusion and adversary-aware intersections are not this paper's phase-formability ordering. State that LCL subsumption is a member-indexed containment condition for heterogeneous Byzantine trust and protocol progress; this paper instead compares two phase predicates over a fixed crash-stop acceptor universe. Claim novelty only for the exact gap-profile correspondence, its operational interpretation, and application to phase-asymmetric Paxos—not for containment as a mathematical tool.

- [ ] **Step 5: Repair wall, valence, and operational claims**

- In the wall section, explain the participation policy each upper-tier witness can encode and include the analytical fallback when no such policy exists.
- In the valence section, say `(1,0)` was indistinguishable from the healthy contender on every recorded metric and may inject accepted values later preserved; say `(0,1)` prevented decision in 50/50 seeds at retry budget 8 under the modeled retry policy.
- In the discussion, map `(0,1)` to ordinary automated failover: restarting a serving incumbent can turn a commit-capable state into a stall because fresh acquisition is impossible.
- Replace the existing service ladder with four capability-state rows and explicitly separate structural capability, fresh runtime authority, and chosen client contract.
- Keep the terrestrial paragraph labeled structural prediction, not evaluation.
- Replace “only mitigation” with “state-aware mitigation requires distinguishing the state.”

- [ ] **Step 6: Update conclusion and traceability**

The conclusion returns from Mars to the general mistake: distance changes visibility and cost, not the existence of the gap. Add Artifact A and `results/capability/quorum_audit_registered.json` to traceability as executable corroboration of the proof. If Artifact B is not yet present, do not cite it.

- [ ] **Step 7: Run claim audit, full tests, and paper build**

Run:

```powershell
$env:UV_PROJECT_ENVIRONMENT='.venv-windows'
uv run pytest
rg -n -i "exactly one|blocks liveness completely|only available mitigation|recover authority|every tier but one" docs/paper/nines/main.tex
```

Expected: pytest passes; every `rg` hit is either absent or explicitly scoped/quoted and manually justified. Compile four LaTeX passes as in Task 6.

- [ ] **Step 8: Commit**

Commit with `docs: state exact capability correspondence and evidence bounds`, then perform the signed timestamp procedure.

---

### Task 8: Optional Wall Capability-Readout CLI

**Schedule gate:** Start only if Tasks 1–7 are accepted and enough time remains for Task 9. Otherwise record “omitted as droppable” in traceability and skip this task without weakening the paper.

**Files:**
- Create: `experiments/capability_readout.py`
- Create: `tests/test_capability_readout.py`
- Create: `examples/capability/planetary_moon_01.json`
- Create: `examples/capability/edge_remote_01.json`
- Modify: `docs/paper/nines/main.tex` only after tests pass.

**Interfaces:**
- Input includes named tiers in slow-to-fast code order, integer node IDs, `phase2_threshold`, initiating tier name, reachable IDs, `configuration_source`, and `connectivity_source`.
- Output includes `R1`, `R2`, state, witnesses, typed missing obligations with tier names, `requires_preexisting_authority`, evidence provenance, `runtime_authority: unknown`, and `service_policy: not-inferred`.
- It never chooses an action or asserts that incumbent authority is valid.

- [ ] **Step 1: Write failing readout tests**

The planetary example is Moon + all Earth reachable, LEO absent, strict `k=5`; expect `(0,1)`, one failed Phase 1 LEO obligation, Phase 2 witness all Earth, and preexisting-authority dependence. The edge example uses `remote=[100,101]`, `metro=[200]`, `cloud=[1,2,3]`, initiator `remote`, strict `k=3`, with remote + cloud reachable and metro absent; expect the same state and a missing metro obligation.

Assert both outputs say runtime authority is unknown and service policy is not inferred.

- [ ] **Step 2: Implement parsing and report conversion around `capability.classify`**

Do not reimplement quorum predicates. Resolve tier names to code indices, validate node uniqueness and reachable subsets, construct `CrumblingWallQuorum`, call `classify`, and translate typed obligations back to names.

- [ ] **Step 3: Add deterministic JSON/text CLI output**

Run each example twice and require byte-identical JSON. Invalid tier names or nodes exit 2 without traceback.

- [ ] **Step 4: Run tests and update traceability**

Run `uv run pytest tests/test_capability_readout.py tests/test_capability.py -v`. Only after PASS, add the CLI and examples to the paper as a demonstration artifact, not empirical evidence.

- [ ] **Step 5: Commit**

Commit with `feat: expose wall capability readout`, then perform the signed timestamp procedure.

---

### Task 9: Build, Claim-Audit, and Package the Anonymous Submission Artifact

**Files:**
- Create: `artifact-manifest.txt`
- Create: `scripts/build_anonymous_artifact.py`
- Create: `tests/test_anonymous_artifact.py`
- Modify: `docs/ai-provenance.md`
- Modify: `docs/paper/nines/main.tex`

**Interfaces:**
- `python scripts/build_anonymous_artifact.py --output dist/eidolon-nines-anonymous.zip` builds only allowlisted tracked files from the working tree after requiring cleanliness.
- The builder rejects case-insensitive content matches for `Tony`, `wamason`, `research@`, `/home/`, and `C:\Users\` and rejects `.git`, `timestamps`, old `docs/paper/main.tex`, marching orders, plans, and specs.
- The artifact includes every path cited in the final traceability table.

- [ ] **Step 1: Write the manifest as a positive allowlist**

Begin with this manifest; remove no entry unless the final traceability table no
longer cites it, and add Artifact B's three paths only if Task 8 completed:

```text
README.md
pyproject.toml
uv.lock
capability.py
datacenter.py
demo_step_9.py
demo_step_10.py
duel.py
entity.py
flip.py
metrics.py
network.py
partition.py
paxos.py
quorums.py
scenarios.py
time_budget.py
quorum_audit.py
experiments/*.py
tests/*.py
results/**/*.csv
results/**/*.svg
tla/*.tla
tla/*.cfg
docs/step9-repro.md
docs/ai-provenance.md
docs/paper/nines/main.tex
docs/paper/nines/references.bib
docs/superpowers/notes/2026-07-29-midround-flip-preregistration.md
docs/superpowers/notes/2026-07-29-midround-flip-results.md
docs/superpowers/notes/2026-07-30-dual-and-gradient-preregistration.md
docs/superpowers/notes/2026-07-30-dual-and-gradient-results.md
docs/superpowers/notes/2026-07-30-quorum-auditor-preregistration.md
```

Do not use `git archive HEAD` over the whole repository: tracked legacy files
contain names and absolute home paths. The builder expands manifest globs only
against tracked files and applies the deny scan after expansion.

- [ ] **Step 2: Write failing packaging tests**

Test that a synthetic identifying file causes the scanner to fail, that a clean synthetic tree packages successfully, that archive member names are relative and contain no `.git`/`timestamps`, and that every final traceability path is present or explicitly represented by its containing directory rule.

- [ ] **Step 3: Implement deterministic packaging**

Read only paths listed by the manifest, reject untracked or missing entries, scan UTF-8-decodable files, and write a ZIP whose member timestamps are fixed and members sorted. Emit a SHA-256 digest and member count. The script does not modify source files.

- [ ] **Step 4: Run the full evidence audit**

Run the full pytest suite; regenerate Artifact A twice and require byte identity; regenerate each existing CSV cited by a claim where the repository reproduction document supplies a bounded command; check the current executable enum has no `Hazard.DISRUPTIVE_ELECTION` reference; verify the `GridQuorum` limitation remains documented; verify both paper occurrences of 27,921 remain correct.

- [ ] **Step 5: Build and inspect the final PDF**

Run `pdflatex`, `bibtex`, `pdflatex`, `pdflatex`. Require no undefined citations/references. Inspect title, abstract, figures, tables, page count, and the page-1 edge mapping visually. Confirm the PDF metadata and author line contain only `Paper #98` or the venue-required anonymous marker.

- [ ] **Step 6: Update AI provenance honestly**

Record that the introduction and revision were machine-drafted from the approved design; the human author reviewed claim-affecting prose; Claude and Codex performed cross-model review; expected outputs preceded implementation; and all claims were audited against proof or artifacts. Do not insert personal identity into the anonymous copy.

- [ ] **Step 7: Build and inspect the anonymous ZIP**

Build to a temporary/output directory outside the allowlisted source. List every member, run the deny scan on extracted content, confirm no Git history or signing identity is present, and manually match the traceability appendix to archive members.

- [ ] **Step 8: Commit the audit machinery and final manuscript**

Commit with `release: audit NINeS capability-gaps submission`, then perform the signed timestamp procedure. The generated ZIP itself need not be committed; record its SHA-256 in the handoff.

---

### Task 10: Upload Checkpoint and Terrestrial-Experiment Handoff

**Files:**
- No repository mutation before author confirmation of upload.
- A later brainstorming session will create a separate terrestrial-experiment spec and preregistration if the gate passes.

- [ ] **Step 1: Hand the author the audited PDF and artifact digest**

Report the final commit, signature verification, timestamp status, PDF path/hash, ZIP path/hash, test count, and any omitted droppable work. The author performs the HotCRP upload.

- [ ] **Step 2: Record the upload checkpoint**

Wait for explicit author confirmation that the audited PDF—not merely an earlier draft—is uploaded. This freezes the submitted claim set before terrestrial experiment design.

- [ ] **Step 3: Apply the terrestrial design-pass gate**

Proceed to a new brainstorming/specification cycle only if a candidate experiment has: a nontrivial falsifiable question not answered by renaming planetary tiers; a non-strawman node-health/failover baseline; a motivated fixed-membership edge topology and trace; preregistered policies, metrics, and null; and information value beyond the theorem and current experiment.

- [ ] **Step 4: Preserve the outcome protocol**

Formal contradiction triggers proof/implementation/model investigation and correction or withdrawal; applicability contradiction narrows/corrects/withdraws the edge claim; inconclusive or mere reproduction stays in the repository without changing the paper; corroboration without new insight ordinarily leaves the paper unchanged; genuine extension is considered for this revision only if it fits cleanly, otherwise it becomes successor work.

---

## Self-Review Checklist

- **Spec coverage:** exact four-way theorem (Tasks 3, 7); pinning and both readings (Tasks 1, 4, 5); safety, deterministic witnesses, validation, JSON IDs, exhaustive mutation check (Tasks 2–5); scarcity fence (Tasks 1, 5); capability-first story, edge recurrence, membership boundary, wall policy, three contributions, RQS/LCL (Tasks 6–7); optional readout (Task 8); claim/anonymization/build audits (Task 9); post-upload terrestrial gate (Task 10).
- **Existing hygiene:** enum rename, `GridQuorum` warning, and 27,921 attribution are regression checks, not reopened implementation tasks. Cassandra version pin remains successor work unless Cassandra is reintroduced as evidence.
- **Deliberate omission:** the dual dashboard remains optional communication
  slack after all upload gates; this plan does not spend implementation time on
  it before the proof, auditor, manuscript, readout decision, or anonymous
  package are complete.
- **Type consistency:** all library witnesses are `frozenset[NodeT] | None`; JSON nodes are strings; library determinism follows universe order, while CLI determinism follows sorted string order; `R1_STRICTLY_IMPLIES_R2` corresponds to only `(0,1)`, and the reverse corresponds to only `(1,0)`.
- **Model consistency:** pinning restricts connectivity states and does not alter safety; self-reachable wall equivalence depends on within-tier symmetry and is never generalized silently.
- **Selection-pressure boundary:** the audited upload precedes terrestrial design and execution.
- **Placeholder scan:** no `TBD`, `TODO`, “implement later,” or unspecified error-handling steps remain.
