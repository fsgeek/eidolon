# NINeS Narrative and Voice Revision Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a rigorous, 12-page NINeS manuscript that leads the reader through the acquisition/commit mystery, presents the crumbling wall as a useful but bounded design tool, and sounds recognizably like the human author.

**Architecture:** The work proceeds through an auditable narrative map, an introduction pilot with a human voice gate, and then section-sized editorial units. Claim-language tests protect formal and evidentiary boundaries, while the map records every move, merge, deletion, and changed implication; the final LaTeX/PDF and anonymous artifact are rebuilt only after those controls pass.

**Tech Stack:** LaTeX (`pdflatex`, `bibtex`, `latexmk`), Python >=3.14, `uv`, pytest, PowerShell, Git/GPG through WSL, Poppler (`pdfinfo`, `pdftotext`, `pdftoppm`).

## Global Constraints

- The approved source of truth is `docs/superpowers/specs/2026-07-30-nines-narrative-voice-revision-design.md`.
- The active manuscript is `docs/paper/nines/main.tex`; do not edit `docs/paper/main.tex`.
- Preserve the title, proofs, theorem meanings, registered measurements, citations, artifact paths, double-blind author line, and 5/1/1/3 topology notation unless an audit identifies a documented error.
- The body, ending with the conclusion, must fit within 12 pages. References and appendices do not count toward that bound. The current PDF reaches body prose on page 13, so the revision must recover at least one rendered page.
- Use transitions to replace duplicated exposition. Do not grow the paper by appending narrative commentary to the existing structure.
- The story sequence supplies the mystery; the manuscript must not describe itself as a murder mystery or conceal results merely to create suspense.
- At first use, gloss acquisition as acquiring proposal authority and connect it to leader election under Multi-Paxos without changing the single-decree meaning of the registered experiment.
- Introduce phase-predicate coincidence before using language about spending or giving it up. State that symmetry makes the predicates equal; a reachable-node count decides that shared predicate only for threshold families such as majority and from a stated connectivity vantage point.
- Define legibility in the introduction as reading phase capabilities and failed obligations from a compact structural representation and a supplied connectivity summary, without quorum-subset enumeration or protocol execution. State the wall readout's `O(tiers)` complexity there.
- Keep the readout boundary explicit: it consumes known configuration and connectivity; it does not detect connectivity, establish current authority, choose recovery policy, or ensure that an operator acts on it.
- Present the wall's positive results before its boundary. Every use of “prevents” or “closes” must name the gap, threshold, tier, and connectivity reading.
- Preserve the registered 5/1/1/3 wall facts: at `k=3`, `(1,0)` is absent at every tier; both gaps are absent for Earth under both readings and for LEO under the self-reachable reading; `(0,1)` remains reachable for Moon and Mars. More generally, `(1,0)` is absent at every tier for `k <= 3`, while `(0,1)` is absent for Earth and self-reachable LEO for `k >= 3`.
- Do not revive the falsified claim that the wall “compensates every other tier with legibility.” Legibility explains residual states; it is not compensation for closing them.
- Bound the valence result to the registered single-decree topology, retry policy, and retry budget. `(0,1)` is not intrinsically harmful; `(1,0)` is not universally benign.
- Treat non-anchor witnesses as participation policy, not an additional Paxos safety requirement.
- Treat the edge mapping as structural applicability and a deterministic demonstration, not terrestrial empirical validation. The terrestrial edge experiment remains outside this revision.
- Keep performance-preserving closure open. Scoped authority, gap-aware proposer behavior, and multi-anchor families may appear only as unvalidated future directions.
- Do not add the production-system census or the optional dual-dashboard figure in this plan.
- Use the author corpus as evidence of tendencies, not a stylometric template. Do not copy idiosyncrasies, legal boilerplate, coauthor phrasing, or source passages into the paper or repository notes.
- Review em-dashes, contrast pivots, slogans, symmetrical lists, personification, repeated interpretations, and “deeper” transitions in context. Counts are discovery aids, never quotas and never an instruction for automatic replacement.
- Preserve useful texture: short conclusions may coexist with longer explanatory sentences, and formal, experimental, and explanatory passages need not share one cadence.
- `tmp/` is untracked author material. Read it, but never stage, modify, quote into tracked notes, or include it in the anonymous artifact.
- Preserve the user-owned untracked review `docs/superpowers/specs/2026-07-30-nines-story-kibbitz.md`; do not edit or stage it.
- Use `uv`, never `pip`. In PowerShell set `$env:UV_PROJECT_ENVIRONMENT='.venv-windows'` before `uv run ...`.
- Make semantic commits from WSL with `git commit -S`. Verify fingerprint `1D7C4A68252F6EC1ACD2FC8E934778A0EB5EABB1`; do not commit from Windows Git.
- At every editorial checkpoint inspect both the source diff and the rendered PDF. A passing string test is not evidence that a paragraph is accurate, readable, or in the author's voice.
- When an audit reveals an unexpected result, stop and update the claim, map, or plan openly. Do not smooth the discrepancy away.

---

## File Map

- Create `docs/superpowers/notes/2026-07-30-nines-narrative-map.md`: paragraph roles, destination map, claim ledger, implication audit, voice observations, and page-budget ledger.
- Modify `docs/paper/nines/main.tex`: abstract; section order; narrative transitions; legibility definition; wall result ordering; common-condition table; evidence bounds; discussion; conclusion; and traceability appendix.
- Modify `tests/test_nines_claim_language.py`: replace brittle slogan checks with regression checks for the approved narrative, wall, legibility, table, and epistemic boundaries.
- Modify `docs/ai-provenance.md`: record the cross-model editorial loop and the human voice/claim review gate without adding identity to the anonymous artifact.
- Rebuild tracked `docs/paper/nines/main.pdf`: final double-blind rendering.
- Inspect, but do not modify unless a verified citation error appears: `docs/paper/nines/references.bib`.
- Read-only evidence inputs: `results/tier_liveness/tier_sweep_full_ci.csv`, `results/capability/dual_gradient_map.csv`, `results/capability/dual_uniform.csv`, `results/flip/flip_map.csv`, `results/flip/flip_sweep.csv`, and `results/capability/quorum_audit_registered.json`.
- Read-only authorial references: `tmp/papers/1812.00276v1.pdf`, `tmp/papers/10193037.pdf`, `tmp/posts/weighted-voting-for-replicated-data.html`, `tmp/posts/what-is-a-file-system.html`, `tmp/posts/a-principle-for-resilient-sharing-of-distributed-resources.html`, and `tmp/opinions/obrien-microsoft-ipr-decl-3.pdf`.

### Commit Protocol Used by Every Task

Stage only the files named by that task, inspect the staged diff, then commit from WSL. Define this PowerShell helper once in the execution session:

```powershell
function Invoke-SignedCommit {
  param([Parameter(Mandatory)][string]$Message)
  git diff --check
  git diff --stat
  git diff --cached --check
  wsl.exe git -C /mnt/c/Users/TonyMason/source/repos/eidolon diff --cached --check
  if ($LASTEXITCODE -ne 0) { throw 'Staged diff check failed' }
  wsl.exe git -C /mnt/c/Users/TonyMason/source/repos/eidolon commit -S -m $Message
  if ($LASTEXITCODE -ne 0) { throw 'Signed commit failed' }
  wsl.exe git -C /mnt/c/Users/TonyMason/source/repos/eidolon verify-commit HEAD
  if ($LASTEXITCODE -ne 0) { throw 'Commit signature verification failed' }
}
```

Expected verification output must name key fingerprint `1D7C4A68252F6EC1ACD2FC8E934778A0EB5EABB1`. Each task supplies the literal argument for `Invoke-SignedCommit`. Untracked author materials may remain; “clean” in this plan means no unintended tracked change.

---

### Task 1: Build the Narrative, Claim, and Voice Map

**Files:**
- Create: `docs/superpowers/notes/2026-07-30-nines-narrative-map.md`
- Read: `docs/paper/nines/main.tex`
- Read: the six authorial references listed in the File Map
- Read: `docs/superpowers/specs/2026-07-30-nines-narrative-voice-revision-design.md`

**Interfaces:**
- Consumes: the approved ten-discovery narrative, current paragraph order, fixed claims, registered artifact paths, and representative samples of the author's explanatory, academic, and expert registers.
- Produces: sequential paragraph IDs beginning at `P001` and ending at the actual current paragraph count; a destination and action for every current paragraph; claim IDs `C01` through `C17`; section-level implication checks; and a page ledger used by Tasks 2–7.

- [ ] **Step 1: Capture the baseline without changing the manuscript**

Run:

```powershell
$text = Get-Content -Raw docs/paper/nines/main.tex
"words=$((($text -split '\s+') | Where-Object { $_ }).Count)"
"em_dash_source=$(([regex]::Matches($text, '---')).Count)"
"rather_than=$(([regex]::Matches($text, '(?i)rather than')).Count)"
pdfinfo docs/paper/nines/main.pdf | Select-String 'Pages|Title|Author'
1..15 | ForEach-Object {
  $page = $_
  $content = pdftotext -f $page -l $page docs/paper/nines/main.pdf - 2>$null
  $first = ($content | Where-Object { $_.Trim() } | Select-Object -First 3) -join ' | '
  "p$page: $first"
}
```

Record the observed baseline in the map: approximately 11,024 whitespace-delimited source tokens, 49 TeX em-dashes, 22 occurrences of “rather than,” 15 PDF pages total, and conclusion prose continuing onto page 13 before the references. Label these counts “discovery aids, not targets.”

- [ ] **Step 2: Read the representative voice sample**

Use `pdftotext <file> -` for the two papers and one declaration. Read the three HTML post bodies directly. Record only tendencies that recur across at least two registers:

```markdown
## Voice observations

- Starts from an observation, question, or concrete example before abstraction.
- Explains why one observation led to the next.
- Uses “we” for choices, measurements, and interpretations.
- Places limitations beside the claim they limit.
- Prefers a concrete mechanism over a polished slogan.
- Allows uneven sentence length and visible intellectual texture.

These are revision heuristics, not a stylometric target. No source wording is copied.
```

Also record register differences: proofs may remain compressed, experimental prose should keep conditions adjacent to measurements, and declarations are useful for qualification placement but not for legal cadence.

- [ ] **Step 3: Create the paragraph map**

Create the note with this exact structure:

```markdown
# NINeS Narrative Map

**Design:** `docs/superpowers/specs/2026-07-30-nines-narrative-voice-revision-design.md`
**Manuscript baseline:** `docs/paper/nines/main.tex` before narrative revision

## Voice observations

- Starts from an observation, question, or concrete example before abstraction.
- Explains why one observation led to the next.
- Uses “we” for choices, measurements, and interpretations.
- Places limitations beside the claim they limit.
- Prefers a concrete mechanism over a polished slogan.
- Allows uneven sentence length and visible intellectual texture.
- Keeps proofs compressed, experimental conditions adjacent to measurements, and legal cadence out of academic explanation.

## Page budget
| Checkpoint | Last body page | Total PDF pages | Change | Decision |
|---|---:|---:|---:|---|
| Baseline | 13 | 15 | — | Recover at least one body page |

## Paragraph map
| ID | Current location/opening words | Role | Claim IDs | Action | Target section | Reason |
|---|---|---|---|---|---|---|

## Claim ledger
| Claim ID | Claim | Kind | Exact scope/evidence | Citation or artifact | Must remain adjacent |
|---|---|---|---|---|---|

## Section implication audit
| Revised section | Likely reader inference | Causal? | General? | Novel? | Empirical? | Evidence permits it? | Correction |
|---|---|---|---|---|---|---|---|

## Contextual style review
| Location | Pattern | Rhetorical work | Keep/rewrite/remove | Reason |
|---|---|---|---|---|
```

Assign every prose paragraph in `main.tex` one ID and one primary role from: question, observation, definition, proof, measurement, interpretation, qualification, transition, or repetition. For each paragraph choose exactly one action: fixed, move, merge, rewrite, or remove. Equations, propositions, tables, and figure environments receive IDs and default to fixed unless a later evidence audit requires a documented correction.

- [ ] **Step 4: Populate the claim ledger before moving prose**

At minimum create ledger entries for:

```text
C01 two operational predicates: acquire authority and commit
C02 phase symmetry makes the predicates equal
C03 majority threshold makes the equal predicate count-readable from a stated vantage
C04 exact containment correspondence and four-way classification
C05 odd/even price of threshold symmetry, labeled post-hoc
C06 wall cross-phase safety and witness-policy boundary
C07 exact k=3 positive wall results under both readings
C08 residual Moon/Mars (0,1) exposure at every k
C09 registered (1,0) observation and accepted-value injection
C10 registered (0,1) retry-budget exhaustion
C11 LEO measured faster than Earth; obligation and physical cost differ
C12 full-versus-sparse network reachability result
C13 crash-relaxation/weakest-link migration result
C14 legibility definition and O(tiers) readout
C15 readout exclusions: detection, authority, policy, operator action
C16 edge applicability is structural, not evaluated
C17 performance-preserving closure remains open
```

For empirical claims, copy the condition and artifact path from the traceability appendix; do not paraphrase the number in the ledger.

- [ ] **Step 5: Complete the initial implication audit**

For each target section in the approved architecture, answer these four questions in the note: what causal inference the new ordering invites; what appears general; what appears novel; and what appears empirically established. Resolve every “Evidence permits it?” cell to `yes` or give an exact correction in the final column.

- [ ] **Step 6: Commit the map**

Run:

```powershell
git add docs/superpowers/notes/2026-07-30-nines-narrative-map.md
```

Use the Commit Protocol:

```powershell
Invoke-SignedCommit 'docs: map NINeS narrative and claims'
```

Expected: only the new map is committed; the manuscript remains byte-identical.

---

### Task 2: Rewrite and Human-Review the Introduction Pilot

**Files:**
- Modify: `tests/test_nines_claim_language.py`
- Modify: `docs/paper/nines/main.tex` (abstract, introduction, and only the minimum first-use background needed by the pilot)
- Modify: `docs/superpowers/notes/2026-07-30-nines-narrative-map.md`
- Rebuild: `docs/paper/nines/main.pdf`

**Interfaces:**
- Consumes: Task 1 claim IDs `C01`–`C05`, `C14`–`C17`, paragraph actions, voice observations, and baseline page count.
- Produces: an approved page-one voice/narrative model; a concrete legibility definition; stable terminology for acquisition, commit, symmetry, coincidence, and majority; regression tests used by later tasks.

- [ ] **Step 1: Establish the existing claim-test baseline**

Run:

```powershell
$env:UV_PROJECT_ENVIRONMENT='.venv-windows'
uv run pytest tests/test_nines_claim_language.py -q
```

Expected: all existing tests pass. If one fails, record the mismatch in the narrative map before editing.

- [ ] **Step 2: Replace rhetorical fingerprints with semantic tests**

Add this helper and introduction contract test to `tests/test_nines_claim_language.py`:

```python
def section(text: str, start: str, end: str) -> str:
    start_index = text.index(start)
    content_start = start_index + len(start)
    end_index = text.index(end, content_start)
    return text[start_index:end_index]


def test_introduction_states_the_two_questions_and_defines_legibility():
    text = PAPER.read_text(encoding="utf-8")
    introduction = section(text, r"\section{Introduction}", r"\section{")
    assert "acquire proposal authority" in introduction
    assert "complete a commit" in introduction
    assert "legible with respect to a supplied connectivity summary" in introduction
    assert "without enumerating candidate quorum subsets" in introduction
    assert "attempting protocol execution" in introduction
    assert r"O(\text{tiers})" in introduction
    for boundary in (
        "does not detect connectivity",
        "establish current authority",
        "select recovery policy",
        "guarantee that an operator consults",
    ):
        assert boundary in introduction


def test_introduction_distinguishes_symmetry_majority_and_vantage():
    text = PAPER.read_text(encoding="utf-8")
    introduction = section(text, r"\section{Introduction}", r"\section{")
    assert "phase symmetry" in introduction.lower()
    assert "majority" in introduction.lower()
    assert "threshold" in introduction.lower()
    assert "vantage" in introduction.lower()
    assert "coincid" in introduction.lower()
    if "spend" in introduction.lower():
        assert introduction.lower().index("coincid") < introduction.lower().index("spend")
```

Remove exact assertions for the current slogans “Demand for Phase~1 is correlated with its price,” “state-aware mitigation requires distinguishing the state,” “magnifying glass,” and “fault-tolerance-independent reason for the odd-cluster convention.” Retain their semantic boundaries through later tests. For the odd/even result, assert the existing technical fragments `cost-minimal split`, `odd $n$`, `even $n$`, and `one additional participant`; for edge scope, retain `N_e`, `C_t`, and `not an evaluated result`. Keep all existing bans on overbroad claims.

- [ ] **Step 3: Run the new tests and observe the intended failure**

Run:

```powershell
uv run pytest tests/test_nines_claim_language.py -q
```

Expected: failure because the introduction does not yet contain the approved legibility definition and exclusions. If it passes, tighten the test to the approved semantics before revising prose.

- [ ] **Step 4: Rewrite the abstract as an answer, not a teaser**

Use this paragraph contract:

```text
1. Symptom: node health does not answer the two service-capability questions.
2. Explanation: phase symmetry made the predicates coincide; majority made that coincidence count-readable.
3. Change: Flexible Paxos can separate them while preserving cross-phase safety.
4. Formal result: exact containment correspondence.
5. Registered behavioral result with retry-policy and single-decree bounds.
6. Constructive result: named k=3 wall closures, followed by residual Moon/Mars exposure.
7. Operational result: O(tiers) readout of supplied connectivity, with its limits.
8. Scope: Mars exposes the issue; edge recurrence is structural rather than evaluated.
```

Keep all numerical claims already present, but do not use an intrinsic positive or negative label for either mixed state. The abstract may disclose the boundary; suspense comes from the body's route to it.

- [ ] **Step 5: Rewrite the introduction as the green-dashboard investigation**

Build seven paragraphs with these jobs:

```text
I1: A concrete healthy-node/absent-capability observation and the two questions.
I2: Why one reachable-replica count seemed to answer both in familiar systems.
I3: The reveal that symmetry, not majority itself, made the predicates equal.
I4: Flexible Paxos separates the predicates; state the exact characterization without proving it.
I5: Introduce the wall as a test and a design tool; give the positive k=3 result before its boundary.
I6: Define legibility and the O(tiers) readout; immediately state its four exclusions.
I7: Contributions and scope, ending with the question that requires “Why One Count Once Worked.”
```

Use this definition closely enough to satisfy the approved contract, adjusting only cadence:

```text
We call a quorum construction legible with respect to a supplied connectivity summary when its phase capabilities and failed obligations can be read from a compact structural representation, without enumerating candidate quorum subsets or attempting protocol execution. For the wall, that read takes O(tiers) time.
```

Follow it immediately with: the interface assumes configuration and connectivity are known; it does not detect connectivity, establish current authority, select recovery policy, or guarantee that an operator consults the result.

- [ ] **Step 6: Apply the voice pass only to the pilot**

Compare the pilot against the Task 1 tendencies. In the contextual style table, log every em-dash and every “not X but Y” or “rather than” construction in the pilot. Keep one only when the contrast carries a genuine reversal not already expressed by the section order. Remove repeated contribution announcements and any sentence that explains the meaning of the preceding sentence a second time.

- [ ] **Step 7: Build and inspect the pilot**

Run:

```powershell
Push-Location docs/paper/nines
latexmk -pdf -interaction=nonstopmode main.tex
Pop-Location
uv run pytest tests/test_nines_claim_language.py -q
pdftotext -f 1 -l 2 -layout docs/paper/nines/main.pdf -
```

Expected: LaTeX exits 0; no new undefined citations or references; claim-language tests pass; the first two rendered pages contain no collision, overflow, or stranded heading. Record the body-page count in the narrative map even if later sections are unchanged.

- [ ] **Step 8: Stop for the human pilot review**

Present the revised abstract, introduction, first two rendered pages, and a compact before/after explanation. Ask the author to judge voice, narrative pull, accessibility, and excess certainty. Do not apply the pilot's cadence to the remaining manuscript until the author approves it.

- [ ] **Step 9: Incorporate the author's pilot corrections**

Record corrections as explicit voice or narrative decisions in the map. Re-run the commands from Step 7. Expected: all checks pass and the author confirms that the pilot is a suitable model for the remaining revision.

- [ ] **Step 10: Commit the approved pilot**

Run:

```powershell
git add tests/test_nines_claim_language.py docs/paper/nines/main.tex docs/paper/nines/main.pdf docs/superpowers/notes/2026-07-30-nines-narrative-map.md
```

Use the Commit Protocol:

```powershell
Invoke-SignedCommit 'docs: establish NINeS narrative voice'
```

---

### Task 3: Rebuild the Formal Core Around the Coincidence

**Files:**
- Modify: `tests/test_nines_claim_language.py`
- Modify: `docs/paper/nines/main.tex` (current Background, Capability Gaps, and Construction material)
- Modify: `docs/superpowers/notes/2026-07-30-nines-narrative-map.md`
- Rebuild: `docs/paper/nines/main.pdf`

**Interfaces:**
- Consumes: approved pilot terminology and claim IDs `C02`–`C08`.
- Produces: ordered sections `Why One Count Once Worked`, `Capability Gaps`, `Putting the Gaps on a Wall`, and `Where the Wall Works, and Where It Stops`; unchanged proof meaning and labels; a bridge from residual `(0,1)` exposure to the registered experiment.

- [ ] **Step 1: Add a section-order regression test**

Add:

```python
def test_formal_investigation_sections_appear_in_order():
    text = PAPER.read_text(encoding="utf-8")
    headings = (
        r"\section{Why One Count Once Worked}",
        r"\section{Capability Gaps}",
        r"\section{Putting the Gaps on a Wall}",
        r"\section{Where the Wall Works, and Where It Stops}",
    )
    positions = [text.index(heading) for heading in headings]
    assert positions == sorted(positions)
```

Run `uv run pytest tests/test_nines_claim_language.py::test_formal_investigation_sections_appear_in_order -q`.

Expected: FAIL because the target headings and order do not yet exist.

- [ ] **Step 2: Build “Why One Count Once Worked”**

Move the minimum Classic Paxos, Flexible Paxos, and uniform-threshold material into this section. The causal order is:

```text
same family in both phases -> equal formability predicates
majority threshold family -> one reachable count decides the equal predicate
cross-phase intersection is the safety requirement -> Flexible Paxos permits different families
different families -> the two operational questions can receive different answers
```

Keep foundational citations at first use. State the odd/even observation here: subject to `q_1+q_2 >= n+1`, a cost-minimal split can be symmetric for odd `n`; for even `n`, symmetry adds one participant across the two phase thresholds. Preserve its post-hoc status in traceability and avoid presenting the odd-cluster convention as newly caused by this result.

- [ ] **Step 3: Make “Capability Gaps” answer the question raised by symmetry loss**

Keep definitions of `N_e`, `C_t`, `Form`, `R_1`, and `R_2`; Proposition `prop:gap`; Corollary `cor:correspondence`; the proof; and the generic auditor. Put the three-line proof immediately after the statement. Then explain why its simplicity is informative: the question was vacuous while the families were identical and became useful once the phase predicates could differ.

Do not claim that every asymmetric family admits exactly one gap. Preserve the four classes `equal`, `R1` strictly implies `R2`, `R2` strictly implies `R1`, and incomparable.

- [ ] **Step 4: Build “Putting the Gaps on a Wall” without claim drift**

Move the system model, 5/1/1/3 topology, per-tier Phase 1 families, Phase 2 family, cross-intersection proof, and verification scope together. Cite Peleg and Wool at first use. Explain the construction in physical order and state plainly:

```text
The Earth anchor supplies the Paxos intersection.
The non-anchor witnesses encode participation policy.
Removing a policy witness changes the construction's policy, not the Paxos safety argument.
```

Keep the existing proposition labels and all artifact references stable so that later cross-references and traceability remain valid.

- [ ] **Step 5: Create the opening of “Where the Wall Works, and Where It Stops”**

Move the structural capability enumeration before the behavioral experiment. Start with the `k=3` positive result and only then state the boundary. End the section with the residual Moon/Mars `(0,1)` state and this bounded forward link: the following registered single-decree experiment tests what happened under its retry policy and budget; the structural state itself has no intrinsic valence.

- [ ] **Step 6: Run formal-core checks and implication audit**

Run:

```powershell
uv run pytest tests/test_nines_claim_language.py -q
Push-Location docs/paper/nines
latexmk -pdf -interaction=nonstopmode main.tex
Pop-Location
rg -n "undefined references|undefined citations|multiply defined" docs/paper/nines/main.log
pdfinfo docs/paper/nines/main.pdf | Select-String Pages
```

Expected: tests and build pass; the `rg` command returns no diagnostic matches. Update actions for every moved paragraph and complete the implication-audit rows for these four sections. Record the new last body page.

- [ ] **Step 7: Commit the formal core**

Stage the test, manuscript, PDF, and map. Use the Commit Protocol:

```powershell
Invoke-SignedCommit 'docs: center NINeS story on phase coincidence'
```

---

### Task 4: State Exactly Where the Wall Works and Audit the Common-Condition Table

**Files:**
- Modify: `tests/test_nines_claim_language.py`
- Modify: `docs/paper/nines/main.tex` (`Where the Wall Works, and Where It Stops`; central per-tier table)
- Modify: `docs/superpowers/notes/2026-07-30-nines-narrative-map.md`
- Read: `results/capability/dual_gradient_map.csv`
- Read: `results/tier_liveness/tier_sweep_full_ci.csv`
- Rebuild: `docs/paper/nines/main.pdf`

**Interfaces:**
- Consumes: `C07`, `C08`, both connectivity readings, and registered per-tier aggregate rows.
- Produces: named positive wall guarantees, exact residual exposure, and one central table whose four tiers all use 186-second Mars latency, 1800-second blackout, hard-blackout, full-coverage, 50-seed evidence.

- [ ] **Step 1: Add wall-boundary and table-consistency tests**

Add:

```python
def test_wall_positive_results_precede_the_boundary():
    text = PAPER.read_text(encoding="utf-8")
    wall = section(
        text,
        r"\section{Where the Wall Works, and Where It Stops}",
        r"\section{",
    )
    required = (
        r"At $k=3$, the $(1,0)$ gap is absent at every tier",
        r"both gaps are absent for Earth under both connectivity readings",
        r"both gaps are absent for LEO under the self-reachable reading",
        r"$(0,1)$ remains reachable for Moon and Mars",
        r"For $k \le 3$, $(1,0)$ is absent at every tier",
        r"For $k \ge 3$, $(0,1)$ is absent for Earth",
    )
    assert all(claim in wall for claim in required)
    assert wall.index(required[0]) < wall.index(required[3])


def test_central_per_tier_table_uses_one_observed_condition():
    text = PAPER.read_text(encoding="utf-8")
    table = section(text, r"\caption{Per-tier global consensus", r"\end{table}")
    assert "1800~s blackout" in table
    assert "900~s blackout" not in table
    assert r"Mars & top (3) & 0.0\%" in table
    assert r"---$^{\dagger}$" not in table


def test_superseded_wall_and_valence_claims_do_not_return():
    text = PAPER.read_text(encoding="utf-8")
    for banned in (
        "compensates every other tier with legibility",
        "the harmful gap",
        "the harmless gap",
        "intrinsically harmful",
        "universally benign",
    ):
        assert banned not in text
```

Run the three tests. Expected: FAIL on the target headings/phrasing and the current mixed 900/1800 presentation.

- [ ] **Step 2: Verify the structural wall facts directly**

Inspect the registered gradient under both readings:

```powershell
Import-Csv results/capability/dual_gradient_map.csv |
  Sort-Object {[int]$_.k}, tier_name, state |
  Format-Table k,tier_name,state,reachable_unconstrained,reachable_self_reachable,min_earth_in_q1,phase2_threshold -AutoSize
```

Match each positive or residual claim to an exact row before writing it. The two reachability columns are the unconstrained and self-reachable readings; do not infer either reading from tier position alone.

- [ ] **Step 3: Verify the common 1800-second experimental condition**

Run:

```powershell
Import-Csv results/tier_liveness/tier_sweep_full_ci.csv |
  Where-Object {
    $_.scenario -eq 'blackout_only' -and
    $_.topology -eq 'full_coverage' -and
    [double]$_.mars_base_latency_s -eq 186.0 -and
    [double]$_.blackout_duration_s -eq 1800.0
  } |
  Sort-Object {[int]$_.tier_index} |
  Format-Table tier_name,during_rate_mean,post_rate_mean,avg_latency_s_mean,recovery_s_mean,n_seeds -AutoSize
```

Expected registered rows:

```text
Mars  during 0.000000  post 1.000000  latency 753.184341  recovery 1351.408681  n=50
Moon  during 1.000000  post 1.000000  latency   5.131478  recovery    3.022124  n=50
LEO   during 1.000000  post 1.000000  latency   0.131199  recovery    8.146090  n=50
Earth during 1.000000  post 1.000000  latency   0.182859  recovery    9.390115  n=50
```

If the command does not produce exactly these four rows and conditions, stop and document the discrepancy instead of editing the table.

- [ ] **Step 4: Rewrite the wall result from positive result to boundary**

Use this order:

```text
1. k=3 closes (1,0) everywhere.
2. k=3 closes both gaps for Earth under both readings.
3. k=3 closes both for LEO only when the initiating acceptor is assumed self-reachable.
4. Reusable threshold statements for k<=3 and k>=3.
5. Unconstrained-reading sensitivity.
6. Moon and Mars retain (0,1), caused by downward participation obligations.
7. Design lesson: choose where coincidence matters while retaining the Earth hot path.
8. Boundary: no tested threshold closes both gaps at every tier.
```

Do not describe residual exposure as the price paid for legibility. The witnesses implement a policy that creates the exposure; legibility makes the resulting obligation readable.

- [ ] **Step 5: Replace the central table with the common 1800-second condition**

Change the caption and preceding method sentence to 186-second Mars latency, 1800-second blackout, hard blackout, full coverage, 50 seeds. Use these rounded rows:

```latex
Earth & bottom (0) & 100.0\% & 0.183~s & 9.4~s \\
LEO & tier 1 & 100.0\% & 0.131~s & 8.1~s \\
Moon & tier 2 & 100.0\% & 5.131~s & 3.0~s \\
Mars & top (3) & 0.0\% & 753.2~s & 1351.4~s \\
```

Remove the undefined-cell dagger and the explanation that mixed a 900-second table with an 1800-second Mars observation. Retain the cadence-artifact qualification and update its numbers to the common condition. Preserve the LEO-faster-than-Earth interpretation: wall position determines obligations; network paths determine measured cost.

- [ ] **Step 6: Verify, render, and update the map**

Run the claim-language tests and LaTeX build. Inspect the table page at high enough resolution to catch column collisions. Update `C07`, `C08`, `C11`, and the page ledger with the exact common condition and rendered location.

- [ ] **Step 7: Commit the wall and table audit**

Stage the test, manuscript, PDF, and map. Use the Commit Protocol:

```powershell
Invoke-SignedCommit 'docs: clarify wall guarantees and common-condition results'
```

---

### Task 5: Put the Registered Behavioral Reversal Immediately After the Structural Gap

**Files:**
- Modify: `docs/paper/nines/main.tex` (current Experimental Design and Valence material)
- Modify: `docs/superpowers/notes/2026-07-30-nines-narrative-map.md`
- Read: `results/flip/flip_map.csv`
- Read: `results/flip/flip_sweep.csv`
- Rebuild: `docs/paper/nines/main.pdf`

**Interfaces:**
- Consumes: residual `(0,1)` forward link from Task 3, claim IDs `C09` and `C10`, and the registered experiment's retry-policy/budget conditions.
- Produces: `What Happens Inside the Gaps`, with method adjacent to result and no intrinsic valence claim; a transition from structural possibility to measured behavior.

- [ ] **Step 1: Reconcile every behavioral number before moving prose**

Run:

```powershell
Import-Csv results/flip/flip_map.csv | Format-Table -AutoSize
Import-Csv results/flip/flip_sweep.csv |
  Group-Object arm,incumbent_max_rounds |
  Sort-Object Name |
  ForEach-Object { "{0}: n={1}" -f $_.Name,$_.Count }
```

Cross-check the exact arm names, 50-seed count, retry budget eight result, accepted-value injection, and recorded-metric comparison against the existing traceability row. Record the commands and result locations in `C09` and `C10`.

- [ ] **Step 2: Move the minimum method next to the result**

Start `What Happens Inside the Gaps` with the registered question, five arms, retry budgets, seeds, single-decree scope, and the definition of “prevented decision” used by the harness. Keep the broader topology and shared simulator parameters in the later evaluation method and appendix; repeat only the conditions needed to interpret this result.

- [ ] **Step 3: Tell the reversal in evidence order**

Use this sequence:

```text
1. State the registered predictions and falsification boundary.
2. Report (1,0): matched the healthy contender on every recorded metric in this experiment.
3. Report the accepted value that future acquisition must preserve.
4. Report (0,1): healthy proposer exhausted retry budget eight in all 50 seeds under the modeled policy.
5. State what neither result establishes outside this experiment.
6. Explain why Multi-Paxos can delay exposure while single-decree Paxos pays Phase 1 per decree.
7. End with the operational question: how can a designer or operator read which state the topology permits?
```

Do not use “benign,” “harmful,” “livelock,” or “blocks liveness completely” as an unqualified label. Keep qualifications in the same paragraph as their numbers.

- [ ] **Step 4: Perform the implication and voice audits**

In the map, explicitly answer whether the new adjacency makes the experiment appear to validate the containment theorem or the wall construction. The correction must state that the theorem is proved independently and the experiment measures behavior inside constructed mixed states. Review strong contrasts here closely; this is the section where one earned reversal is appropriate.

- [ ] **Step 5: Verify and commit**

Run claim-language tests, full LaTeX build, log scan, and page-count check. Inspect the result section in the PDF. Stage manuscript, PDF, and map. Use the Commit Protocol:

```powershell
Invoke-SignedCommit 'docs: connect capability gaps to registered behavior'
```

---

### Task 6: Turn the Remaining Evaluation into “Reading the Wall”

**Files:**
- Modify: `docs/paper/nines/main.tex` (wall readout; topology; baseline; liveness; sparse connectivity; crash relaxation)
- Modify: `tests/test_nines_claim_language.py`
- Modify: `docs/superpowers/notes/2026-07-30-nines-narrative-map.md`
- Rebuild: `docs/paper/nines/main.pdf`

**Interfaces:**
- Consumes: `C11`–`C16`, approved legibility definition, existing tables/figures, and traceability paths.
- Produces: a coherent `Reading the Wall` section that demonstrates what can be inferred from supplied connectivity, supports it with existing evaluation, and keeps detection/authority/policy outside the interface.

- [ ] **Step 1: Add a readout-boundary regression test**

Replace the current one-line demonstration check with:

```python
def test_readout_is_a_bounded_demonstration():
    text = PAPER.read_text(encoding="utf-8")
    reading = section(text, r"\section{Reading the Wall}", r"\section{")
    assert "demonstration artifact, not empirical evidence" in reading
    assert "configuration and connectivity" in reading
    assert "runtime authority" in reading
    assert "service policy" in reading
    assert r"experiments/\allowbreak capability\_readout.py" in text
    assert "not an evaluated result" in text
```

Run the test. Expected: FAIL until the section is assembled and its boundaries are adjacent to the readout.

- [ ] **Step 2: Assemble the section in interpretive order**

Use these subsections:

```text
Readout interface
Evaluation method
Geometry and competitive majority baseline
Per-tier liveness under full coverage
Wall obligations versus sparse reachability
Crash tolerance and coordinated relaxation
```

Open with the readout's inputs and outputs, including typed failed obligations and unknown runtime authority. Then give each measurement its local conditions before its table or figure.

- [ ] **Step 3: Preserve the five interpretive results without repeating the thesis**

Ensure the section supports, once each:

```text
The majority baseline matches blackout survival but pays cross-tier Phase 1 latency.
LEO measures faster than Earth even though it sits higher in the wall.
Full wall structure is insufficient when sparse network reachability removes required paths.
Recovery-lag point values are cadence artifacts; their bound is the protocol-relevant statement.
Coordinated relaxation moves the weakest link between global and Earth-local quorum requirements.
```

Do not append a capability-gap recap after every table. Use the last observation of each subsection to motivate the next.

- [ ] **Step 4: State the edge demonstration boundary where it appears**

Describe the planetary and edge JSON inputs as two inputs to the same deterministic readout. State in the same paragraph that the edge case is a structural mapping and not an evaluated result. Do not add terrestrial latency, availability, or prevalence claims.

- [ ] **Step 5: Audit experimental adjacency and table conditions**

For every table/figure in this section, add its condition set to the claim ledger: topology, initiator, Mars latency, blackout duration, timeout, seed count, and whether the quantity is structural or stochastic. Compare these fields to the caption and preceding prose. Correct prose/caption drift only when the artifact row establishes the correction.

- [ ] **Step 6: Verify the body-page budget**

Build, run all claim-language tests, and inspect each evaluation page. If the conclusion would still spill onto page 13, first remove repeated interpretation, duplicated method, and recapitulative transitions. Do not shrink fonts, margins, figures, or table text merely to satisfy the limit.

- [ ] **Step 7: Commit the reading section**

Stage test, manuscript, PDF, and map. Use the Commit Protocol:

```powershell
Invoke-SignedCommit 'docs: make the wall readout operationally legible'
```

---

### Task 7: Close with the Open Problem, Limits, and Two-Question Diagnostic

**Files:**
- Modify: `docs/paper/nines/main.tex` (`What Remains Unsolved`, Related Work, Threats/Limitations, Conclusion, traceability appendix)
- Modify: `docs/ai-provenance.md`
- Modify: `docs/superpowers/notes/2026-07-30-nines-narrative-map.md`
- Rebuild: `docs/paper/nines/main.pdf`

**Interfaces:**
- Consumes: all claim IDs and implication audits; final section order; existing bibliography and traceability artifacts.
- Produces: an epistemically bounded ending, rebuilt claim-to-artifact table, anonymous account of the editorial process, and body ending by page 12.

- [ ] **Step 1: Write “What Remains Unsolved” as a research opening**

State the problem precisely: can a structured quorum design extend acquisition/commit coincidence farther up the topology without putting inter-tier latency back on the Phase 2 hot path? Explain that the present wall closes named gaps for named tiers but not all gaps everywhere. Mention scoped authority, gap-aware proposer behavior, and multi-anchor families only as candidates whose safety, performance, and operational behavior require separate work.

- [ ] **Step 2: Restore full related work after first-use attribution**

Keep the existing relationship to Classic Paxos, Flexible Paxos, crumbling walls, refined quorum systems, heterogeneous quorum systems, WAN consensus, DTN, and partition-tolerant work. Remove repeated definitions already supplied at first use. Ensure the narrative order is not described as the historical chronology of discovery.

- [ ] **Step 3: Consolidate limitations beside the claims they bound**

Keep a compact limitations section for cross-cutting issues: simulator abstraction, orbital/link simplification, hard-blackout model, single-decree versus Multi-Paxos behavior, known-connectivity assumption, no deployment prevalence, no terrestrial evaluation, and policy/authority exclusions. If a limitation already appears next to its claim, refer to it briefly instead of restating the full argument.

- [ ] **Step 4: Rewrite the conclusion around the portable diagnostic**

The conclusion must do four jobs in this order:

```text
1. Explain why one count once worked: phase symmetry plus a threshold family.
2. State what changes when phase predicates separate and what containment tells us.
3. Give the wall's usable result and exact boundary without compensation language.
4. Leave the reader with: Can this system acquire authority, and can it commit?
```

End with the performance-preserving closure question. Do not introduce a new claimed solution in the conclusion.

- [ ] **Step 5: Rebuild traceability from the final claim ledger**

For each `C01`–`C17`, confirm that the final prose is either formal/structural, empirically supported, a demonstration, or future work. Update table wording and conditions, especially:

```text
common 1800-second full-coverage per-tier result
k=3 positive wall closures under named readings
residual Moon/Mars exposure
single-decree retry-budget bound
post-hoc odd/even threshold observation
readout demonstration and edge non-evaluation
```

Do not change an artifact path unless `Test-Path` confirms the replacement exists and `artifact-manifest.txt` covers it.

- [ ] **Step 6: Record the editorial provenance**

Add a bullet to the methodology/process portion of `docs/ai-provenance.md` with this substance:

```text
The narrative and voice revision used an approved editorial contract, a paragraph-level claim/implication map, and a human-reviewed introduction pilot. One model family reviewed a draft substantially written by that family; a different model family evaluated the review and led the rewrite. Agreement was treated as a confidence signal rather than truth, disagreements were adjudicated against proofs and artifacts, and the human author remained the authority on voice and claim-affecting prose.
```

Keep the wording anonymous and do not name the author or include paths under `tmp/`.

- [ ] **Step 7: Complete the contextual style audit**

Run discovery counts:

```powershell
$text = Get-Content -Raw docs/paper/nines/main.tex
"em_dash_source=$(([regex]::Matches($text, '---')).Count)"
"rather_than=$(([regex]::Matches($text, '(?i)rather than')).Count)"
rg -n -i "it is not|it's not|rather than|---|deeper|the key is|the point is|in other words" docs/paper/nines/main.tex
```

Review every match in context and record keep/rewrite/remove plus reason in the map. Also scan manually for slogans, empty symmetrical lists, personified systems, and repeated interpretive announcements. There is no target count; a remaining match is acceptable when it performs necessary work.

- [ ] **Step 8: Perform the final implication audit and page check**

Resolve every row in the map's implication audit. Build the PDF and confirm visually that conclusion prose ends by page 12. Page 13 may begin with references; it must not contain a continuation of the conclusion before the `References` heading.

- [ ] **Step 9: Commit the completed narrative**

Stage manuscript, PDF, provenance, and map. Use the Commit Protocol:

```powershell
Invoke-SignedCommit 'docs: complete NINeS narrative revision'
```

---

### Task 8: Run the Final Scientific, Anonymous, and Human Acceptance Gates

**Files:**
- Verify: all tracked files changed by Tasks 1–7
- Verify: `artifact-manifest.txt`
- Verify: `scripts/build_anonymous_artifact.py`
- Verify: `tests/test_anonymous_artifact.py`
- Verify: `docs/paper/nines/main.pdf`
- Modify only for verified defects: `docs/paper/nines/main.tex`, `tests/test_nines_claim_language.py`, `docs/superpowers/notes/2026-07-30-nines-narrative-map.md`, `docs/ai-provenance.md`

**Interfaces:**
- Consumes: candidate final manuscript, complete map/ledger, all registered artifacts, and anonymous-artifact allowlist.
- Produces: passing tests/build/package checks, visually inspected anonymous PDF, and explicit human cold-read approval.

- [ ] **Step 1: Run focused and full Python verification**

Run:

```powershell
$env:UV_PROJECT_ENVIRONMENT='.venv-windows'
uv run pytest tests/test_nines_claim_language.py -q
uv run pytest tests/test_anonymous_artifact.py -q
uv run pytest -q
```

Expected: all commands exit 0. Record the full-suite pass count and elapsed time in the map; do not reuse an earlier run.

- [ ] **Step 2: Run a clean full LaTeX citation cycle**

Run:

```powershell
Push-Location docs/paper/nines
latexmk -C
pdflatex -interaction=nonstopmode main.tex
bibtex main
pdflatex -interaction=nonstopmode main.tex
pdflatex -interaction=nonstopmode main.tex
Pop-Location
rg -n "undefined references|undefined citations|Citation.*undefined|Reference.*undefined|multiply defined" docs/paper/nines/main.log
```

Expected: all four build commands exit 0; the log scan returns no matches. Inspect overfull boxes individually; accept only those that do not clip or materially reduce readability.

- [ ] **Step 3: Verify anonymity, metadata, and body length**

Run:

```powershell
pdfinfo docs/paper/nines/main.pdf | Select-String 'Title|Author|Pages'
pdftotext docs/paper/nines/main.pdf - | rg -n -i "Tony|Mason|wamason|fsgeek|research@"
pdftotext -f 13 -l 13 -layout docs/paper/nines/main.pdf -
```

Expected: no personal identifier; the title page displays only `Paper #98` or the venue-required anonymous marker; conclusion prose has ended by page 12.

- [ ] **Step 4: Render and inspect every main-text page**

Create a temporary rendering directory outside the repository and render pages 1–12:

```powershell
$renderRoot = Join-Path $env:TEMP 'eidolon-nines-final-render'
New-Item -ItemType Directory -Force -Path $renderRoot | Out-Null
pdftoppm -png -f 1 -l 12 -r 144 docs/paper/nines/main.pdf (Join-Path $renderRoot 'page')
Get-ChildItem $renderRoot -Filter 'page-*.png' | Sort-Object Name
```

Inspect all images, with special attention to page 1, theorem breaks, the gradient figure, the common-condition per-tier table, the crash-relaxation table, section transitions, and the conclusion. Record any defect by page and fix it before continuing.

- [ ] **Step 5: Build the anonymous ZIP from a clean temporary worktree**

The primary worktree contains user-owned untracked review material, while the package builder correctly rejects any dirty tree. After all editorial changes are committed, create a separate clean worktree at `HEAD`:

```powershell
$auditRoot = Join-Path $env:TEMP 'eidolon-nines-anonymous-audit'
if (Test-Path $auditRoot) { throw "Audit path already exists: $auditRoot" }
git worktree add --detach $auditRoot HEAD
$artifactZip = Join-Path $env:TEMP 'eidolon-nines-anonymous.zip'
Push-Location $auditRoot
$env:UV_PROJECT_ENVIRONMENT='C:\Users\TonyMason\source\repos\eidolon\.venv-windows'
uv run python scripts/build_anonymous_artifact.py --output $artifactZip
Pop-Location
```

Expected: builder exits 0 and reports member count, SHA-256, and output path. This worktree must be created only from the committed candidate; do not copy untracked author materials into it.

- [ ] **Step 6: Inspect the anonymous ZIP and traceability coverage**

Run:

```powershell
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [IO.Compression.ZipFile]::OpenRead($artifactZip)
$zip.Entries | Sort-Object FullName | Select-Object -ExpandProperty FullName
$zip.Dispose()
uv run pytest tests/test_anonymous_artifact.py::test_manifest_covers_every_traceability_artifact -q
```

Expected: no `.git`, `timestamps`, `tmp`, personal path, or unlisted identity-bearing material; every final traceability artifact is covered.

- [ ] **Step 7: Remove only the verified temporary worktree registration**

Resolve and verify the exact path before removal:

```powershell
$resolvedAudit = (Resolve-Path -LiteralPath $auditRoot).Path
$resolvedTemp = (Resolve-Path -LiteralPath $env:TEMP).Path
if (-not $resolvedAudit.StartsWith($resolvedTemp, [StringComparison]::OrdinalIgnoreCase)) {
  throw "Refusing to remove non-temporary path: $resolvedAudit"
}
git worktree remove $resolvedAudit
```

The ZIP in the system temporary directory may remain for submission inspection. Do not remove or alter anything in the primary worktree.

- [ ] **Step 8: Conduct the human cold read**

Give the author the final PDF without commentary that primes particular answers. Ask for a cold read focused on:

```text
Can the reader state the two questions after the introduction?
Does each section make the next question feel necessary?
Does the wall first appear as a useful tool and then as a bounded one?
Do any passages sound formulaic, over-polished, or unlike the author?
Does any sentence imply more causality, generality, novelty, or evidence than intended?
Does the ending leave both a tool usable now and a research question worth carrying?
```

Do not mark the revision complete until the author accepts the cold-read corrections.

- [ ] **Step 9: Apply and verify any cold-read corrections**

For every correction, update the map action/implication row, rerun the focused claim tests and LaTeX cycle, recheck body length/anonymity, and inspect affected pages. If any claim or traceability row changes, rerun the full suite and anonymous ZIP checks.

- [ ] **Step 10: Commit the verified final manuscript**

If the cold read produced tracked changes, stage only those files and use the Commit Protocol:

```powershell
Invoke-SignedCommit 'docs: finalize NINeS manuscript after cold read'
```

If the cold read produced no tracked changes, do not create an empty commit. Finish with `git status --short --branch`, report all verification commands and fresh results, and identify the temporary anonymous ZIP by full path and SHA-256.

---

## Plan Self-Review Record

- **Spec coverage:** Task 1 covers the paragraph map, voice corpus, claim ledger, and implication audit. Task 2 enforces the human-reviewed introduction pilot and legibility definition. Tasks 3–4 implement the layered formal investigation, exact wall positives/boundary, both readings, and common-condition table. Task 5 preserves bounded behavioral reversal. Task 6 makes the readout and existing evaluation coherent while preserving edge limits. Task 7 covers the open problem, related work, limitations, conclusion, traceability, provenance, contextual style review, and 12-page constraint. Task 8 covers fresh tests, complete LaTeX build, visual QA, anonymity, artifact coverage, and human cold read.
- **Scope exclusions:** The terrestrial experiment, production census, and dual-dashboard figure are explicitly outside this plan and do not gate completion.
- **Interface consistency:** Claim IDs `C01`–`C17`, sequential `P` paragraph IDs, section headings, source artifacts, and the common 1800-second condition are named once and used consistently by downstream tasks.
- **No automatic style metric:** All counts are discovery aids followed by contextual judgment; no task asks for bulk substitution or a target punctuation count.
- **Evidence discipline:** The only numerical change specified in advance is the central table's direct transcription from the already registered common-condition rows. Every other measurement is preserved unless an explicit artifact audit documents a discrepancy.
