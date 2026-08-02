# NINeS Story Revision Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reshape the existing NINeS manuscript around the observed failure-to-capability story, preserving its registered technical results while making phase-capability coincidence, the behavioral reversal, and the wall readout form one legible narrative.

**Architecture:** Preserve the copied reviewer manuscript as the evidentiary baseline and revise a real Git worktree created from `origin/nines-2027`. Rewrite the front and back, move the behavioral experiment before the wall, lead the wall material with the executable readout, and demote secondary calibration into an appendix. Gate the full migration on an early cold-reader test of the new introduction.

**Tech Stack:** LaTeX (`pdflatex`, BibTeX), Python 3.14 via `uv`, pytest, existing Eidolon simulation/audit artifacts, Git.

## Global Constraints

- The authoritative reviewer copy is `.worktrees/nines-rc-reviewer-pass/docs/paper/nines/main.tex`, SHA-256 `f1902db0b58a3eeefaa7ed3130d739a3082c4776450981f5023f9943b4309e74`.
- Do not edit or commit anything inside the copied `.worktrees/nines-rc-reviewer-pass` directory; its Git pointer is broken and it remains the review reference.
- Use `uv`, never `pip`; Python must remain `>=3.14`.
- Do not add a new protocol, experiment, production-causality claim, or dynamic-membership model.
- Preserve preregistered results, deviations, retry-policy bounds, single-decree scope, and all claim-to-artifact links.
- Use **phase-capability coincidence** as the only technical name for equality of acquisition and commit formability; use **the smaller question** only as the narrative refrain.
- GitHub supplies the service-level conundrum; LogDevice supplies the phase-capability instance. Never claim the theorem caused or fully explains GitHub's incident.
- Present 5/1/1/3 as an evaluated fixture, not a recommended deployment.
- The wall is the concrete instrument through which the reader sees the distinction; it is not the paper's real-world justification.
- Write the abstract last.
- If a go/no-go gate requires substantial new theory or evidence, stop and recommend not submitting to NINeS '27.

---

## File Map

- `docs/paper/nines/main.tex`: narrative order, production anchors, terminology, experiment placement, readout-first wall material, temporal horizon, limitations, conclusion, and final abstract.
- `docs/paper/nines/references.bib`: GitHub incident and Meta LogDevice sources.
- `docs/superpowers/notes/2026-08-02-nines-cold-reader.md`: verbatim cold-reader answers, test timestamp, and the introduction gate decision.
- `docs/superpowers/specs/2026-08-02-nines-narrative-revision-design.md`: approved design; read-only during execution unless a genuine design change is approved.
- `docs/superpowers/plans/2026-08-02-nines-story-revision.md`: this execution checklist.

## Task 1: Establish a Real Revision Worktree and Exact Baseline

**Files:**
- Source reference: `.worktrees/nines-rc-reviewer-pass/docs/paper/nines/main.tex`
- Source reference: `.worktrees/nines-rc-reviewer-pass/docs/paper/nines/references.bib`
- Working copy: `docs/paper/nines/main.tex`
- Working copy: `docs/paper/nines/references.bib`

**Interfaces:**
- Consumes: `origin/nines-2027`, the current planning branch containing the approved design and plan, and the immutable copied reviewer manuscript.
- Produces: a clean `codex/nines-story-revision` worktree whose paper exactly matches the reviewer copy before story edits.

- [ ] **Step 1: Invoke the worktree workflow**

Read and follow `superpowers:using-git-worktrees`. Create a real worktree on branch `codex/nines-story-revision` from the current planning branch; do not reuse the broken copied worktree.

- [ ] **Step 2: Bring the NINeS paper branch into the revision branch**

Merge the remote NINeS branch without rewriting either history:

```bash
git merge --no-ff origin/nines-2027 -m "merge: establish NINeS story revision"
```

Expected: the approved design and plan remain present, and `docs/paper/nines/` comes from `origin/nines-2027`.

- [ ] **Step 3: Reproduce the reviewer-copy delta with `apply_patch`**

Generate a read-only diff between `origin/nines-2027:docs/paper/nines/main.tex` and the copied reviewer file, then apply that delta to the working `docs/paper/nines/main.tex` using `apply_patch`. Do not copy files with shell redirection and do not modify the reference copy.

- [ ] **Step 4: Verify the baseline byte-for-byte**

Run:

```bash
sha256sum docs/paper/nines/main.tex docs/paper/nines/references.bib
```

Expected:

```text
f1902db0b58a3eeefaa7ed3130d739a3082c4776450981f5023f9943b4309e74  docs/paper/nines/main.tex
53db8f5a2c6b87ddd430dedf5dbd2a925b508de211e1616108ae0dc79c937599  docs/paper/nines/references.bib
```

- [ ] **Step 5: Compile the untouched baseline**

Run from `docs/paper/nines`:

```bash
pdflatex -interaction=nonstopmode -halt-on-error main
bibtex main
pdflatex -interaction=nonstopmode -halt-on-error main
pdflatex -interaction=nonstopmode -halt-on-error main
```

Expected: all commands exit 0 and `main.pdf` is produced.

- [ ] **Step 6: Commit the reproducible baseline**

```bash
git add docs/paper/nines/main.tex
git commit -m "chore: establish NINeS story revision baseline"
```

## Task 2: Add the Two Production Anchors

**Files:**
- Modify: `docs/paper/nines/references.bib:184`

**Interfaces:**
- Consumes: GitHub's October 2018 post-incident analysis and Meta Engineering's March 2022 LogDevice account.
- Produces: citation keys `warner2018github` and `meta2022logdevice`, available to the introduction and related work.

- [ ] **Step 1: Add the GitHub incident entry**

Append this entry to `references.bib`:

```bibtex
@misc{warner2018github,
  author       = {Jason Warner},
  title        = {October 21 Post-Incident Analysis},
  howpublished = {GitHub Blog},
  year         = {2018},
  month        = oct,
  url          = {https://github.blog/news-insights/company-news/oct21-post-incident-analysis/},
  note         = {Accessed 2026-08-02}
}
```

- [ ] **Step 2: Add the LogDevice entry**

Append this entry to `references.bib`:

```bibtex
@misc{meta2022logdevice,
  author       = {{Meta Engineering}},
  title        = {Augmenting Flexible Paxos in LogDevice to Improve Read Availability},
  howpublished = {Engineering at Meta},
  year         = {2022},
  month        = mar,
  url          = {https://engineering.fb.com/2022/03/07/core-infra/augmenting-flexible-paxos-logdevice/},
  note         = {Accessed 2026-08-02}
}
```

- [ ] **Step 3: Parse the expanded bibliography**

Run from `docs/paper/nines`:

```bash
bibtex main
```

Expected: BibTeX exits 0 with no syntax error. The entries become cited when Task 3 replaces the introduction.

- [ ] **Step 4: Commit the source foundation**

```bash
git add docs/paper/nines/references.bib
git commit -m "docs: add production anchors for capability story"
```

## Task 3: Draft the New Introduction Before Moving the Body

**Files:**
- Modify: `docs/paper/nines/main.tex:50-67`

**Interfaces:**
- Consumes: the approved story design and citation keys from Task 2.
- Produces: a self-contained introduction that a cold reader can summarize without `(1,0)` notation.

- [ ] **Step 1: Replace the current introduction with the GitHub mystery**

Write the opening in this factual order:

1. At 22:52 UTC, routine replacement of failing 100G optical equipment interrupted connectivity for 43 seconds.
2. A surviving Orchestrator Raft quorum initiated failover as configured.
3. East retained unreplicated writes, West accepted new writes, and the application could not tolerate the cross-country topology.
4. Recovery and degradation lasted 24 hours and 11 minutes.
5. State explicitly: consensus answered its control question; the service needed a larger answer.

Cite `warner2018github` in this opening. Do not use “split brain,” “all dashboards were green,” or claim that Eidolon's theorem explains the incident.

- [ ] **Step 2: Introduce the failure-backwards method**

Follow the incident with one paragraph asking:

```text
What did recovery need to know, what evidence did the control plane possess, and what capability did that evidence fail to establish?
```

State the broad hypothesis narrowly: component health, protocol success, current authority, and service capability are different answers even when familiar systems make them appear scalar.

- [ ] **Step 3: Narrow through LogDevice**

Write one paragraph establishing the precise technical anchor:

- LogDevice uses flexible Multi-Paxos.
- An incumbent can retain write capability after the larger leader-election/recovery quorum is lost.
- Meta reports recurring stuck recovery and manual intervention.
- GitHub supplies the service-level conundrum; LogDevice supplies the phase-capability split analyzed here.

Cite `meta2022logdevice`. Credit Meta with understanding and engineering around the tradeoff.

- [ ] **Step 4: Deliver the intellectual reversal**

Explain without notation:

- one threshold count appeared to answer acquisition and commit because phase symmetry made their formability equal;
- call this **phase-capability coincidence**;
- the threshold made the coincident answer count-readable;
- Flexible Paxos preserves safety while allowing the capabilities to separate;
- the health/count answer did not become false—the inference from it stopped following.

Use “the smaller question” once here as the refrain.

- [ ] **Step 5: State the bounded contribution as prevent, detect, act**

End the introduction by promising exactly:

- containment characterizes which mixed states a finite quorum-family pair admits;
- the registered experiment shows that direction and policy matter;
- the wall demonstrates prevention and a concrete structural readout;
- runtime authority and service policy govern action and remain outside the model;
- Mars and LEO expose temporal reachability; changing membership remains future work.

Do not enumerate 5/1/1/3 threshold results in the introduction.

- [ ] **Step 6: Compile and inspect only the first two pages**

Run the full LaTeX/BibTeX sequence, then:

```bash
pdftotext -f 1 -l 2 main.pdf - | sed -n '1,260p'
```

Expected: GitHub appears before quorum notation; LogDevice's evidentiary role is explicit; phase-capability coincidence is defined once; no undefined citations appear.

- [ ] **Step 7: Commit the testable introduction**

```bash
git add docs/paper/nines/main.tex
git commit -m "docs: lead NINeS paper from observed failure"
```

## Task 4: Run the Cold-Reader Gate

**Files:**
- Create: `docs/superpowers/notes/2026-08-02-nines-cold-reader.md`
- Modify if gate fails: the complete `Introduction` section in `docs/paper/nines/main.tex`

**Interfaces:**
- Consumes: the compiled introduction from Task 3 and a technically literate reader outside the immediate author/model drafting loop.
- Produces: verbatim answers, a pass/fail decision, and any bounded introduction correction.

- [ ] **Step 1: Give the reader only the title, abstract placeholder, and introduction**

Do not explain the new thesis beforehand. Ask exactly:

```text
1. What does this paper claim?
2. What would you inspect or do differently during the next incident?
```

- [ ] **Step 2: Record the answers verbatim**

Create the dated note with these headings:

```markdown
# NINeS Introduction Cold-Reader Test

## Reader Context

## Answer 1: Claimed Contribution

## Answer 2: Changed Incident Practice

## Diagnosis

## Decision
```

The decision is `PASS`, `REVISE`, or `STOP`.

- [ ] **Step 3: Apply the gate**

- `PASS`: the reader identifies phase-capability coincidence/loss, exact characterization, and separate capability inspection; proceed.
- `REVISE`: change only the introduction, recompile, and repeat the same test with a fresh reader.
- `STOP`: if the reader cannot connect the production story to the theorem without new theory or evidence, recommend no NINeS submission and stop this plan.

- [ ] **Step 4: Commit the reader evidence and any introduction repair**

```bash
git add docs/superpowers/notes/2026-08-02-nines-cold-reader.md docs/paper/nines/main.tex
git commit -m "docs: record NINeS cold-reader gate"
```

## Task 5: Make the Formal Core Pay Off the Story

**Files:**
- Modify: `docs/paper/nines/main.tex`, current sections `Why One Count Once Worked` and `Capability Gaps` (reviewer-copy lines 69-120)

**Interfaces:**
- Consumes: the cold-reader-approved introduction.
- Produces: one formal name, an intuitive set-order explanation, the exact theorem, and the reader's preregistered bet.

- [ ] **Step 1: Define phase-capability coincidence formally**

In `Why One Count Once Worked`, define:

```tex
\operatorname{Form}(\mathcal Q_1)=\operatorname{Form}(\mathcal Q_2)
```

as **phase-capability coincidence**. State that phase symmetry is sufficient but syntactically different families may also induce the same formability predicate.

- [ ] **Step 2: Preserve threshold readability as a separate fact**

Retain the distinction:

- phase-capability coincidence makes the two answers equal;
- threshold structure makes the common answer readable from a count.

Use “the smaller question” once at the end of the section, not as a second definition.

- [ ] **Step 3: Explain predicate order before the proposition**

Before Proposition `prop:gap`, add this intuition in prose:

- if every acquisition-capable connectivity set is commit-capable, only commit-without-acquisition can remain;
- if neither formability set contains the other, each mixed direction has a witness.

Then retain the existing proposition, proof, corollary, complexity, and exhaustive-audit evidence.

- [ ] **Step 4: Invite the reader's prediction**

End `Capability Gaps` with a short transition in venue-appropriate language:

```tex
The two mixed states are structurally symmetric in the characterization, but need not behave symmetrically. Before reading the next section, which would you expect to disrupt a competing healthy proposer: acquisition without commit, or commit capability without fresh acquisition? We registered the first prediction before implementing the experiment.
```

- [ ] **Step 5: Verify terminology consistency**

Run:

```bash
rg -n "predicate coincidence|operational coincidence|phase-capability coincidence|smaller question" docs/paper/nines/main.tex
```

Expected: no `predicate coincidence`; `phase-capability coincidence` is the sole formal name; “the smaller question” appears only as narrative language.

- [ ] **Step 6: Compile and commit**

Run the full paper build, then:

```bash
git add docs/paper/nines/main.tex
git commit -m "docs: make capability coincidence the formal hinge"
```

## Task 6: Move the Behavioral Reversal Before the Wall

**Files:**
- Modify: `docs/paper/nines/main.tex`, current `What Happens Inside the Gaps` section (reviewer-copy lines 291-324)
- Modify: section ordering between `Capability Gaps` and `Putting the Gaps on a Wall`

**Interfaces:**
- Consumes: the reader's prediction from Task 5.
- Produces: an immediate experimental payoff that does not require prior knowledge of the full wall construction.

- [ ] **Step 1: Move the entire valence section intact after `Capability Gaps`**

Move the section before `Putting the Gaps on a Wall`. Preserve its label `sec:valence`, preregistration, table, five deviations, and all numerical bounds.

- [ ] **Step 2: Add a compact fixture definition**

Before the experiment details, define only what this section needs:

- ten acceptors: five Earth, one LEO, one Moon, three Mars;
- strict Phase 2 requires all five Earth acceptors (`k=5`);
- the Moon incumbent's Phase 1 requires Moon, LEO, and at least one Earth acceptor;
- the full construction follows in the next major section.

Do not defend 5/1/1/3 as a deployment.

- [ ] **Step 3: Pay off the lost bet in the first result paragraph**

Retain “The prediction failed. It failed in reverse.” Tie it explicitly to the invitation at the end of the prior section and to the dated preregistration.

- [ ] **Step 4: Add the LogDevice valence contrast**

After the Multi-Paxos paragraph, add one bounded comparison:

- LogDevice exploited incumbent write capability in `(0,1)` as availability;
- the single-decree contention experiment exhausted the healthy proposer's budget in the same structural direction;
- therefore structural state has no policy-independent valence;
- neither system is causal evidence for the other.

Cite `meta2022logdevice`.

- [ ] **Step 5: End by opening the instrument question**

Keep the transition:

```tex
How can a designer or operator read which state the topology permits?
```

- [ ] **Step 6: Compile, inspect cross-references, and commit**

Run the full build and:

```bash
rg -n "undefined references|multiply defined" docs/paper/nines/main.log
```

Expected: no matches.

```bash
git add docs/paper/nines/main.tex
git commit -m "docs: let the registered reversal precede the wall"
```

## Task 7: Lead the Wall Material with the Actual Readout

**Files:**
- Modify: `docs/paper/nines/main.tex`, current `Reading the Wall`, `Putting the Gaps on a Wall`, and `Where the Wall Works` sections
- Read only: `examples/capability/planetary_moon_01.json`
- Read only: `experiments/capability_readout.py`

**Interfaces:**
- Consumes: the instrument question from Task 6 and the existing deterministic readout.
- Produces: a concrete `(0,1)` display before construction machinery, followed by the wall explanation and prevention boundary.

- [ ] **Step 1: Re-run the existing readout demonstration**

Run:

```bash
uv run python experiments/capability_readout.py \
  --input examples/capability/planetary_moon_01.json \
  --format text
```

Expected output:

```text
state: (0,1)
initiating tier: Moon
R1 acquisition formable: False
R2 commit quorum formable: True
requires preexisting authority: True
runtime authority: unknown
service policy: not-inferred
missing Phase 1 LEO obligation: require 1, reachable 0
```

- [ ] **Step 2: Open the wall section with that output**

Create a compact `verbatim` or monospaced figure containing the exact output. Introduce it as a deterministic interpretation of supplied configuration and connectivity, not failure detection or empirical evaluation.

- [ ] **Step 3: Explain what the reader can see before explaining how it works**

In three short paragraphs:

1. acquisition and commit differ;
2. the failed obligation is named rather than inferred from retries;
3. authority and policy remain explicitly unknown.

Then ask what structure makes the readout possible.

- [ ] **Step 4: Present the construction behind the instrument**

Move the existing system model, per-tier Phase 1 families, Phase 2 family, safety proposition, verification scope, and meaning of “global” after the readout. State at entry that extra downward witnesses express participation policy; Earth intersection carries Paxos safety.

- [ ] **Step 5: Present the exact threshold boundary as prevention**

Follow the construction with the existing boundary theorem and `k=3` result. Interpret it through the three-layer response:

- containment prevents selected gaps;
- the readout detects residual gaps;
- authority-aware action remains outside the interface.

- [ ] **Step 6: Remove the unsupported priority posture**

Search for and remove claims equivalent to “first instrument” or “first readout” unless backed by a source:

```bash
rg -ni "first.*(instrument|readout|recognizer)|no one.*read" docs/paper/nines/main.tex
```

Expected: no unsupported priority claim.

- [ ] **Step 7: Compile and commit the concrete payoff**

Run the full build and visually inspect the page containing the readout for overflow and legibility.

```bash
git add docs/paper/nines/main.tex
git commit -m "docs: make the wall readout the concrete payoff"
```

## Task 8: Demote Secondary Wall Calibration and Preserve the Necessary Evidence

**Files:**
- Modify: `docs/paper/nines/main.tex`, current readout/evaluation material (reviewer-copy lines 376-597)

**Interfaces:**
- Consumes: the readout-first wall section from Task 7.
- Produces: a narrative body centered on gap prevention/detection, with calibration results retained in an appendix rather than discarded.

- [ ] **Step 1: Keep the evidence required by the new claim spine in the body**

Retain in the main body:

- the deterministic readout and its provenance limits;
- the exact `k` boundary;
- the crash-relaxation table rows needed to show the prevention cost;
- one concise paragraph distinguishing obligations from sparse reachability.

- [ ] **Step 2: Move secondary calibration to an appendix**

Create `\section{Supplemental Wall Calibration}` after the existing appendices and move, without changing numerical claims:

- design-time quorum-count gradient and its figure;
- geometry versus competitive-majority baseline;
- full-coverage per-tier latency table and cadence discussion;
- full sparse-topology table and detailed explanation.

Add one body pointer to the appendix. Do not delete claim-to-artifact rows.

- [ ] **Step 3: Keep the LEO latency surprise as a bridge, not a result parade**

In the main body's temporal-horizon transition, retain one sourced-to-appendix sentence: LEO's modeled path completed faster than the cross-continental Earth path because wall position and convergence cost are not identical. Do not use this to claim a realistic LEO deployment.

- [ ] **Step 4: Reconcile section labels and traceability references**

Run:

```bash
rg -n "sec:leadership|sec:reachability|tab:flat-vs-wall|tab:pertier|tab:sparse|fig:gradient" docs/paper/nines/main.tex
```

Expected: every remaining reference points to the moved appendix material or is removed with its dependent sentence.

- [ ] **Step 5: Compile and commit**

Run the full build, then:

```bash
git add docs/paper/nines/main.tex
git commit -m "docs: move wall calibration behind the story"
```

## Task 9: Rewrite the Horizon, Positioning, Limits, and Ending

**Files:**
- Modify: `docs/paper/nines/main.tex`, `Related Work`, `What Remains Unsolved`, `Threats to Validity and Limitations`, and `Conclusion`

**Interfaces:**
- Consumes: the finalized main-body claim spine.
- Produces: correct credit, explicit causal boundaries, a temporal research horizon, and an outward-opening conclusion.

- [ ] **Step 1: Credit the production precedent in Related Work**

Add LogDevice beside Flexible Paxos as an observed operational instance. State:

- Meta identified the write-availability/leader-recovery tradeoff;
- this paper does not claim discovery of `(0,1)`;
- the contribution is exact finite-family characterization, behavioral measurement under a registered model, and executable recognition.

Keep the existing distinctions from RQS, subsumption, WPaxos, and reconfiguration only where they support that statement.

- [ ] **Step 2: Rewrite `What Remains Unsolved` as the temporal horizon**

Organize it in this order:

1. Mars: fixed membership and prolonged scheduled reachability loss.
2. LEO: fixed logical membership and rapidly varying visibility.
3. Mobile edge: both reachability and authority-bearing membership vary.
4. Present result: fixed-epoch formability only.
5. Open question: what temporal overlap or transferable authority evidence preserves safety across changing configurations?

Do not propose or evaluate a mobile Paxos protocol.

- [ ] **Step 3: Consolidate armor into precise limitations**

Retain all existing model bounds. Add two causal limits:

- GitHub motivates the failure-backwards method; the capability theorem is not its root-cause analysis.
- LogDevice demonstrates the structural split in a production-derived design, but its recovery implementation and semantics differ from Eidolon's single-decree experiment.

Remove repetitive versions of these caveats from earlier sections once the local claim remains unambiguous.

- [ ] **Step 4: Rewrite the conclusion around changed practice**

End in this order:

- the smaller question worked because of phase-capability coincidence;
- flexible quorums preserve safety while dissolving that inference;
- containment supports prevention, the auditor/readout support detection, and authority/policy govern action;
- ask which actions the system can complete and what evidence supports each answer;
- close on the changing-membership problem, not 5/1/1/3 metrics.

- [ ] **Step 5: Compile and commit**

```bash
git add docs/paper/nines/main.tex
git commit -m "docs: open the NINeS story toward temporal consensus"
```

## Task 10: Write the Abstract Last

**Files:**
- Modify: `docs/paper/nines/main.tex:46-48`

**Interfaces:**
- Consumes: the completed and compiled manuscript.
- Produces: an abstract that presents conundrum, explanation, result, reversal, instrument, and boundary in that order.

- [ ] **Step 1: Replace the abstract with a six-move version**

Use one or two sentences per move:

1. A correct quorum/control answer can fail to establish service capability; name GitHub only if space permits and cite only in the body.
2. Phase-capability coincidence explains why one reachable threshold once answered acquisition and commit together.
3. Containment exactly characterizes both mixed directions.
4. The preregistered result reversed the predicted ordering under its stated single-decree policy and budget.
5. The wall readout makes residual capability and failed obligations concrete while leaving authority/policy unknown.
6. Mars/LEO expose temporal reachability; changing membership remains outside the fixed-epoch result.

Do not include the tier-by-tier `k=3` result table in prose.

- [ ] **Step 2: Check abstract vocabulary independence**

Run:

```bash
pdftotext -f 1 -l 1 main.pdf - | sed -n '1,180p'
```

Expected: a reader can understand the problem before seeing `(1,0)`; if notation remains, both directions are defined in words first.

- [ ] **Step 3: Keep the current title for this revision pass**

Retain `Legible Consensus: Capability Gaps in Flexible Quorums`. A title change is not required to validate the narrative and should not consume the five-day path unless the cold-reader evidence shows the current title actively misstates the claim.

- [ ] **Step 4: Compile and commit**

```bash
git add docs/paper/nines/main.tex
git commit -m "docs: write NINeS abstract from the finished story"
```

## Task 11: Verify Claims, Rendering, and Narrative Momentum

**Files:**
- Verify: `docs/paper/nines/main.tex`
- Verify: `docs/paper/nines/references.bib`
- Verify: `docs/paper/nines/main.pdf`
- Verify: `docs/superpowers/notes/2026-08-*-nines-cold-reader.md`

**Interfaces:**
- Consumes: the complete revision.
- Produces: evidence for the final go/no-go decision.

- [ ] **Step 1: Run the focused claim-language tests**

From the repository root:

```bash
uv run pytest tests/test_nines_claim_language.py tests/test_anonymous_artifact.py -v
```

Expected: all tests pass. If a test encodes superseded narrative rather than a scientific boundary, update the test only after documenting why; never weaken a causal or experimental bound to make prose pass.

- [ ] **Step 2: Run the full test suite**

```bash
uv run pytest
```

Expected: all tests pass.

- [ ] **Step 3: Run the final paper build**

Run the four-command LaTeX/BibTeX sequence. Check:

```bash
rg -n "undefined|multiply defined|Overfull \\hbox" docs/paper/nines/main.log
```

Expected: no undefined citations/references or multiply defined labels; inspect and repair every overfull box.

- [ ] **Step 4: Audit the narrative momentum contract**

For each transition, record the exact closing question and the next section's payoff:

1. GitHub to LogDevice.
2. LogDevice to phase-capability coincidence.
3. Coincidence to containment.
4. Containment to the reader's bet.
5. Bet to reversal.
6. Reversal to readout.
7. Readout to prevent/detect/act.
8. Fixed epoch to changing membership.

If any transition merely announces the next section, rewrite it as a genuine unresolved question or remove the unnecessary section boundary.

- [ ] **Step 5: Verify claim boundaries by search**

Run:

```bash
rg -ni "all dashboards|every.*healthy|caused.*GitHub|explains.*GitHub|first.*instrument|intrinsically harmful|solves.*mobile|recommended deployment" docs/paper/nines/main.tex
```

Expected: no prohibited claim; manually inspect any match used in a negated limitation.

- [ ] **Step 6: Inspect the rendered PDF page by page**

Confirm figures, tables, readout text, references, section ordering, and appendix moves render cleanly. Check that the readout appears before the full wall machinery and that the abstract/introduction do not read as a limitations section.

- [ ] **Step 7: Commit verification repairs**

```bash
git add docs/paper/nines/main.tex docs/paper/nines/references.bib tests docs/superpowers/notes
git commit -m "docs: verify NINeS narrative revision"
```

Omit `tests` from `git add` if no claim-language test required a justified update.

## Task 12: Make the Submission Go/No-Go Decision

**Files:**
- Create: `docs/superpowers/notes/2026-08-02-nines-go-no-go.md`

**Interfaces:**
- Consumes: cold-reader evidence, compiled PDF, verification results, and the nine gates in the narrative design.
- Produces: an explicit `GO` or `NO-GO`; this task does not submit or upload the paper.

- [ ] **Step 1: Evaluate each design gate with evidence**

Use a table with columns `Gate`, `Pass/Fail`, and `Evidence`. Include the cold reader's two answers, body-to-story alignment, wall role, theorem centrality, causal bounds, fixed-epoch boundary, changed reader model, naming consistency, and full-paper momentum.

- [ ] **Step 2: Apply the decision rule**

- `GO`: every gate passes without a promise of future work needed to make the current claim true.
- `NO-GO`: any failed gate requires substantial new theory, evidence, or a plausible deployment defense for 5/1/1/3.

Do not use proximity to the deadline as evidence.

- [ ] **Step 3: Commit the decision record**

```bash
git add docs/superpowers/notes/2026-08-02-nines-go-no-go.md
git commit -m "docs: record NINeS submission decision"
```

- [ ] **Step 4: Stop before external submission**

Report the decision and evidence to the user. Uploading to HotCRP, replacing a registered manuscript, or pushing a branch requires a separate explicit request.
