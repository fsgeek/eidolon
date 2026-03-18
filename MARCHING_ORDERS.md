# Eidolon: Marching Orders for arXiv Submission

**Paper:** `/home/tony/projects/vmtp/docs/paper/main.tex` (265 lines, 11pp, compiles clean)
**Goal:** arXiv-ready submission. Honest, scientifically rigorous, useful contribution.

---

## The Central Problem: Novelty Framing

Two independent Rikuy review runs (7 and 5 fatals respectively) converge on the same vulnerability: the paper's thesis — "safety depends on quorum intersection, not majority" — is Flexible Paxos (Howard et al., 2016). The paper even says so: "Our construction follows that line directly."

**This is not a fatal flaw in the research. It is a fatal flaw in the framing.**

The contribution is NOT the quorum intersection theorem. It is:

1. **The liveness envelope characterization.** Nobody has measured what happens to Paxos liveness under scheduled multi-hour disconnection with topology-aware quorum shaping. The sweep data (18 points, 50 seeds each) showing where liveness degrades and where it holds is genuinely new.

2. **The Earth-local latency invariance.** 251.0-251.0 ms mean across all 18 sweep points despite Mars delay varying from 186s to 1342s. This is the structural payoff of topology-aware quorum shaping — the hot path is genuinely isolated from the slow tier.

3. **The repeater-assisted connectivity model.** The comparison between hard blackout (0% during-blackout success everywhere) and repeater-assisted (100% for most points, graceful degradation at extremes) is the operationally interesting result.

4. **The topology-scoped consistency framing** (Section 7/Discussion). During blackout, each tier maintains local strong consistency while cross-tier state becomes stale in a structured way. This is a genuinely useful way to think about consistency in topologically asymmetric systems.

### Action

Rewrite the abstract and introduction to lead with the liveness envelope and topology-scoped consistency, not the quorum intersection theorem. Flexible Paxos is the foundation, not the contribution. The contribution is "here's what happens when you actually build this and measure it across a realistic parameter space."

Current introduction (line 27) lists four contributions. Reframe:
- **Old framing:** "an explicit quorum-family definition" → this is methodology, not contribution
- **New framing:** Lead with the empirical liveness envelope, the Earth-local invariance finding, the repeater-assisted vs hard-blackout comparison, and the topology-scoped consistency analysis. The quorum family definition is how you get there, not what you found.

---

## The Safety Proof Gap

**Proposition 1 (lines 68-72)** proves that Q1 ∩ Q2 ≠ ∅. Both review runs flag this as circular — it follows immediately from the definitions (Q1 must contain an Earth node, Q2 = E). That's correct: it IS trivial given the definitions. But it's also not wrong. The question is whether the paper is claiming this is a deep result or merely confirming the construction satisfies the Flexible Paxos requirement.

### Action

Reframe Proposition 1 as a **verification step**, not a contribution. Something like: "We verify that the chosen quorum families satisfy the Flexible Paxos cross-intersection requirement" rather than presenting it as a theorem. This is honest — you chose the families to satisfy intersection, and you're confirming they do.

### The Timeout-Safety Interaction (Substantive)

One reviewer flagged: if a timeout triggers Phase 1 with only Earth-local nodes reachable (cross-tier links down during blackout), does the resulting Phase 1 quorum satisfy the structural intersection requirement? The paper asserts "Timeouts affect liveness only" (line 39) without proof.

**Check:** In the simulator, can a Phase 1 round complete with only Earth nodes? If `is_phase1_quorum` requires nodes from all four tiers, then a Phase 1 attempt during blackout will simply fail (liveness loss, not safety violation). If it can somehow succeed with Earth-only nodes, that's a real problem. Verify against `quorums.py` and `demo_step_9.py`. If the code enforces the tier-spanning requirement, add a sentence confirming this: "Phase 1 attempts during blackout fail to form a quorum because the structural family requires at least one node from every tier. This is a liveness failure, not a safety violation."

---

## Tautological Results

### 0% Blackout Success (Table 3)

Both runs flag this: under hard blackout, cross-tier links are removed, and Phase 1 requires cross-tier nodes. 0% during-blackout success is a logical necessity of the model, not an empirical finding. The 50-seed sweep adds no information.

### Action

Acknowledge this directly. Something like: "Under hard blackout, global during-blackout success is 0% by construction: the Phase 1 quorum requires nodes from all tiers, and the blackout model removes all cross-tier links. We report this not as a finding but as a baseline against which the repeater-assisted model is compared." This is more honest and makes the repeater results more interesting by contrast.

### Earth-Local Latency Variance of 0.0 ms

The 251.0-251.0 ms range is essentially deterministic given the network model. The reviewers flag it as trivially expected.

### Action

Reframe: the point isn't that the variance is zero (which is model-dependent), it's that Earth-local operations are **structurally isolated** from Mars-tier behavior. The zero variance is evidence that the isolation is complete in the simulator, which is the design goal. In a real deployment with more realistic network jitter, you'd expect small variance but the structural isolation would hold. Say this.

---

## Structural Changes

### Move Related Work Before Results

Both review runs recommend this. Currently Related Work is Section 5 (after Results). Readers encounter novelty claims in the Results without knowing what's been done before.

### Action

Move Related Work to Section 3, between Formal Model and Experimental Design. The current Related Work (lines 206-213) is already well-written with 15 references and clear positioning. It just needs to appear earlier so the reader has context before seeing results.

New section order:
1. Introduction
2. Formal Model
3. Related Work (moved from Section 5)
4. Experimental Design
5. Results
6. Threats to Validity
7. Discussion
8. Conclusion
+ Appendix A (Traceability)

### Traceability Table

The claim-to-artifact traceability table (Appendix A) is a strength — keep it. But `docs/step9-repro.md` is an external reference that arXiv reviewers can't verify. If the repository will be public at submission time, add the repo URL. If not, consider inlining the key reproduction commands.

---

## Conciseness

Both runs flag repetition: key results (Earth-local invariance, 0% blackout, quorum-intersection thesis) are restated 3-5 times across abstract, intro, results, discussion, and conclusion.

### Action

Each section should add new information or analysis, not restate findings. The abstract states results. The introduction motivates. Results present data. Discussion interprets. Conclusion draws implications. If a sentence in the Discussion is a copy of a sentence in Results, cut it from Discussion and replace with interpretation.

The Related Work section was recently rewritten (by the prior instance) and is somewhat verbose. Tighten it — each paragraph should be 2-3 sentences: what they did, how we differ.

---

## Copy-Editing Notes

The Rikuy copy editor flagged rendering failures (raw `\mathcal`, missing ± symbols, exposed preamble). **Check whether this is in the actual PDF or just in how Rikuy ingested the text.** The paper compiles to a clean 11-page PDF. If the PDF renders correctly, these are Rikuy text-extraction artifacts, not paper bugs.

Genuine copy issues to check:
- "lagrange" should be "Lagrange" if referring to Lagrange points
- Topology notation consistency: is it 5/1/1/3 or 3/1/1/5? Pick one and use it throughout
- p95 should be defined on first use (95th percentile)
- CrumblingWallQuorum (from the code) — if mentioned in the paper, gloss it

---

## What NOT to Change

- **The data.** The sweep results are clean. 50 seeds, 18 points, CI reported throughout.
- **The figures.** Both liveness envelope plots exist and are referenced correctly.
- **The Threats to Validity section.** It's thorough and honest (lines 215-228). It already acknowledges single topology, stylized workload, crash-stop only, and the Q2={E} availability tradeoff. This is one of the paper's strengths.
- **The Discussion section.** The topology-scoped consistency framing is genuinely interesting. It should stay and arguably be elevated in the introduction as a contribution.
- **The traceability table.** Rare in papers. Keep it.

---

## Verification Checklist

Before declaring the paper ready:

- [ ] Abstract leads with liveness envelope and topology-scoped consistency, not quorum intersection
- [ ] Introduction reframes contributions around empirical findings
- [ ] Proposition 1 is a verification step, not a claimed contribution
- [ ] Timeout-safety interaction is addressed (verify against code)
- [ ] 0% blackout result acknowledged as by-construction baseline
- [ ] Earth-local invariance framed as structural isolation evidence
- [ ] Related Work moved before Results
- [ ] Repetition across sections reduced
- [ ] LaTeX compiles clean: `pdflatex main && bibtex main && pdflatex main && pdflatex main`
- [ ] All `\ref` and `\cite` resolve (no `[?]` markers)
- [ ] Consistent topology notation throughout
- [ ] Repository URL included if public, or reproduction commands inlined

---

## Context

- **Rikuy reviews:** `/home/tony/projects/rikuy/reviews/eidolon/review_20260314_211501.jsonl` and `review_20260314_211535.jsonl` (101 and 105 findings respectively)
- **Simulator code:** `/home/tony/projects/eidolon/` (quorums.py, demo_step_9.py are key files for the timeout-safety verification)
- **Also at:** `/home/tony/projects/vmtp/` (same project, different directory)
- **References:** `/home/tony/projects/vmtp/docs/paper/references.bib` (15 entries)
- **Figures:** `/home/tony/projects/vmtp/docs/paper/figures/` (two PDF liveness envelope plots)
