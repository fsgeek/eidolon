# Eidolon: Marching Orders Round 2 (Rikuy Review 2026-03-16)

**Review file:** `/home/tony/projects/rikuy/reviews/eidolon/review_20260316_010915.jsonl`
**Review stats:** 204 findings (1 Fatal, 59 Major, 144 Minor)
**Reviewers:** arxiv_honesty, arxiv_accessibility, arxiv_buildability, redundancy, conciseness, copy_editor, narrative
**Paper PDF compiled:** 2026-03-16 from `/home/tony/projects/vmtp/docs/paper/main.tex`

**Context:** Round 1 orders (MARCHING_ORDERS.md) addressed novelty framing, tautological results, section reordering, and conciseness. Many of those changes were applied. This round uses arXiv-calibrated personas (honesty, accessibility, buildability). The honesty reviewer called the paper "substantially more epistemically honest than typical systems papers" — the novelty fatal from R1 is gone. New issues surfaced.

**TLA+ state space exploration is still running — do not touch the TLA+ files or interrupt that process.**

---

## FATAL: Topology-Scoped Consistency Is Undefined (ADV-C-008)

This is the one fatal. "Topology-scoped consistency" is Contribution 4 in the Introduction and the subject of the Discussion section, but it is never formally defined. The accessibility reviewer could not explain it to labmates after reading the paper.

### Action

Either formalize it or demote it. Two options:

**Option A (formalize):** Add a definition, something like: "A system exhibits topology-scoped consistency if (i) each connected component C maintains linearizability for operations within C, (ii) cross-component state staleness is bounded by reconnection delay, and (iii) the relaxation parameter k determines failure absorption per domain." This could go at the start of the Discussion or in the Formal Model.

**Option B (demote):** Remove it from the contributions list. Keep the Discussion section as an interpretive framing ("one way to think about what these results mean") rather than a claimed contribution. This is the lower-risk option if the formalization doesn't feel ready.

My recommendation: Option A if you can write a clean definition that adds real content. Option B if it would be hand-waving dressed as formalism. Honesty over ambition.

---

## MAJOR: Accessibility for Non-Paxos Readers (ADV-C-001 through ADV-C-004)

The paper's natural audience is broader than Paxos specialists — the interplanetary setting attracts systems people, space systems people, DTN people. But the paper assumes deep Flexible Paxos knowledge:

1. **Phase 1 / Phase 2 quorum semantics** are used without sufficient background. A reader who knows Paxos but not Flexible Paxos won't follow the q1 + q2 > |N| argument.
2. **Grid and crumbling-wall quorum constructions** are name-dropped without explanation.
3. **The core design insight** (Phase 2 on Earth, Phase 1 spanning tiers) appears before the reader understands why this decomposition matters.
4. **The Phase 1 size formula** needs more scaffolding.

### Action

Add a ~half-page "Background: Flexible Paxos" subsection early (Section 2.1 or a new Section 2 before the Formal Model). Cover:
- Classic Paxos requires majority quorums for both phases
- Flexible Paxos observation: only the cross-phase intersection matters (q1 + q2 > |N|), so phases can use different quorum sizes
- This means Phase 2 (the fast path) can be small if Phase 1 (the recovery path) is large
- Grid/crumbling-wall are prior quorum constructions that exploit this; ours exploits topology instead

This isn't padding — it's making the paper accessible to its actual audience. Three paragraphs would suffice.

---

## MAJOR: Buildability Gaps (ADV-B-001 through ADV-B-004)

The buildability reviewer wants to replicate or extend the work but can't from the paper alone:

1. **Simulator architecture undescribed.** Event-loop structure, message-passing model, timer semantics — none of this is in the paper.
2. **Reproduction instructions are external.** `docs/step9-repro.md` is referenced but an arXiv reader can't see it without the repo.
3. **TLA+ model not characterized.** 67M+ states explored, but what properties? What abstractions? What was checked?
4. **Relay parameters unjustified.** 350-360ms Earth-to-relay, 220s relay-to-Mars, 10s jitter — where do these come from?

### Action

1. Add a paragraph in Experimental Design describing the simulator: discrete-event, message-passing with configurable link delays, deterministic link failure model for blackout, event ordering by timestamp. Two sentences on the core loop.
2. Either inline the key reproduction commands (the exact sweep invocation and plot generation) or add a footnote with the repo URL if it will be public.
3. Add 2-3 sentences on TLA+ verification: what the spec models (safety = no two leaders with conflicting values accepted in same round), what it abstracts away (network timing, crash recovery), state space size, and that no violations were found. Mention the specs are in the repo's `tla/` directory.
4. Add a sentence justifying relay parameters: derived from L1/L2 Lagrange point orbital mechanics at Mars opposition/conjunction, with jitter modeling signal processing variation. Cite or calculate.

---

## MAJOR: Honesty Calibration (ADV-A-001, -002, -003, -005, -008)

The paper is already honest (reviewer's words), but five places where presentation blurs the line:

1. **Hard-blackout 0%** is by construction, not empirical. (Already addressed in R1 orders — verify it was applied.)
2. **Earth-local 251ms invariance** is structurally guaranteed by fixed links. Frame as "the simulator confirms the design goal" not "we discovered."
3. **"Single degraded relay restores near-complete liveness"** depends on an idealized relay model. Add a sentence acknowledging this.
4. **"More resilient than flat quorums"** conflates design property with empirical finding. Reword.
5. **All results are design-level.** The Threats section covers this, but the abstract/intro should also flag it. One sentence: "All results are design-level; deployment would introduce orbital dynamics, antenna scheduling, and variable link quality not modeled here."

### Action

Most of these are single-sentence fixes. The pattern: wherever a structural consequence of the model is presented, add "by construction" or "by design" to signal its epistemic status. The reviewer explicitly praised this practice where it already exists — extend it to the remaining cases.

---

## MAJOR: Section Ordering (NAR-003)

Section 7 "Design" appears after Section 6 "Results" but introduces new empirical analysis (relaxed Phase 2 with crash tolerance). This inverts the expected flow.

### Action

Rename Section 7 to something like "Extension: Crash-Tolerant Phase 2 Relaxation" or "Relaxed Phase 2: Crash Tolerance and Coordinated Quorum Design." Frame it explicitly as a second analysis building on the baseline results. The narrative judge suggested this reads as a natural "what happens if we relax the constraint?" follow-up, which is fine — it just needs a name that signals extension rather than foundational design.

---

## Conciseness Pass (14 Major, 27 Minor)

The conciseness judge found the paper over-explains in predictable places:
- Re-explains results that follow from definitions
- Duplicates model descriptions across sections
- Includes repo file paths that belong in a README
- Over-hedges interpretations
- Restates the topology-scoped consistency formula three times (Intro, Discussion, Conclusion)

### Action

One tightening pass targeting 10-15% reduction. Rules:
- Each section adds new information or analysis. If a sentence in Discussion duplicates Results, replace it with interpretation.
- The topology-scoped consistency framing should live in the Discussion. Intro and Conclusion get one-sentence summaries pointing there.
- File paths and CSV output descriptions go in the repo README, not the paper.
- "This is unsurprising because..." — if it's unsurprising, state it in one sentence and move on.

---

## Copy Editing (30 Major, 80 Minor)

Most of the 110 copy-editing findings are **PDF extraction artifacts** (mid-word hyphens like "deter-mines", stray page numbers like "confidence\n8\ninterval"). These are NOT in the actual LaTeX — they're Rikuy's PDF reader breaking on line wraps.

**Genuine issues to fix:**
- Undefined acronyms on first use: CRDTs, IPC, DSN, DTN — define or expand each on first occurrence
- "lagrange" → "Lagrange" (proper noun)
- One garbled sentence flagged: "In the simulator, is phase1 quorum enforces..." — check the actual tex for this
- Compound-modifier hyphens: "topology-aware" should be hyphenated when used as adjective (likely already is — verify)
- Consistent unit formatting: "ms" vs "milliseconds" — pick one

---

## Redundancy (1 Major, 17 Minor)

The topology-scoped consistency three-part formula ("quorum geometry determines... network topology determines... relaxation parameter determines...") appears near-identically in Introduction, Discussion, and Conclusion. Discussion should own the full version; the others should summarize.

The 17 minor redundancy findings are mostly restated qualitative observations across sections. The conciseness pass will catch these naturally.

---

## Verification Checklist (Addendum to R1)

Before declaring arXiv-ready:

- [ ] Topology-scoped consistency either formalized with definition or demoted from contributions
- [ ] Flexible Paxos background subsection added (~half page)
- [ ] Simulator architecture paragraph added to Experimental Design
- [ ] TLA+ verification characterized (properties checked, state space, abstractions)
- [ ] Relay parameters justified (orbital mechanics derivation or citation)
- [ ] Reproduction instructions inlined or repo URL added
- [ ] Section 7 renamed to signal "extension" not "foundational design"
- [ ] "By construction" / "by design" labels added to structural results
- [ ] Design-level caveat in abstract or introduction
- [ ] Conciseness pass: 10-15% reduction target
- [ ] Acronyms defined on first use (CRDTs, IPC, DSN, DTN)
- [ ] Copy-editing fixes (Lagrange capitalization, garbled sentence, unit consistency)
- [ ] Topology-scoped consistency formula deduplicated (Discussion owns it)
- [ ] LaTeX compiles clean after all changes
- [ ] All R1 checklist items still satisfied

---

## What the Reviewers Liked (Preserve These)

- Epistemic honesty: "substantially more honest than typical systems papers"
- Threats to Validity section: thorough and genuine
- Claim-to-artifact traceability table: rare and valuable
- Confidence intervals reported throughout
- The core engineering result (Phase 2 on Earth, Phase 1 spanning tiers) is "clever" and "sound"
- The liveness envelope sweep data is "genuinely new"
- Repeater-vs-blackout comparison is "operationally interesting"
