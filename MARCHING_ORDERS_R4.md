# Eidolon: Marching Orders Round 4 (Rikuy Review 2026-03-19)

**Review file:** `/home/tony/projects/rikuy/reviews/eidolon/review_20260319_174401.jsonl`
**Review stats:** 268 findings (1 Fatal, 67 Major, 200 Minor)
**Paper:** `/home/tony/projects/eidolon/docs/paper/main.pdf` (6259 words, 13 sections)

**Context:** R3 had 249 findings (1F, 69M). R4 has 268 findings (1F, 67M). The delta is essentially zero — no R3 findings were resolved, one new one appeared. The paper grew from 5068 to 6259 words (Discussion expanded significantly) but the added content didn't address flagged issues. The reviewers noticed.

**The message this round: Stop adding content. Start fixing flagged issues.** The paper is long enough. Every additional word that doesn't fix a finding makes the conciseness problem worse and leaves the real issues unaddressed.

---

## What's Resolved Across All Rounds

- Novelty framing — solid. "Crumbling wall mapped to physical topology" is accepted.
- Contribution clarity — buildability reviewer says "I would cite this paper for the specific claim that crumbling-wall quorum geometry produces legible, tier-scoped liveness degradation under scheduled disconnection."
- Epistemic honesty — "largely epistemically honest and commendably self-aware in several places."
- Traceability table — "exemplary practice."

---

## FATAL: Index Convention (ADV-C-003) — PERSISTENT, Third Round

The clarifying sentence was added ("We index tiers from top to bottom: i=0 is Mars... j ≥ i selects tier j at or below tier i in the wall"). The reviewer still finds it confusing after three re-reads. The sentence tells the reader the convention but doesn't fix the underlying problem: `j ≥ i` meaning "lower/closer to Earth" is spatially backwards.

### Two Options

**Option A (re-index):** Flip the indices. Earth=0 (bottom), Mars=3 (top). Then `j ≤ i` means "go down from tier i toward Earth," which matches spatial intuition. This requires updating every formula and tier reference, but it's a mechanical find-and-replace.

**Option B (worked example):** Keep the indexing but add a small concrete example immediately after the formula. The accessibility reviewer specifically suggested: "A 3-tier, 6-node toy that traces through Phase 1 quorum formation, the intersection proof, and what happens when one tier goes dark." This resolves the index convention AND the scaffolding complaints in one shot.

**Recommendation:** Option A is cleaner if you can stomach the churn. Option B is safer and adds more pedagogical value. Either way, the current approach (explanatory sentence alone) has been tried twice and failed twice. Do something different.

---

## PRIORITY FIXES (Address These Before Adding Any Content)

### 1. Abstract/Introduction Calibration (ADV-A-001, ADV-A-004)

The honesty reviewer: "The abstract and introduction systematically present logical consequences of quorum definitions as empirical discoveries, and the generalization claims to terrestrial deployments are unsupported speculation."

**Action:** Audit the abstract and introduction. For each claim, ask: is this a structural consequence of the design, an empirical measurement, or speculation? Label accordingly.

- "0% vs 100%" — structural consequence, label it as such ("by construction")
- Earth-local latency — empirical measurement, fine as-is
- Generalization to edge/cloud/CDN — speculation, either remove from abstract or explicitly qualify ("we conjecture that...")
- "Legibility" — this is the real contribution, foreground it

### 2. Flat Construction Needs a Definition (ADV-B-006) — NEW

The flat construction baseline ("the prior version of this work") has no formal quorum family definition, no cross-intersection verification, and no citation. The reader can't independently verify the 0% result that anchors the entire comparison.

**Action:** Add the flat construction's formal definition alongside the wall construction: "The flat Phase 1 family requires intersection with all four tiers: Q_1^{flat} = { Q ⊆ N | ∀j ∈ {0,1,2,3} : Q ∩ T_j ≠ ∅ }." One formula, one sentence explaining why this guarantees 0% during blackout ("any blackout that removes a tier makes the quorum unformable").

### 3. Commit Hash / Dependency Pinning (ADV-B-003)

Still no pinned commit, no release tag, no Python/SimPy version. Same issue as ai-honesty.

**Action:** Add commit hash and dependency versions. One line in the reproducibility section.

### 4. Safety-Depends-on-Tier-Membership Claim Unproven (ADV-A-002, ADV-B-002)

The claim that safety depends only on tier membership (not node count) is asserted without proof, particularly for the relaxed k-of-|E| case.

**Action:** Either prove it (a short argument: "cross-intersection requires Q1 ∩ Q2 ≠ ∅; since every Q1 contains at least one Earth node and Q2 ⊆ E, intersection holds regardless of |E|") or explicitly state it as a property verified by TLA+ rather than proven analytically.

### 5. Crash-Tolerance Arithmetic (ADV-A-003)

Persists from R3. The crash-tolerance advantage of the wall construction over flat is an arithmetic consequence of larger quorum pools (10 vs 5 nodes), not a structural insight.

**Action:** Acknowledge directly: "The wall construction's crash tolerance advantage is arithmetic: the global quorum draws from 10 nodes rather than 5, permitting more failures before quorum loss. The structural insight is not the count but the legibility — which crashes matter depends on their tier, not just their number."

### 6. Section Numbering (NAR-002)

Section 3 ("Background") internally uses subsections 2.1-2.3, Section 4 ("Construction") uses 3.1-3.6. This is a LaTeX numbering bug.

**Action:** Fix the section/subsection counters. Mechanical.

---

## LOWER PRIORITY (After Fixes Above)

### Accessibility Scaffolding

- **ADV-C-001:** Crumbling wall gets a terse parenthetical before the reader must follow the whole argument. More intuition needed on first mention.
- **ADV-C-002:** q1+q2>|N| needs flagging as the uniform special case.
- **ADV-C-004:** TLA+ verification needs 2-3 sentences of context.
- **ADV-C-006:** |E|-k+1 pigeonhole formula needs one sentence of derivation.
- **ADV-C-008:** Table 6 hard to parse — improve headers, explain abbreviations.

These are all from R3 and all persist. If Option B (worked example) is chosen for the fatal, it will naturally address C-001, C-002, and C-006.

### Conciseness (19 Major, ~620 words cuttable)

The paper grew by 1200 words between R3 and R4. The conciseness findings increased from 41 to 44. The Discussion grew from 222 to 733 words.

**Action:** Cut the Discussion back. It should interpret results, not restate them. The redundancy finding (RED-014) flags Discussion 9.1 as echoing Results 6.1 verbatim. Target: bring total word count back under 5500.

### Related Work Placement (NAR-005)

Related Work appears after the Construction, so readers encounter prior art only after the construction is complete. Moving it before the Construction would let readers understand the construction as a response to gaps.

**Action:** Consider moving Related Work to between Background and Construction. This was the recommendation in R2 as well.

---

## Simulator Parameters (ADV-B-001)

Persists from R3. Processing delay, jitter, timeouts, link delays per tier-pair — not in the paper.

**Action:** Add a parameter table. Five rows. Makes the paper self-contained for replication.

---

## DO NOT

- Do not add more Discussion content. It's already 3x longer than R3 and the redundancy findings increased.
- Do not add speculative generalization claims. The honesty reviewer is watching.
- Do not add content that doesn't fix a specific finding. The paper is long enough.

---

## Verification Checklist

- [ ] Index convention resolved (re-index OR worked example — not another explanatory sentence)
- [ ] Abstract/intro claims calibrated (structural vs empirical vs speculation)
- [ ] Flat construction formally defined
- [ ] Commit hash and dependency versions pinned
- [ ] Safety-tier-membership claim either proven or attributed to TLA+
- [ ] Crash-tolerance arithmetic acknowledged
- [ ] Section numbering fixed
- [ ] Discussion trimmed (target: ≤300 words, currently 733)
- [ ] Total word count ≤5500 (currently 6259)
- [ ] Parameter table added
- [ ] LaTeX compiles clean

---

## What the Reviewers Liked (Preserve These)

- "Largely epistemically honest and commendably self-aware"
- "Crumbling-wall quorum geometry produces legible, tier-scoped liveness degradation" — citable contribution
- "The core idea — map quorum geometry to physical topology so failure modes are readable from the structure — is compelling"
- "I could explain it to labmates at a high level"
- Traceability table is "exemplary practice"
- The 0% vs 100% comparison is "powerful" (even though tautological — that's the point)
- Limitations section is substantive
