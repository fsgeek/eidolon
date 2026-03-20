# Eidolon: Marching Orders Round 5 (Rikuy Review 2026-03-20)

**Review file:** `/home/tony/projects/rikuy/reviews/eidolon/review_20260320_122514.jsonl`
**Review stats:** 277 findings (1 Fatal, 85 Major, 191 Minor)
**Paper:** `/home/tony/projects/eidolon/docs/paper/main.pdf` (6943 words, 13 sections)
**Prior rounds:** R4: 268 (1F, 67M), R3: 249 (1F, 69M)

**Context:** The R4 fatal (tier indexing) is **resolved** — the re-indexing to Earth=0, Mars=3 worked. A new fatal emerged around TLA+ verification explanation. Major count rose from 67→85, driven primarily by conciseness doubling (19→38) and new adversarial findings against the expanded Discussion. Copy editor count stable at 32 but dominated by PDF extraction noise (diacritics, page numbers mid-sentence, stray hyphens). Word count is 6943, up from 6259 — the paper is still growing.

**Perplexity adversarial novelty check:** Tony asked Perplexity to find evidence this is a relabeled rehash of edge/cloud hierarchical consensus (Nomad, Saguaro, EdgePQR, WPaxos). Result: "partly supported" — the hierarchy pattern is known, but the specific combination of single-protocol legible degradation + crumbling-wall geometry mapped to physical topology was not falsified. The novelty burden is positioning, not existence.

**New insight this session:** Load-bearing vs non-load-bearing tiers have asymmetric transparency costs. The load-bearing tier (Earth) can hide replication behind a single logical Paxos node cheaply. Non-load-bearing tiers pay more nodes to hide replication than to expose it. This is a design principle for building tiered systems, and existing edge systems make this decision ad hoc.

---

## What's Resolved Across All Rounds

- Novelty framing — solid across five rounds.
- Contribution clarity — buildability reviewer consistently finds this citable.
- Epistemic honesty — praised in R3, R4, R5.
- Traceability table — exemplary.
- **Tier indexing (R4 FATAL)** — resolved. Earth=0, Mars=3 accepted without complaint.
- Commit hash / Zenodo DOI — pinned as of latest commit (7ea9606).
- Section numbering bug — no longer flagged.

---

## FATAL: TLA+ Verification Explanation (ADV-C-002) — NEW

The paper gives a one-sentence definition of TLA+, then makes claims requiring understanding of what exhaustive model checking means in practice: "11,789 states," "67M states," "6.5 billion states," "reduced topology," "structurally equivalent." A reader who has heard of TLA+ but never used it cannot evaluate whether the safety proof is complete or relies on the reduction.

### Assessment

This is legitimate. The paper currently says both "exhaustively explores all reachable states" and "full state-space exploration at this scale is computationally infeasible." That sounds contradictory without explanation.

### Fix (3-5 sentences)

Add to Section 3.4, after the TLA+ introduction:

> TLC checks safety by exhaustively exploring every reachable state — every possible interleaving of messages and failures — and verifying that the safety invariant holds in each one. For the reduced 6-node topology (which preserves all tier-membership relationships and thus all quorum intersection properties), this exploration is complete: 11,789 states for quorum intersection, 67M states for the full Paxos protocol. For the full 10-node topology, the state space exceeds what TLC can exhaust (6.5B states explored in 24 hours with the frontier still expanding), so we rely on the complete reduced-topology result plus the monotonicity argument: adding nodes to a tier cannot break quorum intersection.

One paragraph. Resolves the fatal and also addresses R4's ADV-B-002 (reduction informality) and R4's ADV-C-004 (TLA+ context) in one shot.

---

## FINDINGS THAT NEED ATTENTION (~30 minutes of editing)

### 1. Define "Legibility" (ADV-C-010) — NEW

The central property is never formally defined. The reviewer is right that this is a gap, even if the fix is one sentence.

**Action:** Add early in the paper (Section 1 or 2):

> We call a quorum construction *legible* if an operator can determine which tiers retain global consensus capability by inspecting the wall structure and current connectivity state, without runtime probing or failure detection.

### 2. Contribution 2 Framing: 0%/100% (ADV-C-006, ADV-A-001)

The 0% result under flat quorums during blackout is simultaneously called a "contribution" (Introduction, Contribution 2) and a "tautology" / "calibration" (Results). Removing it invites "did you even test the baseline?" Keeping it as a contribution invites "this is obvious."

**Action:** Reframe Contribution 2 from "we measured 0% vs 100%" to "the flat construction's 0% result is a structural guarantee, not an empirical finding — our simulator confirms this, serving as calibration that the model faithfully reproduces the quorum geometry's implications." Downgrade from "contribution" to "baseline validation" in the Introduction list.

### 3. 500ms Timeout Rationale (ADV-B-003)

The reviewer wants to know if 500ms was principled or convenient. The suggestion is reasonable.

**Action:** Add one sentence to the parameter table or Section 5: "The 500ms per-phase timeout is chosen to accommodate Earth-local round-trip consensus (~10ms) with margin, while remaining short enough relative to the reconciliation interval (120s) that blackout-induced failures are detected within one cycle. Results are qualitatively stable across 200ms–2s; the blackout effect is dominated by light-travel time (6–44 minutes), not timeout choice."

### 4. "Magnifying Glass" Verbatim Echo (RED-007)

Introduction and Discussion both say "The interplanetary setting is not the point; it is the magnifying glass" nearly verbatim.

**Action:** In the Discussion, replace with a back-reference: "As noted in the Introduction, the interplanetary topology serves as a magnifying glass; the structural properties apply wherever..." Saves ~3 lines, eliminates the echo.

### 5. Conciseness (38 Major findings, up from 19)

The paper grew from 6259→6943 words despite R4's "stop adding content" directive. The Discussion is the likely source.

**Action:** Target cuts in the Discussion and Conclusion, which restate results rather than interpreting them. The top 5 conciseness findings collectively identify ~300 words of saveable text. Realistic target: bring the paper under 6500 words. The R4 target of ≤5500 may be unrealistic for the current scope.

---

## FINDINGS THAT ARE PERSONA CEILINGS (Do Not Chase)

### Simulator Reimplementability (ADV-B-001) — PERSISTENT

The buildability reviewer wants the paper to contain enough detail to reimplement the simulator from text alone. The code is shared, the commit hash is pinned, and the traceability table maps claims to artifacts. Reimplementability from prose is not a standard requirement. **No action.**

### Reduction Theorem Formality (ADV-B-002) — PERSISTENT

The reduction from 10→6 nodes preserves quorum intersection by monotonicity — adding nodes to a tier cannot break intersection. This is obvious to anyone who works with quorums. Requiring a formal reduction theorem adds apparatus without adding insight. **The TLA+ paragraph above addresses this implicitly** by stating the monotonicity argument. No separate proof needed.

### Crumbling-Wall Definition (ADV-C-001) — PERSISTENT

This is a complaint about Peleg & Wool's construction, not this paper's. The paper applies their construction to a physical topology and cites them. The definition belongs in the cited reference. **No action** beyond ensuring the citation is prominent.

### Topology-Scoped Consistency (ADV-A-002) — NEW, BUT CONFUSED

The reviewer calls the Discussion's observation about per-tier consistency levels "speculation dressed as a theorem." But Paxos provides sequential consistency by definition. A single-tier system trivially provides sequential consistency. There is nothing to "prove" here — it follows from the protocol. The paper's observation is that the *wall structure makes this per-tier property visible*, which is a restatement of legibility, not a new claim. **No action** beyond ensuring the Discussion uses "observe" rather than "claim."

### Quorum Count ≠ Robustness (ADV-A-003)

The reviewer says 4.6x more Phase 1 quorums doesn't mean "more robust" without shared-critical-node analysis. This is technically fair but misses the point — the paper says "more valid quorums" and "faster," not "more fault-tolerant." Check that the paper's language doesn't overreach. **Minor wording audit only.**

### Anchor Concentration as Conjecture (ADV-A-004)

The claim that anchor concentration is "essential" to legibility is flagged as an unproven impossibility claim. Fair — soften "essential" to "sufficient" or "central to our construction." **One word change.**

---

## COPY EDITOR FINDINGS (172 total, 32 Major)

Dominated by PDF extraction artifacts. Known issue: Rikuy's PDF parser introduces phantom findings from mid-word hyphens, stray page numbers, and garbled diacritics. Prior session estimated 60-70% of copy-edit findings are noise.

### Real issues worth fixing:
- **File paths in traceability table** have spaces instead of underscores (EDIT-154–157) — would break if copied literally
- **"Python 3.14"** flagged as bogus — it's correct (confirmed: Python 3.14.0 installed). No action.
- **Diacritics** (Vukolić, Guidec, Mahéo) — check that LaTeX source has correct Unicode/escapes; the PDF rendering may be fine even if extraction garbles them
- **Run-on sentences** — a handful of genuine grammar fixes scattered across the paper

### Recommendation for future reviews:
Switch the Rikuy config back to LaTeX source format if the directory-ingestion bug has been fixed. This would eliminate the majority of phantom copy-edit findings.

---

## RELATED WORK POSITIONING (Informed by Perplexity Analysis)

The Perplexity adversarial search surfaced five relevant edge/cloud systems:
- **Nomad** (INFOCOM 2019): Edge/cloud split with consensus at both levels — multi-protocol, not single-protocol
- **Saguaro** (Stony Brook): Hierarchical edge/fog/cloud with per-domain consensus — same pattern as Nomad
- **EdgePQR** (2025): Extends quorum systems to edge-cloud with hierarchical structure — **closest prior work, needs reading**
- **WPaxos**: Already cited; grid quorums, not crumbling walls

**Action:** ~~Read EdgePQR.~~ **EdgePQR appears to be a Perplexity hallucination.** Web searches for "EdgePQR" return nothing. The ScienceDirect DOI Perplexity cited (S2542660525002379) does not resolve. The description ("extending quorum systems to edge-cloud environments with hierarchical quorum structures") reads like a plausible synthesis of the design space, not a citation of an actual paper. The other Perplexity citations (Nomad, Saguaro, WPaxos) are real and already in the related work. The prior-art region is less dense than the Perplexity analysis suggested — the closest real work is multi-protocol (Nomad, Saguaro), not single-protocol quorum geometry.

The paper's differentiation is three-fold:
1. **Single protocol** — one Paxos instance with quorum geometry that produces tiered behavior, not layered consensus protocols stitched together
2. **Transparency cost gradient** — load-bearing tiers hide replication cheaply; non-load-bearing tiers pay more to hide than to expose
3. **Scheduled disconnection as design parameter** — not failure recovery, but predictable topology changes the quorum geometry handles structurally

Consider whether the transparency cost observation belongs in the paper or is better held for a follow-up that targets edge/cloud venues specifically.

---

## NARRATIVE NOTE (NAR-002)

Related Work after Construction means readers encounter prior art only after absorbing the full technical content. This has been flagged in R2, R4, and R5. Moving Related Work to between Introduction and Background (or Background and Construction) would let readers understand the construction as a response to gaps in prior work.

**Assessment:** This is a stylistic choice. Systems papers often put Related Work late. But the reviewer has flagged it three times, and the Perplexity analysis suggests the differentiation story needs to be front-loaded given the dense prior-art region. **Consider moving it.**

---

## VERIFICATION CHECKLIST

- [ ] TLA+ "what is verified vs. assumed" paragraph added (FATAL fix)
- [ ] "Legibility" formally defined (one sentence)
- [ ] Contribution 2 reframed as baseline calibration
- [ ] 500ms timeout rationale added (one sentence)
- [ ] "Magnifying glass" echo eliminated
- [ ] Discussion trimmed for conciseness (~300 words cuttable)
- [ ] "Essential" → "sufficient" for anchor concentration (one word)
- [ ] Traceability table file paths: spaces → underscores
- [ ] Diacritics verified in LaTeX source
- [ ] EdgePQR paper read and differentiated (or cited)
- [ ] Word count target: ≤6500 (realistic for current scope)
- [ ] LaTeX compiles clean

---

## WHAT THE REVIEWERS LIKED (Preserve These)

Consistent across R3–R5:
- "Largely epistemically honest and commendably self-aware"
- Crumbling-wall contribution is well-framed and citable
- Core idea — map quorum geometry to physical topology — is compelling
- Traceability table is exemplary
- Limitations section is substantive
- The 0%/100% comparison is powerful as calibration

---

## DO NOT

- Do not add content that doesn't fix a specific finding
- Do not chase persona ceilings (reimplementability, reduction formality, Peleg & Wool definitions)
- Do not add speculative generalization claims — the honesty reviewer is watching
- Do not expand the Discussion — it needs trimming, not growth
- Do not re-derive prior art that belongs in cited references
