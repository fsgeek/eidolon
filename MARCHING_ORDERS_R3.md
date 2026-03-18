# Eidolon: Marching Orders Round 3 — Crumbling Walls Version (Rikuy Review 2026-03-17)

**Review file:** `/home/tony/projects/rikuy/reviews/eidolon/review_20260317_165951.jsonl`
**Review stats:** 249 findings (1 Fatal, 69 Major, 179 Minor)
**Reviewers:** arxiv_honesty, arxiv_accessibility, arxiv_buildability, redundancy, conciseness, copy_editor, narrative
**Paper:** `/home/tony/projects/eidolon/docs/paper/main.pdf` (5068 words, 13 sections)

**Context:** The paper has been substantially reframed around crumbling-wall quorum construction (Peleg and Wool) mapped to physical topology, replacing the prior Flexible Paxos framing. The flat-vs-crumbling comparison table is new. TLA+ state space exploration ran for 24+ hours. Prior round's fatal (topology-scoped consistency undefined) is resolved. The honesty reviewer now calls the paper "largely epistemically honest." The buildability reviewer says they would cite it for "crumbling-wall quorum geometry applied to tiered Flexible Paxos, with per-tier liveness legibility." The reframe worked.

---

## What's Resolved (Do Not Revisit)

- Novelty framing — no longer flagged. The contribution is the construction, not the theorem.
- Topology-scoped consistency — no longer fatal.
- Tautological results framing — the paper already says "a tautology of its own design." Reviewers acknowledge this.
- Section ordering — Background, Construction, Related Work, Experimental Design, Results flow is accepted.

---

## FATAL: Index Convention on Core Formula (ADV-C-003)

The formula `Q(i)_1 = { Q ∈ N | ∀j ≥ i : Q ∩ T_j ≠ ∅ }` is the most important equation in the paper, and the index convention is confusing. Tiers are indexed 0=Mars(top) to 3=Earth(bottom), so `j ≥ i` means "at or below tier i" — opposite of the natural reading where "greater" suggests "higher."

### Action

Before the formula, add one sentence: "We index tiers from top to bottom: i=0 is Mars (slowest, most remote) and i=3 is Earth (fastest, most connected). Thus j ≥ i selects tier j at or below tier i in the wall."

Consider also adding a small concrete example immediately after the formula: "For i=1 (Lagrange Uplink): Q(1)_1 requires intersection with T_1, T_2, and T_3 — every tier from Lagrange Uplink down to Earth."

---

## Honesty: 50-Seed CIs Over Deterministic Outcomes (ADV-A-002)

The reviewer flags that reporting 50-seed confidence intervals over outcomes that turn out to be 0% or 100% quantifies scheduling noise, not protocol uncertainty.

### Response

We didn't know the outcome was deterministic when we designed the experiment. The zero variance across 50 seeds is itself a finding — it confirms the result is structurally determined rather than stochastically influenced by concurrent event scheduling. This was not obvious a priori.

### Action

Add a sentence: "The zero variance across 50 seeds confirms that the outcome is structurally determined by quorum geometry and link state, not influenced by message scheduling or concurrency artifacts — a fact that was not obvious before running the sweep." This turns the criticism into a result.

---

## Honesty: Flat-vs-Wall 0%/100% Is Tautological (ADV-A-001)

The paper already says this ("a tautology of its own design"). The reviewer wants it framed even more explicitly.

### Action

The current text is honest. Optionally strengthen: "We present this comparison not as an empirical finding but as a calibration: it isolates quorum geometry as the sole variable and confirms that the simulator correctly implements both constructions." One sentence, no structural change needed.

---

## Honesty: Partial TLA+ Model Check (ADV-A-003)

The reviewer notes that the partial model check (6.5B states, terminated early) is presented alongside exhaustive results without distinguishing their epistemic statuses.

### Response

The partial check ran for 24 hours. Full state space exploration at the 10-node topology would generate terabytes of data and require more than a week of compute. The exhaustive check over the structurally equivalent reduced topology provides the safety proof. The partial full-scale check provides additional confidence.

### Action

Distinguish the two explicitly: "Safety is verified exhaustively over the reduced topology (Section X). As additional confidence, we explored 6.5 billion states of the full 10-node topology over 24 hours of continuous model checking with zero safety violations. Full state space exploration at this scale is computationally infeasible; the reduced-topology proof is the primary safety argument." Two sentences, clear epistemic status.

---

## Honesty: "Counterintuitive" Crash Resilience (ADV-A-004)

The reviewer says the global quorum's better crash resilience vs flat local quorum is a straightforward consequence of having 10 nodes vs 5, not a novel discovery.

### Action

Check whether this is actually just an arithmetic consequence. If so, reframe: "The global quorum tolerates more crashes than the Earth-local quorum because it draws from a larger node set (10 vs 5), not because of any deep structural property." If there's more to it than set size, say what that is.

---

## Accessibility: Crumbling Wall Needs Earlier Definition (ADV-C-001)

"Crumbling-wall quorum construction" appears 6 times in the introduction before being defined. The Background section (Section 2) defines it, but readers without Peleg-Wool background are lost by then.

### Action

In the introduction, on first use, add a parenthetical: "crumbling-wall quorum construction (a quorum system where nodes are arranged in rows of varying width; a quorum reads from successive rows, and the wall 'crumbles' as rows lose members to failure [Peleg and Wool 1995])." One sentence. The full definition stays in Section 2.

---

## Accessibility: q1+q2>|N| Is the Uniform Special Case (ADV-C-002)

The paper presents the Flexible Paxos formula without flagging that it's the uniform-quorum case, then immediately builds non-uniform families, which confuses readers.

### Action

When introducing q1+q2>|N|, add: "This is the uniform special case where all quorums have the same size. The crumbling-wall construction generalizes this: per-tier quorum families of different sizes, where the cross-intersection property holds by construction rather than by arithmetic majority."

---

## Accessibility: TLA+ as Black Box (ADV-C-004)

TLA+ and model checking are mentioned prominently but never explained. The partial exploration (6.5B states) is presented without explaining what confidence it provides.

### Action

Add 2-3 sentences in the experimental design or wherever TLA+ first appears: "TLA+ is a formal specification language for concurrent systems; its model checker (TLC) exhaustively explores all reachable states to verify that safety invariants hold. For the reduced topology, exploration is complete — every reachable state was checked. For the full topology, TLC explored 6.5 billion states in 24 hours before being terminated; no safety violations were found, but the exploration is incomplete."

---

## Accessibility: Table 5 Hard to Read (ADV-C-008)

Column headers not self-explanatory, abbreviations defined only in caption, dash meaning unclear.

### Action

Review Table 5. Add a legend row or footnote. Make column headers self-contained. Replace dashes with "N/A" or "—(not applicable)" with explanation.

---

## Accessibility: Pigeonhole Formula (ADV-C-006)

The `|E|-k+1` formula for relaxed Phase 2 is presented without derivation.

### Action

Add one sentence of intuition: "If Phase 2 requires k-of-|E| Earth nodes, then any two Phase 2 quorums overlap in at least |E|-k+1 nodes (by pigeonhole). For k=4 out of 5 Earth nodes, this guarantees at least 3 nodes in common — sufficient for single-crash tolerance."

---

## Buildability: Missing Simulator Parameters (ADV-B-001)

Processing delay, jitter, timeouts, loss rates — not in the paper.

### Action

Add a parameter table (can go in experimental design or an appendix): link delays per tier-pair, processing delay, jitter distribution, timeout budget, loss model (none — deterministic link failure for blackout). Five rows, makes the paper self-contained for replication.

---

## Buildability: "Structurally Equivalent Reduced Topology" Undefined (ADV-B-002)

The reduced topology used for exhaustive TLA+ verification is never described — what nodes were collapsed, why the reduction preserves safety.

### Action

Add: "The reduced topology collapses each tier to a single representative node, yielding a 4-node system. Safety (quorum cross-intersection) depends only on tier membership, not on the number of nodes per tier, so the reduction preserves all safety properties. Liveness properties (crash tolerance, progress under partial failure) are topology-size-dependent and are evaluated only at full scale."

---

## Buildability: Relaxed Phase 1 Family Undefined (ADV-B-003)

The formal definition of the relaxed Phase 1 quorum family (for the crash tolerance extension) is scattered across prose, not given as a formula.

### Action

Add the formal definition alongside the relaxed Phase 2 definition, using the same notation as the original construction.

---

## Buildability: No Persistent Archive (ADV-B-008)

No Zenodo DOI, no commit hash, no version tag.

### Action

Add commit hash (same approach as ai-honesty — pin at submission time, redact for blind review if submitting to a venue). For arXiv, include the full URL + hash.

---

## Narrative: Table 1 Misplaced (NAR-003)

The experimental design table appears at the end of Related Work instead of the start of Experimental Design.

### Action

Move it. Straightforward.

---

## Conciseness (~620 words cuttable, ~12% reduction)

Main targets:
- Discussion restates Results conclusion verbatim (redundancy RED-014)
- 0%→100% transition paragraph in Discussion repeats what Results established
- Flexible Paxos explanation uses 3 sentences where 1 suffices
- "What Global Means" paragraph takes 4 sentences for 1 point
- Conclusion restates sparse-vs-full and crash-tolerance with unnecessary framing

### Action

One tightening pass. Discussion should interpret, not restate. Conclusion should draw implications, not re-summarize.

---

## Copy Editing (156 findings, mostly PDF extraction artifacts)

Same noise problem as ai-honesty — mid-word hyphens, stray page numbers. Skim for genuine issues:
- Check that all tier names are consistent (Mars/Lagrange Uplink/Lagrange Downlink/Earth vs T_0/T_1/T_2/T_3)
- Verify reference formatting
- Check Table 5 formatting

---

## Verification Checklist

- [ ] Index convention explained before core formula
- [ ] Concrete example after formula (optional but recommended)
- [ ] 50-seed zero-variance framed as a finding, not just a procedure
- [ ] Flat-vs-wall calibration framing strengthened (optional)
- [ ] TLA+ partial vs exhaustive clearly distinguished with epistemic status
- [ ] Crash resilience arithmetic acknowledged if trivial
- [ ] Crumbling wall defined parenthetically on first use in intro
- [ ] q1+q2>|N| flagged as uniform special case
- [ ] TLA+ briefly explained for non-specialist readers
- [ ] Table 5 readability improved
- [ ] Pigeonhole formula given one-sentence derivation
- [ ] Simulator parameter table added
- [ ] Reduced topology described and reduction justified
- [ ] Relaxed Phase 1 family formally defined
- [ ] Commit hash pinned
- [ ] Table 1 moved to Experimental Design
- [ ] Conciseness pass (~620 words target)
- [ ] LaTeX compiles clean

---

## What the Reviewers Liked (Preserve These)

- "Largely epistemically honest" — limitations section "substantive rather than pro-forma"
- Core contribution is clear and citable: "crumbling-wall quorum geometry applied to tiered Flexible Paxos"
- "The wall crumbles from the top" metaphor works
- Flat-vs-wall comparison table is powerful (even if tautological — that's the point)
- Traceability table is valued
- The interplanetary setting is "an effective magnifying glass"
- The paper is accessible to distributed systems readers who know Paxos
