# NINeS '27 Revision Briefing: From Topology Story to Coincidence Thesis

**For:** the constructor instance working in the paper repo
**Deadline:** August 6, 2026, AoE (HotCRP: nines27.hotcrp.com). Paper is registered; all fields editable until deadline. Current HotCRP entry feeds PC bidding, so the abstract update is urgent, not cosmetic.
**Status of this document:** synthesis of a day-long session (desktop Claude + constructor + Sol/OpenAI loop). Claims below are individually labeled with epistemic status. Nothing here overrides the repo's own ledger where they conflict — if this briefing and a registered result disagree, the result wins and the divergence should be reported.

---

## 1. What changed, and why

### 1.1 The problem with the submitted draft

The registered draft is a correct composition — Flexible Paxos × crumbling walls on a tiered topology — whose diagnosed defect is that it is *thesis-free*. Its strongest sentence ("the degradation is epistemic before it is electoral") is buried in Section 9. NINeS's CFP explicitly selects for "conceptual daring and intellectual novelty" over detailed evaluation, deprioritizes incremental refinement, and lists "debunk current practices" and "retrospectives that explain why past designs succeeded" among the paper types it exists for. The draft as submitted is a refinement-venue paper at an ideas venue.

### 1.2 What the session killed (and why the deaths matter)

These are settled; do not relitigate, but the paper's positioning depends on stating them correctly:

- **"We name a new failure class" is dead.** FPaxos §4.3 describes the (1,0) state twice, including the recovery use ("complete Q1 and thus recover all past decisions... fall back to a reconfiguration protocol"). Found by two refuters independently, in the paper we were positioning against. The *characterization* is unclaimed; the *phenomenon* is not.
- **The hazard reading of (1,0) is dead — falsified in reverse.** Registered experiment (verdict table in the ledger): a (1,0) incumbent is metric-for-metric indistinguishable from a healthy one, and its partial Phase 2 makes it a *value injector* — Howard's recovery reading, observed with mechanism. Do not use the word "hazard" for (1,0) anywhere.
- **The cost lives in the dual.** The unregistered observation: (0,1) — commit-capable, election-incapable — blocks liveness completely (50/50 seeds at budget 8). Mechanism: failure is cheap (fast NACK, backoff recycle), success is expensive (full Phase 2 timeout before retry). Only committing terminates the contention game; a (0,1) proposer can never commit, so it never stops, and its cheap retries aim at the shared anchor.
- **Coterie domination (GMB) ≠ capability-completeness.** Separated by enumeration: 51 of 108 configs capability-complete but not GMB. Related-work asset, not a foundation.
- **RQS (Guerraoui & Vukolić, PODC'07) already uses containment as a design criterion.** So "nobody applies containment" is unsayable; "nobody uses it to characterize joint capability states" survived checking.

### 1.3 The reorganizing question and the new thesis

The question that reorganized the material: **why are flexible quorums useful at all?** The textbook answer is frequency arbitrage — Phase 2 runs on every command, Phase 1 only at elections, so shift cost to the rare path. The arbitrage's hidden premise is that *rare* means *independent*, and elections are demanded exactly when connectivity has degraded: Phase 1 demand is adversarially correlated with Phase 1 price.

The deeper answer: **majority quorums were providing two guarantees, and the field only ever named one.**

1. **Intersection** — a witness to the *past*. Every electorate contains a node that remembers every committed decree. This is the guarantee the entire coterie literature refined, and the one Howard proved is the only *safety* requirement.
2. **Coincidence** — a witness to the *future*. When the two phase families are identical, "can elect" and "can commit" are the same predicate over connectivity states. This is why node-count monitoring was ever sufficient: one predicate, one number.

Under threshold quorums both guarantees fall out of the same arithmetic, so they were observationally identical and coincidence was never named. **Flexibility is, definitionally, the act of spending coincidence.** Every departure from phase symmetry opens exactly one of two capability classes, and the direction of asymmetry selects which:

- q1 ≥ q2 (election at least as hard): only (1,0) reachable — the **injector**. Benign: self-throttling (pays the Phase 2 timeout to learn anything), terminates by helping (partial Phase 2 + value adoption).
- q2 ≥ q1 (commit at least as hard): only (0,1) reachable — the **spoiler**. Harmful: fast-failing, cheap to retry, aimed at the anchor, structurally unable to end the game.

The recommended FPaxos configuration (small q2 for the hot path, so q1 > q2) admits **exclusively the spoiler class**. The field chose its asymmetry direction on a cost-per-execution argument that never priced valence-per-state.

**Important correction, already caught in review — do not overstate:** majority is the coincidence point of the *cardinality-parameterized family only*. Any self-intersecting coterie used for both phases gives coincidence for free. Coincidence requires **phase symmetry**, not majority. Majority is the sloppy (load-suboptimal, availability-optimal — cite Peleg–Wool) way to get both witnesses at once.

On tiered topologies with per-initiator families, phase symmetry becomes expensive: every tier the commit family spans, the hot path pays for on every command. The conjecture (pending enumeration — see §3) is that coincidence is affordable at generically one tier. The wall's real design content, in this light: **allocate the affordable unit of coincidence to the anchor, and compensate every other tier with legibility** — the O(tiers) capability readout. The thesis sentence:

> *When the two predicates can no longer be made identical, the engineering objective is to make them jointly readable.*

Legibility is not a bonus feature. It is what the construction pays the tiers that lost coincidence.

### 1.4 Why this fits NINeS specifically

- CFP: "debunk current practices" (the field's asymmetry direction admits the measured-harmful class), "retrospectives that explain why past designs succeeded" (why majority-era monitoring worked: coincidence), "conceptual daring... practical deployment neither required nor expected" (quote this sentence verbatim in the limitations section).
- **PC composition (checked):** Argyraki, Antichi, Panda, Bustamante, Foster, Meng, Mittal, Schapira. Eight networking people; **no quorum-systems/consensus theorist.** Consequences: (a) write for the smart networking generalist — accessibility is the binding constraint, not a nicety; (b) the interplanetary framing is an *asset* for this PC (topology, connectivity states, monitoring are their native ontology) — do not demote it; (c) formal results stay visible (Foster, Schapira will engage them) but every theorem gets a one-sentence operational gloss in the same breath; (d) carry arguments with operational imagery (the insurance company whose claims all arrive in the hurricane that flooded its reserves), not measure-theoretic vocabulary.
- Positioning lineage for this audience: a decade of NSDI-tradition work put the network in service of consensus; this paper runs the dependency the other way — consensus capability as a readable property of network state.

---

## 2. Claim inventory with epistemic status

Every claim in the revised paper must carry one of these statuses. The daring/manifesto boundary is decided entirely by labeling discipline.

**Proven and machine-checked (state at full strength):**
- Containment characterization: (1,0) empty iff every Q1 contains some Q2. Sufficiency unconditional; necessity requires the non-degeneracy hypothesis surfaced in refutation (the degenerate-wall constructor counterexample). TLC: 20,480 states exhaustive + 5 mutation negative controls.
- Tier-uniformity of the (1,0) boundary (anchor-content term is the only one containment depends on; Q2 ⊆ E makes witnesses invisible). Enumerated: all four tiers, k = 1..5, |E| = 2..9, zero mismatches. Note in paper: this is a consequence of single-anchor Phase 2 and breaks by design under multi-anchor (future-work tie-in).
- Figure-4 decomposition: hazard fraction 30/31 tier-invariant; 32/7 ratio k-invariant. Four independent routes.

**Derived, enumeration pending (write conditionally until census returns):**
- Dual characterization: (0,1) empty iff every Q2 contains some Q1 (mirrored star-topology argument).
- Uniform-case corollaries: (1,0) empty iff q1 ≥ q2; (0,1) empty iff q2 ≥ q1; both iff q1 = q2. Minutes of census work; these carry §1.
- (0,1) tier-gradient: Earth's dual class empty; upper tiers' witness obligations manufacture graded (0,1) exposure. Derived two rounds ago, never enumerated.
- **Scarcity lemma (conjecture):** phase symmetry affordable at generically one tier under per-initiator families. Pre-register the prediction before enumerating. **Fallback if it dies:** "phase symmetry off the anchor is prohibitively expensive on the hot path" — already supported by existing latency results. The thesis retreats from "scarce" to "spent," which still stands but sells for less. The bracketed abstract sentence and title candidate 2 depend on this lemma.

**Measured, n = 1 (state as measured; do not generalize):**
- The valence result: P1 falsified in reverse, P2 falsified, P3/P4 confirmed. Backoff-policy dependence is flagged in the ledger and premortem D6 bans backoff-policy studies. **One paragraph is permitted** on the policy-independent core: completing a phase imposes a timeout floor on retry that failing does not, and only Phase 2 completion terminates contention. Nothing further.

**Verified against sources, version pin owed:**
- Cassandra as the consensus-layer (0,1)/(1,0) witness: StorageProxy.java:300-301, ConsistencyLevel.java:228-239 (two independent CL knobs; EACH_QUORUM whitelisted for CAS commit; ballot genuinely consumed). **Owed:** pin the branch/tag those line numbers came from and check whether Paxos v2 (CEP-14, 4.1+) preserves the EACH_QUORUM commit path.
- Kafka: excluded, and *usefully* — min.insync.replicas is numeric, rack awareness is placement-only. Kafka is the heuristic's worked false positive (see §4, census section). Its dual pathology (numeric predicate, structural intent → silent durability exposure) gets at most one sentence.
- MongoDB: excluded at the consensus layer (tag write concern is an ack gate; replication majority-commits regardless). The exclusion demonstrates the definition's edges — say so in one sentence.

**Owed reads (gate items):**
- FPaxos §4.3 in full (fpaxos.txt ~lines 350–450): confirm it is silent on *detection* of the state and on the (0,1) direction. This bounds the completion-register wording. If Howard also sketched a recognizer, the recognizer claim shrinks — better to know now.
- Deployment-prevalence half-day: who actually runs q1 > q2 in production vs. research systems? Expected answer: production is overwhelmingly majority-majority (Raft), which *supports* the conservatism arc ("the field has been protected by its own conservatism; every latency economy pushes toward spending it") but kills any "every deployed system" phrasing. The conservatism paragraph ships only if this check lands.

---

## 3. Paper structure: old → new

Target: ≤ 12 pages two-column (refs/appendices free; papers explicitly not judged on length — do not pad).

- **Abstract:** replace with the bidding version (§5 below) immediately. Final version may add the scarcity-lemma sentence if the enumeration lands.
- **Title:** keep the *Legible Consensus* anchor (registered identity, still true). Candidates, pick in loop: (1) *…: Reading Election and Commit Capability from Network State* [PC-native; default]; (2) *…: What Majority Quorums Were Also Providing* [only if scarcity lemma lands]; (3) *…: The Unpriced Cost of Flexible Quorums* [only if prevalence check lands].
- **§1 (rewrite, ~1.5 pp; Tony drafts):** frequency arbitrage → correlated demand → two witnesses (past/testimony, future/capability) → coincidence as the unnamed second guarantee → flexibility as spending it → valence of the two directions → thesis sentence. Quote FPaxos §4.3 here, in the completion register: *the inference was sound everywhere it was tested; this is the first construction class where it fails — the class Howard et al. proposed exploring.* Howard is she/her.
- **§2 Background:** compress existing. Keep the TLA+ explainer register — it's the right voice for this PC everywhere.
- **§3 (new, ~2 pp):** the two predicates over connectivity states; four capability quadrants; containment theorem + non-degeneracy + dual + uniform corollaries + whatever the enumerations return. Quantifier-scope note: the star-topology converse makes the geometric characterization tight over unconstrained graphs, which licenses reading the 2^|N| enumeration as an upper bound for any fixed deployment. Realizable hazard set = geometry ∩ topology ∩ failure model (this is the current Contribution 3, now load-bearing).
- **§4 Construction (keep math, change vocabulary):** anchor = the tier allocated the affordable unit of coincidence; witness obligations = priced as graded (0,1) exposure; k = 3 = the self-dual point where both classes close (majority-of-anchor — the coincidence point resurfacing one level down).
- **§5 (new):** the valence experiment, from the verdict table. Registered predictions stated as registered, nulls as nulls, deviations as recorded. Howard's recovery reading confirmed with the value-injection mechanism visible. The permitted policy-independence paragraph. D6 fence stated.
- **§6 (retained, re-narrated):** per-tier liveness = the legibility readout demonstrated; **sparse LEO promoted to the (1,0) exhibit** (it was always the live instance — wall says works, network says 0%, every node healthy); flat/majority calibration kept; crash relaxation re-read as the k-ladder closing the classes.
- **§7 (new): census.** The practitioner heuristic ("is your election predicate numeric while your commit predicate is structural?") stated, then **its own worked misfire**: Kafka as false positive (configuration looks structural; predicate is numeric). Note explicitly that the false-negative direction was not found and remains open. Predicted-clean controls (etcd, ZooKeeper: majority-majority, both classes empty by the corollary) reported as predicted-clean-confirmed — the table is a test of the theorem's discriminating power, not a parade of horribles. Cassandra as the consensus-layer witness (post version-pin).
- **§8 Related work:** completion register for FPaxos (§4.3 quoted in §1, related work carries the full relationship); three-way separation, each by enumeration or definition: GMB domination (51/108), LCL subsumption (Definition 6: membership-indexed closure, Byzantine personalized-trust setting — different quantifier shape, different question), RQS containment (design criterion for correctness, not capability characterization). Three good names it isn't is how the fourth is earned.
- **§9 Limitations:** quote the CFP's evidence-bar sentence verbatim. One construction, one backoff policy, design-level, deterministic link removal. D6.
- **Cut/demote:** hazard calendar (stays fenced; at most one sentence as a corollary of geometry ∩ topology ∩ deterministic failure model); capability-gated proposing → **one sentence of future work** (a (0,1)-aware proposer that consults its own readout and self-suppresses out of the spoiler role — the successor paper's opening experiment; pre-register in the repo when there's room, not now); terrestrial-mapping paragraph → conservatism arc, conditional on the prevalence check; the word "hazard" globally purged except where (0,1) earned it.

---

## 4. Hygiene items (one commit, before the writing freeze)

- `capability.py`: rename `Hazard.DISRUPTIVE_ELECTION` — its valence is falsified on the record. `Hazard.INCUMBENT_ONLY` ((0,1)) is the one that earned the name. Comment pointing at the ledger entry.
- `main.tex:184`: attributes 11,789 states to ExhaustiveIntersection.tla — wrong spec, stale count. Current QuorumIntersection.tla reports 27,921. **This defect is in the arXiv version; queue an arXiv v2 regardless of what else ships.**
- `quorums.py:179–184`: the "Intersection guarantee" paragraph argues from "every Q1 contains ≥1 Earth node," which does not imply intersection for k < |E|. The code is correct for a different reason; the comment sits directly below the already-fixed docstring, so the file is internally inconsistent.
- Traceability table (Appendix A): regenerate against the final claim set. Every new claim gets an artifact row or doesn't ship.
- Repo state: an unclaimed writer modified `docs/paper/nines/main.pdf` during the parallel-agent session. Identify it (likely a latexmk watcher — confirm with a process, not a "probably") before staging anything beyond the quorums.py fix.

---

## 5. Bidding abstract (enter in HotCRP now; blind-safe; settled claims only)

> Distributed-systems monitoring answers one question — which nodes are healthy — but the questions that govern a replicated service are properties of network connectivity, not of nodes: can a leader be elected, and can a commit complete? For decades a configuration accident kept this gap harmless: under majority quorums the two questions have the same answer in every connectivity state, so counting healthy nodes sufficed. Flexible quorums, adopted to keep the commit path fast, quietly spend that coincidence. We show that every departure from phase-symmetric quorums opens exactly one of two capability gaps — states where a leader can be elected but cannot commit, or states where a commit-capable coalition cannot win election — and characterize both exactly: the first class is empty iff every election quorum contains a commit quorum, the second under the mirrored condition. In pre-registered simulation experiments the two states show opposite valences: the alarming-looking one is a self-throttling value injector, confirming a recovery reading sketched in the Flexible Paxos paper, while its unexamined dual — the only one the standard small-commit-quorum configuration admits — blocks liveness completely, with every node healthy and no dashboard changing color. For physically tiered topologies we present a wall-shaped quorum construction that allocates phase symmetry to the commit tier and compensates the others with legibility: an O(tiers) readout of each tier's capability state from a connectivity summary. A 10-node Earth/LEO/Moon/Mars topology makes the phenomenon physically undeniable; the same states exist wherever quorum geometry meets structured topology, masked only by generous timeouts. Quorum properties are verified exhaustively in TLA+; all results are design-level.

Deliberately absent: the scarcity lemma (pending), any deployment-prevalence claim, the word "hazard." HotCRP topics if checkboxes exist: fault tolerance/reliability, network monitoring/management, formal methods. Not satellite/space networking — wrong evaluation frame.

---

## 6. Eight-day queue

**Gates (Jul 29–30, parallel):** abstract into HotCRP · FPaxos §4.3 full read · three enumerations with pre-registered predictions (uniform corollaries; (0,1) tier-gradient; scarcity lemma) · prevalence half-day · Cassandra version pin. **Nothing in the writing spine that depends on the scarcity lemma starts until its enumeration returns.**

**Writing spine (Jul 31–Aug 3):** §1 (Tony) · new §3 · §4/§6 re-narration · §5 from the verdict table.

**Assembly (Aug 4):** §7 census · dual-dashboard figure (render one existing blackout run twice: node-health view, all green throughout, vs. capability view, quadrant transitions with prescribed responses — this figure is also the recorded talk's centerpiece; there are no live presentations) · hygiene commit · traceability regeneration.

**Audit (Aug 4–5):** adversarial pass routes cross-family to Sol (OpenAI) — constructor and desktop reviewer are same-family; that review counts as harness-independent only, and the session log should tag prior convergences accordingly · derivation audit and related-work three-way check on the desktop side · anonymization audit with the extended surface (no ayllu archive links, no personal domains, no googleable teaching-phrase patterns) · **LLM-use disclosure** for the HotCRP form: honest methods statement — multi-model loop, pre-registered predictions, three registered nulls, adversarial cross-family review, claim-to-artifact traceability. The venue is explicitly gathering this; write it with the same care as the paper.

**Submit Aug 5. Aug 6 is buffer, not schedule.**

---

## 7. Register notes

Completion, not correction, throughout: Howard proved intersection is the only safety requirement; this paper shows coincidence was the only valence protection, and spends a page saying why that was invisible (vacuous on the uniform family — the inference was sound everywhere it was ever tested). Every theorem gets its operational gloss in the same sentence. The three registered nulls are not damage to conceal; at this venue, stated plainly, they are the credibility mechanism that licenses stating the thesis at full strength. The paper's job is to be the first page of the program the coterie literature never started: they refined intersection for forty years; nobody refined containment, because cardinality's sloppiness kept it invisible.
