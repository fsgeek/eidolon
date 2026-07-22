# Pre-mortem: dueling-proposer hazard experiment

Date: 2026-07-22
Status: COMPLETE — all four critic reports in; awaiting Tony's review (this is the phase gate)
Subject: the experiment specified in `docs/superpowers/specs/2026-07-19-four-layer-capability-model-design.md` §"Dueling-proposer hazard experiment" (~line 164), pre-mortemed BEFORE implementation.

Method: four independent critic agents — harness validity, experimental design/statistics,
hostile workshop reviewer, and one open-remit critic (no assigned lens) — plus a collision
read of Li/Chan/Lesani, "Satrapy" (Distributed Computing 39:16, 2026), the paper the spec's
success criterion #14 requires related work to address. Raw reports live in the session
transcript; this document is the synthesis and the design contract.

## Verdict

The experiment is worth running, but as specified-by-default it would have failed in both
directions at once: two independent harness findings would each have **decided the outcome
before any offset was swept** (one toward false null, one toward baked-in "confirmation"),
and the framing as spec'd invites a fatal self-prior-art review. Every one of these is
fixable at design time. The constraints below are the price of admission.

## A. Design contract (implementation MUST satisfy all of these)

Ranked by severity. Sources: H=harness critic, S=stats critic, R=reviewer critic.
Convergent findings (found independently by two critics) marked ★.

### A1. Shared quorum object, or the lemma dissolves (H1) — false-null trap
The reusable Earth proposer (`demo_step_9.py:280`) runs `FlexibleQuorum(q1=4, q2=2)`, not
the wall. Under that config LEO's Phase 1 poisons only the 3 reachable Earth nodes and
Earth commits via the 2 untouched ones — no NACK, no hazard, config artifact.
**Constraint:** both proposers are built from the *same* `CrumblingWallQuorum` instance,
same `phase2_threshold`, Earth with `initiator_tier=3`; startup assertion of object
identity and threshold equality. Do not reuse `demo_step_9`'s `earth_prop`.

### A2. ★ Explicit ballot priority, both polarities (H2, S3) — pre-decided-winner trap
Ballots are `counter*1000 + entity.id` (`paxos.py:356-358`); registry IDs follow creation
order (`entity.py:42-52`). Whoever is created later wins every tie at every offset — the
"result" is an artifact of construction order, invariant across the sweep.
**Constraint:** ballot high-bits come from an explicit, documented proposer-priority field;
the full sweep runs under BOTH polarities (LEO-high and Earth-high) and reports them
separately; startup assertion of the intended ordering; assert `max(entity_id) < 1000`.

### A3. ★ RNG isolation (H3, S7) — the repair-campaign failure class, again
`network.py` draws jitter from the module-global `random` in packet-send order
(`network.py:77,90,172,186,191`); adding the LEO proposer perturbs Earth's jitter stream,
so baseline-vs-contended deltas can be pure RNG realignment — the delta report's §13
mechanism for the old 92% row.
**Constraint:** per-link (or per-stream) `random.Random` seeded from `(seed, endpoints)`,
independent of event order, so one seed = one reproducible noise field shared across
offsets and arms. Enables the paired design in A5.

### A4. Deterministic primary map; stochastic sweep separated (S1) — fake-CI trap
With jitter=0 the run is deterministic per offset: 50 seeds → 50 identical rows → ±0.0%
CI presented as infinite precision. Even jitter>0 doesn't vary duel structure under the
global RNG.
**Constraint:** the primary result is an explicitly deterministic offset→outcome map
(jitter=0), presented as a map, not as mean±CI. A separate stochastic sweep (jitter>0,
post-A3 RNG) is a labeled robustness check. Write-time assertion: no CI cell from
degenerate inputs; Wilson/Clopper-Pearson (not normal approx) for proportions (S9);
flag all-success/all-fail cells instead of printing ±0.0% (S9).

### A5. Inert-LEO paired baseline (S8, enabled by A3)
Deleting the LEO proposer changes the event schedule and RNG stream, confounding
"contention" with "different execution." **Constraint:** baseline = identical run with
LEO present but its messages suppressed; report within-offset paired differences.

### A6. ★ Retry budget is a declared experimental condition (H6, S2)
`max_rounds=1` (sweep default) makes retries/NACKs/livelock trivially zero → "no hazard"
as a round-cap artifact; very large budgets let Earth out-escalate everywhere → ~100%
everywhere. The offset-dependence lives between.
**Constraint:** run single-shot (`max_rounds=1`) and bounded-retry as separate, labeled
conditions; the retry cap is reported on every result row; sensitivity to the cap is
reported (also bounds censoring, A8).

### A7. Same slot, verified regime, validated clocks (H5, H8, H9, S12)
- Contention exists only on a shared slot: assert `earth_slot == leo_slot` per trial,
  fresh slot per trial (demo pattern uses disjoint ranges — copying it makes the duel
  vacuous).
- LEO's (1,0) state is an assumption until verified: pre-trial, run
  `capability.classify()` with LEO's reachable set **derived from actual network links**
  and assert `r1 and not r2 and DISRUPTIVE_ELECTION in hazards`. (The classifier artifact
  gates the experiment that corroborates it — dogfooding as validation.)
- Both proposers' timeouts validated via `validate_time_budget` against their own worst
  route, and required to be commensurate (within a small factor); timeout-limited runs
  are classified `temporally-unavailable`, never "no hazard."

### A8. Outcome taxonomy and metric denominators fixed in advance (S6, S10, H7, H10)
- Explicit livelock criterion: N consecutive mutual-preemption cycles without commit —
  distinct from horizon exhaustion. Competing-risks reporting: committed / livelocked /
  censored-by-horizon, with counts.
  *(Ruling 2026-07-22, plan-conflict adjudication: implemented as TOTAL preempted-round
  counts — not consecutive — with mutuality required: Earth ≥ N rounds AND LEO ≥ 1 round
  preempted. At Earth retry budgets ≤ 5, total and consecutive coincide in practice.
  One-sided starvation is classified `leo_blocked`, per §B3's asymmetric-spoiler framing —
  classic mutual livelock may be structurally rare in the (1,0) regime, and if so that is
  a reportable property, not a harness gap.)*
- Latency computed over committed attempts only; distributions, not just means.
- Primary success = committed-slots / offered-slots with the offered-slot set fixed
  exogenously; attempts/retries/NACKs as separate rates with explicit denominators.
- NACK counters split by phase and responder tier; drain and classify late responses
  (`_collect_responses` currently discards post-quorum arrivals — the preemption NACK
  evidence the spec asks for would be systematically undercounted).
- Record winning ballot + originating proposer per chosen slot (value carry-over means
  a slot can carry Earth's value through a LEO round); Learner consistency assert stays
  live and trips fail the run.

### A9. Offset sweep geometry (S4, S5, H4)
- Step size in units of round_time (~0.18 s Earth-scale), a fraction of one round; the
  collision window is ~1 round, i.e. ~1000× smaller than the 120 s cadence — a coarse
  uniform grid over 120 s samples almost no actual contention.
- Two-stage grid: coarse scan to locate transition bands, fine scan at RTT/backoff
  resolution around each boundary; report grid spacing with boundary claims.
- Sweep 2–3 reconciliation periods, not one, to expose aliasing/drift vs. true
  periodicity.
- First-class diagnostic: fraction of trials where the two proposers' rounds actually
  overlapped in sim time — makes a null distinguishable from under-sampling.
- Pre-register the full grid; report the ENTIRE success-vs-offset curve; characterize
  the escape set structurally and validate the prediction on a held-out finer grid (S11
  — no "offsets where it looks worst" selection).

### A10. Determinism hygiene (S13, H11)
No outcome may depend on same-timestamp event insertion order: offsets strictly nonzero,
deterministic tiebreak on simultaneous events, both-polarity runs (A2) as the audit.

## B. Framing contract (the paper text MUST hold these lines)

### B1. The delta over our own arXiv paper is stated in one sentence (R1 — the fatal one)
The capability characterization is prior art we own (arXiv 2603.28788). The genuine
novelty of the uncertain-connectivity work lives in the separate epistemic paper and must
NOT be imported as implied backing. The NINeS increment is the operator-facing classifier
artifact (hazard flags + evidence provenance); the experiment corroborates a named,
admitted-standard lemma **in service of that artifact**. Say exactly that, once.

### B2. Measure the cost surface, not the lemma (R2 — the tautology defusal)
The lemma guarantees disruption is *reachable*; the experiment measures its *cost as a
function of relative schedule offset* (degradation magnitude, collision-vs-escape
structure, livelock bands and exits). Never phrase results as "we confirm LEO disrupts."
The experiment must be capable of surprising us; the cost surface is where it can.

### B3. The asymmetric-spoiler positioning (R5)
Classic dueling-proposers is symmetric — either can win; "let one win" is the escape.
Topology-induced (1,0) is asymmetric: LEO can never commit, the standard escape does not
exist, and the spoiler's reach is derivable from topology. That asymmetry is the
capability map's property (not a new dynamics theorem) and is the one legitimate
differentiator from 1998 folklore. Single-decree is presented as the conservative
worst-case exhibit (R3); leases/epochs narrow the per-slot dynamics but not the
reachability question the map answers; the (0,1) state stays declared analysis.

### B4. Cadence-quantized quantities are labeled as such (R4)
Any offset/recovery quantity aligned to the 120 s reconcile interval is labeled a
cadence-quantized bound before a reviewer does it for us — same discipline the delta
report applied to recovery lag (and same standing directive from the data acceptance).

### B5. Pre-existing prose landmines fixed BEFORE the hazard section lands (R7)
- `main.tex` lines ~55, 272, 525: "Phase 1 can learn the global history" contradicts the
  four-layer spec's learner discipline (observation is a separate capability; one
  acceptor notification proves acceptance, not chosenness). Reconcile first.
- Abstract line ~35: Multi-Paxos "leadership cost gradient" claim must be explicitly
  analytical/structural, disjoint from what the single-decree experiment measures.
- Line ~525 "epistemic before electoral": back it with the decision-certificate learner
  path or cut it.

### B6. Optional extension that would strengthen coherence (R6)
Run the same LEO offset sweep at k=3 (relaxed Q2): the same experiment then shows LEO
converting from futile spoiler to functioning failover — demonstrating the relaxation-
reachability knob (the near-novel part) instead of only the standard part. If not run,
the two-claims/two-evidence split (lemma→experiment, relaxation→classifier table) must be
stated as deliberate division of labor. **Recommend running it: it is one more sweep
under the same harness and directly answers the coherence attack.**

## C. Satrapy (related-work input, from the collision read)

Complement, not collision: Byzantine + heterogeneous per-process trust vs. our crash-fault
+ single global topology-driven construction; in their terms we are homogeneous and
intersection remains sufficient. Cite it as the canonical recent HQS result. Three uses:
(1) their blocking-set lemma is citable precedent for our n−k+1 hitting-set argument —
frame ours as the crash-fault, topology-indexed analogue; (2) their related-work taxonomy
positions the whole heterogeneous-trust lineage in one stroke — we spend global knowledge
on physical topology, not subjective trust, an axis none of that lineage addresses;
(3) their "termination for a set P" scoping is standard vocabulary for our
"liveness failure scoped to the unreachable tier." One terminology landmine: their
"quorum subsumption" must not leak into our wall prose ("read down the wall" is a
different property); disambiguate or avoid.

## D. Open-remit critic: portfolio and process risk (O)

The unprimed critic found the layer the assigned lenses structurally miss: every primed
critic evaluated the experiment *in isolation*; the open critic evaluated it as a move in
the project's current position. Not a null result.

### D1. The experiment is the thin end of the restructure; that's where the risk lives (O-F1) [HIGH]
A submittable NINeS draft already exists (essentially arXiv + one paragraph) and was
judged registration-worthy. Full paper due 2026-08-06 AoE (~15 days; 07-30 registration
is abstract-only). The experiment's real consequence is committing scarce runway to
restructuring a working paper, foreclosing the safe fallback.
**Constraint:** the restructure gets an explicit go/no-go with a hard
fallback-to-current-draft date (proposed: 2026-08-02); the experiment is time-boxed to a
small fraction of runway. Pre-commit the escape from "ran the experiment, no time to
restructure safely."

### D2. Integrity regression risk: this recreates today's closed failure pattern (O-F3) [HIGH]
The repair campaign closed the same day this experiment was queued. Its precipitating
pattern: a compelling headline result added under pressure without the project's standard
adversarial scrutiny. **Constraint:** pre-register the lemma-predicted outcome before
running (deviation = harness-bug signal first, discovery second); at least one
adversarial cross-model review round before the result enters the paper. The deadline is
not a waiver — this pre-mortem is round one, not the whole standard.

### D3. Marginal evidentiary value must be earned on the cost surface (O-F2)
The hazard state already has two witnesses: the lemma (analytical) and the shipped
classifier row (sparse_leo_k5 emits R1=1,R2=0 + DISRUPTIVE_ELECTION). A dynamic
re-confirmation is a third witness to an undisputed fact. Converges with B2 from the
opposite direction: the experiment is justified *only* by the temporal contention-cost
result neither static witness can produce. If the honest deliverable is "re-proves the
lemma," cut the experiment and cite lemma + classifier row.

### D4. The likely deliverable is structurally undramatic, because the determining knob is forbidden (O-F6)
LEO can never commit, so the offset-response is plausibly dominated by retry/ballot
policy — exactly the backoff study the spec bans. The offset curve may be degenerate or
monotone, and the one dramatic regime (livelock) is the one we may not explain.
**Constraint:** decide in advance what a publishable curve looks like; if the compelling
regime needs the forbidden knob, narrow the claim to "contention imposes bounded retry
cost of magnitude X" and downgrade the figure to a table row. (Interacts with A6: the
two labeled retry conditions are also the hedge against a degenerate curve.)

### D5. Zenodo freeze coupling and fresh de-anonymization surface (O-F4)
Fix the artifact freeze point relative to the experiment BEFORE running it. If the
experiment is in the DOI'd bundle, its files and outputs clear the same
de-identification checklist (absolute paths, author strings, machine-specific seeds);
if it cannot clear in time, it stays out of the bundle — an un-reproducible appendix
row in a paper selling traceability is the worst outcome.

### D6. Prohibition erosion across the write-up handoff (O-F5)
The spec's three bans (no FLP labeling, no Multi-Paxos authority claims, no backoff
study) exist because the result's narrative gravity pulls toward all three. Write-up may
happen in a later session under deadline. **Constraint:** lift the bans verbatim into
the result-section skeleton and figure caption at creation time, with a one-line
write-time check (does any sentence use FLP / leader / epoch / backoff?).

### D7. AI-disclosure accuracy (O-F7)
The planned disclosure asserts adversarial multi-model review as standing practice. If
this largely-AI-designed experiment skips those rounds under the clock, the disclosure —
an integrity claim in a paper about legible provenance — silently overstates the rigor
applied. Either apply the same rounds (D2 does this) or scope the disclosure to note
where the process was abbreviated.

### Open critic's single-action recommendation
If only one thing is acted on: D1 (explicit restructure go/no-go + fallback date) and
D2 (adversarial review gate on the result).

## E. What the critics converged on independently

- Ballot tiebreak decides the duel (H2 ∥ S3) — highest-confidence finding.
- Global RNG confounds baseline vs. contended (H3 ∥ S7) — and it is the documented
  repair-campaign mechanism.
- Retry budget pins the outcome at either extreme (H6 ∥ S2).

- The experiment earns its keep only on the offset-dependent cost surface (R2 ∥ O-F2,
  reviewer and open-remit critics, from opposite directions: tautology-defusal and ROI).

Convergence from independent lenses is the strongest evidence in this document.

## F. Decisions this document cannot make (for Tony)

1. **Restructure go/no-go structure (D1):** accept the 2026-08-02 fallback-to-current-
   draft date, or set a different one?
2. **Scope (B6 + D3):** run the k=3 relaxed-case extension (one more sweep, directly
   answers the coherence attack and gives the experiment a non-degenerate second act) —
   or minimal scope?
3. **Zenodo freeze point (D5):** experiment inside or outside the DOI'd artifact bundle?
4. **Adversarial review gate (D2):** confirm one cross-model review round on the result
   before it enters the paper — consistent with the disclosure's "standing practice"
   claim (D7).
