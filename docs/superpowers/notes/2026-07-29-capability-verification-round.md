# Capability-state verification round (2026-07-29)

A session proposed that the capability state `(1,0)` — a proposer able to
complete Phase 1 and structurally unable to complete Phase 2 — was an
unnamed failure class, and derived a boundary condition for when it is
reachable. This note records what a 17-agent verification round did to
those claims. Two of the three claims submitted to adversarial refutation
died. That is the point of writing it down.

Method: 7 independent verification tasks; the 3 carrying embarrassment
risk (boundary proof, literature novelty, deployed systems) each faced 3
refuters on separate lenses (correctness / sources / overreach), with a
claim surviving only if fewer than 2 of 3 refuted it. Vote tally:
proof 0/3 refuted (survives), literature 3/3 (dies), deployed 3/3 (dies).
The other four tasks are recorded at the strength of a single unopposed
agent, which is weaker evidence — "no refuters" is not "survived
refutation."

## DIED: the novelty claim

**Flexible Paxos §4.3 already describes `(1,0)`.** Twice — the
$|Q_1|{=}1 / |Q_2|{=}N$ thought experiment, and the row-failure passage
("FPaxos would be able to complete Q1 and thus recover all past
decisions, it can then safely fall back to a reconfiguration protocol").
The literature agent quoted lines 375–377 of its own source text and
stopped before 392–399 — the failure-mode paragraph of the same
subsection. Two refuters found this independently.

It is in the paper this work positions against. That is where we should
have looked first.

Also dead, from the same round:

- **Coterie domination (Garcia-Molina & Barbara 1985) is not
  capability-completeness.** Over 108 configurations of this repo's own
  quorum system, 51 are capability-complete but not GMB-dominated (48
  because $Q_2$ is not a coterie, 3 violating GMB's $R \neq S$ clause,
  which also excludes classic Paxos). At $k{=}3$ on the deployed 5/1/1/3
  topology capability-completeness holds and GMB is false. A good lead,
  correctly chased, that does not hold.
- **Guerraoui & Vukolić, Refined Quorum Systems (PODC'07)** already uses
  cross-family containment as a quorum-design criterion
  ($QC_1 \subseteq QC_2 \subseteq RQS$).

**What survives is the characterization, not the phenomenon.** "We name a
new failure class" is gone. "We give it an exact reachability criterion"
stands.

## DIED: the deployed-systems claim

The claim that (1,0) appears wherever a system pairs a *numeric* election
predicate with a *structural* commit predicate, with MongoDB as the clean
witness. Killed on method: the census switched layers mid-count, treating
MongoDB's custom tag write concern as a commit predicate while dismissing
Cassandra's commit-CL knob as post-decision replication — the same layer.
Applied uniformly, the headline count holds under neither standard.

Corrections that came out of the refutation and are independently sourced:

- **Cassandra LWT is the stronger witness, not MongoDB.**
  `StorageProxy.java:300-301` (cassandra-4.1): `consistencyForPaxos` can
  only be SERIAL or LOCAL_SERIAL; `consistencyForCommit` can be anything
  except those. `ConsistencyLevel.java:228-239` explicitly whitelists
  EACH_QUORUM for CAS commit. Two independent knobs, structural predicate
  legal on the second, and the ballot is genuinely consumed.
- **Kafka is a counterexample to the structural/numeric split being
  necessary.** Election is ISR membership (cost 1), commit is
  `|ISR| >= min.insync.replicas` — both numeric. KIP-36 rack awareness is
  placement-only and enters neither predicate. Any practitioner-facing
  heuristic phrased as "numeric election, structural commit" has a known
  false positive here.

A parallel review by a different model family (Claude desktop) reached the
opposite conclusion on both Cassandra and MongoDB from documentation
prose. That divergence is unresolved; the source-level citations above
are the stronger evidence but have not been reconciled with it.

## SURVIVED

- **Boundary theorem, sufficiency.** If $|E|-k+1 \geq k$ (i.e.
  $k \leq \lceil |E|/2 \rceil$) then `(1,0)` is unreachable.
  Unconditional — no dependence on tier count, tier sizes, initiator
  tier, disjointness, or nonemptiness. Three refuters failed to dent it.
- **Necessity, with a hypothesis nobody had stated.** The converse needs
  non-degeneracy: `CrumblingWallQuorum([[0],[],[4],E])` is accepted by the
  shipped constructor and breaks it.
- **The containment lemma.** `(1,0)` is unreachable iff every $Q_1$
  contains some $Q_2$. This is the general statement; the arithmetic
  boundary is its instantiation for this construction. Independently
  derived by the parallel desktop review — cross-family convergence.
- **Model checking.** `tla/CapabilityReachability.tla`: 20,480 distinct
  states, exhaustive; `NoHazard` holds at $k=1,2,3$ (4,096 states each),
  violated at $k=4,5$ with counterexamples that decode back through
  `quorums.py`. Five mutation negative controls fail as required, so the
  agreement is not vacuous. Re-run and confirmed by the controlling
  instance, not accepted on the agent's report.
- **The Figure 4 decomposition.** At $k{=}5$, $|Q_1| = 992/496/248/217$
  decomposes into $(1,1) = 32/16/8/7$ and $(1,0) = 960/480/240/210$. The
  hazard fraction is exactly $30/31$, identical across all four tiers
  because the tier factor cancels. The 4.571 Earth:Mars ratio is
  $M_E/M_M = 32/7$ and is $k$-invariant.
- **No TLA+/Python disagreement.** A null result, recorded as one.

## Defects found in existing material

- **`main.tex:184` (shipped, arXiv).** Attributes 11,789 states to
  `ExhaustiveIntersection.tla`. Wrong spec and stale number: 11,789 is
  `QuorumIntersection.tla`'s pre-Relaxed3 count
  ($1953 + 1638 \cdot 6 + 8$); the current spec reports 27,921.
  `docs/crumbling-wall-status.md:52` confirms the provenance.
- **`quorums.py` docstring drift**, two items fixed in this commit: the
  Earth-proposer Phase 1 size, and an "Intersection guarantee" paragraph
  that argued from "every Q1 contains one Earth node" — which does not
  imply intersection for any $k < |E|$. The code was correct for a
  different reason. Three further contradictions remain unfixed and are
  listed in the ledger.
- **Briefing error by the controlling instance.** Three agents were told
  the `(0,1)` state had never been checked. `capability.py` already
  implements the four-state model and names `(0,1)` as
  `Hazard.INCUMBENT_ONLY`; `tests/test_capability_exhaustive.py` already
  evaluates all four. The *census* is new; "never checked" was false.

## Highest remaining risk

**Any sentence connecting `(1,0)` to an actual cost.** Nothing was run.
Every artifact in this round is combinatorial — sets, predicates, counts.
The TLA+ model has no ballots, messages, or acceptor state.

Worse, the mechanism does not require the state: `paxos.py
_handle_prepare` raises `_highest_promised` on any higher prepare,
quorum or not. So disruption does not require being in `(1,0)`, which
undercuts the state's claimed operational significance.

Until a mid-round-flip experiment runs in `duel.py` (assessed at ~40–60
additive lines, with timing constraints recorded in the full ledger), the
honest word is **state**, not **hazard**.

## Artifacts

- `tla/CapabilityReachability.tla` + per-$k$ configs
- `tla/CapabilityBoundary.tla` + configs (boundary for $|E|=2..9$, flat
  companion, cross-family containment)
- `docs/boundary-theorem.tex` — the proof, LaTeX drop-in
- Full ledger and per-agent journal:
  `.../subagents/workflows/wf_c70b2e61-56c/journal.jsonl`
