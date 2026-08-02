# Pre-registration: the dual characterization and the (0,1) tier-gradient

**Written before any enumeration code exists.** Gate order is checkable:
this document must be committed and stamped before the first line of
`experiments/` code that touches the dual or the gradient. Precedent:
`128e10f` (mid-round flip) and `1864401` (duel campaign), both committed
before their sweeps ran, both with registered predictions subsequently
falsified and kept as findings.

Predecessors: `2026-07-29-capability-verification-round.md`,
`2026-07-29-midround-flip-results.md`.

External input: `docs/nines27-revision-briefing.md`, a synthesis from a
Claude desktop session. **Non-binding and within-family.** Desktop and
this instance are the same model family; per the correction recorded in
the verification round, convergence between them is harness-independent
only and must not be cited as cross-model replication. Two of its
concrete repo claims were checked on receipt and one was false (it
located the `quorums.py` "Intersection guarantee" defect at the lines
holding the *repair*, and missed the live `GridQuorum` instance at
`:58`). It is treated here as a source of hypotheses, not of results.

## The question, and why it is the one that blocks

The boundary theorem characterizes `(1,0)` exactly: unreachable iff
`|E|-k+1 >= k`. It survived three refuters and 20,480 exhaustive states.

The flip round then measured `(1,0)` as **benign** — a self-throttling
value injector, Howard's recovery reading observed with mechanism — and
found the liveness cost living in `(0,1)`, which blocked the healthy
proposer in 50 of 50 seeds at retry budget 8.

So the project holds an exact characterization of the harmless class and
**no characterization of the harmful one.** Every claim about `(0,1)` in
this repository is either unregistered observation from a single arm or
hand derivation that has never been enumerated.

That is the blocking gap. A thesis sentence of the form "allocate
coincidence to the anchor and compensate the other tiers with
legibility" presupposes that the wall's own witness obligations do not
manufacture `(0,1)` at those tiers. That presupposition has never been
tested, and it is load-bearing for the paper's framing.

## Registered predictions

**P1 — dual characterization.** `(0,1)` is unreachable iff every `Q2`
contains some `Q1`. The mirror of the containment lemma, by the mirrored
star-topology argument.

*Falsified by* a single construction in the enumerated family where the
containment condition and the reachability of `(0,1)` disagree.

**P2 — uniform corollaries.** Over pure threshold families, where
capability depends only on the reachable count `r`: `(1,0)` is
reachable iff `q1 < q2`, `(0,1)` iff `q2 < q1`, and both are empty iff
`q1 = q2`. Derivation: `(1,0)` requires some `r` with `q1 <= r < q2`.

*Falsified by* any counterexample. This prediction is close to
arithmetic and a failure most likely indicates the enumerator disagrees
with the predicate implementation — which is itself a finding, and is
the reason it is registered rather than asserted.

**P3 — the tier-gradient (primary, and the one that can wound the
thesis).** On the deployed 5/1/1/3 wall, with a tier-`i` initiator
needing one witness from its own tier and each tier below, plus
`|E|-k+1` Earth:

(a) `(0,1)` is **reachable for every initiator tier having at least one
tier strictly between it and the anchor** — Moon and Mars — **at every
`k`**. Mechanism: a missing intermediate witness (the single LEO node,
or Moon) fails `Q1` while all five Earth nodes remain reachable, so
`Q2` is formable and `Q1` is not.

(b) `(0,1)` is **empty for Earth and LEO whenever `k >= |E|-k+1`**,
because `Q2` formable then implies the Earth floor is met, and each of
those initiators can serve its own tier obligation from its colocated
acceptor.

(c) `(0,1)` is **reachable for Earth when `k < |E|-k+1`** — at `|E|=5`,
`k <= 2` — because reaching exactly `k` Earth nodes forms `Q2` while
leaving the `|E|-k+1` floor unmet.

(d) Therefore **`k=3` does not close both classes.** It closes `(1,0)`
by the boundary theorem and closes `(0,1)` for Earth and LEO, but Moon
and Mars retain `(0,1)` exposure. The "self-dual point where both
classes close" is predicted to be true only of the anchor-only reading
and false for tier initiators carrying witness obligations.

*Falsified by* an enumeration showing `(0,1)` empty for Moon and Mars at
any `k`, or non-empty for Earth at `k >= 3`.

These are analytic sketches by hand and are registered as predictions
precisely because hand derivation is what failed in the novelty claim.
The enumeration adjudicates.

## Declared consequences, both directions

Registered in advance so that neither outcome can be narrated as the
good one after the fact.

**If P3 holds** (the wall manufactures `(0,1)` at non-anchor tiers at
every `k`): the "compensate with legibility" framing is *not* available
as written. The honest claim becomes that even a deliberately
topology-aware construction admits the measured-harmful class at every
tier but one, and the `O(tiers)` readout is the only thing standing
between an operator and an invisible stall. This is a sharper paper and
a worse construction, and both halves ship.

**If P3 is falsified** (the gradient is empty or confined to the anchor):
the briefing's framing survives contact, the wall genuinely does buy
coincidence at the anchor and pay only legibility elsewhere, and §1 can
be written as desktop proposed.

Either way there is a paper. This gate selects which true one, and is
not a viability test.

## Deliberately not registered here

**The scarcity lemma** — phase symmetry affordable at generically one
tier under per-initiator families — gets its own pre-registration.
Bundling a conjecture about a *class* of constructions with derivations
about *this* construction would let a favourable P1/P2/P3 result
launder an unfavourable scarcity result through a single "mostly
confirmed" verdict. Separate documents, separate declared losses.

## What would invalidate the enumeration rather than the predictions

- Any disagreement between the enumerator's capability predicate and
  `capability.py` / `quorums.py` as shipped. The enumeration must call
  the repository's own predicates, not a reimplementation.
- Non-degeneracy not enforced. The necessity direction of the
  containment lemma needs it; `CrumblingWallQuorum([[0],[],[4],E])` is
  accepted by the shipped constructor and breaks the converse.
- Determinism: byte-identical output across two runs at the same
  parameters, as in the duel campaign's Task 7 and flip A4.
- Any tier or `k` silently dropped from the sweep. Coverage is declared
  in the results note or the run does not count.

## Prose bans (inherited, extended)

- No FLP claims.
- No Multi-Paxos authority claims; single-decree is the conservative
  worst case.
- No backoff-policy study.
- "Physics" is reserved for light-travel effects, never ballot
  mechanics.
- No claim that `(1,0)` is novel. FPaxos §4.3 has it, twice.
- No claim that any deployed system "has a bug." Set-system
  reachability is not an operational defect claim. A *configuration*
  being reachable is sayable; a defect is not.
- **New:** the word `hazard` is used only of `(0,1)`, which earned it.
  `capability.py`'s `Hazard.DISRUPTIVE_ELECTION` is a falsified name
  and is to be corrected, not cited.
- **New:** no claim that the terrestrial/edge mapping is evaluated. It
  is an argument from structure and is labelled as one.

## Deviations

To be recorded, with reasons, in the results note. Not absorbed.
