# Restructure contract (hazard-section integration)

Drafted 2026-07-23, after: supervisor verification of the revision round,
Sol's ayllu review (all five blocking findings verified and fixed, commits
`20a64aa`/`3ef3b2c`/`e9ded01`), and the rikuy duel-material gate PASS
(`rikuy/reviews/review_20260723_163739.md`). This document collects every
binding constraint on the restructure so the writer — this instance or a
successor — starts from one page.

## Goal (Sol's recommended shape)

Replace weaker material with the stronger result; do not append:

1. Topology-shaped Phase 1 participation plus anchor Phase 2 (already in
   draft, §2.3 decomposition language landed).
2. A static capability map per tier: acquire authority / extend /
   certify-learn / neither.
3. Mars blackout as the clean positive example.
4. Sparse LEO as the (1,0) hazard state — can form Phase 1, cannot reach
   Phase 2 — structurally visible in the map.
5. The duel result: durable promises make the (1,0) hazard operationally
   consequential (poison outlives the poisoner).
6. k=3 relaxation as the conversion of LEO from futile spoiler to
   legitimate failover peer.
7. Exact safety proof primary; formal verification narrowly stated
   (already landed: 27,921-state three-family enumeration).

## Non-negotiable conditions

### From the rikuy duel gate (2026-07-23)

- (a) **Two-axis reporting**: any duel figure/table separates "did
  Earth's attempt succeed" from "whose value was decided" as distinct
  columns. The conflation cost the gate auditor a false FATAL; the
  rescue at offset −1.05 is real (`earth_success` 0→1, `earth_rounds`
  1→2 in `duel_map.csv`).
- (b) **Durable poison framed as known mechanism**: promises-as-durable-
  state is definitional Paxos — cite it as such. The delta is measured
  structure: denial persisting ≈ the spoiler's full attempt span past
  its activity (band edge ±0.05 s); the retry-budget ceiling as a
  parameterized rule (budget of b rounds cannot outrank a standing
  counter-s ballot when b < s under adverse polarity — general, not a
  5<8 artifact); value capture at zero latency cost.
- (c) **B3 paragraph**: single-decree is the conservative worst case;
  leader election narrows per-slot dynamics but the hazard is live
  during transitions; the asymmetric (1,0) spoiler has no "let one win"
  escape.
- (d) **Livelock-0**: one sentence; structural rarity is a reportable
  property, not a headline.
- (e) **"Physics" is reserved** for light-travel effects; never for
  ballot mechanics.
- (f) **P4**: state that the "Earth's-value-carried" sub-clause of the
  CONFIRMED verdict was vacuously satisfied.

### Inherited from the duel campaign close (W1/W2)

- **W1 — leo_commit is value provenance**: at k=5, "leo_commit" means
  Earth commits LEO's carried value (all 40 such rows have
  `leo_success=0`). Every figure/table caption must say "LEO's value,
  committed by Earth" — anything else contradicts the §B3
  asymmetric-spoiler claim it supports. (Same seam as rikuy condition
  (a); two independent gates converged on it.)
- **W2 — the B1–B6 framing contract is inherited in full** (authoritative
  source: premortem docs via `.superpowers/sdd/progress.md`). Already
  satisfied by the current draft and MUST NOT regress: B1 novelty-delta
  sentence (related work), B4 cadence-quantization framing, B5
  chosenness-certificate epistemics (abstract, contribution 2,
  topology-scoped-consistency paragraph). Still owed by the restructure:
  B2 (condition (b) above), B3 (condition (c) above).

## Anchor points in the current draft (post-`e9ded01`)

- §3.5 contention-scoping sentence ("sole, timely proposer … durable
  promises can delay an otherwise-capable peer") — the hazard section's
  natural entry hook.
- Topology-scoped-consistency paragraph (sparse LEO "observes values
  whose commitment it cannot confirm") — the (1,0) state is already
  epistemically characterized; the duel adds its temporal cost.
- Majority-baseline analysis ("slack buys survival; geometry buys
  Phase 1 and legibility") — the capability-map framing extends this.
- Relaxed section (k=3) — receives the spoiler-to-failover conversion.

## Data and provenance

- `results/duel/` + `duel_map.csv`; gate record with Provenance block
  (outcome classification is by value provenance — declared there).
- Duel campaign SDD ledger: `.superpowers/sdd/progress.md` (authoritative
  for campaign state and premortem §A/§B contracts).

## Open Tony calls (unchanged)

- D4: SVG eyeball — duel offset map as figure vs. table.
- Zenodo bundle boundary (anonymized artifact plan memory).

## Deadlines

- 2026-08-02: fallback gate — registered draft #98 ships if the
  restructure is not safely landed.
- 2026-08-06 AoE: full-paper deadline.
- Page budget: 12pp two-column excluding references/appendices; current
  body is comfortably inside — the constraint is coherence, not space.
