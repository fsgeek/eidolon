# §1 framing decisions (2026-07-30)

Decisions reached in conversation and recorded because they existed nowhere
else. Findings live in `2026-07-30-dual-and-gradient-results.md`; this note
holds the *framing* built on top of them, which is otherwise session-scoped
and dies with the instance.

Supersedes the corresponding parts of `docs/nines27-revision-briefing.md`
where they conflict. That document remains the source of the coincidence
idea and of the venue analysis; it is superseded on the self-dual claim and
on the compensation framing, both falsified at `e9eb2f4`.

## The bound goal

`docs/paper/nines/main.tex` is submission-ready at every commit and
improves monotonically.

- **Stage 1 — bounded insertion.** Boundary theorem, dual characterization,
  and the tier-gradient into §"Liveness: Reading the Wall", plus the
  three-part relationship to FPaxos §4.3.
- **Stage 2 — the coincidence reframe** of §1 and §3.
- **Never begin stage 2 before stage 1 compiles and is committed.**

This is the replacement for the killed 2026-08-02 fallback gate (see
`2026-07-23-restructure-contract.md`). Under this ordering there is no
moment at which stopping leaves the paper worse than the registered draft,
so no calendar checkpoint is needed.

## Thesis sentence

> When a capability gap cannot be closed by construction, making it legible
> is not a consolation — it is the only available mitigation.

Earned, not asserted: the enumeration shows `(0,1)` is unreachable-by-
configuration at **no** value of `k` for Moon or Mars. An operator running
this wall cannot configure the state away. Seeing and understanding it is
therefore the only remaining mitigation, which converts legibility from a
nice property into the load-bearing contribution.

This is deliberately *not* the briefing's framing ("allocate the affordable
unit of coincidence to the anchor and compensate every other tier with
legibility"). Compensation implies the exposure was traded away for
something. It wasn't; it remains, at two of four tiers, at every
relaxation.

## The see/understand distinction — and the honest limit

Tony's phrasing: *"being able to see the problem and understand it is
essential for mission critical services."* Two verbs, and **this work
delivers only the second.**

- **See** = detection. Requires failure detection, or in this setting
  orbital schedule knowledge. Not contributed here. The draft already
  concedes this in §1's parenthetical and that concession must not be
  weakened.
- **Understand** = interpretation, given connectivity state. The
  `O(tiers)` readout. This is the contribution.

The gap between the verbs is the finding rather than merely a limitation:
`(0,1)` is **not a detection failure.** Every node is healthy, the
connectivity summary is available, nothing is concealed — and no existing
monitoring reports that elections have become impossible while commits
still succeed. The data is sufficient; the predicate is missing. State it
that way, because a reviewer who reads "see the problem" will fairly ask
whether we built a detector, and we did not.

## Who cares — in priority order

1. **Present tense, source-verified.** Cassandra LWT exposes
   `consistencyForPaxos` and `consistencyForCommit` as independent knobs
   with `EACH_QUORUM` whitelisted for CAS commit. An operator editing
   config — writing nothing novel, violating nothing documented — can
   place a production system in the class the flip round measured as
   liveness-blocking, with a green dashboard throughout. **Owed before
   this ships:** pin the branch those line numbers came from, and check
   whether Paxos v2 (CEP-14, 4.1+) preserves that commit path.
2. **This paper's own construction.** After the gradient result, the gap
   is not only in someone else's config surface — it is in the best
   construction we could build, and cannot be relaxed away. This is the
   stronger form of (1) and it is ours to state.
3. **Future, labelled as prediction.** Every latency economy — edge,
   multi-region, DIL — pushes toward shrinking `q2`, which is the
   direction that opens the harmful class. One paragraph. **Not
   evaluated**, and the existing terrestrial-mapping paragraph already
   has the correct register ("an argument from structure, not an
   evaluated result"). Do not raise its confidence.

"Mission-critical" is motivation, never claimed validation.

## Interplanetary framing

Stays, and stays a **magnifying glass** — the draft's own word. Nobody
operates Earth-Mars; its value is that twenty-two light-minutes means no
reviewer can attribute the phenomenon to timeouts being set too tight. It
is an instrument, not a motivation, and conflating the two is what makes
the paper look like a space-systems submission. (Briefing's HotCRP advice
concurs: not satellite/space networking — wrong evaluation frame.)

## Modeling choice, declared

Self-reachable is primary: a proposer is credited with its colocated
acceptor. Precedent is the flip campaign's D3. The unconstrained map
ships as the conservative bound. The paper declares this rather than
assuming it, because the affordable unit of coincidence is one tier or two
depending on it. Full reasoning in the results note.

## Bans that must not regress

Inherited from the two 2026-07-30 registrations and the flip premortem:

- `hazard` only for `(0,1)`. `capability.py`'s
  `Hazard.DISRUPTIVE_ELECTION` is a falsified name, still unfixed.
- No claim `(1,0)` is novel. FPaxos §4.3 has it twice, with positive
  valence.
- No claim any deployed system "has a bug." A reachable *configuration*
  is sayable; a defect is not.
- No claim the terrestrial/edge mapping is evaluated.
- No FLP claims, no Multi-Paxos authority claims, no backoff-policy study.
- "Physics" reserved for light-travel effects.
- Desktop/Fable input is **within-family**. Not citable as cross-model
  replication. The genuinely cross-model inputs remain the OpenAI review
  and the rikuy panels.
