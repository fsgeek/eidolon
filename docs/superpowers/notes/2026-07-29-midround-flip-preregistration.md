# Pre-registration: mid-round capability flip

**Written before any modification to `duel.py`.** Gate order matters and is
checkable: this document must be committed and stamped before the first
line of experiment code exists. Precedent: `1864401`, the duel campaign's
pre-registration, committed before any sweep ran, with two of its
predictions subsequently falsified and kept as findings.

Predecessor documents: `2026-07-22-dueling-proposer-premortem.md`,
`2026-07-22-duel-results.md`, `2026-07-29-capability-verification-round.md`.

## The question, and why it is the only one left

The verification round established the capability-state characterization
exactly (boundary theorem, containment lemma, TLA+ at 20,480 states) and
killed the claim that the state `(1,0)` was unnamed — Flexible Paxos §4.3
describes it twice.

Howard reads it with **positive** valence: a recovery affordance. Complete
Phase 1, learn past decisions, fall back to reconfiguration; tolerate $f$
failures with $f+1$ acceptors. This work has been reading the same state
as **harm**.

Nothing run so far adjudicates between those readings. Every artifact to
date is combinatorial — sets, predicates, counts; the TLA+ model has no
ballots, messages, or acceptor state. The valence question, not the
existence question, is therefore the load-bearing wall between "we
complete Howard's picture" and "we merely re-shade it."

## Registered harm (operational definition)

The harm is **not** that a `(1,0)` proposer raises the ballot high-water
mark. `paxos.py _handle_prepare` raises `_highest_promised` on any higher
prepare, quorum or not, so ballot disruption requires only reachability
and a higher ballot. Registering that as the harm would register something
`(1,0)` is not necessary for, and the experiment would confirm a
foregone conclusion.

The registered harm is that a `(1,0)` proposer is **success-signaled**:

> A proposer that fails Phase 1 *knows* it failed. It backs off and keeps
> its failover machinery armed. A `(1,0)` proposer *completes* Phase 1,
> believes it holds authority, stands its failover logic down, and — if it
> re-prepares on commit timeout — becomes a contender that can win rounds
> but can never end the game, because only committing ends it.

The claim under test is that this belief state, not the promise itself,
is what converts a disconnected node from a transient nuisance into a
persistent one.

## Predictions

**P1 (primary).** With a flip-induced `(1,0)` incumbent that re-prepares
on commit timeout, the healthy proposer's time-to-first-commit degrades
qualitatively — livelock within the simulation horizon, or a heavy-tailed
distribution — relative to control C2.

**P2 (control behaviour).** Under C2, disruption is transient per round:
the healthy proposer commits within one retry cycle in the large majority
of seeds, with a distribution that is not heavy-tailed.

**P3 (mechanism discriminator).** A `(1,0)` incumbent configured with
`retries=1` — no re-prepare on commit timeout — does **not** produce P1's
degradation. If P3 fails and retries=1 degrades too, the harm is the
standing promise rather than the retry loop, the "success-signaled"
framing is wrong as written, and that is the finding.

**P4 (timing specificity).** A proposer flipped *before* acquiring
promises is in `(0,0)` or `(0,1)` and is statistically
indistinguishable from C2. If P4 fails, the flip is not isolating what it
claims to isolate.

## Controls

- **C1 — incumbent absent.** Baseline time-to-first-commit.
- **C2 — incumbent held in `(0,0)`.** *The critical control.* A `(0,0)`
  proposer still sends prepares and still raises the high-water mark, so
  C2 holds ballot disruption constant and varies only the belief-and-retry
  state. Any difference between the treatment and C2 is attributable to
  the registered harm and to nothing else.
- **C3 — incumbent in `(1,1)`.** The ordinary competing-proposer case;
  establishes that the harness reproduces classic symmetric duelling.

## Design constraints (harness must satisfy these or the experiment lies)

Established by the feasibility pass; recorded here because ignoring any of
them silently produces a clean commit and a false null.

**A1 — the flip is not schedulable between phases.** `paxos.py` has no
yield between Phase 1 quorum detection (:524) and Phase 2 fan-out (:548),
so `p2_start − p1_end = 0.0` exactly. The usable window is the **Phase 2
return leg**, ~90.5 ms at `jitter_scale=0` for na-west↔africa. The flip
must be scheduled inside that window and the achieved flip time recorded
per trial as config provenance.

**A2 — the capability gate is currently partition-blind.** `duel.py`'s
gate never consults `_partitioned_locations` via `get_link`, so the
premortem-A7 assertions keep certifying `(1,1)` after the flip. This is a
harness defect, not an experimental variable. It must be fixed **and the
fix verified against a known-`(1,0)` scenario** before any trial is
counted, or every treatment row is mislabelled.

**A3 — partition granularity is LOCATION, not entity.** A flip cuts the
proposer's colocated acceptor too. Either accept this and declare it, or
place the flipped proposer at a location with no colocated acceptor.
Whichever is chosen is declared here before the data exists.

**A4 — determinism.** Byte-identical CSVs across two runs at the same
seeds, as in the duel campaign's Task 7 cross-check. Any nondeterminism
invalidates the run.

## Falsification, stated in advance

P1 is falsified if the healthy proposer's time-to-first-commit under the
treatment lies within control C2's distribution — operationally, if the
Wilson interval on "committed within one retry cycle" overlaps between
treatment and C2 across the seed set.

**Declared consequence of a null:** `(1,0)` is a state with an open
valence question; Howard's optimistic reading survives contact; and the
paper ships as the combinatorial note — boundary theorem, the Figure 4
decomposition, and the honest three-part relationship to §4.3. That is a
smaller and clean paper, and a registered null is a result. This project
already carries two.

## Prose bans (inherited from premortem D6, extended)

- No FLP claims.
- No Multi-Paxos authority claims; single-decree is the conservative
  worst case, and leader election narrows per-slot dynamics without
  answering the reachability question.
- No backoff-policy study.
- "Physics" is reserved for light-travel effects, never ballot mechanics.
- **New:** no claim that `(1,0)` is novel. FPaxos §4.3 has it. What is
  claimed is the characterization, and — if P1 holds — the valence.
- **New:** no claim that any deployed system "has a bug." Set-system
  reachability is not an operational defect claim.

## What would invalidate the experiment rather than the hypothesis

- A2 not fixed, or fixed without the known-`(1,0)` verification.
- Flip time not achieved inside the A1 window (record and discard).
- Any trial where the capability gate and an independent recomputation of
  the capability state from the partition set disagree.
- Nondeterminism under A4.

## Deviations

*(To be appended if the plan changes after this document is committed.
Any deviation is recorded here with its reason, not silently absorbed.)*
