# Dual characterization and the (0,1) tier-gradient: results

Pre-registration: `2026-07-30-dual-and-gradient-preregistration.md`,
committed and stamped at `bf45d2a` before `experiments/capability_dual_sweep.py`
existed. Verifiable against the OTS chain.

**Headline: P3 holds. There is no `k` at which this wall closes both
capability classes.** The briefing's "self-dual point" is falsified.

## Validity gates (all pass; the run counts)

| Gate | Result |
|---|---|
| Enumerator vs. shipped predicates | **0 mismatches** over 5 `k` x 4 tiers x 1,024 states. `is_phase1_quorum`/`is_phase2_quorum` (direct) vs. `capability.classify` (obligation-based) — two independent in-repo implementations. |
| Monotonicity of both predicates | **OK.** No node addition revokes a capability. |
| A4 determinism | **Byte-identical** across two runs (`9810db27…`, `b520c0b6…`, `799561dc…`). |
| Non-degeneracy | All enumerated tiers non-empty; the degenerate constructor case is excluded by construction, not by luck. |

## Verdicts

| Pred | Registered | Observed | Verdict |
|------|-----------|----------|---------|
| P1 | `(0,1)` unreachable iff every `Q2` contains some `Q1` | 20/20 (k, tier) cells agree. Containment computed as genuine set containment over minimal `Q2` sets, not as the reachability restatement. | **CONFIRMED** |
| P2 | over threshold families, `(1,0)` reachable iff `q1<q2`; `(0,1)` iff `q2<q1` | 80 configs (n=3..7, all valid q1/q2 via `FlexibleQuorum`), **0 violations** | **CONFIRMED** |
| P3(a) | `(0,1)` reachable for tiers with >=1 tier strictly between them and the anchor (Mars, Moon), at every `k` | Mars YES and Moon YES at k=1..5 | **CONFIRMED** |
| P3(b) | `(0,1)` empty for Earth and LEO when `k >= |E|-k+1` (k>=3) | Holds for both under the self-reachable reading. **Fails for LEO** under unconstrained enumeration. | **SPLIT — see D1** |
| P3(c) | `(0,1)` reachable for Earth when `k < |E|-k+1` (k<=2) | Earth YES at k=1,2; empty at k>=3. Exact. | **CONFIRMED** |
| P3(d) | `k=3` does not close both classes | At k=3: `(1,0)` empty at every tier; `(0,1)` reachable at Mars and Moon | **CONFIRMED** |

## The map

`(0,1)` reachability. Left: all 1,024 connectivity states. Right: states
where at least one node of the initiator's own tier is reachable (the
colocated-acceptor reading).

```
 k    Mars   Moon    LEO  Earth  |  Mars   Moon    LEO  Earth
 1     YES    YES    YES    YES  |   YES    YES    YES    YES
 2     YES    YES    YES    YES  |   YES    YES    YES    YES
 3     YES    YES    YES      .  |   YES    YES      .      .
 4     YES    YES    YES      .  |   YES    YES      .      .
 5     YES    YES    YES      .  |   YES    YES      .      .
```

`(1,0)` reachability, for contrast — the boundary theorem asserting
itself, empty exactly at `k <= ceil(|E|/2) = 3`:

```
 k    Mars   Moon    LEO  Earth
 1-3     .      .      .      .
 4-5   YES    YES    YES    YES
```

## The finding that matters

**No `k` closes both classes.** Reading the two maps together:

- `k <= 2`: `(1,0)` empty everywhere, `(0,1)` reachable at **all four** tiers.
- `k = 3`: `(1,0)` empty everywhere, `(0,1)` reachable at **Mars and Moon**.
- `k >= 4`: `(1,0)` reachable everywhere, `(0,1)` reachable at Mars, Moon, LEO.

The k-ladder trades the classes against each other and never clears both.
`k=3` is the best cell on the board and it still leaves the *measured-harmful*
class open at the two tiers furthest from the anchor. The relaxation that
the paper reads as "converting LEO from futile spoiler to legitimate
failover peer" does convert LEO — and does nothing for Moon or Mars.

Mechanism, and it is not arithmetic: Moon's `Q1` needs one LEO witness, and
this topology has exactly one LEO node. Sever it while all five Earth nodes
stay reachable and `Q2` forms while `Q1` cannot. The exposure comes from the
**downward-chain witness obligations**, not from the Earth-floor arithmetic
the boundary theorem governs. Two different mechanisms, two different
classes; the theorem covers one of them.

## Consequence, as declared in advance

The pre-registration stated: *"If P3 holds, the 'compensate with legibility'
framing is not available as written. The honest claim becomes that even a
deliberately topology-aware construction admits the measured-harmful class
at every tier but one, and the O(tiers) readout is the only thing standing
between an operator and an invisible stall."*

That is what happened, with one correction to my own registered wording:
it is **two of four tiers, not "every tier but one."** At `k>=3` under the
colocated-acceptor reading, `(0,1)` is confined to Mars and Moon — the
tiers with an intervening tier below them. The claim is narrower than I
registered and should be stated at its observed width.

So the wall does not purchase coincidence and hand out legibility as
compensation. It purchases coincidence **for the anchor and (under one
modeling choice) its adjacent tier**, and the remaining tiers carry live
`(0,1)` exposure that only the readout makes visible.

## The unresolved modeling choice, stated rather than settled

How many tiers get coincidence at `k=3` depends on whether a proposer is
credited with its colocated acceptor:

- **Unconstrained** (any connectivity state): only Earth. LEO's single node
  can be severed while Earth stays whole, giving LEO `(0,1)`.
- **Self-reachable** (proposer's own tier has a reachable node): Earth and LEO.

This is a declaration the paper owes, not a fact the enumeration can
supply. The flip campaign's D3 already leaned on colocated acceptors, so
there is precedent for the second reading — but it must be stated, because
the affordable unit of coincidence is one tier or two depending on it.

## Addendum: the price of coincidence (post-hoc, from committed data)

Not pre-registered. Derived by re-reading `dual_uniform.csv` after a
parallel Fable session argued that under single-decree Paxos "symmetric
majority is the equilibrium: both off-diagonal classes closed by default."
Labelled post-hoc because it is: the data existed, the question did not.

Under single-decree both phases run per decree, so both quorums want to be
small, and intersection forces `q1 + q2 >= n + 1`. Asking whether the
cost-minimal configurations can close both classes:

| n | min q1+q2 | both closed among minimal? | cheapest both-closed |
|---|---|---|---|
| 3 | 4 | yes | 4 |
| 4 | 5 | **none** | 6 |
| 5 | 6 | yes | 6 |
| 6 | 7 | **none** | 8 |
| 7 | 8 | yes | 8 |

**Closed-by-default holds only at odd `n`.** At even `n`, `n+1` is odd, so no
cost-minimal split can satisfy `q1 = q2` — every cheapest configuration
opens one class. The cheapest both-closed configuration costs `n+2`.

So the price of coincidence is **0 at odd `n` and exactly 1 at even `n`.**
This is a second, independent reason for the odd-cluster-size convention:
the familiar one is that even `n` buys no additional fault tolerance under
majority; this one is that even `n` cannot buy coincidence at the
cost-minimal configuration.

The claim it corrects: symmetric majority is the cost-minimal equilibrium
at odd `n` only. The generalization to all `n` is false.

## Deviations from the registered plan

- **D1 — enumeration constraint not specified in advance.** The
  pre-registration's P3(b) argued from a proposer serving "its own tier
  obligation from its colocated acceptor" but never specified whether the
  enumeration would constrain the initiator's tier to be reachable. Both
  variants were run and both are reported; P3(b) holds under the reading
  the prose implies and fails for LEO under the other. Recorded as a
  registration defect, not resolved in favour of the confirming variant.

## Threats to this result

- **One topology.** 5/1/1/3 only. The tier-gradient's mechanism (a
  singleton intermediate tier) is sharpened by LEO and Moon each having
  exactly one node; a fatter intermediate tier would need more
  simultaneous failures to produce the same exposure. Whether the gradient
  survives on wider walls is unenumerated.
- **Combinatorial, not operational.** This is sets and predicates. `(0,1)`
  being *reachable* is not a claim that any deployment enters it, and the
  cost attached to `(0,1)` comes from the flip round's single arm, which
  characterized one instance rather than the state.
- **P1 is close to a restatement.** Under monotone predicates, "every `Q2`
  contains some `Q1`" and "no state yields `(0,1)`" are nearly the same
  proposition. The containment side was computed as set containment over
  minimal `Q2` sets to keep the check non-vacuous, and monotonicity was
  verified rather than assumed — but this is a structural equivalence, not
  an empirical discovery, and should be presented as the mirror of the
  containment lemma.
- **Scarcity lemma untouched**, deliberately. It has its own registration
  owed. Nothing here licenses a claim about constructions in general.

## Artifacts

- `experiments/capability_dual_sweep.py` — enumerator; calls the repo's own
  predicates, never a reimplementation
- `results/capability/dual_gradient_map.csv` — 80 rows (5 k x 4 tiers x 4 states)
- `results/capability/dual_containment.csv` — 20 rows, P1
- `results/capability/dual_uniform.csv` — 80 rows, P2
