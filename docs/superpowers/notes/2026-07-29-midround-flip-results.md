# Mid-round capability flip: results

Pre-registration: `2026-07-29-midround-flip-preregistration.md`, committed
at `128e10f` before any experiment code existed (`git status -- duel.py
experiments/` clean at that commit; verifiable against the OTS chain).

**Headline: P1 is falsified, and falsified in reverse.** The (1,0) state
carries no measurable liveness cost. The state that does is `(0,1)` — the
one the premortem parked as "declared analysis" and never examined.

## Verdicts

| Pred | Registered | Observed | Verdict |
|------|-----------|----------|---------|
| P1 | flip-induced (1,0) incumbent degrades healthy time-to-first-commit relative to C2 | At retry budget 8: treatment P(attempt ok) = **1.000** vs C2 **0.000**, Wilson intervals disjoint. Treatment is *better*, never worse, at every budget. | **FALSIFIED (reverse)** |
| P2 | C2's disruption is transient — healthy commits within a retry cycle in the large majority | Transient at budgets 1–4 (1.000), **total at budget 8 (0.000)**. The control is the harmful arm. | **FALSIFIED** |
| P3 | treatment with `retries=1` does not degrade; harm is the retry loop | Treatment is *perfectly flat* in retry budget: P(ok) 1.000 and median ttfc 3.420 s at budgets 1, 2, 4, 8, while its Phase 1 quorum count climbs 1→8. | **CONFIRMED** |
| P4 | timing of the Phase-1-failing state is irrelevant | Early (0,1) vs late (0,1): CIs overlap at every budget; both go to 0.000 at budget 8. | **CONFIRMED** |

## The cleanest statement of the null

A (1,0) incumbent is **metric-for-metric indistinguishable from a healthy
(1,1) incumbent**, at every retry budget:

| budget | arm | P(ok) | P(own value) | med ttfc | med rounds | med nacks |
|---|---|---|---|---|---|---|
| 1–8 | treatment (1,0) | 1.000 | 0.000 | 3.420 | 2 | 7 |
| 1–8 | c3 healthy (1,1) | 1.000 | 0.000 | 3.420 | 2 | 7 |

Every column identical. Whatever a (1,0) incumbent costs the system, a
perfectly healthy competing proposer costs exactly the same. The cost is
*contention*, not the capability state.

## Why, mechanically

**Failure is cheap; success is expensive.** `paxos.py:530` backs off
`0.010 * (round + 1)` when Phase 1 fails, so a (0,1) proposer recycles
ballots every ~10–80 ms. A (1,0) proposer *completes* Phase 1 and then
stalls in Phase 2 for the full per-phase timeout (3.2 s here) before it can
retry. Its poisoning rate is two orders of magnitude lower — throttled by
its own success.

Registered harm was that the (1,0) proposer is *success-signaled*. It is.
That turns out to be **self-limiting rather than harmful**: completing
Phase 1 buys the standing ballot and pays for it in cycle time.

## Howard's reading survives contact — and the mechanism is visible

FPaxos §4.3 reads (1,0) as a recovery affordance. In the treatment arm the
incumbent's value **is** decided (`P(own value) = 0.000` for the healthy
proposer means the *incumbent's* value won): its partial Phase 2 reaches
four of five Earth acceptors, the next proposer's Phase 1 surfaces that
accepted value, and Paxos value adoption carries it to commitment.

The (1,0) proposer is a value injector, not a spoiler. That is
"complete Q1 and recover past decisions," observed.

## The unregistered finding

**(0,1) — can commit, cannot elect — is where the liveness cost lives.**
It blocks the healthy proposer completely once its retry budget reaches 8,
in 50 of 50 seeds, with 48–49 NACKs absorbed. The premortem declared (0,1)
out of scope; the boundary theorem never distinguished it; and it is the
only arm in this experiment that costs anyone anything.

Reported as observed, not as a claim: it was not pre-registered, one arm
does not characterize a state, and the next experiment should register it
properly rather than harvest it here.

## Deviations from the registered plan

Recorded, not absorbed.

- **D1 — flip timing.** Registered: flip inside the Phase 2 *return leg*
  (~90.5 ms). Realized: flip **before** Phase 2 fan-out. Reason: a
  return-leg flip lets the Accept reach the severed acceptor and only loses
  its response — committed-but-unobserved, which is the *acknowledgment
  gap* the deployed-systems census disqualified MongoDB for. Pre-fan-out is
  the only timing under which Q2 is genuinely unformable.
- **D2 — C2 realized as (0,1), not (0,0).** (0,0) is unrealizable while
  still poisoning Earth: any isolation that fails an incumbent's Phase 1
  also stops its prepares reaching Earth. The registered *intent* — raises
  the high-water mark, does not believe it holds authority — is preserved
  exactly.
- **D3 — incumbent is Moon-tier, not Earth-tier.** An Earth-tier proposer
  at an Earth location has a colocated Earth acceptor and needs only one
  Earth node for Phase 1, so no partition can fail its Phase 1 while
  leaving Earth poisonable. C2 does not exist for an Earth-tier incumbent.
- **D4 — arm `p4_late_phase1_fail` added.** P4 as registered was
  untestable: `TREATMENT_CUT` removes one Earth node, and Phase 1 needs
  one, so a pre-promise flip still yields (1,0) and never (0,x). The added
  arm tests P4's intent — whether the *timing* of the Phase-1-failing state
  matters — and it does not.
- **D5 — k=5 only.** At k=4 the single-link cut leaves 4 Earth reachable
  and Q2 needs 4, so no flip occurs; k=3 is capability-complete by the
  boundary theorem, so no (1,0) state exists to flip into. **This
  experiment only exists at k=5**, which is the boundary theorem asserting
  itself on the experiment's own design.

## Threats to this result

- **Backoff-policy dependence — the dominant threat.** The effect is driven
  by the asymmetry between a 10 ms Phase-1-failure backoff and a
  timeout-bounded Phase 2 stall. Premortem D6 bans backoff-policy studies,
  and this result sits on that edge. What is *structural* and not policy: a
  proposer that completes Phase 1 must wait at least one Phase 2 timeout —
  which must exceed the round trip — before retrying, whereas a
  Phase-1-failing proposer has no such floor. Policy sets how small the
  floor-free cycle is; the existence of the asymmetry does not depend on it.
- **"NEVER" is budget exhaustion, not livelock.** The healthy proposer has
  `max_rounds=8`; C2's blocking is one retry budget out-racing another.
  No livelock claim is made.
- **Single-decree only**, as in the duel campaign. Leader election narrows
  per-slot dynamics.
- **The stochastic sweep adds little.** Wilson intervals are degenerate at
  0 or 1 in every cell; jitter does not move the outcome. The deterministic
  map carries the claims and the sweep confirms only that jitter is not
  hiding a distribution.
- **One flip site.** `moon↔africa`. Other severed links were not swept.

## Consequence, as declared in advance

The pre-registration stated: *"if the experiment returns null, the declared
loss is that (1,0) is a state with a valence question, Howard's optimistic
reading survives contact, and the paper ships as the combinatorial note."*

That is what happened. The paper's residual claim is the characterization —
boundary theorem, containment lemma, the Figure 4 decomposition — and the
word **hazard** is not earned and must not be used. `capability.py` names
the state `Hazard.DISRUPTIVE_ELECTION`; on this evidence that name is
wrong, and `Hazard.INCUMBENT_ONLY` — the (0,1) state — is the one that
earned it.

## Artifacts

- `flip.py` — wiring, arms, trial runner
- `experiments/flip_sweep.py` — deterministic map + stochastic sweep
- `experiments/flip_verdict.py` — scores the sweep against the registered
  criteria mechanically
- `results/flip/flip_map.csv` — 20 rows, jitter 0, primary result
- `results/flip/flip_sweep.csv` — 1000 rows (5 arms x 4 budgets x 50 seeds)
- A4 determinism: two full runs byte-identical (md5 `901c03d6…`)
