# Pre-registration: does Caerus's (0,1) state stay put?

**Written and committed before any model code exists.** Same gate as
`2026-07-29-midround-flip-preregistration.md`.

Source: Hildred, Abebe, Daudjee, "Caerus: Low-Latency Distributed
Transactions for Geo-Replicated Systems", PVLDB 17(3), 2023
(`../cloudflare/docs/references/p469-hildred.pdf`). Prior reading:
`../cloudflare/docs/synthesis.md` sections 11-12 (2026-10-10).

## The mapping (paper's own words, our boundary result)

Section 4.7: each region's partial sequence is replicated to K other regions
before commit; recovery of a failed region needs N-K live regions; "equivalent
to running Flexible Paxos with a long-lived leader for each partial
sequence." So |Q2| = K+1, |Q1| = N-K. Containment (Q1 contains a Q2) holds
iff N-K >= K+1, so for N=6, K=1 the (1,0) state is impossible and (0,1) is
reachable whenever 2-4 regions are down: live sequencers commit, no failed
sequencer can be replaced.

## The question

The paper is silent on more than K failures and runs no failure experiment.
Our reading of Algorithm 1 and section 4.5: a multi-region transaction X that
touches a dead region's items never appears in that region's partial
sequence, so X never becomes *complete* (Property 4.5) and is never released.
Any later transaction conflicting with X on a live item gets an edge to X and
is also never released; it then blocks later conflicting transactions on its
other items. If so, the stall spreads along the conflict graph to work that
never touched a dead region: (0,1) decays toward (0,0).

## Model (what is and is not represented)

Represented: per-region partial sequencers; one observing sequence merger at
a live region; Algorithm 1 edge rules (most-recent-writer, readers-since-writer);
release of complete vertices / complete SCCs with no outgoing edges to
unreleased vertices (Algorithm 2); one-way latencies from the paper's Table 1
regions; failure of F regions at t_f with no recovery (|F| > K).

Not represented: the deterministic scheduler and storage (release from the
merger is taken as the commit point for non-sequencing regions), batching,
intra-region replication, the recovery protocol, throttling (section 4.4;
throttling can only shrink admitted work further, so omitting it cannot
manufacture the predicted decay). Line 16 of Algorithm 1 reads
`if T*.removed then addEdge`; we read it as `!removed`, matching lines 9
and 21 — an edge to an already-released vertex constrains nothing either way.

Workload: H items per region; each transaction writes one item at its
origin region and, with probability p_mr, one item at a uniformly chosen
other region. Two batcher policies after failure: SUBMIT (transactions
touching dead items are still submitted, as the paper describes nothing
else) and REJECT (batchers drop them; only in-flight transactions seed
the stall).

Metric: *unaffected goodput* — releases per second, at the observer, of
transactions submitted after t_f whose items are all on live regions,
divided by their submission rate.

## Predictions (N=6, K=1, two regions killed)

- **P1.** p_mr = 0: unaffected goodput stays at ~1.0 after failure. (No
  multi-region edges, no path for the stall.)
- **P2.** p_mr > 0, SUBMIT: unaffected goodput decays to < 0.05 within the
  simulated horizon, and decays faster as p_mr increases.
- **P3.** p_mr > 0, REJECT: unaffected goodput also decays to < 0.05, more
  slowly than SUBMIT at the same p_mr, because the frozen seed is finite but
  the item conflict graph under uniform pairing is connected.

## What would falsify the reading

- Unaffected goodput plateauing at a non-trivial level (> 0.2) for p_mr > 0
  under REJECT at the end of the horizon falsifies P3 and the percolation
  argument.
- Any decay at p_mr = 0 means the model has a bug, not a finding.
- If P2/P3 hold in the model, the claim is still only about our reading of
  the paper's algorithm. The real test is the public code
  (github.com/josh-hildred/Caerus) under fault injection; a batcher or
  merger behaviour not described in the paper could stop the spread.
