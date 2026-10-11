# Results: Caerus (0,1) contagion (model)

Pre-registration: `2026-10-11-caerus-contagion-preregistration.md` (commit
`8298931`, before any code). Model: `caerus_model.py`. Sweep:
`experiments/caerus_contagion.py` -> `results/caerus/contagion.csv`.
Defaults: N=6, K=1, regions 2 and 5 killed at t=2 s, 1000 txn/s, 20 items per
region, horizon 10 s (+1 s drain), 5 seeds.

Unaffected goodput by submit-time window (min-max over seeds):

| policy | p_mr | 2-3 s | 5-6 s | 9-10 s |
|---|---|---|---|---|
| submit | 0    | 1.00 | 1.00 | 1.00 |
| submit | 0.01 | 0.93-0.99 | 0.70-0.82 | 0.39-0.55 |
| submit | 0.05 | 0.81-0.90 | 0.09-0.15 | 0.00 |
| submit | 0.1  | 0.59-0.75 | 0.00-0.02 | 0.00 |
| submit | 0.3  | 0.17-0.27 | 0.00 | 0.00 |
| reject | 0    | 1.00 | 1.00 | 1.00 |
| reject | 0.01 | 0.99-1.00 | 0.98-1.00 | 0.93-1.00 |
| reject | 0.05 | 0.96-1.00 | 0.56-1.00 | 0.03-1.00 |
| reject | 0.1  | 0.93-1.00 | 0.23-1.00 | 0.00-1.00 |
| reject | 0.3  | 0.31-0.45 | 0.00 | 0.00 |

## Verdicts against the registered predictions

- **P1 (p_mr=0 unaffected): held.** 1.00 in every window, both policies.
- **P2 (SUBMIT decays below 0.05, faster with p_mr): held for p_mr >= 0.05,
  failed at p_mr = 0.01** (0.39-0.55 at the horizon). Decay is monotone in
  p_mr. The registered horizon was too short for the lowest rate; we keep the
  failure as registered rather than extending the horizon after the fact.
- **P3 (REJECT decays below 0.05): failed as stated** except at p_mr = 0.3.
  The registered falsifier (a plateau above 0.2 at the horizon) is met by
  some seeds at p_mr = 0.05 and 0.1, which stay at 1.00.

## Post-hoc explanation (not registered; checked once)

Under REJECT the only seeds are transactions in flight to a dead sequencer
at t_fail: 0-4 per run here. Per seed (p_mr in {0.01, 0.05, 0.1}): every run
with zero stuck dead-touching transactions ends at 1.00; every run with at
least one decays (to 0.93-0.98 at p_mr=0.01, to 0.00-0.48 at 0.05 and 0.1).
So the percolation argument survived; what the prediction got wrong is that
seeding under REJECT is a chance event, not a certainty. One stuck
transaction is enough.

## What this does and does not show

Shows: under our reading of Algorithms 1-2, Caerus's (0,1) state is not
stable. With more than K regions down, the stall spreads through the
conflict graph to transactions that never touched a dead region, and
rejecting new dead-touching work at the batcher only changes whether the
spread starts, not whether it stops.

Does not show: that real Caerus behaves this way. Rates and item counts
(1000 txn/s, 20 items/region) are our choices and set the decay speed. A
mechanism the paper does not describe (abort or timeout of incomplete
vertices, batcher behaviour, partial recovery) could stop the spread. The
next step is fault injection on github.com/josh-hildred/Caerus.
