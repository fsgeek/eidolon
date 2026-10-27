# Caerus restart arm: results

**Scored:** 2026-10-26 by the Eidolon steward (Claude Opus 5.5), against
[the registration](2026-10-26-caerus-restart-arm-preregistration.md) (8094dc5)
as written, using the analysis script committed with it.
**Data:** `results/caerus/restart-arm/` (12 trials, 0 failed; run 11:31-13:05
PDT; `analysis.json`; the unscored harness check is in `check-R6/`).

## Predictions, scored

| ID | Outcome | Data |
|---|---|---|
| R1 | **holds** | R6 cluster T_regain 32.3, 34.8, 35.7 s (pmr 0) and 32.6, 32.6, 33.2 s (pmr 30); all <= 45 s |
| R2 | **holds** | R6 minimum 30 s window after +60 s: 0.956-0.964 in all six trials |
| R3 | **falsified as worded** | The five-region cluster started in 6/6 trials, but its cluster T_regain is **undefined** in 6/6. Three of the five regions never produce a sample at or above 0.9 x their six-region pre-fault median. Regions 1 and 4 crossed once, at about 30 s |
| R4 | **holds, narrowly** | R5 minimum window 0.817-0.821 in all six trials (threshold 0.80) |

## What the restarted five-region cluster does

Every restarted region settles on the same plateau, about 0.82 of its own
six-region rate, from the first window onward. The plateau has no trend and
does not depend on workload (pmr 0 and pmr 30 both give 0.817-0.821).
Cluster throughput is about 0.71 of the pre-fault six-region total
(R5_pmr0_rep1: 48.8k vs 68.7k txns/s). *Exploratory, not registered:* the
time for every region to first reach 0.75 of its six-region median is
28.7-30.7 s across the six R5 trials.

**Why the per-region rate falls is not known.** Candidates (post hoc,
untested): MovR's key layout with five partitions, or the per-epoch
cross-region batching cost changing with the region count. It is not a
stall, so per the registration (R4 fails alone -> report the throughput
change) it is reported as a throughput change.

## The Caerus row

| Arm | Time to regain | State kept | Source |
|---|---|---|---|
| Recovery, 1 region killed (pmr 0) | **never**: all survivors stalled, no recovery in 600 s (3/3) | survivors keep state but serve nothing | 2026-10-14 single-region results |
| Restart, idealized (all six, D = 0) | **32-36 s** to >= 0.9, then >= 0.96 | **none**: no load path (code); restarts from MovR's generated state | R6, this round |
| Restart without the dead region (five) | never reaches 0.9 of the six-region level; reaches a steady 0.82 within about 30 s (exploratory) | **none** (same) | R5, this round |

**The ledger reading.** Restart beats recovery on time at every detection
delay we can observe, because recovery never regains: the break-even D is
unbounded within 600 s. Neither arm keeps the data. So Caerus fails the
paper's own fault-tolerance claim in both arms. An operator's best option
after losing a region is the five-region restart: back to about 82% per
region in about 30 s, with every transaction since start-up gone.

At pmr 30 the recovery cell is still pending. There is no single-region
recovery measurement at 30% yet; C1 in the isolation round supplies it.

## Limits

- Safety is from code, not measured: no acked-write ledger exists.
- Containers on one host with emulated delays (Table-1 matrix); same image,
  RUNTIME=1200, no CPU pinning; 3 reps per cell.
- Host clock (checked 2026-10-27, after Codex reported clock discontinuities on
  this WSL2 host): in the restarted clusters' 1 s samples, gaps above 1.3 s
  number 0-5 per 300 s, the largest 1.59 s (spot check of four trials). That
  amounts to at most about 1.6 s of T_regain error, which is small next to
  32-36 s. On 2026-10-27, a probe found much heavier stalls (about 1.4 s
  about every 31 s, with the wall clock stepped by Hyper-V time sync). Those
  conditions did not hold during these trials.
- "Restart" here is a fresh cluster at the fault instant. A real operator
  adds detection time, D; the row reports D = 0 because recovery's T_regain
  is unbounded, so any finite D still favours restart on time.
