# Caerus restart arm: registration

**Registered:** 2026-10-26, in the signed commit introducing this file, before
any trial. **Predictions by:** Claude (Opus 5.5), Eidolon steward.
**Plan:** [the restart ledger](2026-10-26-restart-ledger-plan.md), Caerus row.
**Recovery arm (already measured):** [single-region results](2026-10-14-caerus-artifact-results.md)
and `results/caerus/artifact-rt1200-single/`: killing one region stalls every
survivor within seconds and the stall does not lift within 600 s
(percent_mr = 0, 3/3).

## Question

Against the restart baseline: how long does a full restart take to regain
throughput, and what does it keep? The answer fills the Caerus row of the
table (T_regain and safety for recovery, idealized restart, and the restart
available when the dead region stays dead).

## Prior exposure (disclosed)

Before writing the predictions I measured cold start in five existing logs
(`rt1200-control` pmr0/pmr30 and `rt1200-single` reps 1-3). Time from launching
the containers to every region's first sample at or above 0.9 x its pre-fault
median: 23.3, 24.1, 28.5, 24.4, 28.2 s. I also read the source in the image:
`SimpleStorage` is an in-memory map; `DumpToFile` writes it out, but there is
no load path; MovR derives the region count from the config.

## Arms (image `caerus-run:aaf0eb6-nopin-rt1200`, harness `external/caerus/run/run_restart.sh`)

Six regions, Table-1 delays, MovR, batch 10, hot records 240, percent_mp 0,
warm-up 120 s after all six report. At the fault instant, stop all six
(docker kill) and immediately start a fresh cluster (detection delay D = 0):

- **R6 (idealized restart):** all six regions restart, so the fault counts
  as transient.
- **R5 (restart without the dead region):** france-c (region 2) stays dead,
  and the other five restart with a five-region config (renumbered 0-4, same
  physical delays). This is the only restart available to an operator whose
  region is really gone.

percent_mr in {0, 30}, 3 reps each arm: 12 trials, observed 300 s after the
fault, run back to back in order R6/0, R6/30, R5/0, R5/30. Containers use the
prefix `rr`; nothing else is touched. One **harness check** (R6, pmr 0, warm-up
30 s, observation 60 s, directory `check-*`) runs first and is not scored. It
can only stop the campaign (it fails if generation b never logs throughput).

## Measures (`experiments/caerus_restart_analyze.py`, committed with this file)

- **T_regain(region):** time from t_fail to that region's first post-restart
  sample at or above 0.9 x its median in the 60 s before t_fail (same physical
  region). **Cluster T_regain** = the maximum over restarted regions; if any
  region never regains, it is undefined. This is cloudflare's round-1
  definition, applied to Caerus's 1 s sequencer samples.
- **Sustain:** each restarted region's mean normalized throughput in 30 s
  windows from t_fail + 60 s to the end.
- **Safety:** from code, not measured. No load path exists, so every
  transaction since the original start is lost, and the restarted cluster
  begins from MovR's generated initial state. The harness has no acked-write
  ledger; adding one would mean instrumenting Caerus's client, which is out of
  scope for this round.

## Predictions

| ID | Prediction | Confidence |
|---|---|---|
| R1 | R6: cluster T_regain <= 45 s in every trial, both workloads | High (exposure: 23-29 s cold start) |
| R2 | R6: every region's sustain window >= 0.8 in every window | High |
| R3 | R5: the five-region cluster starts, and cluster T_regain <= 45 s in every trial | Moderate; untested configuration |
| R4 | R5: every region's sustain window >= 0.8 in every window (normalized against the six-region pre-fault median) | Low-moderate: five regions change the key layout and the cross-region mix |

## Interpretation, fixed now

- **The row** (R1/R2 hold): idealized restart regains in under a minute and
  loses everything. Recovery never regains. Break-even detection delay against
  the recovery arm: unbounded within the observed 600 s. Restart wins on time
  at any detection delay we can observe, recovery "wins" on safety only in the
  sense that its survivors keep their state while serving nothing, and neither
  arm delivers the paper's claim.
- **R3 fails** (five-region restart does not start or never regains): the
  artifact has no restart at all for a permanent region loss. That is a
  stronger row; report the failure verbatim and do not repair it.
- **R4 fails alone:** report the throughput change and don't call it a stall
  unless it falls below 0.05.
- Percent_mr 30 has no single-region recovery arm yet; C1 in the isolation
  round supplies it. Until then, its row cell reads "pending C1".
- Any change after the harness check is an amendment, recorded before the next
  trial.
