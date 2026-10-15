# Pre-registration: single-region failure (f = 1 <= K = 1)

**Written 2026-10-14, before the run.** Predecessor:
`2026-10-14-caerus-artifact-results.md` (two regions killed: every survivor
stops within 3-10 s; multiplexer thread blocked in ZMQ `send()`).

## Why

The paper's guarantee (section 4.7, Lemma 4.11) covers up to K region failures;
the deployment shape we test corresponds to K = 1. The completed campaign killed
two regions, outside that guarantee. This run kills one: the failure the paper
says it tolerates.

## Setup

Exactly as the rt1200 campaign (image `caerus-run:aaf0eb6-nopin-rt1200`,
six regions, Table-1 delays, MovR, batch 10, 240 hot records, warm 120 s,
observe 600 s), except `DEAD=(2)` (france-c only). percent_mr = 0, 3 reps;
run back-to-back. Harness: `run_failure.sh` with a `DEAD` override
(`DEAD_REGIONS="2"`), committed with this note. Gate: the rt1200 no-kill
controls already passed; not re-run. Analysis: the registered Amendment 2
statistic over the five live regions (0, 1, 3, 4, 5).

## Predictions

- **S1.** Every live region falls below 0.05 normalised throughput in the
  first 30 s window starting at or before t_fail + 30 s, and stays below 0.05
  for every later window (transport stall is indifferent to how many peers die).
- **S2.** The conflict graph is empty afterwards (`remaining size 0`), i.e. no
  stuck transactions — same mechanism as the two-region runs.

## Falsifiers

- Any live region with a window > 0.2 at t_fail + 300 s.
- Any live region whose merger throughput stays > 0.8 in every window (single
  failure tolerated in practice, contrary to the transport-stall reading).
