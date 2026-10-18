# Additive registration: does bounded isolation remove the dependency or delay it?

**Registered:** 2026-10-18, in the signed commit introducing this file, before
the communication-isolation intervention is implemented or run.
**Predictions by:** Claude (Opus 5.5, the Eidolon instance). Codex's
predictions C1-C3 in
[the Caerus isolation registration](2026-10-18-caerus-communication-isolation-preregistration.md)
are not modified; this file adds predictions and one observation to that round.

## Why

The isolation intervention keeps the dead region in every send set and stores
its messages in a bounded 64 MiB per-destination queue, with backpressure at
the bound and no discard. That cleanly separates a *resource* repair (stop one
destination from blocking the communication loop) from an *authority* repair
(revoke the dead region's membership so nothing is addressed to it). But a
bounded queue that only fills cannot remove the dependency on the dead peer;
it can only postpone the point at which backpressure reaches producers. If
that point lies beyond fault + 600 s, C2 can hold for a reason that says
nothing about whether the stall was a resource failure or an authority one.

From the code read (prior exposure: `partial_sequencer.cc:219-222` sends a
batch message to every region every 5 ms epoch, empty or not), traffic
addressed to the dead region continues at every workload, including 0%
multi-region. I have not measured message sizes, so I do not predict a time.

## Predictions (isolated version only)

- **Q1.** In every isolated failure trial, pending payload bytes queued for the
  dead region increase over [fault + 30 s, fault + 600 s] with no sustained
  decrease (least-squares slope > 0, and no 60 s interval with net decrease
  of more than 1% of the queue).
- **Q2.** The projected time to the 64 MiB bound, T = (64 MiB - B(600)) /
  slope over [fault + 300 s, fault + 600 s], is finite in every isolated trial.
- **Q3.** In one extended isolated trial per workload (same configuration,
  observation continued to min(T x 1.5, 3 h) after the fault), backpressure
  reaches at least one producer within 10% of the projected T, and survivor
  merger output then falls below 0.05 in every later 30 s window.

## Interpretation, fixed now

- **Q1-Q3 hold and C2 holds:** communication isolation restores local-only
  work only until the queue to the dead peer fills. The dependency is on the
  dead region's continued *membership*, not only on a shared thread; sustained
  service needs a revoke. Isolation without revoke trades a fast stall for a
  delayed one (the "block or grow" pair: Caerus blocks, Detock grows).
- **Q1 fails** (queue to the dead peer stops growing): something already
  stops addressing the dead region, so the stall was a resource failure and
  the "revoke absent" reading of Caerus is wrong. Report it as such; it would
  correct a claim we and the cloudflare thread have both made.
- **Q3 fails** (no stall after the bound): backpressure on one destination
  does not reach the work survivors need; the dependency is weaker than
  predicted.
- None of this is a safety claim. C2's correctness checks govern that.

## Observation added to the round

Sample pending bytes and message counts for the dead-region queue at 1 s
intervals from fault - 60 s to the end of observation, plus the time and
producer at which backpressure first applies. This is the queue telemetry the
parent registration already requires, sampled at a fixed rate so Q1-Q2 can be
computed. The extended trials are additional and do not replace primary trials.
