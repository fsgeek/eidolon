# Pre-registration: dueling-proposer sweep predictions

Committed BEFORE the first full sweep run. Deviations from these
predictions are treated as harness-bug signals FIRST (premortem D2);
only after the harness is exonerated do they become findings.

## Grid and conditions (fixed)
Offsets: fine [-12.0, +6.0] step 0.05 (0 excluded) + coarse to 118.0
step 5.0. Conditions: polarity {leo_high, earth_high} x k {5, 3} x
earth_max_rounds {1, 5}; leo_max_rounds 8. Deterministic map primary;
jitter sweep seeds 40-59 on the reduced offset set. The FULL curve is
reported for every condition (no selected offsets).

## Predictions

P1 (k=5, leo_high, earth retries=1): Earth fails exactly on the offsets
where a LEO round overlaps Earth's single round; elsewhere commits
~0.4s. The failure band edges align with LEO's Phase 1 span (~9.3s of
repeated attempts for offsets < 0) and Earth's round span (< ~1.5s for
offsets > 0). rounds_overlapped=1 on every failed row (a failure with
rounds_overlapped=0 is a harness bug).

P2 (k=5, leo_high, earth retries=5): Earth ultimately commits at ALL
offsets (its ~1.2s escalation cycle out-paces LEO's ~1.1s+ cycle enough
to land a fresh higher counter), with commit latency elevated by one to
a few retry cycles inside the collision band; livelock count 0 or
near-0. If livelock appears, report the offsets; do not tune backoff.

P3 (k=5, earth_high, retries=1): strictly weaker disruption than P1 -
equal-counter ties go to Earth, so only a LEO promise from a LATER
counter blocks. The failure band is narrower than P1's; if it is not,
suspect ballot-rank wiring (premortem A2 gate).

P4 (k=3, any polarity): the capability gate certifies LEO (1,1); LEO
becomes a legitimate failover peer. Some offsets end leo_commit (with
Earth's value carried where Earth's Phase 2 partially landed first).
decided_value is NEVER two different values across acceptors' final
quorum certificate (safety; a violation fails the trial's assert).

P5 (baselines, leo_enabled=False): earth_commit at every condition,
zero NACKs, latency ~0.4s (retries=1 and =5 identical - no contention).

P6 (jitter sweep): no cell's Wilson interval contradicts the
deterministic map's outcome at the same offset except inside the
collision band's edge cells (edge offsets may flip under +-jitter of
link latencies); degenerate cells appear only deep inside or far
outside the band.

## What would make us stop and audit the harness
- Any failed Earth row with rounds_overlapped=0 (P1 violation).
- Any commit without a decision certificate, or certificate/proposer
  disagreement (trips an assert).
- earth_high band wider than leo_high band (P3 violation).
- Baseline rows differing across retries (P5 violation).
- k=3 rows still showing DISRUPTIVE_ELECTION-style pure spoiling
  (capability gate should have made this impossible).
