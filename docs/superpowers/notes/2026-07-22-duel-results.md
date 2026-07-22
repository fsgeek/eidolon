# Dueling-proposer sweep: results vs. pre-registration

## Prose bans (premortem D6, repeated verbatim)

- no FLP claims
- no Multi-Paxos authority claims
- no backoff-policy study

Results language: the lemma establishes reachability of the hazard; the
experiment measures its cost. The lemma establishes reachability of the
hazard; this experiment measures its cost as a function of relative
schedule offset.

## Provenance

- Pre-registration: `docs/superpowers/notes/2026-07-22-duel-preregistration.md`
  (committed 1864401, BEFORE the first full sweep).
- Data: `results/duel/duel_map.csv` (3060 rows: 8 conditions x 382
  offsets + 4 baselines), `results/duel/duel_jitter.csv` (2320 rows:
  116 cells x 20 seeds), `results/duel/duel_jitter_ci.csv` (116 cells).
- Grid: fine [-12.0, +6.0] step 0.05 s (0 excluded; 360 offsets) +
  coarse 11.0..116.0 step 5.0 s (22 offsets). Baselines (leo_enabled=0)
  at a single out-of-band offset (30.0). All band edges below carry the
  fine-grid resolution of +-0.05 s.
- Mechanical checks (plan Step 3, amended guard): P1 preemption-evidence
  guard PASS (0 rows), P3 band subset PASS, P5 baselines PASS.
  Outcome census (duel rows): earth_commit 1136, leo_commit 1002,
  leo_blocked 918. livelock 0, censored 0, no_decision 0.
- `decided_by`/`leo_commit` semantics: value provenance. A `leo_commit`
  outcome means LEO's proposed VALUE was decided; at k=5 the committing
  quorum was assembled by Earth (Paxos value adoption), at k=3 LEO
  itself committed (leo_success=1 on all 962 k=3 leo_commit rows;
  leo_success=0 on all 40 k=5 leo_commit rows).

## Per-prediction verdicts

| Pred | Predicted (summary) | Observed | Verdict |
|------|---------------------|----------|---------|
| P1 | Earth (k=5, leo_high, retries=1) fails exactly where a LEO round temporally overlaps Earth's round; rounds_overlapped=1 on every failed row; commits ~0.4 s elsewhere | Failure band is the FULL negative fine range [-12.0, -0.05]; 68 of its 240 offsets ([-12.0, -8.65]) fail with rounds_overlapped=0 — LEO has fully quiesced before Earth starts and Earth is still denied by LEO's standing ballot. Commits at all positive offsets at 0.183 s | **FALSIFIED AS WRITTEN** (temporal-overlap premise was wrong physics; see below) |
| P2 | Earth (k=5, leo_high, retries=5) ultimately commits at ALL offsets, latency elevated inside the band; livelock 0 or near-0 | Denial band identical to retries=1: all 240 negative offsets fail, every one burning the full 5 rounds (e.g. offset -12.0: 15 P1 + 20 P2 + 15 late NACKs). Livelock 0 | **REFUTED** — retry-budget ceiling: Earth's 5 rounds top out at ballot counter 5 while LEO's 8 rounds leave a standing counter-8 ballot; under leo_high polarity no ballot within Earth's budget outranks it, so extra rounds cannot help (same durable-state physics that falsified P1) |
| P3 | earth_high (k=5, retries=1) band strictly narrower than leo_high's | earth_high denial band [-12.0, -1.05] (220 offsets) is a strict subset of leo_high's [-12.0, -0.05] (240 offsets). Mechanical subset check PASS | **CONFIRMED** |
| P4 | k=3: capability gate certifies LEO (1,1); some offsets end leo_commit; decided value never forks | 962 k=3 leo_commit rows, all genuine LEO commits (leo_success=1), at negative offsets (plus +0.05/+0.1 under leo_high only) — failover race wins, not disruption. Zero leo_blocked rows in any k=3 condition. All in-harness safety certificates held (sweep completed with no assertion failure) | **CONFIRMED** |
| P5 | Baselines (leo_enabled=0) all commit, zero NACKs, retries=1 and =5 identical, latency ~0.4 s | All 4 baselines commit in 1 round, zero NACKs, retry-invariant. Latency 0.183 s (k=5) / 0.241 s (k=3) — structure exactly as predicted, magnitude below the predicted ~0.4 s | **CONFIRMED** (structure; predicted latency magnitude was imprecise) |
| P6 | No jitter cell's Wilson interval contradicts the deterministic map except band-edge cells; degenerate cells only deep inside or far outside the band | 111 of 112 duel cells agree with the map (95 degenerate at rate 0/1 on shared offsets; 16 far cells at offsets 30/90, all rate 1.0, bracketing map offsets 26/31 and 86/91 all commit). The single contradiction is earth_high k=5 retries=1 at offset -1.0: rate 0.95, Wilson [0.764, 0.991] vs. map commit — one grid step from the deterministic band edge at -1.05, the exact exception P6 carves out. It is also the only non-degenerate cell | **CONFIRMED** (with the pre-registered band-edge exception, observed once) |

### The durable-poison result (P1/P2, in the §B2 frame)

The hazard's denial window is not the spoiler's activity window; it
persists until superseded by a higher ballot. Paxos promises are durable
acceptor state: a spoiler's denial outlives its activity. Audit evidence
(controller audit, 2026-07-22): k=5, offset -12.0 — LEO's last round
ends t~16.6, Earth starts t=20.0, and Earth is still NACKed by standing
ballot 8502 held at the 3 LEO-reachable Earth acceptors. The no-overlap
failure band ends at offset -8.65 ~ LEO's 8-round attempt span,
identically in ALL FOUR k=5 conditions (both polarities x both retry
budgets) — polarity-independent, as durable-state physics requires. The
k=3 negative-offset leo_commit rows are legitimate failover race wins
(LEO commits before Earth starts), not disruption.

## Offset -> outcome map, per condition (band edges +-0.05 s)

All conditions: leo_max_rounds=8; Earth baseline latency 0.183 s (k=5)
/ 0.241 s (k=3).

**leo_high, k=5, retries=1** — Earth denied on [-12.0, -0.05] (240
offsets; the left edge is the grid boundary — the standing-ballot
mechanism implies denial at any earlier LEO start). Sub-band
[-12.0, -8.65]: denial with zero temporal overlap (standing promises
alone). Commits at every offset >= +0.05 in 1 round, 0.183 s, zero
NACKs. Outcomes: leo_blocked 240, earth_commit 142.

**leo_high, k=5, retries=5** — identical denial band [-12.0, -0.05];
every failing row burns all 5 rounds. Added cost inside the band: total
denial within the retry budget (no commit), 50 NACKs per trial (every
failing row: 15 P1 + 20 P2 + 15 late).
Outcomes: leo_blocked 240, earth_commit 142. Livelock 0.

**earth_high, k=5, retries=1** — Earth denied on [-12.0, -1.05] (220
offsets). Value-capture band [-1.0, -0.1]: Earth commits at full speed
(1 round, 0.183 s, zero latency penalty) but the decided value is LEO's
(Phase 1 adopts the value standing at the 3 poisoned acceptors). At
-1.05 the trial's certificate still resolves to LEO's value
(decided-or-inevitable) though Earth's own round failed. Earth's own
value decides from -0.05 upward. Outcomes: leo_blocked 219, leo_commit
20, earth_commit 143.

**earth_high, k=5, retries=5** — denial band [-12.0, -1.1] (219
offsets); the budget rescues exactly one edge offset, -1.05: commit in
2 rounds at 1.1955 s (+1.0125 s over baseline). Value-capture band as
above. Outcomes: leo_blocked 219, leo_commit 20, earth_commit 143.

**leo_high, k=3, retries=1** — LEO wins the race and commits on
[-12.0, +0.1] (242 offsets, leo_success=1); Earth's single round is
denied there (238 of those offsets with zero temporal overlap: Earth's
round arrives after LEO has already decided). Earth commits its own
value from +0.15 upward at 0.241 s. Outcomes: leo_commit 242,
earth_commit 140.

**leo_high, k=3, retries=5** — same LEO race-win band [-12.0, +0.1];
Earth now also commits inside it, carrying LEO's decided value, in
exactly 2 rounds at 1.251-1.3715 s (+1.010 to +1.131 s over baseline).
Outcomes: leo_commit 242, earth_commit 140. Earth failures: 0.

**earth_high, k=3, retries=1 and =5 (identical)** — LEO race-win band
[-12.0, -0.1] (239 offsets). Earth commits at EVERY offset in 1 round,
0.241 s — zero latency penalty; inside the band it commits LEO's
already-decided value. The retry budget is never used. Outcomes:
leo_commit 239, earth_commit 143.

**Cost summary.** Inside the contention band the cost takes one of
three forms: (a) total denial within the retry budget when the
spoiler's standing ballot outranks everything the budget can reach
(k=5 leo_high, both budgets; k=5 earth_high except one edge offset);
(b) one extra round, +1.01 to +1.13 s, where the budget suffices (k=3
leo_high retries=5; the single k=5 earth_high edge offset); (c) zero
latency cost with value-provenance capture — Earth commits on schedule
but the decided value is LEO's (k=5 earth_high near-band; k=3
earth_high in-band). Livelock count: 0 in all 3056 duel map rows and
all 2240 duel jitter rows. Censored: 0.

## Jitter sweep (secondary)

Seeds 40-59, per-link RNG, 14 offsets x 8 conditions + 4 baseline
cells. Baseline cells: success rate 1.0 (all 4). No livelock, no
censoring anywhere. Agreement with the deterministic map is total
except the single pre-registered band-edge cell noted under P6.

## Deviations (premortem D2: harness-bug signal first)

1. **First run attempt crashed at grid point one** — `earth_start` was
   not passed through the sweep call sites. Real harness bug; fixed in
   commit 65f404a (also added earth_start/tail to trial rows). The
   Step 3 gate did its job.
2. **Original P1 overlap guard failed 510 rows** on the second attempt
   (every Earth failure with rounds_overlapped=0). Controller audit
   EXONERATED the harness with acceptor-state evidence and FALSIFIED
   the guard's premise: standing promises are durable poison (evidence
   block above). The 510 rows decompose exactly: 272 = 68 x 4 k=5
   conditions (durable-poison band [-12.0, -8.65]) + 238 k=3 leo_high
   retries=1 rows (Earth denied after LEO had already decided — race
   loss, not disruption). Guard amended to the preemption-evidence
   guard (no Earth failure without some higher-ballot NACK) in commit
   7a1265f; the amended guard passes with 0 rows.
3. **P2 refuted** (see verdict table). No code change: the mechanism is
   the same audit-verified durable-state physics plus the retry-budget
   ceiling (5-round budget cannot outrank a standing counter-8 ballot
   under leo_high). Harness exonerated by the same acceptor-state
   evidence; recorded as a finding, not a bug.
4. **Baseline latency magnitude** — pre-registration guessed ~0.4 s;
   observed 0.183 s (k=5) / 0.241 s (k=3). Zero NACKs and
   retry-invariance held exactly as pre-registered; magnitude
   imprecision only, no structural deviation.
5. **Determinism re-verification** — for this record both sweeps were
   re-run from scratch at the same code and diffed against the prior
   generation's files: `duel_map.csv`, `duel_jitter.csv`, and
   `duel_jitter_ci.csv` are byte-identical (deterministic map; seeded
   per-link RNG jitter sweep).

## Cadence-quantized bounds

No recovery-style quantity appears in these CSVs: commit latencies are
event-driven message-arrival times, not cadence-polled observations.
No row in `duel_map.csv`, `duel_jitter.csv`, or `duel_jitter_ci.csv` is
a cadence-quantized bound.
