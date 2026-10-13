# Pre-registration: Caerus artifact under region failure

**Written and committed before any Caerus process has been started.**
Predecessors: `2026-10-11-caerus-contagion-preregistration.md` (model),
`2026-10-11-caerus-contagion-results.md` (model results + code-read addendum).
Build: `external/caerus/Dockerfile` (image `caerus:aaf0eb6`, commit `00f9754`).

## What is being tested

Whether the released artifact behaves as the model plus the code read
predict when regions die: no recovery exists in the code, stuck transactions
never release, and the per-origin admission cap (more than 2000 active local
transactions stops admission, `partial_sequencer.cc:195`) turns stuck work
into a full stop at each live origin.

## Setup (fixed before running)

- Six regions, one machine each (`<id>:<replica>:<host>:<port>`, as in
  `local.conf`), one container per region on one Docker network.
- One-way delays via `tc netem` from the paper's Table 1 bounds, same matrix as
  `caerus_model.py` (NA-NA 3 ms, EU-EU 6, AP-AP 17, NA-EU 41, EU-AP 80,
  NA-AP 100). Region order as paper section 5.1: us-east, us-east-2,
  france-c, eu-west, se-asia, east-asia.
- Workload: MovR (`--experiment=MovR`), `--percent_mp=0`, `--max_batch_size=10`
  (the README example), `--hot_records=240` (README example), default
  locality. `--percent_mr` in {0, 10, 30}.
- Failure: `docker kill` (SIGKILL) of france-c and east-asia at t = 120 s after
  all six report throughput. Observation continues to t_fail + 600 s.
- Repetitions: 3 per percent_mr.
- Metric: per-region committed throughput as logged by the merger
  (`SEQUENCER THROUGHPUT ... txns/sec`) at the four live regions, normalised
  to each region's mean over the 60 s before the kill.

### Departures from the artifact, all measurement or placement

1. Compiled with `-DTHROUPUT` so the merger logs throughput (the logging code
   exists behind that macro; it is not defined by default).
2. All six processes share one host; their hard-coded thread affinities
   (`connection.cc`, `merger.cc`) collide on the same CPUs. Absolute
   throughput is therefore not comparable to the paper; only normalised
   throughput is reported.
3. Latency is emulated, not WAN.

Any further change to their code needed to make it run is a departure that
must be recorded in the results before results are looked at.

## Predictions

- **A1. percent_mr = 0: live regions keep pre-failure throughput** (normalised
  > 0.8 for the full 600 s). Lowest-confidence prediction: if the scheduler or
  connection layer waits on every region (e.g., per-origin batch ordering in
  `deterministic_scheduler.cc`), throughput falls for a reason outside the
  merger. That outcome falsifies A1 and is itself a finding: a dependency on
  every region that the paper does not describe.
- **A2. percent_mr = 10: every live region's throughput falls below 0.05
  within 120 s of the kill.** Arithmetic: README load is 200 x batch_size =
  2000 txn/s per machine; 10% multi-region with 2 of 5 other regions dead puts
  ~80 txn/s/region directly into permanent incompleteness, reaching the
  2000-transaction admission cap in ~25 s before counting contagion.
- **A3. percent_mr = 30: same as A2, faster** (below 0.05 within 60 s).
- **A4. No recovery:** for percent_mr > 0, throughput stays below 0.05 for
  the remainder of the 600 s window.
- **A5. Restart baseline loses all state.** Storage is in-memory
  (`simple_storage.cc`; `DumpToFile` is a debug dump, not a log). Restarting
  all six processes yields a running system with no transaction committed
  before the restart. This is predicted from the code and will be checked by
  dumping a record's value before kill and after restart.

## Falsifiers

- A2/A3: any live region sustaining normalised throughput > 0.2 at
  t_fail + 300 s.
- A5: any record value written before the restart readable after it.
- If the artifact cannot be made to run without changes to sequencing,
  merging or admission logic, the experiment is void, not a result; we report
  that instead.
