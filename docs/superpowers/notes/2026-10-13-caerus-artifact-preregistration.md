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

## Amendment 1 (2026-10-13, before any registered run; no kill observed)

Plumbing runs (no kill) with the registered image showed the baseline was
overloaded: ~600 committed txn/s per region against 2000 offered, scheduler
`pending` growing ~2400/s without bound. Cause: each process pins 23 threads
to CPUs 0-22 (8 `pthread_attr_setaffinity_np` calls on the Caerus path), so six
co-located regions shared 23 cores. A saturated baseline invalidates the
normalised metric (survivors would gain CPU after the kill).

Change: image `caerus-run:aaf0eb6-nopin` (`external/caerus/run/Dockerfile.nopin`)
removes those 8 affinity calls and nothing else; Linux places threads. Plumbing
result: ~1900 committed txn/s per region, `pending` near zero, ~12 cores and
~5.5 GiB per region. Synchronized throughput dips across all six regions
(to ~0.13 of normal, a few seconds long) occur with and without pinning; they
are artifact behaviour and are left in the baseline mean.

Also added: `--memory=8g` per container to protect the shared host. A region
that is OOM-killed is reported as such (`docker inspect .State.OOMKilled`), not
read as a throughput result.

Predictions, thresholds, workload and failure schedule are unchanged.

## Amendment 2 (2026-10-13, before any registered run; no kill observed)

**Statistic.** Plumbing logs (no kill) show synchronized per-second dips to
~0.13 of normal in every region while 30 s means stay at 0.99-1.04. A
per-second reading of A1 ("> 0.8 for the full 600 s") would therefore fail on
baseline behaviour alone. All thresholds are evaluated on non-overlapping 30 s
window means of per-second merger throughput (`SEQUENCER THROUGHPUT -- Sent`),
normalised to the region's mean over [t_fail - 60 s, t_fail), windows aligned
to start at t_fail. Concretely:
- A1: every post-failure window mean > 0.8, at every live region.
- A2/A3: at every live region, some window starting at or before
  t_fail + 90 s (A2) / t_fail + 30 s (A3) has mean < 0.05.
- A4: every later window at that region also has mean < 0.05.
- Falsifier for A2/A3: the window starting at t_fail + 300 s has mean > 0.2
  at any live region.
- A live process with no throughput line in a window scores 0 for that window
  and the window is flagged in the output.
Implemented in `experiments/caerus_artifact_analyze.py`, committed with this
amendment.

**A5 method.** The registered check (dump a record before the kill and after
restart) needs a call to `SimpleStorage::DumpToFile`, which nothing in the
artifact invokes. Code inspection decides A5 instead: the only file read
anywhere in the source is the cluster config (`cluster_config.cc:131`), and
startup populates storage with `Movr::InitializeStorage` (`movr.cc:187`), so a
restarted cluster cannot hold any transaction committed before the restart.
A5 is therefore reported as a code-derived result, not a measurement.

**Run order.** Interleaved (percent_mr 0, 10, 30, then repeat) so host drift
does not line up with a condition. A pre-flight check aborts the campaign if
the host 1-minute load average is above 8 or available memory is below 60 GiB.

## Deviation log

**2026-10-14 ~07:37 PDT, after run 1 of 9 (pmr0_rep1), campaign still running.**
A plumbing check (container exit codes, intended to catch OOM) showed exit 139
(SIGSEGV) at all four live regions; killed regions show 137 (SIGKILL). To rule
out a harness fault I read run 1's logs around the kill, i.e. I saw run 1's
outcome before the campaign finished. Observed in run 1 (percent_mr = 0): live
regions committing ~1930 txn/s at t_fail; local `Completed` reached 0 within
~2 s, merger `Sent` reached 0 within ~3 s, and every live process exited with
SIGSEGV ~6 s after the kill. No error line precedes the crash.

Consequences for analysis, decided now, before runs 2-9 complete:
- Verdict thresholds are unchanged. The analysis script's rule "a live process
  with no throughput line scores 0" will score post-crash windows as 0. Every
  A-verdict will therefore be reported twice: as registered, and with crashes
  separated out. A crash is not the stall mechanism A2-A4 predicted, so A2-A4
  "holding" numerically after a crash will not be reported as support for the
  admission-cap/contagion mechanism.
- Exit codes and the time of each live region's last log line are reported per
  run.
- The campaign continues unchanged; nothing about runs 2-9 is altered.
