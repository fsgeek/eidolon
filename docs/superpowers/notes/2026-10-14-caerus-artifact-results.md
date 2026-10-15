# Results: Caerus artifact under region failure

Pre-registration with Amendments 1-3 and deviation log:
`2026-10-13-caerus-artifact-preregistration.md`. Diagnostic:
`2026-10-14-caerus-stall-backtrace-preregistration.md`.

## History, briefly

The first campaign (`results/caerus/artifact/`) is **void**: the kill at
t_ready + 120 s coincided with the artifact's compiled-in 120 s run timer
(`RUNTIME`), so every run observed a programmed shutdown and its teardown
segfault, not failure behaviour. Explanations offered for it (a transport
freeze; a spontaneous crash) were withdrawn. Found by a no-kill control.

## Valid campaign (`results/caerus/artifact-rt1200/`, RUNTIME 1200)

Gate: two full-length no-kill controls passed (all regions alive at t_end, no
stop line, every 30 s window > 0.8). Nine runs, interleaved, all killed
regions exit 137, all live regions alive at t_end. pmr30_rep3 had a harness
readiness misfire (warm-up ~340 s instead of ~120 s); verdicts are identical
with and without it.

Normalised merger throughput at the four live regions, first 30 s window after
the kill / every later window:

| percent_mr | first window | windows 2-20 |
|---|---|---|
| 0  | 0.093-0.105 | 0.0 |
| 10 | 0.032-0.054 | 0.0 |
| 30 | 0.023-0.049 | 0.0 |

- **A1 (percent_mr = 0 unaffected): falsified, 3/3.** Single-region work stops
  completely within ~3-10 s of the kill and does not resume in 600 s.
- **A2, A3, A4: hold numerically, 6/6**, falsifier not triggered. **Not support
  for the predicted mechanism.** The conflict graph is empty after the stop
  ("remaining size 0", scheduler pending 0) — there are no stuck transactions.
  The stall is global and upstream of the merger, identical at 0% multi-region.
- **A5 (restart loses all state):** holds, from code.

## Mechanism (post-hoc hypothesis, then registered test)

`results/caerus/diag-backtrace/pmr0/`: percent_mr = 0, gdb at kill + 60 s,
three samples 2 s apart at each live region. **12/12** backtraces show the
process's single `ConnectionMultiplexer::Run` thread (`connection.cc:225`)
inside a blocking ZMQ PUSH `send()` (`flags_=0`), waiting in `poll` with
infinite timeout. That one thread carries all of a process's message traffic,
local and remote, so the whole process receives nothing.

Not verified: which peer the blocked send targets (consistent with a dead
peer at the ZMQ high-water mark, not proven), and whether a real WAN failure
(no RST, silent loss) behaves identically to `docker kill`.

## What this means

In the released artifact, the death of any region stops every surviving
region within seconds, regardless of workload, through the transport layer —
before the consensus-level questions (K-replication, recovery, merger
contagion) arise at all. The paper's region-level fault-tolerance mechanism
is not in the release (code read), and the release's actual failure behaviour
is a total stall that only a restart ends — and a restart loses all state
(A5). Against the restart baseline: recovery does not beat a restart because
there is no recovery, and the restart itself recovers nothing.

The merger-contagion question the model raised is untested by this campaign:
the transport stall masks it.

## Single-region failure (f = 1 <= K = 1)

Pre-registration: `2026-10-14-caerus-single-region-preregistration.md`. Data:
`results/caerus/artifact-rt1200-single/` (percent_mr = 0, france-c killed,
3 reps; all regions ready, no stop line, killed region exit 137, all five
live regions alive at t_end).

| rep | first window (5 live regions) | every later window |
|---|---|---|
| 1 | 0.115-0.148 | 0.0 |
| 2 | 0.108-0.141 | 0.0 |
| 3 | 0.113-0.115 | 0.0 |

- **S1, as worded, is ambiguous and is reported both ways.** Strict reading
  (window 0 itself < 0.05): fails, because window 0 contains the ~4 s before
  the stall. Reading consistent with A3's definition (some window starting at
  or before t_fail + 30 s < 0.05): holds. Substance either way: every survivor
  stops within ~4 s and stays at 0 for the remaining 570 s.
- **S2 holds:** conflict graph empty (`remaining size 0`).
- **Falsifiers not triggered** in any rep.

So the failure the paper says it tolerates stops every surviving region, at a
workload with no multi-region transactions, and the only exit (restart)
discards all state.
