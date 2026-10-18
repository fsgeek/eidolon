# Caerus communication isolation experiment registration

**Registered:** 2026-10-18, in the signed commit introducing this file.
**Predictions by:** Codex (OpenAI).
**Parent:** [Authority composition hypotheses and
predictions](2026-10-18-authority-composition-preregistration.md).
**Status:** Methods fixed before the new intervention and harness are built.

## Question and predictions

Does isolating communication with a dead region restore local-only work,
and does a different dependency stop mixed-region work once communication
can proceed? This experiment separates a resource failure from a possible
failure of recovery. It does not assume the first is an authority defect.

| ID | Prediction in each valid failure trial | Confidence |
|---|---|---|
| C1 | With the baseline communication path, every survivor's normalized merger output is below 0.05 in every full 30-second window from fault +30 to +600 seconds, for both workloads | High; replication at 0%, extrapolation at 30% |
| C2 | With communication isolation and 0% multi-region work, each survivor's normalized application completion rate and merger output are at least 0.80 in every window from fault +450 to +600 seconds | Moderate; untested intervention |
| C3 | With communication isolation and 30% multi-region work, every survivor's normalized merger output is below 0.05 in every window from fault +450 to +600 seconds | Moderate; predicted second stall |

A valid trial contradicting a stated per-trial prediction falsifies it as
worded. Report all three repetitions; do not replace this rule by a majority
vote after results arrive. A missed threshold still counts when the program
is slow but the controls passed. A process crash is reported separately and
is not support for the predicted stall mechanism.

## Existing knowledge and source

This instance inspected the earlier logs: the last positive merger samples
in the single-region trials were about 3-7 seconds after the harness's old
recorded failure time. Twelve earlier debugger records, from a two-region
failure, show the multiplexer in a blocking send. They do not identify the
send destination. Those runs are background, not trials in this registration.
Earlier 30% trials killed two regions; the 30% single-region condition here
is new. The same stall is predicted, rather than claimed to have been measured.

Pin Caerus to `aaf0eb64ddbc89f9c45593fd60d0dba9686be1d5` and ZeroMQ to
`v4.3.5`, matching `external/caerus/Dockerfile`. Both versions use the
existing no-CPU-pinning placement change, throughput logging, and
`RUNTIME=1200`. Record the actual dependency commits and image digests in
the signed implementation manifest. A source or dependency substitution
requires an amendment before fault runs.

## Intervention

The baseline reproduces the existing communication path. The experimental
version separates pending sends by destination and lets the communication
loop continue receiving and serving other destinations while one destination
cannot accept messages. Preserve the original per-destination message order,
retry unaccepted sends, and retain the distinction between a locally queued
message and a delivered message. Do not synthesize acknowledgements, skip
required transaction participants, or alter sequencing, merging, recovery,
or admission rules.

Use bounded per-destination pending payload storage of 64 MiB. On reaching
the bound, apply backpressure to work for that destination while other
destinations remain serviceable. Do not silently discard, expire, or
acknowledge that work. Record queue bytes, queue lengths, and where
backpressure reaches producers. If the current APIs cannot express this
policy without changing application admission or message semantics, stop
and amend the design before fault trials. An implementation that drops
messages to appear live is not this intervention.

At each blocked/retried send, record its actual destination, channel,
message type, return status, and time. Logging must identify whether the
blocked destination is the failed peer; do not infer it from the failure
schedule. Apply the same observation changes to both versions.

## Setup and schedule

- Six region processes, one machine per region, containers on one host.
  Reuse the Table-1 delay matrix and MovR workload in the prior harness:
  `percent_mp=0`, batch size 10, hot records 240, `percent_mr` in `{0,30}`.
- Stop only region 2 (france-c), within the previously tested K=1 shape.
  All five other regions remain running. No silent network failure, restart,
  or additional failed region is part of this round.
- Give containers a unique experiment prefix. Never remove unrelated
  containers or reuse the historical global `r0`-`r5` names.
- After all six processes report positive output for ten consecutive seconds,
  warm for 120 seconds. Then issue the failure and observe for 600 seconds
  after confirmed process exit. Collect an additional 30 seconds solely to
  flush logs. Controls use a synthetic fault time with the same duration.
- Record fault-request start, request completion, the killed container's
  `State.FinishedAt`, and observation end. Align analysis to `FinishedAt`,
  not the time before a possibly slow `docker kill`. Capture wall and
  monotonic clocks and flag clock discontinuities.
- Keep the previous 8 GiB container memory limit. Begin only with host
  available memory at least 60 GiB and one-minute load below 8. Record host
  pressure throughout. Abort before fault injection on a failed preflight;
  resource pressure arising afterward is reported with the trial.

## Trial count and order

There are two versions and two workloads. Run two full-duration no-failure
controls per version/workload (eight controls total), before any failure
trial. If a control fails, stop this round and investigate; do not tune the
registered fault settings to rescue it.

If all controls pass, run three failure trials per version/workload (twelve
failure trials). For repetitions 1 and 3 use baseline/0, isolated/0,
baseline/30, isolated/30. For repetition 2 reverse that order. Do not run
cells concurrently or overlap them with another experiment campaign. This
is a mechanism experiment, not an estimate of production failure prevalence.

A no-failure control passes if every region remains running without the
programmed shutdown, produces a sample in every 30-second analysis window,
and has normalized merger output and application completions above 0.80
throughout the synthetic post-fault interval. Instrumentation and replay
checks must also pass. Normalize each metric to that metric's own mean over
the 60 seconds immediately before the fault or synthetic fault time.

## Observations and correctness checks

Compute non-overlapping 30-second windows aligned to the fault, ending at
600 seconds. Record sample counts and raw rates before normalization.
Missing samples from a running process are missing data, not measured zero.
They invalidate that cell's rate verdict; report the missing interval.
An explicit logged rate of zero is measured zero. Record application
completions separately from merger output; merger output is not a client
acknowledgement.

Collect process state and memory, scheduler backlog, conflict-graph size,
active and waiting transactions by origin, and pending sends by destination.
Take three debugger samples two seconds apart at fault +60 and +300 seconds
in a separately labeled diagnostic replication of any stalled isolated cell.
Diagnostic runs do not replace primary trials and their timing is not used
for primary throughput scoring.

Record unique transaction identities, their operations, execution order and
application completion events. Check duplicate application, incompatible
replica results, and completed operations against an independent replay of
the recorded operations and ordering constraints. The replay must not reuse
the production merger or scheduler. Test it on a known-valid history and on
histories with a removed write, duplicate application and inconsistent
replica value before the campaign. A detectable correctness violation
precludes a claim that the intervention restores correct useful service.

If this prototype exposes no client success contract or enough information
for the replay, record that limitation before the fault campaign. C1-C3's
rate observations may still be scored, but C2 cannot support restoration of
correct client service, and the experiment yields no client-level safety
verdict. Logging neither an error nor a conflicting decision is not a
safety proof. Do not silently promote an internal completion to client
success to fill this gap.

## Mechanism and interpretation

- C2 holding, with correctness evidence, supports communication isolation as
  sufficient for local-only service in this setup. It does not establish an
  authority defect or repair. A C2 contradiction remains on the record even
  if an implementation defect is subsequently found. Diagnose whether a
  blocked live-peer path violates the registered communication policy or
  exposes another shared application resource. Backpressure propagated by
  the registered queue cap is a design limitation, not grounds for excluding
  the trial or silently increasing the cap.
- C3's rate condition alone does not identify why mixed-region work stops.
  The predicted second mechanism requires persistent incomplete transactions
  and active-transaction/admission observations consistent with the code's
  per-origin limit. An empty conflict graph or another blocked transport
  path defeats that mechanism explanation even if C3 holds numerically.
- Useful sustained output at 30% defeats C3. Unsafe output defeats any claim
  of successful recovery even if its rate exceeds 0.80.
- If the baseline no longer stalls, score C1 as contradicted after checking
  the source pins and observation validity. Do not discard it simply because
  it differs from our earlier campaign.
- No finite timeout is reported as permanent nontermination. A stall is
  non-recovery within this registered observation interval.

Before trials, sign the implementation, build manifests, observer and checker
validation outputs, exact invocation, and this registration's commit hash.
Use `results/authority-composition/caerus/<implementation>/<attempt>/`.
Apply the parent registration's correction and preservation rules.
