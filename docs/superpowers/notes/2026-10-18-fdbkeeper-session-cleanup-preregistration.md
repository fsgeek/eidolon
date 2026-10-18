# FDBKeeper session cleanup experiment registration

**Registered:** 2026-10-18, in the signed commit introducing this file.
**Predictions by:** Codex (OpenAI).
**Parent:** [Authority composition hypotheses and
predictions](2026-10-18-authority-composition-preregistration.md).
**Status:** Methods fixed before building or executing this experiment.

## Question and predictions

Can a cleanup decision based on an old session expiration remove an object
created after that session successfully renews? Does checking the old session
key again within the cleanup transaction prevent that deletion?

This tests whether the existing code-derived counterexample occurs in the
running system. It is not a blind discovery: this instance read Cloudflare's
model, issue draft, and the pinned cleaner and session source before writing
these predictions. Cloudflare's earlier artifact-selection rule chose this
system; the system was not selected here for an expected failure.

| ID | Prediction under the confirmed schedule | Confidence |
|---|---|---|
| F1 | In every released-version race trial, cleanup removes the first client's new object while the renewed session remains live | High, conditional on the real renewal/create path permitting the schedule |
| F2 | In every repaired-version race trial, that cleanup preserves the object and the second client's conflicting create fails because the object exists | High for this specific repair and race |
| F3 | When renewal completes before the cleaner scans, both versions preserve the subsequently created object | High; negative control |

Any valid contradictory trial falsifies the corresponding per-trial
prediction. We do not require all ten trials to contradict it. An impossible
renewal/create sequence can refute the model's reachability claim; it is not
silently discarded as an inconvenient setup failure.

## Pin and feasibility limit

Pin `https://github.com/DASE-iDDS/FDBKeeper` to
`58c2ff7a841b1857ddac40a864a5f4d100471272`. Use its documented local build
and configuration with a dedicated local FoundationDB test cluster and no
paid cloud services. Record exact FoundationDB version, dependency revisions,
compiler, image digests and build commands in a signed implementation
manifest before outcome trials. Keep the same dependencies in both versions.

Limit the first build/stand-up attempt to four hours of elapsed active work,
excluding time waiting for Tony or unrelated machine downtime. Preserve the
build log and errors. If the artifact cannot be built and reached through its
actual client interface within this attempt, report a feasibility failure.
Do not substitute the existing model or redraw the artifact without a new
signed amendment following Cloudflare's selection rule. Feasibility does not
score F1 or F2.

## Interface and observation

Use two clients with distinct sessions and a fresh private namespace for each
attempt. The protected object is one ephemeral node at `<namespace>/lock`.
Request a 60-second session timeout; record the server-accepted timeout and
session identifier. Pin the exact client library and record response codes,
session-state events, object state, and server-side transaction outcomes.

An observer marks the cleaner's completed read of the expired-session list,
the heartbeat transaction's commit, object creation, and cleanup commit or
abort. A barrier pauses only between existing cleaner operations. It must
not change FoundationDB conflict handling, expiration checks, heartbeat
logic, session-state events, or object creation rules. Both versions receive
the same observation and barrier instrumentation.

The race outcome must be inspected within ten seconds after successful
renewal and at least five seconds before the accepted new expiration. If
the actual timeout or scheduling cannot satisfy this condition, document
the obstruction and amend before trials. Do not extend deadlines after
seeing whether the object disappeared.

## Forced race

1. Establish client A's session and record its initial expiration key.
2. Delay A's heartbeat long enough for the cleaner to list that expired key.
   Pause the cleaner after that scan and before its cleanup transaction.
   Preserve A's actual session-state handling; do not suppress an expiration
   error to make it appear live.
3. Let A's real renewal path run. Require a successful committed renewal
   with the same session identity and a later expiration key.
4. While the cleaner is paused, have A create the ephemeral object. Require
   a successful response and independently confirm that the object exists
   under A's ownership.
5. Resume the cleaner and record its completion or transaction rejection.
   Re-read the object and the renewed session state. In every trial, ask
   client B to create the same object under its own session and record the
   response and object ownership. F2 predicts rejection when A's object
   survives; F1 predicts B can create after the object is deleted.
6. After recording the race outcome, remove B's corroborating object if B
   created one and confirm its deletion. Explicitly close A and verify
   normal cleanup permits a fresh create by B. In a trial where A's object
   survived, closing A must actually remove it. A repair that prevents all
   cleanup has not passed the cleanup control.

If step 3 or 4 is rejected because the real session rules forbid the modeled
sequence, record the exact state, error and guard. This contradicts the
reachable-race premise under that interface. Distinguish it from a build
error, unavailable server, or broken barrier, which is inconclusive.

The primary violation is deletion of A's newly created object by cleanup
based on the older expiration, despite successful renewal and a still-live
session. B's create is corroboration. Two clients' beliefs alone are not
counted as two valid lock holders or conflicting application effects. Any
claim about mutual exclusion requires a separately defined application
contract and observation of its effects.

## Intervention and negative control

The repaired version reads the specific old session key non-snapshot within
the cleanup transaction. If it is absent or renewed, skip cleanup for that
old record. If it still names the expired session, perform its invalidation
and object cleanup in that same transaction, preserving transaction conflict
checks. This is the narrow paper-inspired repair already represented by the
existing model. Do not replace cleanup with an unconditional no-op or add an
unrelated delay to avoid the race.

The negative schedule pauses the cleaner before its scan, lets A renew,
then permits the scan. A creates its object before cleanup proceeds. The
old expired record must not authorize deletion in this schedule. Verify
the object state and B's conflicting create as in step 5, then normal
cleanup after A closes as in step 6.

## Trials, controls and order

First run two no-fault lifecycle controls per version: create, renew, verify
continued ownership, close and verify cleanup. Controls must finish without
spurious object loss, session confusion, or a disabled cleaner. Also test
that the observer notices an intentionally deleted disposable object and
distinguishes a failed create from a successful create; these are instrument
checks, not race trials.

Then run ten repetitions of each combination of version
`{released,repaired}` and schedule `{race,renew-before-scan}`: 40 primary
trials, plus the four lifecycle controls. Each repetition uses fresh sessions
and namespaces. In odd repetitions use released/race, repaired/race,
released/negative, repaired/negative; reverse that order in even repetitions.
Run serially without competing experiment campaigns.

For every attempt, record whether the prescribed interleaving actually
occurred. A trial with a failed barrier or missing observation remains in the
denominator of attempts and is reported as inconclusive, not as a pass. Stop
on an instrument defect and apply the parent correction protocol before a
new attempt. A functioning instrument that demonstrates an impossible
precondition is evidence against reachability, as described above. Report
counts of scored, contradicted, unreachable and inconclusive attempts for
each cell; never quietly replace a failed attempt to reach ten successes.

## Interpretation and record

F1 holding establishes this session-cleanup defect in the tested release and
schedule. F2 holding supports this repair for that race. It does not prove
all session recovery safe or establish a consensus defect in FoundationDB.
F3 failing requires investigation of additional behavior while retaining its
contradicted verdict; it is not support for F1's predicted race.

An unexpected deletion in the repaired version falsifies F2. Preserve and
minimize the trace before broadening the repair. A live session protected
by an unmodeled check in the released version is a result against F1, not a
reason to remove that check from the experiment.

Sign the instrumentation, source patch, client driver, observation checks,
build manifest and exact commands before primary trials. Store each attempt
under `results/authority-composition/fdbkeeper/<implementation>/<attempt>/`.
Record the registration and implementation commits, actual response codes,
session identities and deadlines, snapshot/renew/create/cleanup order,
transaction outcomes, object ownership and final cleanup. Sign and commit
each results round and every correction using the parent protocol.
