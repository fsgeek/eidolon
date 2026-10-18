# Authority composition hypotheses and predictions

**Registered:** 2026-10-18, in the signed commit introducing this file.
**Predictions by:** Codex (OpenAI), following discussion with Tony Mason.
**Stage:** Before implementation or execution of the new experiments below.

## Read this first

We are investigating whether failures of useful service can be predicted by
examining how one component's decision authorizes another component's actions.
An individually correct consensus protocol does not establish that all of the
dependencies needed to use or replace its authority remain available.

My predictions are deliberately mixed. I expect a communication repair to
restore Caerus's local-only work without an authority repair. I expect its
mixed-region work to remain vulnerable to a separate dependency. I expect
FDBKeeper's session cleanup to act on obsolete evidence, and revalidation to
prevent the specific deletion predicted by the existing model. These outcomes
could separate resource dependencies from authority dependencies instead of
supporting one explanation for everything.

| Prediction | Expected outcome | Evidence already seen |
|---|---|---|
| C1 | Caerus with its existing communication path stalls after one region stops | Prior runs and debugger records |
| C2 | Isolating a dead peer's communication restores sustained local-only work | Code-read reports and the Python model; this intervention is untested |
| C3 | At 30% multi-region work, communication isolation exposes another persistent stall | Prior merger model and code-read reports; untested in the isolated implementation |
| F1 | FDBKeeper cleanup deletes a newly created object belonging to a successfully renewed session under the forced schedule | Existing code read and TLA+ counterexample; no execution |
| F2 | Revalidating the old session key in the cleanup transaction prevents that deletion | Existing model; no execution |
| M1 | A model with individually valid components admits a composed failure when evidence between their authority scopes is omitted | Reasoned expectation; model not built |
| M2 | Explicit checks and state transfer suffice for safety in the proposed finite model; progress additionally needs available handoff dependencies | Reasoned expectation; model not built |
| G1 | Authority analysis will make useful additional predictions on at least one of the next three eligible, previously unexamined systems | Low-confidence research expectation; targets not selected |

C1-C3 have the scoring rules in
[the Caerus registration](2026-10-18-caerus-communication-isolation-preregistration.md).
F1-F2 have the scoring rules in
[the FDBKeeper registration](2026-10-18-fdbkeeper-session-cleanup-preregistration.md).
M1-M2 and G1 are recorded expectations, not executable registrations: their
finite model, target selection, comparison predictions, and scoring methods
must be signed before their experiments are implemented. They cannot be
reported as confirmed under this file alone.

## Hypotheses and competing explanations

**H1, prediction.** Under specified faults within a system's advertised
failure envelope, a map of authority relationships makes correct prospective
predictions about service failure that quorum availability alone does not.
For an authority relationship, identify the authorizing decision, the action
it permits, the participant that enforces its scope, and the evidence needed
when that authority changes. Identify these before seeing the experimental
outcome.

**H2, causation.** Repairing a particular, identified relationship removes
the predicted failure without changing the underlying consensus algorithm.
The claim is specific to that repair and failure; a new failure exposed
afterward is a separate result.

**H3, reuse.** The same obligations concerning prior decisions, obsolete
authority, and available handoff evidence explain failures at more than one
scope, such as leadership, configuration, session ownership, or process
incarnation. This is not a claim that every distributed failure is an
authority failure.

**Competing resource explanation.** Shared queues, threads, storage, or CPU
can stop a service while authority remains unchanged. A resource dependency
counts as an authority dependency only if an independently identified
authorizing decision and its enforcement actually depend on it. Caerus's
communication stall may be explained entirely by this alternative.

**Competing implementation explanation.** A prototype may omit the recovery
that the paper describes. A failure of the release does not, by itself,
demonstrate a defect in the published recovery algorithm.

The present Caerus and FDBKeeper rounds test concrete mechanism predictions.
They do not estimate how common these mechanisms are across the field. H1
needs a later comparison against both quorum-only predictions and a stronger
comparison that includes ordinary resource dependencies. H3 needs more than
one authority scope; finding two bugs is not sufficient by itself.

## What I predict and what would change my mind

**Caerus:** C1 is high confidence and is replication of known behavior.
C2 is moderate confidence and is the most useful opportunity to be wrong.
The Python model permits local-only work, but an unmodeled dependency on a
dead region's ordering stream could still stop the real scheduler. A valid
C2 failure defeats the prediction that this communication intervention is
sufficient. C3 is moderate confidence: incomplete transactions and the
per-origin admission limit may become visible after communication unblocks.
A sustained recovery of mixed-region work would defeat C3. A stalled result
supports its proposed mechanism only if the separate queue and scheduler
observations identify it.

**FDBKeeper:** F1 and F2 are high confidence, conditional on the real API
allowing the modeled renewal-and-create sequence. If it forbids that sequence
or protects the object elsewhere, the reachable-race claim is wrong. If
revalidation still permits the deletion, the proposed repair is insufficient.
Two clients merely believing that they hold a lock will not be counted as
proof of two valid application-level effects.

**Composition model:** I predict that omitting required state transfer or
enforcement of authority permits a counterexample even when the individual
components satisfy their declared local contracts. I also predict a safe
version with all proposed conditions enabled. A counterexample in that
version falsifies the claimed sufficiency for its declared model. Deliberately
broken versions check the instrument and illustrate mechanisms; they are not
independent evidence that real systems contain those defects. A delayed
response to an operation committed under valid old authority must remain
permissible; response arrival alone does not identify a stale action.

**Generality:** I expect at least one additional correct prediction among
three future eligible systems, but this is the least grounded prediction.
The later registration must define eligibility and how the systems are
selected, document prior exposure, and freeze both our predictions and the
comparison predictions before fault runs. A correct prediction shared with
the comparison is not additional predictive value. No successes would
falsify G1 for that sample. A small or incomplete sample cannot establish
population prevalence or a general null result.

## Evidence boundary

Before this registration, this instance read prior Caerus logs and debugger
records, the existing analysis and harness, the Caerus build recipes, and
Cloudflare's FDBKeeper model and proposed issue. It also read the pinned
FDBKeeper cleaner and session implementation through the public source.
These are informed tests, not blind discoveries. No new implementation,
intervention trial, FDBKeeper execution, or composition-model exploration
was performed before the registration.

Relevant existing records:

- [Caerus release results](2026-10-14-caerus-artifact-results.md).
- [Caerus model results](2026-10-11-caerus-contagion-results.md).
- `../cloudflare/docs/hotos/models/C4_RESULT.md` and `C4_FDBKeeper.tla`
  (paths relative to the Eidolon repository root).
- `../cloudflare/docs/hotos/measure/FOURTH-SYSTEM-SELECTION.md`: the earlier
  rule selected FDBKeeper. This registration does not redraw that choice.
- Howard and Mortier, [A Generalised Solution to Distributed
  Consensus](https://arxiv.org/html/1902.06776v1), sections 3-4, motivates
  making the evidence between rounds explicit; it does not establish H1-H3.

## Signed record for every round and correction

1. Sign and commit hypotheses, predictions, methods, controls, scoring rules,
   and prior exposure before implementing that round. A signature records
   the repository identity; it does not turn these predictions into findings
   or imply that Tony independently audited them.
2. Implement the experiment and independent checks. Sign and commit the
   tested code, source pins, environment manifest, and exact run commands
   before any outcome-bearing trial. Give each attempt its own directory.
   The manifest records the registration commit and implementation commit,
   image digests, source patches, workload, fault schedule, host resources,
   start/end times, and instrument/checker versions.
3. Keep raw data and exclusions for every attempt, including failed controls,
   timeouts, and crashes. Never overwrite a prior attempt or call the absence
   of output a safety pass. Sign and commit each completed experiment round's
   data, analysis, interpretation, and its link to the implementation commit.
4. When a test, harness, model, or analysis defect is found, record what
   exposed it, what data were already seen, affected attempts and claims,
   the proposed correction, and whether the old evidence remains usable.
   Sign and commit that record and the correction before collecting new
   outcome data. A focused regression check belongs with the correction.
5. Reruns use new attempt directories and explicitly reference both versions.
   Recompute prior analyses when possible; report original and corrected
   interpretations. Voided results stay in the record with their reason.
   Uncertain impact is reported as unresolved, not silently accepted.
6. Change a method or hypothesis with an additive amendment before using it.
   State whether the amendment was informed by an outcome. Preserve the
   original prediction and its verdict; a revised prediction starts a new
   comparison. No history rewriting to make a prediction appear prospective.
7. Use the existing OpenTimestamps convention to stamp registration and
   results commit hashes. A calendar receipt is pending until its external
   attestation is verified; do not describe receipt creation as completed
   blockchain anchoring. An author reply is optional evidence and is not a
   condition for running or interpreting the experiments.

The new output namespaces are `results/authority-composition/caerus/` and
`results/authority-composition/fdbkeeper/`. Later model and generality rounds
receive their own registrations and directories. The NINeS paper and its
existing result artifacts are not rewritten by this registration.
