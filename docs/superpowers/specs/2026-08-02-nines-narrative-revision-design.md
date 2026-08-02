# NINeS Narrative Revision Design

**Date:** 2026-08-02  
**Status:** Proposed for review  
**Source manuscript:** `.worktrees/nines-rc-reviewer-pass/docs/paper/nines/main.tex`

## Purpose

Determine whether the existing NINeS manuscript can be reshaped into a clear,
surprising, and evidence-bounded paper without inventing a new technical result.
The revision should expose the teaching already present in the work rather than
make the wall construction carry a claim it cannot support.

The source manuscript remains immutable during this design review. The deadline
does not decide whether the paper ships. If the existing evidence cannot support
the revised story, the correct outcome is not to submit to NINeS '27.

## Provisional Thesis

For familiar, phase-symmetric Paxos configurations, a smaller operational
question---which participants are reachable?---could stand in for two questions
operators actually needed answered: can proposal authority be acquired, and can
that authority be exercised to commit? The proxy worked because phase symmetry
made those capabilities coincide and a threshold made their common predicate
count-readable.

Flexible quorum families preserve the cross-phase intersection required for
safety while allowing those capabilities to separate. The resulting problem is
not that Paxos becomes unsafe or that health signals become false. The problem is
that an inference that once worked no longer follows.

The paper characterizes one class of this broader capability-observability
problem. It shows when acquisition and commit formability can diverge, how quorum
design can prevent selected divergences, how an auditor and structural readout
can expose those that remain, and where runtime authority and service policy lie
beyond the model.

## The Story in One Page

### 1. An expected failure produces unexpected behavior

In October 2018, routine work to replace failing 100G optical equipment severed
connectivity inside GitHub's network for 43 seconds. Orchestrator's Raft group
formed a surviving quorum and initiated database failover as configured. The
resulting topology exposed unreplicated writes on one side and accepted new
writes on the other; applications also could not tolerate the cross-country
latency. A brief and expected network failure led to more than 24 hours of
degradation and reconciliation.

Consensus did not simply fail. A control subsystem answered its question and
took an authorized action. The larger service needed answers that the control
decision did not provide.

This incident motivates the method; the paper does not claim its theorem caused
or fully explains the GitHub incident.

### 2. A production system exhibits the specific distinction

Meta's LogDevice provides the closer technical anchor. Its flexible Multi-Paxos
design allowed an incumbent leader to continue writing after the system had lost
the larger quorum needed to elect and recover a replacement. At fleet scale,
stuck recovery became a recurring source of read-SLA violations and sometimes
required manual inspection before recovery could proceed.

LogDevice was not simply available or unavailable. It retained commit capability
under existing authority while lacking acquisition/recovery capability. Meta
already understood this tradeoff; the paper does not claim discovery of the
phenomenon. The paper supplies a general characterization and executable
recognizers for the underlying phase-capability distinction.

### 3. Why did one answer once seem sufficient?

Classic majority-based Paxos commonly uses one threshold quorum family for both
phases. Phase symmetry makes acquisition and commit formability the same
predicate. The threshold makes that shared predicate readable from a count.

The count never directly measured both capabilities. A smaller answer happened
to settle the larger questions, and the convenience became easy to mistake for
a general property of consensus.

### 4. Flexibility exposes the hidden inference

Flexible Paxos separates the quorum families while preserving the intersection
needed for safety. A connectivity state may therefore support acquisition but
not commit, commit but not acquisition, both, or neither. Nothing about the mixed
states is intrinsically a safety violation.

The containment result gives the exact boundary:

- acquisition without commit is impossible exactly when every Phase 1 quorum
  contains a Phase 2 quorum;
- commit without acquisition is impossible under the mirrored containment;
- strictly ordered formability predicates admit one mixed direction;
- incomparable predicates admit both.

The proof is short because the formability predicates are monotone. Its value is
not mathematical difficulty but naming exactly when the old operational
inference remains valid.

### 5. Direction matters

The preregistered experiment expected acquisition without commit to be the more
harmful state. Under the modeled single-decree retry policy and fixed budgets,
the result reversed that expectation. The acquisition-only contender matched a
healthy contender on every recorded metric and injected an accepted value that
later acquisition preserved. The commit-capable but acquisition-incapable
incumbent prevented the healthy proposer from deciding in all measured seeds at
the stated budget.

These are bounded behavioral observations, not intrinsic labels for the two
states. They establish that detecting "a gap" is insufficient: its direction,
runtime authority, retry policy, and intended response matter.

### 6. The response has three layers

1. **Prevent:** Choose quorum families satisfying the desired containment when
   its latency, availability, and participation costs are acceptable.
2. **Detect:** Use the construction-independent auditor at design time and a
   structural readout at runtime to report the capabilities and failed
   obligations that remain.
3. **Act:** Combine structural capability with fresh authority state and service
   policy to decide whether to continue, fail over, reconfigure, or stop. This
   layer is necessary but is not solved by the current paper.

### 7. The wall is an instrument, not the hero

The wall construction is a controlled case study in prevention and detection.
It makes quorum obligations structurally visible and permits selected capability
gaps to be closed without returning all inter-tier latency to Phase 2. Its
5/1/1/3 topology is an evaluated fixture, not a recommended deployment and not
the conceptual foundation of the work.

Mars stretches connectivity loss until timeout intuition cannot hide it. LEO
suggests a fixed membership with rapidly changing visibility. Mobile edge marks
the harder boundary where authorized membership itself changes during
operation. The present results apply within fixed configuration epochs and do
not solve continuous reconfiguration.

### 8. The changed practice

The paper should leave the reader with a different diagnostic question:

> Do not ask only whether the system is healthy. Ask which actions it can
> presently complete, what authority those actions require, and what evidence
> supports each answer.

## Proposed Manuscript Structure

### 1. Introduction: The Smaller Question That Used to Work

- Open with the 43-second GitHub event.
- Explain the failure-backwards method: what did recovery need to know?
- Introduce LogDevice as the exact phase-asymmetric production case.
- State the scalar-health/vector-capability hypothesis.
- Narrow explicitly to the paper's fixed-configuration contribution.
- State the prevention--detection--action hierarchy.
- Give bounded contributions only after the reader wants them.

### 2. Why One Count Once Worked

- Preserve most of the current section.
- Separate phase symmetry from threshold count-readability.
- Replace historical universals with bounded language such as "familiar" and
  "commonly."
- Use a small intuitive example before formal notation.

### 3. Capability Gaps

- Preserve the formability definitions, proposition, corollary, and auditor.
- Explain predicate ordering with set containment before presenting the formal
  statement.
- Frame the theorem as the boundary of the historical inference, not as a newly
  discovered production failure class.

### 4. What Happens Inside the Gaps

- Move the current behavioral experiment before the wall construction.
- Preserve preregistration, deviations, policy bounds, and single-decree scope.
- Emphasize the reversal and why direction matters.
- Do not generalize contention behavior to Multi-Paxos or production systems.

### 5. A Controlled Wall Case Study

- Introduce the wall only after the general result and behavioral consequence.
- State immediately that extra tier witnesses are participation policy rather
  than Paxos safety requirements.
- Present 5/1/1/3 as the existing evaluated fixture.
- Remove any implication that singleton LEO or Moon tiers form a recommended
  distributed-system design.
- Preserve the construction and safety argument where technically necessary.

### 6. Preventing and Reading Capability Gaps

- Organize the exact threshold boundary as prevention.
- Organize the auditor and wall readout as detection.
- Retain the distinction among structural formability, current authority, and
  service policy.
- Keep only evaluation that demonstrates the prevention/detection claims or a
  necessary cost boundary.
- Demote results that exist mainly to make the wall appear deployment-complete.

### 7. The Larger Temporal Space

- Mars: stable membership, prolonged and partly scheduled reachability loss.
- LEO: stable logical membership, rapidly changing and partly predictable
  visibility.
- Mobile edge: changing reachability and changing authority-bearing membership.
- Define fixed-epoch analysis as the present contribution.
- Formulate safe authority transfer across changing configurations as future
  work without implying that the paper solves it.

### 8. Related Work

- Position Flexible Paxos as identifying the safe quorum freedom.
- Credit LogDevice for observing and engineering around the production
  tradeoff.
- Position this work as exact capability characterization plus executable
  recognition, not discovery of the underlying phenomenon.
- Preserve distinctions from domination, containment, and reconfiguration work
  only where they support the revised thesis.

### 9. Limitations and Conclusion

- State causal limits of the production examples.
- State fixed-membership, supplied-connectivity, and single-decree bounds.
- Reprise the changed diagnostic practice.
- End with the broader temporal question rather than with 5/1/1/3.

The abstract should be written last.

## Migration Map

| Current material | Disposition | New role |
|---|---|---|
| Abstract | Rewrite last | Compact version of validated story |
| Introduction | Rewrite | Incident, technical anchor, thesis, boundaries |
| Why One Count Once Worked | Keep and tighten | Explain the historical proxy |
| Capability Gaps | Keep; add intuition | Exact characterization |
| Putting the Gaps on a Wall | Move later; reframe | Controlled case study |
| Where the Wall Works, and Where It Stops | Keep selectively | Prevention boundary |
| What Happens Inside the Gaps | Move earlier | Consequence and reversal |
| Reading the Wall | Keep selectively | Detection and cost evidence |
| Related Work | Rewrite positioning | Credit precedents; bound novelty |
| What Remains Unsolved | Expand and refocus | Temporal reachability and membership |
| Threats and Limitations | Preserve and extend | Causal and abstraction boundaries |
| Conclusion | Rewrite | Changed diagnostic practice |

## Evidence and Claim Boundaries

### Supported strongly

- Exact containment characterization for finite quorum families.
- Auditor behavior and exhaustive self-check within its tested universe.
- Wall capability results for the registered 5/1/1/3 fixture.
- Bounded behavior of both mixed states under the preregistered single-decree
  experiment, modeled retry policy, and stated budgets.
- Structural readout from supplied connectivity for the wall construction.

### Supported as external observations

- GitHub: a 43-second network interruption during replacement of failing 100G
  optical equipment initiated a correctly configured failover and more than 24
  hours of degradation.
- LogDevice: flexible Multi-Paxos retained incumbent write availability after
  leader-election/recovery capability was lost; stuck recovery became a
  recurring operational problem.
- etcd: learner health/readiness endpoints have been observed returning success
  while ordinary RPC operations were unsupported.

### Must not be claimed

- The capability-gap theorem caused or fully explains the GitHub incident.
- Every component or dashboard was green during the GitHub incident.
- This paper discovered LogDevice's operational state.
- Either mixed state has policy-independent intrinsic valence.
- The wall is a recommended 5/1/1/3 deployment.
- The current work solves dynamic membership or mobile consensus.
- The edge community has failed to recognize or solve the broader problem.

## Five-Day Scope

The revision is feasible only if it remains narrative surgery:

- no new protocol;
- no new production-causality claim;
- no dynamic-membership model;
- no attempt to rehabilitate 5/1/1/3 as a deployment recommendation;
- no new experiment unless required to correct an existing claim;
- preserve registered results and their bounds;
- cut rather than expand secondary wall results when page pressure appears.

## Go/No-Go Gates

Proceed toward NINeS submission only if all gates pass after migration:

1. A reader can state the problem and contribution after the introduction
   without knowing the `(1,0)` notation.
2. The technical body answers the question posed by the production stories.
3. The wall can be presented honestly as a controlled case without carrying
   the paper's real-world plausibility.
4. The theorem and auditor remain central after the wall is demoted.
5. Every external example is cited and bounded against causal overreach.
6. The revised paper does not imply a solution to continuous reconfiguration.
7. The resulting narrative is more than a clearer version of a mediocre claim:
   it changes how the reader distinguishes health, capability, and authority.

If any gate requires substantial new theory or evidence, do not submit to
NINeS '27. Preserve the outline as the starting point for the larger work.

