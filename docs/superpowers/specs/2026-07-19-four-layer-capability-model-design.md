# Four-Layer Capability Model for Legible Consensus

**Status:** Approved design direction

**Date:** 2026-07-19

**Scope:** NINeS workshop revision of *Legible Consensus* and the experiments and formal artifacts needed to support it

## Purpose

The interplanetary topology is a magnifying glass, not the subject of the paper. Its purpose is to make consequences of structured latency and disconnection visible that are easier to overlook at terrestrial scale. The revised paper will therefore organize its argument around the protocol capabilities induced by quorum geometry, effective connectivity, and existing protocol authority. Physical scenarios earn space only when they reveal a distinct capability state or transition.

The central claim is:

> Quorum geometry specifies the participation an action requires; effective topology determines whether that participation is reachable; protocol state determines which authority has already been acquired; and service policy determines which client-visible contract to offer.

This formulation replaces the current identification of Paxos Phase 1 with “learning the global history.” Phase 1 is an authority-acquisition and recovery mechanism that may reveal accepted values for particular instances, but it is not a non-disruptive learner or read interface and does not by itself recover an entire log.

## Goals

1. Give students and systems builders a reusable procedure for diagnosing degraded consensus capability per tier.
2. Separate structural potential from authority already held and from service-level promises.
3. Cover the complete Phase 1/Phase 2 reachability space, including the state in which an incumbent leader can replicate but a replacement leader cannot be elected.
4. Preserve Mars conjunction as a physically forceful example without making the paper a Martian datacenter design.
5. Position the contribution as a composition and operationalization of existing ideas, not as the invention of phase separation, heterogeneous quorum availability, topology-aware quorums, or partition-aware consistency.

## Non-goals

1. Model orbital mechanics, antenna scheduling, radio propagation, or multi-hop routing in full physical detail.
2. Define the service’s degraded consistency policy. The model exposes available protocol capabilities; the service chooses whether to reject, serve stale reads, use a local replicated scope, or employ a separate conflict-tolerant mechanism.
3. Treat local operation as evidence of global consensus availability.
4. Claim that Phase 1 is a safe observational read.
5. Develop temporal delegation, leases, CRDT reconciliation, or contact-plan scheduling in this revision. Those remain future work.

## The Four Layers

### 1. Quorum obligation

For an initiating tier `i`, the quorum construction defines Phase 1 family `Q1(i)` and Phase 2 family `Q2(i)` (or a shared `Q2` where applicable). These families state which acceptor combinations are sufficient for each protocol action. The crumbling wall makes the obligation shape tier-indexed: Phase 1 reads downward through the required wall rows, while Phase 2 uses the anchor tier.

This layer is combinatorial. It does not say whether any quorum is currently reachable.

### 2. Effective reachability

Let `Reach(i, C)` be the nodes reachable from tier `i` under effective connectivity state `C`. “Effective” means that an end-to-end route can deliver protocol traffic within the liveness assumptions used by the experiment. It need not mean a direct physical radio link. Scheduled disconnection, a failed relay, missing ground coverage, or a timeout shorter than the route delay can all remove a node from the effective reachable set, but the experiment must report which cause applies.

Define the structural reachability predicates:

```text
R1(i, C) = exists q in Q1(i) such that q is a subset of Reach(i, C)
R2(i, C) = exists q in Q2(i) such that q is a subset of Reach(i, C)
```

These predicates say that a Phase 1 or Phase 2 quorum can be contacted. They do not say that the initiator is entitled to issue Phase 2 messages.

### 3. Protocol authority

Phase 1 establishes or recovers a ballot/epoch and discovers accepted values relevant to that authority. In Multi-Paxos, a leader that has already completed Phase 1 may propose subsequent commands using Phase 2 without repeating Phase 1 for each command. Consequently, actual extension capability depends on both `R2` and authority already held.

The paper will distinguish:

- **Acquire or recover authority:** requires `R1` and successful completion of Phase 1.
- **Exercise existing authority:** requires a valid incumbent authority and `R2`.
- **Recover after authority loss:** requires `R1`; `R2` alone is insufficient.

Authority is scoped by ballot/epoch and by the log instances covered by Phase 1. The paper must not imply that topology alone identifies the current leader or proves that its authority remains valid.

### 4. Service contract

The service maps protocol capabilities to client-visible behavior. Possible policies include refusal, bounded or unbounded stale reads, local linearizable operation in a separate replica group, or a weaker mergeable update model. These policies have different semantics and are not selected or implemented by the wall.

The paper’s claim ends at making the capability boundary explicit. Any stated client consistency guarantee must additionally specify the service policy and standard client/leader assumptions on which it relies.

## Complete Phase-Reachability Matrix

The capability basis is the complete `R1 × R2` matrix, interpreted together with authority state:

| `R1` | `R2` | Structural capability | Operational interpretation |
|---:|---:|---|---|
| 1 | 1 | Phase 1 and Phase 2 quorums are reachable | A tier can acquire or recover authority and then exercise it; it has resilient global-operation potential. |
| 1 | 0 | Only a Phase 1 quorum is reachable | A tier can attempt election/recovery but cannot commit through Phase 2. Running Phase 1 may preempt an incumbent, so this state is not a harmless read-only observer mode. |
| 0 | 1 | Only a Phase 2 quorum is reachable | A valid incumbent may continue committing, but a new leader cannot safely acquire authority. The tier is productive but brittle: loss of incumbent authority ends progress. |
| 0 | 0 | Neither quorum is reachable | No global Paxos action is structurally available from the tier. A separate local scope may still operate under its own contract. |

The matrix describes protocol-action reachability, not read semantics. If the paper discusses what a tier can observe or know, it must define a separate learner/read path.

## Observation Is a Separate Capability

Let `O(i, C)` denote whether tier `i` can obtain the chosen prefix required by the service through a non-disruptive learner or read mechanism. The exact predicate depends on that mechanism and is not inferred from `R1`.

The NINeS revision has two acceptable choices:

1. Model and evaluate an explicit learner/read path, then include `O` in the reported per-tier capability profile; or
2. Keep the experimental contribution focused on Phase 1 and Phase 2 reachability and remove claims that the wall proves what a tier can learn or know.

The second choice is the default for scope control. Epistemic language may remain as interpretation only when tied to an explicitly stated observation mechanism.

## Scenario Admission Rule

A physical or terrestrial scenario belongs in the evaluation only if it realizes a matrix state or a transition not already demonstrated. The minimum scenario basis is:

| Scenario class | Target state or transition | Purpose |
|---|---|---|
| Fully reachable wall | `R1=1, R2=1` | Establish ordinary resilient global operation. |
| Phase-2 cut with Phase-1 path intact | `R1=1, R2=0` | Show that quorum obligations and commit reachability can diverge. The current sparse-LEO construction is the candidate. |
| Broken intermediate Phase-1 obligation with anchor reachable | `R1=0, R2=1` | Expose incumbent-only progress and failure to recover leadership. This is the missing experimental state. |
| Hard upper-tier cut | `R1=0, R2=0` | Show loss of global protocol capability while a separate local scope may remain. Mars conjunction is the candidate. |
| Relay restoration, if space permits | `00 → 11` or another explicit transition | Show that the astronomical event is not itself decisive; changing effective reachability changes capability without changing wall geometry. |

Cislunar occultation is not included merely to justify short blackout durations. It belongs only in future work on predictable or periodic capability transitions. Cloud/edge and mobile-wireless deployments should appear as careful mappings of the model unless a terrestrial experiment realizes a capability state that the planetary topology cannot.

## Treatment of Time and Mars Conjunction

The physical Mars conjunction interval remains useful motivation because a multi-week operational restriction makes stale knowledge and disconnection difficult to dismiss as transient implementation details. The structural classification itself is duration-independent: while connectivity state `C` is fixed, changing how long it persists does not change `R1` or `R2`.

Duration can still affect workload-dependent outcomes such as the number of failed attempts, accumulated local divergence, incumbent failure probability, reconciliation backlog, and recovery scheduling. The paper must therefore separate:

- **Capability classification**, which depends on quorum families and effective connectivity; from
- **Workload consequences**, which can depend on blackout duration and operation cadence.

Values such as 300, 900, and 1,800 seconds must not be described as physically realistic Mars-conjunction durations. If retained, they must be labeled accelerated observation windows or workload-sensitivity parameters. A literal two-week run may be added as a validation point with a sparse workload, but it is not needed to establish the structural matrix.

## Evidence and Experiment Design

### Static capability classifier

Implement one independently testable classifier whose inputs are quorum families, initiating tier, and effective connectivity, and whose output is `(R1, R2)` plus witness quorums when they exist. A failed classification should identify the missing obligation rather than merely report timeout.

### Multi-Paxos authority experiment

The `R1=0, R2=1` state requires protocol state absent from a single-decree “run both phases for every operation” experiment. To claim empirical demonstration of this state, the model must represent an established leader/ballot whose completed Phase 1 covers a defined sequence of future slots. The test sequence is:

1. Establish authority while `R1=1, R2=1`.
2. Change connectivity to `R1=0, R2=1`.
3. Confirm that the incumbent can complete Phase 2 for covered/new Multi-Paxos instances under that authority.
4. Fail or revoke the incumbent.
5. Confirm that replacement authority cannot be acquired while `R1=0`, so progress stops without violating safety.

If the simulator cannot faithfully represent this sequence within the workshop schedule, the paper must present the matrix as analysis and narrow empirical claims to the states the simulator actually implements.

### Existing experiment repair

Before regenerating results:

1. Define logical end-to-end Mars-to-required-tier reachability before blackout so Mars has the claimed pre-blackout capability.
2. Remove those effective routes during hard blackout.
3. Reconcile the paper’s timeout units with the implementation.
4. Regenerate the stale crash-tolerance sweep rather than explaining the obsolete 98%/92% interaction.
5. Relabel the trade-off table’s “Commit” column as the Earth-local Phase 2 quorum.
6. Correct the `Li et al.` bibliography entry to name Xiao Li and Eric Chan, and add the 2026 Satrapy follow-on where the related-work distinction relies on it.

### Formal evidence

The TLA+ claims must match the checked models:

- State that quorum intersection, not the full ten-node protocol, is exhaustively checked over the ten-node construction.
- Add the claimed relaxed `k=3` case if it is absent from the current exhaustive specification.
- Update or replace the reduced Paxos specification so its quorum families match the current tier-indexed design.
- Treat the incomplete full-scale TLC run only as a bounded search, never as proof.

The capability classifier can be checked exhaustively over the finite topology states used in the paper. This is separate from Paxos safety verification.

## Paper Structure

The revision should make the capability model visible early:

1. **Introduction:** Mars as magnifying glass; the four-layer diagnostic chain; contribution framed as legibility and composition.
2. **Background:** Paxos Phase 1 as authority acquisition/recovery and Phase 2 as replication under a ballot; Flexible Paxos cross-intersection.
3. **Construction:** tier-indexed quorum obligations and intersection proof.
4. **Capability model:** effective reachability predicates, authority state, complete matrix, and limits of topology-only inference.
5. **Evaluation:** one scenario per matrix state, followed by crash-tolerance and latency results that support separate claims.
6. **Service semantics:** explicit boundary between protocol capabilities and consistency contracts; no Phase-1-as-reader claim.
7. **Related work:** distinguish the composition from topology-aware quorum optimization, heterogeneous quorum availability, Heterogeneous Paxos, Satrapy, HAT/CAP, and epistemic distributed computing.
8. **Limitations and future work:** explicit learner paths, temporal legibility/contact plans, delegation, conflict-tolerant reconciliation, and terrestrial empirical validation.

## Novelty Boundary

The paper will not claim novelty for:

- cross-phase quorum intersection;
- differing costs or availability of Phase 1 and Phase 2;
- per-process or heterogeneous quorum availability;
- topology-aware quorum optimization;
- the observation that partitions constrain consistency and availability; or
- epistemic reasoning about distributed systems.

The proposed contribution is the topology-indexed composition of these ideas into a legible operational procedure: derive per-tier protocol-action reachability from structured quorum obligations and effective connectivity, combine it with incumbent authority state, and expose the resulting boundary to service policy. The crumbling wall is the construction that makes this procedure inexpensive and readable for ordered tier topologies.

## AI Process Disclosure

If NINeS requests disclosure, describe the use accurately rather than as editing assistance:

> Generative AI systems were used iteratively for adversarial review, repository and artifact consistency analysis, experimental diagnosis, literature discovery, counterexample generation, and conceptual reframing. The interaction influenced both the paper’s argument and the identification of additional protocol states. AI-generated claims and citations were checked against code, experimental artifacts, primary sources, and human review. The human authors retain responsibility for the paper’s claims and conclusions.

## Acceptance Criteria

The design is realized when:

1. The paper defines all four layers and does not conflate Phase 1 with nondisruptive learning.
2. The complete `R1 × R2` matrix appears with authority-state qualifications.
3. Every empirically claimed matrix state has a reproducible scenario and traceable artifact.
4. The missing `R1=0, R2=1` state is either demonstrated with established Multi-Paxos authority or explicitly limited to analysis.
5. Mars has its claimed pre-blackout effective reachability and loses the required routes during blackout.
6. Timeout units, table labels, TLA+ scope, and regenerated crash-tolerance results agree across prose, code, and artifacts.
7. Blackout durations are described as physical durations only when physically grounded; otherwise they are observation or sensitivity windows.
8. Related work explicitly positions the contribution against the closest prior abstractions, including the 2026 Satrapy result.
9. A reader can apply the four-layer procedure to a new cloud/edge topology without relying on planetary details.
