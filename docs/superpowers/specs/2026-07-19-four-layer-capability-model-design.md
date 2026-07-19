# Four-Layer Capability Model for Legible Consensus

**Status:** Revised design approved after external generative critique

**Date:** 2026-07-19

**Scope:** NINeS workshop revision of *Legible Consensus* and the experiments and formal artifacts needed to support it

## Purpose

The interplanetary topology is a magnifying glass, not the subject of the paper. Its purpose is to make consequences of structured latency and disconnection visible that are easier to overlook at terrestrial scale. The revised paper will therefore organize its argument around the protocol capabilities induced by quorum geometry, effective connectivity, and existing protocol authority. Physical scenarios earn space only when they reveal a distinct capability state or transition.

The central claim is:

> Legibility is stratified by evidence source and freshness. Quorum geometry exposes a structural capability envelope; effective topology narrows it; runtime protocol state determines which structurally possible actions are presently usable; and service policy declares which client-visible contract to offer.

This formulation replaces the current identification of Paxos Phase 1 with “learning the global history.” Phase 1 is an authority-acquisition and recovery mechanism that may reveal accepted values for particular instances. It does not reveal whether an accepted value was chosen unless the observer also obtains equivalent Phase 2 quorum evidence, and a proposer may need to re-drive the value through Phase 2 to make its status certain. Phase 1 is therefore neither a non-disruptive learner/read interface nor a mechanism that by itself recovers an entire log.

## Goals

1. Give students and systems builders a reusable procedure for diagnosing degraded consensus capability per tier.
2. Separate structural potential from authority already held and from service-level promises.
3. Cover the complete Phase 1/Phase 2 reachability space, including the state in which an incumbent leader can replicate but a replacement leader cannot be elected.
4. Preserve Mars conjunction as a physically forceful example without making the paper a Martian datacenter design.
5. Position the contribution as a composition and operationalization of existing ideas, not as the invention of phase separation, heterogeneous quorum availability, topology-aware quorums, or partition-aware consistency.
6. Make explicit where static structural legibility ends and runtime observation begins.
7. Turn legibility into an artifact whose output identifies evidence provenance, missing obligations, authority dependence, and hazardous actions.

## Non-goals

1. Model orbital mechanics, antenna scheduling, radio propagation, or multi-hop routing in full physical detail.
2. Define the service’s degraded consistency policy. The model exposes available protocol capabilities; the service chooses whether to reject, serve stale reads, use a local replicated scope, or employ a separate conflict-tolerant mechanism.
3. Treat local operation as evidence of global consensus availability.
4. Claim that Phase 1 is a safe observational read.
5. Develop temporal delegation, leases, CRDT reconciliation, or contact-plan scheduling in this revision. Those remain future work.

## The Four Layers

The layers are stratified by the evidence required to evaluate them. This is not a universal claim that their monetary or computational costs are monotonically ordered: authority may be locally cheap to read while acquiring an accurate connectivity summary may be expensive. The invariant is the source and freshness of the evidence, not a fixed cost ranking.

### 1. Quorum obligation

For an initiating tier `i`, the quorum construction defines Phase 1 family `Q1(i)` and Phase 2 family `Q2(i)` (or a shared `Q2` where applicable). These families state which acceptor combinations are sufficient for each protocol action. The crumbling wall makes the obligation shape tier-indexed: Phase 1 reads downward through the required wall rows, while Phase 2 uses the anchor tier.

This layer is combinatorial and comes from static configuration. It does not say whether any quorum is currently reachable.

### 2. Effective reachability

Let `Reach(i, C)` be the nodes reachable from tier `i` under effective connectivity state `C`. “Effective” means that an end-to-end route can deliver protocol traffic within the liveness assumptions used by the experiment. It need not mean a direct physical radio link. Scheduled disconnection, a failed relay, missing ground coverage, or a timeout shorter than the route delay can all remove a node from the effective reachable set, but the experiment must report which cause applies.

Define the structural reachability predicates:

```text
R1(i, C) = exists q in Q1(i) such that q is a subset of Reach(i, C)
R2(i, C) = exists q in Q2(i) such that q is a subset of Reach(i, C)
```

These predicates require a connectivity summary or schedule. They say that a Phase 1 or Phase 2 quorum can be contacted. They do not say that the initiator is entitled to issue Phase 2 messages.

### 3. Protocol authority

Phase 1 establishes or recovers a ballot/epoch and discovers accepted values relevant to that authority. In Multi-Paxos, a leader that has already completed Phase 1 may propose subsequent commands using Phase 2 without repeating Phase 1 for each command. Consequently, actual extension capability depends on both `R2` and authority already held.

The paper will distinguish:

- **Acquire or recover authority:** requires `R1` and successful completion of Phase 1.
- **Exercise existing authority:** requires a valid incumbent authority and `R2`.
- **Recover after authority loss:** requires `R1`; `R2` alone is insufficient.

Authority is runtime state, scoped by ballot/epoch and by the log instances covered by Phase 1. The paper must not imply that topology alone identifies the current leader or proves that its authority remains valid. In the protocol studied here, this is the boundary at which structural legibility ends and fresh temporal evidence becomes necessary.

### 4. Service contract

The service declares how protocol capabilities map to client-visible behavior. Possible policies include refusal, bounded or unbounded stale reads, local linearizable operation in a separate replica group, or a weaker mergeable update model. These policies have different semantics and are not selected or implemented by the wall.

The paper’s claim ends at making the capability boundary explicit. Any stated client consistency guarantee must additionally specify the service policy and standard client/leader assumptions on which it relies.

## Complete Phase-Reachability Matrix

The capability basis is the complete `R1 × R2` matrix, interpreted together with authority state:

| `R1` | `R2` | Structural capability | Operational interpretation |
|---:|---:|---|---|
| 1 | 1 | Phase 1 and Phase 2 quorums are reachable | A tier can acquire or recover authority and then exercise it; it has resilient global-operation potential. |
| 1 | 0 | Only a Phase 1 quorum is reachable | A tier can complete Phase 1 but cannot commit through Phase 2. A completed higher-ballot Phase 1 necessarily prevents every lower-ballot Phase 2 quorum from completing, so this state is a futile/harmful election hazard during the current connectivity window. |
| 0 | 1 | Only a Phase 2 quorum is reachable | A valid incumbent may continue committing, but a new leader cannot safely acquire authority. The tier is productive but brittle: loss of incumbent authority ends progress. |
| 0 | 0 | Neither quorum is reachable | No global Paxos action is structurally available from the tier. A separate local scope may still operate under its own contract. |

The matrix describes protocol-action reachability, not read semantics. If the paper discusses what a tier can observe or know, it must define a separate learner/read path.

## Observation Is a Separate Capability

Let `O(i, C)` denote whether tier `i` can obtain the chosen prefix required by the service through a non-disruptive learner or read mechanism. The exact predicate depends on that mechanism and is not inferred from `R1`.

For this revision, `O` is an analytical capability backed by an explicit standard learner path. A learner must receive either:

- matching `Accepted` evidence from a complete Phase 2 quorum; or
- a trusted decision notification carrying equivalent quorum evidence.

One acceptor notification proves acceptance, not chosenness. The learner path requires no ballot authority and may use one-way, delay-tolerant delivery rather than a proposer’s round trip, but its evidence requirement remains explicit. The paper may therefore retain epistemic language only with this mechanism as its referent. It will not claim that Phase 1 alone determines what a tier knows.

## Scenario Admission Rule

A physical or terrestrial scenario belongs in the evaluation only if it realizes a matrix state or a transition not already demonstrated. The minimum scenario basis is:

| Scenario class | Target state or transition | Purpose |
|---|---|---|
| Fully reachable wall | `R1=1, R2=1` | Establish ordinary resilient global operation. |
| Sparse LEO with strict or 4-of-5 Phase 2 | `R1=1, R2=0` | Demonstrate the futile/harmful election state and derive an operational warning not to initiate elections from that tier. |
| Sparse LEO with 3-of-5 Phase 2 | `10 → 11` | Show that quorum relaxation is also a reachability knob: it restores commit reachability and disarms the election hazard. |
| Broken intermediate Phase-1 obligation with anchor reachable | `R1=0, R2=1` | Expose incumbent-only progress and failure to recover leadership analytically; no new Multi-Paxos experiment is required for this revision. |
| Hard upper-tier cut | `R1=0, R2=0` | Show loss of global protocol capability while a separate local scope may remain. Mars conjunction is the candidate. |

Cislunar occultation and relay restoration are not included in the evaluation. They belong in future work on predictable capability transitions and changing reachability. Cloud/edge and mobile-wireless deployments should appear as careful mappings of the model unless a terrestrial experiment realizes a capability state that the planetary topology cannot.

## Treatment of Time and Mars Conjunction

The physical Mars conjunction interval remains useful motivation because a multi-week operational restriction makes stale knowledge and disconnection difficult to dismiss as transient implementation details. The structural classification itself is duration-independent: while connectivity state `C` is fixed, changing how long it persists does not change `R1` or `R2`.

Duration can still affect workload-dependent outcomes such as the number of failed attempts, accumulated local divergence, incumbent failure probability, reconciliation backlog, and recovery scheduling. The paper must therefore separate:

- **Capability classification**, which depends on quorum families and effective connectivity; from
- **Workload consequences**, which can depend on blackout duration and operation cadence.

Values such as 300, 900, and 1,800 seconds must not be described as physically realistic Mars-conjunction durations. If retained, they must be labeled accelerated observation windows or workload-sensitivity parameters. A literal two-week run may be added as a validation point with a sparse workload, but it is not needed to establish the structural matrix.

## Evidence and Experiment Design

### Static capability classifier

Promote the independently testable classifier to a named paper contribution. Its inputs are quorum families, initiating tier, and effective connectivity. Its output is:

```text
structural envelope:       R1, R2
witness or missing sets:   quorum witnesses or unsatisfied obligations
authority dependency:      whether progress requires a valid incumbent
hazard flags:              disruptive-election, incumbent-only
evidence provenance:       configuration, connectivity, runtime state, policy
```

A failed classification must identify the missing obligation rather than merely report timeout. The provenance field makes stratified legibility operational: it marks which conclusions are structural, which require a connectivity summary, which require fresh authority state, and which are service declarations.

### Disruptive-election theorem

Let an incumbent hold ballot `b`, and let another proposer complete Phase 1 at `b' > b` using `q1`. Every acceptor in `q1` promises to reject `b`. Because every allowed `q2` intersects `q1`, each Phase 2 quorum contains a rejector, so the incumbent can no longer complete Phase 2 at `b`.

For an Earth anchor of size `n` with `k`-of-`n` Phase 2, a completed Phase 1 contains at least `n-k+1` Earth promises. That set intersects every `k`-subset by pigeonhole. For strict all-of-Earth Phase 2, even one Earth promise blocks the sole Phase 2 quorum. For relaxed Phase 2, an arbitrary smaller partial attempt is not guaranteed to block every quorum; the theorem applies to the completed Phase 1 hitting set.

This theorem is scoped to the ballot and instances covered by the promises. In the current simulator, promises are per slot. Under epoch-style Multi-Paxos, Phase 1 may cover a range of future slots.

### Dueling-proposer hazard experiment

Use the existing single-decree machinery to run Earth and sparse-LEO proposers against the same slots. Compare Earth progress with and without repeated higher-ballot LEO election attempts while LEO has `R1=1, R2=0`. Report Earth commit success, latency, retries, and NACKs, together with LEO’s completed Phase 1 and failed Phase 2 counts.

The experiment demonstrates the same-slot disruption theorem; it does not claim to model persistent Multi-Paxos authority. Scheduling and ballot assignment must be explicit so that the baseline and contended runs are reproducible rather than accidental races.

### Analytical incumbent-only state

The `R1=0, R2=1` state requires protocol state absent from a single-decree “run both phases for every operation” experiment. It will remain an analytical matrix state in this revision: an authority already established over future slots may continue through reachable Phase 2 quorums, but replacement authority cannot be acquired while Phase 1 is unreachable.

The paper must label this as analysis and cite standard Multi-Paxos authority semantics. Building a new epoch-style Multi-Paxos model is outside the NINeS revision scope.

### Relaxation as a reachability transition

Run the classifier over sparse LEO with Earth size five and `k ∈ {5,4,3}`. The expected states are `(1,0)`, `(1,0)`, and `(1,1)` respectively because sparse LEO reaches three Earth nodes while the Earth witness requirement is `5-k+1`. This result joins the reachability and crash-tolerance arguments: quorum relaxation changes both failure tolerance and which topology cuts remain operable.

### Existing experiment repair

Before regenerating results:

1. Define logical end-to-end Mars-to-required-tier reachability before blackout so Mars has the capability claimed for the tested delay/timeout pair.
2. Remove those effective routes during hard blackout.
3. Reconcile the paper’s timeout units with the implementation. Remove the claim that a 372-second round trip exceeds a 500-second per-phase timeout; after the route repair, close-approach Mars should fit within each phase budget, while longer Mars delays may not.
4. Regenerate the stale crash-tolerance sweep rather than explaining the obsolete 98%/92% interaction.
5. Relabel the trade-off table’s “Commit” column as the Earth-local Phase 2 quorum.
6. Correct the `Li et al.` bibliography entry to name Xiao Li and Eric Chan, and add the 2026 Satrapy follow-on where the related-work distinction relies on it.

### Formal evidence

The TLA+ claims must match the checked models:

- State that quorum intersection, not the full ten-node protocol, is exhaustively checked over the ten-node construction.
- Add the claimed relaxed `k=3` case if it is absent from the current exhaustive specification.
- Update or replace the reduced Paxos specification so its quorum families match the current tier-indexed design.
- Treat the incomplete full-scale TLC run only as a bounded search, never as proof.

The capability classifier and its hazard flags can be checked exhaustively over the finite topology and threshold states used in the paper. This is separate from Paxos safety verification. The disruptive-election theorem should be stated and proved analytically; its simulation is corroboration, not proof.

## Paper Structure

The revision should make the capability model visible early:

1. **Introduction:** Mars as magnifying glass; stratified legibility; the boundary between structural inference and runtime evidence; the classifier as a named contribution.
2. **Background:** Paxos Phase 1 as authority acquisition/recovery and Phase 2 as replication under a ballot; Flexible Paxos cross-intersection.
3. **Construction:** tier-indexed quorum obligations and intersection proof.
4. **Capability model:** effective reachability predicates, authority state, complete matrix, evidence provenance, and limits of topology-only inference.
5. **Hazard theorem:** prove that a completed higher-ballot Phase 1 disables every lower-ballot Phase 2 quorum and derive the operational election warning.
6. **Evaluation:** classifier outputs, sparse-LEO hazard and `10→11` relaxation, hard-cut behavior, and repaired crash-tolerance and latency results.
7. **Observation and service semantics:** specify decision-certificate learning; separate observation, authority, and consistency contracts; no Phase-1-as-reader claim.
8. **Related work:** distinguish the composition from topology-aware quorum optimization, heterogeneous quorum availability, Heterogeneous Paxos, Satrapy, HAT/CAP, and epistemic distributed computing.
9. **Limitations and future work:** empirical incumbent-only authority, temporal legibility/contact plans, relays, delegation, conflict-tolerant reconciliation, and terrestrial empirical validation.

## Novelty Boundary

The paper will not claim novelty for:

- cross-phase quorum intersection;
- differing costs or availability of Phase 1 and Phase 2;
- per-process or heterogeneous quorum availability;
- topology-aware quorum optimization;
- the observation that partitions constrain consistency and availability; or
- epistemic reasoning about distributed systems.

The proposed contribution is stratified legibility for topology-shaped quorum systems: derive a per-tier structural capability envelope from quorum obligations and effective connectivity; identify where fresh authority evidence becomes necessary; expose futile/harmful and incumbent-only states; and carry the resulting boundary to service policy. The crumbling wall makes the structural portion inexpensive and readable for ordered tier topologies. The classifier turns that reasoning into an operator-facing artifact by reporting both the result and the evidence channel on which each conclusion depends.

## AI Process Disclosure

If NINeS requests disclosure, describe the use accurately rather than as editing assistance:

> Multiple generative AI systems were used iteratively in alternating constructive and adversarial roles for repository and artifact consistency analysis, experimental diagnosis, literature discovery, counterexample generation, and conceptual reframing. Intermediate designs were deliberately passed between systems for critique, and the resulting disagreements changed both the paper’s argument and the experiments selected. AI-generated claims and citations were checked against code, experimental artifacts, primary sources, and human review. The human authors retain responsibility for the paper’s claims and conclusions.

## Acceptance Criteria

The design is realized when:

1. The paper defines all four layers, identifies the evidence source and freshness required by each, and does not conflate Phase 1 with nondisruptive learning.
2. The complete `R1 × R2` matrix appears with authority-state qualifications and explicit hazardous-action labels.
3. The classifier reports the structural envelope, witnesses or missing obligations, authority dependence, hazard flags, and evidence provenance.
4. The disruptive-election theorem is stated with ballot/slot scope and the completed-Phase-1 qualification, and its proof is separate from experimental corroboration.
5. The sparse-LEO dueling-proposer experiment is reproducible and reports collateral effects on contested Earth progress without claiming Multi-Paxos semantics.
6. The sparse-LEO classifier demonstrates the expected `(1,0)`, `(1,0)`, `(1,1)` sequence for `k=5,4,3`.
7. The `R1=0, R2=1` state is explicitly limited to analysis in this revision.
8. Learner observation requires a Phase 2 decision certificate or equivalent quorum evidence; one acceptance is never described as proof of chosenness.
9. Mars has the effective pre-blackout routes required by the tested quorum and loses those routes during blackout; latency claims respect the per-phase timeout actually configured.
10. Timeout units, table labels, TLA+ scope, bibliography, and regenerated crash-tolerance results agree across prose, code, and artifacts.
11. Blackout durations are described as physical durations only when physically grounded; otherwise they are observation or sensitivity windows.
12. Related work explicitly positions the contribution against the closest prior abstractions, including the 2026 Satrapy result.
13. A reader can apply the stratified-legibility procedure to a new cloud/edge topology without relying on planetary details.
