# NINeS 2027 Capability-Gaps Revision Design

**Date:** 2026-07-30  
**Status:** Proposed for author review  
**Submission deadline:** 2026-08-06 AoE  
**Manuscript:** `docs/paper/nines/main.tex`

## Objective

Revise the NINeS paper around its strongest supported finding: Phase 1
acquisition and Phase 2 commit are separate formability predicates over network
state, and flexible quorum designs can make them disagree. The revision must
characterize that disagreement correctly, show why its two directions have
different consequences, and make the result useful without promising an
operator or terrestrial evaluation the repository does not yet contain.

The paper's title is:

> **Legible Consensus: Capability Gaps in Flexible Quorums**

The title uses *capability gaps* deliberately. The four pairs `(R1, R2)` are
the capability states; `(1,0)` and `(0,1)` are the capability gaps. The plural
does not assert that every design admits both, or even that syntactically
different families induce different predicates.

## Narrative Contract

The paper uses **capability-first analysis with a decision-support payoff**.
Its evidentiary spine is the finding, not an unmeasured claim about decision
quality:

1. Phase symmetry supplies coincidence: acquisition and commit formability
   are the same predicate over connectivity state.
2. Flexible quorum designs can spend that coincidence.
3. The induced predicates may be equal, strictly directionally ordered, or
   incomparable; those relations admit neither gap, exactly one gap, or both
   gaps, respectively. The reachable gap profile is a complete invariant of
   this four-way predicate ordering.
4. The two mixed states have sharply different measured consequences under
   the registered experiment and modeled retry policy.
5. The wall is a case study in both the value and price of encoding
   participation policy in quorum geometry.

The payoff follows from that spine:

- quorum designers receive a containment audit;
- operators receive a structural capability readout and state-action guidance;
- service designers receive an analytically derived consistency ladder;
- edge and control-plane architects receive a precise applicability boundary;
- safe reconfiguration remains a separate mechanism for changing membership.

The primary constituency is designers and operational owners of quorum-backed
services over structured, intermittently connected topologies. “Edge devices”
alone is too broad because many edge devices are clients or learners rather
than consensus participants.

## Scope Boundary: Participation Versus Membership

For configuration epoch `e`, let `N_e` be the logical acceptor universe and
let `C_t` be the effectively reachable participants during decision window
`t`, with `C_t` a subset of `N_e`. This paper analyzes changes in `C_t` while
`N_e` remains fixed within the decision window.

The results apply directly when effective participation changes faster than
logical membership. They apply epoch by epoch when membership changes. Safety
across epochs requires a reconfiguration protocol and state-transfer rule that
this paper does not analyze. Reconfiguration changes the legislature;
capability analysis reports what the configuration currently in force can do.

## Required Manuscript Corrections

### Predicate-level characterization

For a finite quorum family `Q`, define its formability predicate by

```text
Form(Q) = { C subseteq N | some q in Q satisfies q subseteq C }.
```

Semantic phase symmetry means `Form(Q1) = Form(Q2)`, not that the two families
are syntactically identical. The paper must state:

- different predicates imply at least one capability gap;
- equal predicates admit neither gap;
- `R1` strictly implying `R2` admits only `(0,1)`;
- `R2` strictly implying `R1` admits only `(1,0)`;
- incomparable predicates admit both gaps;
- uniform threshold families give the exact trichotomy already derived from
  comparing `q1` and `q2`.

These four mutually exclusive cases form an exact correspondence: the
predicate-ordering class determines the reachable gap profile, and the gap
profile determines the ordering class. Equality must be tested before either
non-strict implication in prose and executable classification so equality is
not mislabeled as a directional case.

The phrases “every departure from phase symmetry opens exactly one gap” and
“the direction selects which gap” are permitted only when explicitly scoped
to ordered families. All occurrences in the abstract, introduction,
capability section, contributions, and conclusion must be audited.

### Evidence-calibrated consequence claims

The abstract and body must report that the `(0,1)` arm prevented a healthy
proposer from deciding in 50 of 50 seeds at retry budget eight under the
modeled retry policy. They must not generalize this to unconditional
livelock or say that the state “blocks liveness completely.”

The `(1,0)` state must not be described as futile or inherently hazardous.
The registered experiment found it indistinguishable from a healthy contender
on every recorded metric. Its partial Phase 2 may inject accepted values that
a later acquisition must surface and preserve; completion still requires a
reachable Phase 2 quorum.

Legibility is not “the only available mitigation.” The defensible claim is
that distinguishing the capability state is required to select a
state-appropriate mitigation where the gap remains possible.

### Why deploy the wall

Upper-tier witnesses are not required for Paxos agreement in this
construction. They encode participation policy: local-representative,
administrative, sovereignty, path-coupled leadership, or control-plane fencing
requirements. The paper must say both:

- when such a policy exists, the capability analysis states exactly what
  exposure that obligation purchases;
- when no such policy exists, the wall is an analytical instrument rather
  than a deployment recommendation.

### Mars and the edge

Mars remains the opening magnifying glass. The edge is the recognition moment,
not a claimed evaluation result. Page 1 will explain that the same structural
condition occurs in systems whose effective participation changes faster than
membership. Later edge references should clarify `(0,1)`, automated failover,
and client-visible degradation without relabeling planetary evidence as
terrestrial evidence.

### Operational payoff

Immediately after the capability characterization, add a four-state table.
Its guidance must preserve the experimental result:

- `(1,1)`: ordinary structural capability;
- `(1,0)`: acquisition is possible, commit is not; accepted-value injection
  is possible and later acquisition must preserve it;
- `(0,1)`: commit is structurally possible only for pre-existing authority;
  preserve a valid incumbent and do not apply an uninformed restart policy;
- `(0,0)`: neither global capability is structurally available; use only an
  explicitly scoped degraded contract.

The table must distinguish structural formability, runtime authority, and
service policy. The classifier cannot infer the latter two from connectivity.

### Contributions and related work

Compress the headline contributions to:

1. the predicate-level capability characterization;
2. the measured difference between the mixed states;
3. the topology-readable wall as a case study.

Crash tolerance, leadership gradient, and related observations support these
headlines rather than compete with them.

Related work must position the result against:

- Flexible Paxos's existing descriptions of `(1,0)`;
- Refined Quorum Systems' use of containment as a correctness/design
  criterion;
- Li, Chan, and Lesani's quorum subsumption in heterogeneous personalized
  trust systems;
- the distinct question asked here: joint Phase 1/Phase 2 formability over
  connectivity state.

Primary sources must be checked before this paragraph is written. The paper
must not claim that containment itself is novel.

## Artifact A: Generic Quorum-Family Auditor

Artifact A is mandatory-adjacent: it is the executable verification vehicle
for the corrected characterization.

### Interface

Create a library and runnable command that accept:

- a finite universe `N`;
- explicit finite Phase 1 and Phase 2 quorum families;
- an optional pinned set `P`, defaulting to empty, whose nodes must be present
  in every analyzed connectivity state;
- an optional request for exhaustive small-universe self-checking.

The library may accept hashable node identifiers. The JSON command accepts
string identifiers so ordering and serialization remain portable. `N` and
both families must be nonempty; every quorum must be a nonempty subset of
`N`, and `P` must be a subset of `N`. Violations are input errors and produce
a nonzero command exit status.

JSON input and deterministic text/JSON output are sufficient. The command must
report:

- whether every Phase 1 quorum intersects every Phase 2 quorum;
- each family's canonical inclusion-minimal antichain;
- whether the induced predicates are equal, `R1 => R2`, `R2 => R1`, or
  incomparable;
- which capability gaps are reachable;
- a deterministic minimal connectivity witness for every reachable gap.

The four relation labels are mutually exclusive. The two implication labels
mean strict implication; implementation checks semantic equality first.

When several minimal-family members witness the same gap, choose the one with
the smallest cardinality and then the lexicographically smallest sorted node
sequence. Reports sort all nodes and quorums lexicographically.

Unsafe cross-intersection does not prevent classification, but it must be
reported prominently so the input is not mistaken for a valid Paxos quorum
configuration.

### Algorithm

Remove every quorum that strictly contains another quorum in the same family.
The remaining minimal antichain induces the same upward-closed formability
predicate. With pinned nodes, lift each minimal quorum to `q union P`, then
minimize again; the resulting antichain represents formability over the
restricted domain `{C subseteq N | P subseteq C}`. Cross-phase intersection
safety is still checked against the original quorum families, because pinning
restricts observed connectivity states rather than changing the Paxos
configuration.

`(1,0)` is reachable exactly when some lifted minimal Phase 1 quorum contains
no lifted minimal Phase 2 quorum. That lifted Phase 1 quorum is itself a
connectivity witness. The dual test decides `(0,1)`. Classification therefore
requires pairwise set-containment checks, not enumeration of all `2^|N|`
connectivity states.

For explicit families, expected complexity is
`O(|min(Q1)| * |min(Q2)| * |N|)`, excluding parsing, deterministic sorting,
and the quadratic-in-family-size antichain minimization pass.

`P` models a specific colocated acceptor or other node known to remain
reachable. The wall's published self-reachable reading requires at least one
node from the initiator's tier, not necessarily one named node. Pinning a
representative node reproduces the wall result because its quorum predicates
are symmetric within each tier; the registration must state that equivalence.
For an asymmetric deployment, reproduce an “at least one of this set” reading
by auditing each possible pinned representative and combining the results,
not by silently treating the conditions as identical.

### Registered expected outputs

Before implementation, commit and OpenTimestamps-stamp a registration note
containing expected results for:

- semantically equal but syntactically different families;
- `R1 => R2` only;
- `R2 => R1` only;
- the intersecting incomparable example
  `N={a,b,c}`, `Q1={{a,b}}`, `Q2={{a,c}}`, which must report both gaps;
- uniform threshold families below, at, and above equality;
- the wall at `k >= 4`, with every expected cell explicitly labeled as either
  unconstrained (`P` empty) or self-reachable (a named, symmetry-equivalent
  colocated acceptor pinned), including tiers where both gaps are reachable;
- at least one unsafe cross-intersection negative control.

The registration must fence off the scarcity lemma and unplanned derived
claims. Any divergence between the auditor, existing CSVs, and registered
expectations under the same explicitly named reading is recorded and
investigated as a finding. A difference between the unconstrained and pinned
readings is expected model sensitivity, not itself a new finding.

### Verification

Tests are written before implementation. For small universes, an independent
enumerator computes `R1(C)` and `R2(C)` for every `C subseteq N` and compares
the observed mixed states with the containment result. Mutation-style negative
controls must demonstrate that the self-check can fail.

## Artifact B: Wall Capability-Readout CLI

Artifact B wraps the existing `capability.py` classifier; it is valuable but
droppable under schedule pressure because its absence costs a demonstration,
not a headline claim.

The command accepts a wall configuration, initiating tier, and reachable-node
summary. It reports:

- `R1` and `R2`;
- minimal witnesses when formable;
- each failed typed obligation;
- whether progress requires pre-existing authority;
- evidence provenance and the boundary between structure, runtime authority,
  and service policy.

Provide one 5/1/1/3 example and one cloud/metro/remote configuration example.
The terrestrial example is a runnable configuration, not evaluation evidence.
The CLI must not automatically choose a service contract or claim that an
incumbent's authority is currently valid.

## Terrestrial Experiment: Post-Upload Gate

The audited paper will be uploaded before a terrestrial experiment is run.
The experiment is then designed and registered separately, so it cannot exert
selection pressure on the submitted claim set.

It proceeds only if a short design pass can state:

- a falsifiable question not answered by renaming the planetary tiers;
- a non-strawman node-health or failover baseline;
- a motivated fixed-membership terrestrial topology and connectivity trace;
- registered policies, metrics, and null interpretation;
- the information added beyond the theorem and existing valence experiment.

Outcome protocol:

- a conflict with the formal characterization triggers investigation of the
  proof, implementation, or model and may require correction or withdrawal;
- a conflict with operational interpretation or edge applicability narrows,
  corrects, or withdraws the relevant claims;
- an inconclusive result or mere reproduction remains in the repository and
  does not change the paper;
- corroboration without new insight ordinarily leaves the paper unchanged;
- a genuine extension is considered for a pre-deadline revision only if it
  fits cleanly, otherwise it becomes successor work.

Empirical results corroborate or challenge applicability; they do not prove
the formal characterization.

## Priority and Ownership

1. The author updates the HotCRP title and abstract. This is complete.
2. Register and timestamp Artifact A's expected outputs.
3. Implement and verify Artifact A test-first.
4. Correct the predicate-level theorem and narrative throughout the paper.
5. Verify RQS/LCL primary sources and repair related-work positioning.
6. Implement Artifact B if schedule permits.
7. Complete the manuscript claim, traceability, anonymization, build, and
   artifact audit. This audit includes regression checks that the executable
   `(1,0)` label remains `ACQUIRE_WITHOUT_COMMIT`, the documented
   `GridQuorum` intersection limitation remains visible, and the current paper
   retains the corrected 27,921-state attribution. It also verifies that the
   submission artifact is a clean export without Git history or signing
   identity. Upload the revised paper.
8. Design, register, and run the gated terrestrial experiment.

If schedule remains after Artifact B and all upload gates, a dual-dashboard
rendering of one existing blackout trace is optional communication work: the
same run shown once as node health and once as capability state. It is below
Artifact B in priority and supplies no new evidence.

The Cassandra version pin is successor-work hygiene unless the current paper
uses Cassandra as evidence. If such a claim is reintroduced, pinning the
source version and checking the Paxos-v2 path becomes an upload gate.

Codex drafts Section 1 and the other manuscript revisions. The author reviews
the prose rather than producing the first draft, preserving a fresh editorial
view. Every drafting prompt carries the ban list up front: no arbitrary-family
“exactly one” claim, no anchor-scoped claim without its tier quantifier, no
“recovery” synonym for acquisition, and no unconditional livelock claim.
External HotCRP changes remain the author's action.

All repository commits are made through WSL against the mounted Windows
working tree, signed with the `research@wamason.com` key, and OpenTimestamps-
stamped. Windows-side `uv` commands explicitly select `.venv-windows`; WSL
commands select `.venv-linux`.

## Completion Criteria

The revision is ready for audited upload when:

- title and abstract use the approved capability-gap framing;
- no arbitrary-family claim asserts exactly one gap;
- the four predicate-ordering classes and gap profiles are stated as an exact,
  mutually exclusive correspondence;
- all empirical claims state their modeled conditions and observed bounds;
- membership/reconfiguration scope is explicit;
- the wall's participation-policy purpose and analytical fallback are clear;
- Artifact A passes registered examples and independent exhaustive checks;
- Artifact B is either traced and tested or explicitly omitted as droppable;
- every headline claim has a proof, registered measurement, executable
  artifact, or explicit hypothesis label;
- the full test suite passes;
- the paper builds cleanly with resolved references;
- claim-to-artifact traceability and anonymization audits pass, including a
  history-free submission export with no signing identity;
- the audited PDF is uploaded before the terrestrial experiment begins.
