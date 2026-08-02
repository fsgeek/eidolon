# NINeS Introduction Cold-Reader Test

## Reader Context

Long-weekend proxy test using two LLM readers, Grok and Luna. Each received the
title, current abstract, and revised introduction, followed by the two prescribed
questions. Neither participated in the drafting conversation. Because the
current abstract is not yet the planned placeholder or final rewritten abstract,
it disclosed detailed results before the introduction; this weakens the test of
whether the introduction alone communicates the formal contribution, but not the
test of whether readers infer a changed incident practice.

## Answer 1: Claimed Contribution

### Grok (verbatim)

**Claims (from title, abstract, and introduction)**

The paper’s core claim is that flexible quorum systems (Flexible Paxos and relatives) can create *capability gaps*—reachable sets where a proposer can form one phase but not the other—and that **containment** exactly characterizes them.

- Acquisition-without-commit ((1,0)) is impossible if and only if every Phase-1 quorum contains some Phase-2 quorum.
- Commit-without-acquisition ((0,1)) is impossible under the dual (mirrored) containment.

Classic majority Paxos hid this because the same quorum family was used for both phases (*phase-capability coincidence*), so a single reachable-node count answered both “can I acquire?” and “can I commit?” Flexible Paxos keeps the necessary cross-phase intersection for safety while allowing the two formability predicates to separate. Once they separate, node-health dashboards or simple counts no longer tell you which of the four joint states ((1,1), (0,0), (1,0), (0,1)) you are in.

A pre-registered single-decree experiment is claimed to reverse the authors’ own predicted ordering: an acquisition-only ((1,0)) incumbent behaved like a healthy contender and even supplied the value that was later decided, whereas a commit-capable but acquisition-incapable ((0,1)) incumbent at an eight-round budget blocked the healthy proposer in every seed.

They then instantiate the characterization with a crumbling-wall construction over a 5/1/1/3 Earth/LEO/Moon/Mars topology. At Phase-2 threshold \(k=3\), the (1,0) gap disappears at every tier; both gaps disappear for Earth (under both connectivity readings) and for LEO (under the self-reachable reading); Moon and Mars retain (0,1) exposure. The wall keeps the hot commit path on Earth and supplies an \(O(\text{tiers})\) structural readout from a supplied connectivity summary—reporting capability, not current authority.

The methodological framing is that component health, protocol success, current authority, and service-level capability are distinct answers. The GitHub 2018 incident and Meta’s LogDevice experience are presented as illustrations of the mismatch that arises when the control plane answers only one of them.

### Luna (verbatim)

Based on the title, abstract, and introduction, I read the paper as making four connected claims.

### 1. What the paper claims

1. **A single reachable-replica count can be misleading.**  
   In ordinary majority Paxos, Phase 1 and Phase 2 use the same quorum family, so “can acquire authority?” and “can commit?” happen to have the same answer. Flexible Paxos separates those quorum families, so the two answers can diverge.

2. **The divergence can be characterized exactly.**  
   The paper defines two capabilities:

   - \(R_1\): a reachable set can form a Phase 1 quorum and acquire proposal authority.
   - \(R_2\): it can form a Phase 2 quorum and commit.

   The mixed states \((1,0)\) and \((0,1)\) are the paper’s “capability gaps.” Its central formal claim is that quorum containment determines exactly whether either gap is impossible.

3. **The two gaps do not have equivalent operational behavior.**  
   In the paper’s bounded, single-decree experiment, the acquisition-capable/commit-incapable case behaved relatively well for the healthy contender, while the commit-capable/acquisition-incapable case prevented decision in all 50 tested seeds. The paper is careful to say that this result depends on the retry policy and round budget; it does not assign an inherent value to one gap direction.

4. **Operators should expose three separate answers.**  
   The practical recommendation is to distinguish:

   - structural acquisition capability,
   - structural commit capability, and
   - current runtime authority.

   Node health, quorum success, and service capability are not interchangeable. The planetary “crumbling-wall” example is intended as a readable diagnostic construction, not as evidence for a real deployment.

The introduction also makes important limitations explicit: the paper does not explain the GitHub incident, does not establish deployment prevalence, does not model changing membership, and does not claim that its structural readout alone guarantees availability or selects the correct recovery action.

## Answer 2: Changed Incident Practice

### Grok (verbatim)

**What I would inspect or do differently in the next incident**

I would stop treating “do we have a quorum / is the leader healthy / how many nodes are up?” as a sufficient control question and instead ask the two predicates separately:

1. **Acquisition formability (R1)**: Does the currently reachable set contain a Phase-1 quorum for the relevant proposer tier/family?
2. **Commit formability (R2)**: Does it contain a Phase-2 quorum?

That immediately tells me whether I am looking at a pure (1,1) or (0,0) situation or one of the two mixed states. The paper’s experiment suggests the operational response should differ:

- In a (1,0) state, acquisition can still recover and finish work that an earlier proposer began; simply “restart everything” may be counterproductive.
- In a (0,1) state, an authorized leader may still be able to commit (under Multi-Paxos) even though it cannot re-acquire; killing it can discard the only usable authority.

I would therefore demand a legible structural readout (exactly the kind the wall construction is claimed to provide) that takes the configured quorum families plus a connectivity summary and returns the two-bit capability vector, the witnesses, and the failed obligations—without requiring a full protocol run or assuming current leadership. Node-health dashboards that collapse everything to a single scalar would be treated as incomplete.

In short: separate the questions the paper separates, treat the two mixed states as first-class operational conditions rather than “the system is down,” and prefer designs whose capability surface is readable from the connectivity data operators already collect.

### Luna (verbatim)

### 2. What I would inspect or do differently during the next incident

I would change the incident checklist from “How many replicas are reachable?” to the following sequence:

1. **Freeze the configuration epoch.**  
   Record membership, quorum definitions, Phase 1 and Phase 2 thresholds, leader/term or ballot state, and the network vantage from which reachability is being assessed.

2. **Compute the two capabilities separately.**  
   Report a small matrix for each relevant proposer or site:

   | Question | Answer |
   |---|---|
   | Can this side form Phase 1? | Yes/No |
   | Can it form Phase 2? | Yes/No |
   | Resulting state | \((1,1)\), \((1,0)\), \((0,1)\), or \((0,0)\) |

   This would immediately reveal whether the system is acquisition-capable but unable to commit, or commit-capable but unable to acquire.

3. **Inspect runtime authority independently.**  
   Verify who currently holds leadership or ballot authority, whether that authority is still valid, what values have been accepted, and whether the relevant logs or state have propagated. Structural capability does not establish current authority.

4. **Check the service contract before failing over.**  
   Ask separately whether the application can tolerate the latency, stale state, divergent writes, or read/write unavailability implied by the topology. A successful consensus-control action may still produce an unusable service.

5. **Use protocol-specific terminology in the runbook.**  
   For Multi-Paxos, “acquisition” may mean retaining or obtaining leadership; for single-decree Paxos, it may mean per-decree ballot acquisition. The incident tooling should state which interpretation applies.

6. **Preserve the evidence needed to reconstruct the decision.**  
   Capture reachability by tier and vantage point, quorum-family configuration, authority transitions, accepted values, retries/timeouts, and application-level latency. This would allow post-incident analysis to distinguish a quorum-capability problem from an authority, propagation, or service-policy problem.

The paper’s strongest operational message is therefore: do not promote or continue writing merely because a quorum appears healthy. First determine separately whether the system can acquire, whether it can commit, who actually has authority, and whether either state satisfies the service’s contract.

One title improvement might be to make that message more explicit. For example: **“When Replica Counts Mislead: Separating Acquisition and Commit Capability in Flexible Quorums.”** “Legible Consensus” is memorable, but its meaning becomes clear only after the introduction defines the diagnostic readout.

## Diagnosis

Both readers identified phase-capability coincidence and its loss, the exact
containment characterization, and the need to inspect acquisition capability,
commit capability, and runtime authority separately. Both translated the paper
into changed incident practice without asking for new theory or evidence. The
abstract likely accounts for their unusually precise recovery of experimental
and 5/1/1/3 results; the final abstract must therefore be tested for invitation
rather than merely information density. Luna's proposed title is recorded but
deferred until the abstract and full narrative exist, so that the current title
is not changed in response to an incomplete front matter.

## Decision

**PASS.** Proceed to migrate the formal core and behavioral reversal.
