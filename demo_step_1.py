"""Step 1: Single Datacenter - "Everything Works"

Three Paxos nodes in one datacenter, ~1ms latency.
Consensus is easy when everyone is close.

This is the baseline. Every later step introduces a problem
that breaks this comfortable world.

Demonstrates:
- Paxos Prepare/Promise/Accept/Accepted mapped to VMTP transactions
- 2 packets per acceptor per phase (request + response)
- Consensus in ~2ms (two 1ms round-trips)
- Total: 12 packets for 3-node consensus (6 per phase)
"""

import simpy

from entity import EntityRegistry
from datacenter import DatacenterNetwork, single_dc_topology
from paxos import (
    Acceptor, Proposer, Learner,
    MajorityQuorum, ConsensusResult,
)


def run_single_consensus(verbose: bool = True) -> ConsensusResult:
    """Run a single Paxos consensus in one datacenter."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = single_dc_topology(env)

    # Create 3 acceptors + 1 proposer, all in the same DC
    acceptor_entities = [registry.create(name=f"acceptor-{i}") for i in range(3)]
    proposer_entity = registry.create(name="proposer")

    # All in the same datacenter
    for e in acceptor_entities + [proposer_entity]:
        network.assign_entity(e.id, "dc-west")

    # Start acceptors
    acceptors = [
        Acceptor(env, e, network, process_time=0.0005)  # 0.5ms processing
        for e in acceptor_entities
    ]

    # Proposer with majority quorum
    acceptor_ids = [e.id for e in acceptor_entities]
    quorum = MajorityQuorum(acceptor_ids)
    proposer = Proposer(
        env, proposer_entity, network, acceptor_ids, quorum,
        timeout=0.100,  # 100ms timeout (generous for local DC)
    )

    # Learner (in-process, not networked)
    learner = Learner()

    # Run consensus
    result_holder = [None]

    def run():
        result = yield proposer.propose(slot=0, value="hello-world")
        result_holder[0] = result
        if result.success:
            learner.learn(result.slot, result.value)

    env.process(run())
    env.run(until=5.0)

    result = result_holder[0]

    if verbose:
        print("=" * 70)
        print("STEP 1: Single Datacenter Paxos on VMTP")
        print("=" * 70)
        print()
        print(f"  Topology:    1 datacenter (dc-west), 3 acceptors, 1 proposer")
        print(f"  Local RTT:   ~2ms (1ms one-way)")
        print()
        print(f"  Consensus:   {'ACHIEVED' if result.success else 'FAILED'}")
        print(f"  Value:       {result.value}")
        print(f"  Rounds:      {result.rounds}")
        print(f"  Time:        {result.total_time * 1000:.1f}ms")
        print()
        print(f"  Phase 1 (Prepare/Promise):")
        print(f"    Sent:      3 Prepare requests (VMTP transactions)")
        print(f"    Received:  {result.phase1_responses} Promise responses")
        print(f"    Quorum:    {quorum.phase1_quorum_size()} of {quorum.n} needed")
        print()
        print(f"  Phase 2 (Accept/Accepted):")
        print(f"    Sent:      3 Accept requests (VMTP transactions)")
        print(f"    Received:  {result.phase2_responses} Accepted responses")
        print(f"    Quorum:    {quorum.phase2_quorum_size()} of {quorum.n} needed")
        print()
        print(f"  Packets:     {result.packets_sent} sent by proposer")
        print(f"                + {result.phase1_responses + result.phase2_responses} responses")
        print(f"                = {result.packets_sent + result.phase1_responses + result.phase2_responses} total")
        print()
        print(f"  Network:     {network.topology_stats}")
        print()
        print(f"  Learner log: {learner.decided}")
        print()

    return result


def run_multiple_slots(num_slots: int = 10, verbose: bool = True) -> list[ConsensusResult]:
    """Run consensus on multiple slots sequentially."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = single_dc_topology(env)

    acceptor_entities = [registry.create(name=f"acceptor-{i}") for i in range(3)]
    proposer_entity = registry.create(name="proposer")

    for e in acceptor_entities + [proposer_entity]:
        network.assign_entity(e.id, "dc-west")

    acceptors = [
        Acceptor(env, e, network, process_time=0.0005)
        for e in acceptor_entities
    ]

    acceptor_ids = [e.id for e in acceptor_entities]
    quorum = MajorityQuorum(acceptor_ids)
    proposer = Proposer(env, proposer_entity, network, acceptor_ids, quorum)

    learner = Learner()
    results = []

    def run():
        for slot in range(num_slots):
            result = yield proposer.propose(slot=slot, value=f"value-{slot}")
            results.append(result)
            if result.success:
                learner.learn(result.slot, result.value)

    env.process(run())
    env.run(until=30.0)

    if verbose:
        print()
        print("=" * 70)
        print(f"MULTI-SLOT CONSENSUS: {num_slots} decisions in one datacenter")
        print("=" * 70)
        print()
        print(f"  {'Slot':>4}  {'Value':<15} {'Time':>8} {'Pkts':>5} {'Rounds':>6}")
        print(f"  {'─'*4}  {'─'*15} {'─'*8} {'─'*5} {'─'*6}")
        for r in results:
            status = r.value if r.success else "FAILED"
            print(f"  {r.slot:>4}  {str(status):<15} {r.total_time*1000:>7.1f}ms {r.packets_sent:>5} {r.rounds:>6}")

        total_time = sum(r.total_time for r in results)
        total_pkts = sum(r.packets_sent for r in results)
        successes = sum(1 for r in results if r.success)
        print()
        print(f"  Total:  {successes}/{num_slots} succeeded, "
              f"{total_time*1000:.1f}ms, {total_pkts} packets")
        print(f"  Avg:    {total_time/num_slots*1000:.1f}ms per decision, "
              f"{total_pkts/num_slots:.0f} packets per decision")
        print()
        print(f"  Learner verified: {len(learner)} slots decided, all consistent")
        print()

    return results


def run_competing_proposers(verbose: bool = True):
    """Two proposers compete for the same slot. Shows Paxos conflict resolution."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = single_dc_topology(env)

    acceptor_entities = [registry.create(name=f"acceptor-{i}") for i in range(3)]
    proposer_a_entity = registry.create(name="proposer-A")
    proposer_b_entity = registry.create(name="proposer-B")

    for e in acceptor_entities + [proposer_a_entity, proposer_b_entity]:
        network.assign_entity(e.id, "dc-west")

    acceptors = [
        Acceptor(env, e, network, process_time=0.0005)
        for e in acceptor_entities
    ]

    acceptor_ids = [e.id for e in acceptor_entities]
    quorum = MajorityQuorum(acceptor_ids)

    proposer_a = Proposer(env, proposer_a_entity, network, acceptor_ids, quorum)
    proposer_b = Proposer(env, proposer_b_entity, network, acceptor_ids, quorum)

    learner = Learner()
    results = {"a": None, "b": None}

    def run_a():
        result = yield proposer_a.propose(slot=0, value="value-from-A")
        results["a"] = result
        if result.success:
            learner.learn(result.slot, result.value)

    def run_b():
        # Start slightly after A to create a realistic race
        yield env.timeout(0.0005)
        result = yield proposer_b.propose(slot=0, value="value-from-B")
        results["b"] = result
        if result.success:
            learner.learn(result.slot, result.value)

    env.process(run_a())
    env.process(run_b())
    env.run(until=10.0)

    if verbose:
        print()
        print("=" * 70)
        print("COMPETING PROPOSERS: Two proposers, same slot")
        print("=" * 70)
        print()
        ra, rb = results["a"], results["b"]
        print(f"  Proposer A: {'SUCCEEDED' if ra.success else 'FAILED'}, "
              f"value={ra.value}, {ra.rounds} rounds, {ra.total_time*1000:.1f}ms")
        print(f"  Proposer B: {'SUCCEEDED' if rb.success else 'FAILED'}, "
              f"value={rb.value}, {rb.rounds} rounds, {rb.total_time*1000:.1f}ms")
        print()

        # Both should agree on the same value (Paxos safety)
        if ra.success and rb.success:
            if ra.value == rb.value:
                print(f"  SAFETY CHECK: Both agreed on '{ra.value}' - CORRECT")
            else:
                print(f"  SAFETY VIOLATION: A={ra.value}, B={rb.value} - BUG!")
        elif ra.success:
            print(f"  A won with '{ra.value}', B failed after {rb.rounds} rounds")
        elif rb.success:
            print(f"  B won with '{rb.value}', A failed after {ra.rounds} rounds")
        else:
            print(f"  Both failed! (Possible with concurrent proposals)")

        print()
        print(f"  Learner: {learner.decided}")
        print()
        print("  Key insight: Paxos guarantees at most ONE value per slot,")
        print("  even with competing proposers. Conflicts cost extra rounds,")
        print("  but never violate safety.")
        print()


if __name__ == "__main__":
    # Demo 1: Single consensus - the basics
    run_single_consensus()

    # Demo 2: Multiple decisions - throughput
    run_multiple_slots(num_slots=10)

    # Demo 3: Competing proposers - safety under contention
    run_competing_proposers()
