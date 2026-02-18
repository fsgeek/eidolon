"""Step 2: Two Datacenters - "Latency Has a Cost"

NA-West + Europe. 50ms one-way between them.
Three acceptors: 2 in NA-West, 1 in Europe.

When everything works, majority quorum (2 of 3) can be satisfied
locally in ~2ms. But kill one local acceptor and you MUST include
Europe - consensus jumps to ~100ms. Partition the DCs and you
lose quorum entirely.

This is the cliff. This is what motivates everything that follows.
"""

import simpy

from entity import EntityRegistry
from datacenter import DatacenterNetwork, two_dc_topology
from paxos import (
    Acceptor, Proposer, Learner,
    MajorityQuorum, ConsensusResult,
)


def run_two_dc_happy(verbose: bool = True) -> list[ConsensusResult]:
    """Two DCs, all healthy. Quorum satisfied locally."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = two_dc_topology(env)

    # 2 acceptors in NA-West, 1 in Europe
    acc_entities = [registry.create(name=f"acc-nawest-{i}") for i in range(2)]
    acc_eu = registry.create(name="acc-europe")
    proposer_entity = registry.create(name="proposer-nawest")

    for e in acc_entities:
        network.assign_entity(e.id, "na-west")
    network.assign_entity(acc_eu.id, "europe")
    network.assign_entity(proposer_entity.id, "na-west")

    all_acc_entities = acc_entities + [acc_eu]
    acceptors = [Acceptor(env, e, network, process_time=0.0005) for e in all_acc_entities]

    acc_ids = [e.id for e in all_acc_entities]
    quorum = MajorityQuorum(acc_ids)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=0.500)

    results = []

    def run():
        for slot in range(10):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=30.0)

    if verbose:
        print("=" * 70)
        print("STEP 2a: Two DCs, All Healthy")
        print("=" * 70)
        print()
        print("  Topology:  NA-West (2 acceptors + proposer) + Europe (1 acceptor)")
        print("  Inter-DC:  ~100ms RTT (50ms one-way)")
        print("  Quorum:    2 of 3 (majority)")
        print()
        print(f"  {'Slot':>4}  {'Time':>8}  {'Rounds':>6}  Note")
        print(f"  {'─'*4}  {'─'*8}  {'─'*6}  {'─'*30}")
        for r in results:
            note = "local quorum (2 NA-West)" if r.total_time < 0.010 else "needed Europe"
            print(f"  {r.slot:>4}  {r.total_time*1000:>7.1f}ms  {r.rounds:>6}  {note}")

        avg = sum(r.total_time for r in results) / len(results)
        print()
        print(f"  Average: {avg*1000:.1f}ms")
        print()
        print("  With 2 local acceptors, quorum is satisfied without Europe.")
        print("  Europe's responses arrive later but aren't needed.")
        print()

    return results


def run_one_local_down(verbose: bool = True) -> list[ConsensusResult]:
    """One NA-West acceptor down. Must include Europe for quorum."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = two_dc_topology(env)

    # Same setup but one local acceptor will be "down" (not started)
    acc_local = registry.create(name="acc-nawest-0")
    acc_down = registry.create(name="acc-nawest-1-DOWN")
    acc_eu = registry.create(name="acc-europe")
    proposer_entity = registry.create(name="proposer-nawest")

    network.assign_entity(acc_local.id, "na-west")
    network.assign_entity(acc_down.id, "na-west")
    network.assign_entity(acc_eu.id, "europe")
    network.assign_entity(proposer_entity.id, "na-west")

    all_acc_ids = [acc_local.id, acc_down.id, acc_eu.id]

    # Only start 2 of 3 acceptors (one local is down)
    Acceptor(env, acc_local, network, process_time=0.0005)
    # acc_down is NOT started - simulates failure
    network.register(acc_down.id)  # Register mailbox so packets don't error, they just queue
    Acceptor(env, acc_eu, network, process_time=0.0005)

    quorum = MajorityQuorum(all_acc_ids)
    proposer = Proposer(env, proposer_entity, network, all_acc_ids, quorum, timeout=0.500)

    results = []

    def run():
        for slot in range(10):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=60.0)

    if verbose:
        print("=" * 70)
        print("STEP 2b: One Local Acceptor Down - The Latency Cliff")
        print("=" * 70)
        print()
        print("  Topology:  NA-West (1 acceptor + proposer) + Europe (1 acceptor)")
        print("             One NA-West acceptor is DOWN")
        print("  Quorum:    Still 2 of 3, but now MUST include Europe")
        print()
        print(f"  {'Slot':>4}  {'Time':>8}  {'Rounds':>6}  Note")
        print(f"  {'─'*4}  {'─'*8}  {'─'*6}  {'─'*30}")
        for r in results:
            note = "MUST wait for Europe"
            print(f"  {r.slot:>4}  {r.total_time*1000:>7.1f}ms  {r.rounds:>6}  {note}")

        avg = sum(r.total_time for r in results) / len(results)
        print()
        print(f"  Average: {avg*1000:.1f}ms")
        print()

    return results


def run_partition(verbose: bool = True) -> list[ConsensusResult]:
    """DCs partitioned. No quorum possible with 2-of-3 split 2+1."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = two_dc_topology(env)

    acc_entities = [registry.create(name=f"acc-nawest-{i}") for i in range(2)]
    acc_eu = registry.create(name="acc-europe")
    proposer_entity = registry.create(name="proposer-nawest")

    for e in acc_entities:
        network.assign_entity(e.id, "na-west")
    network.assign_entity(acc_eu.id, "europe")
    network.assign_entity(proposer_entity.id, "na-west")

    all_acc_entities = acc_entities + [acc_eu]
    acceptors = [Acceptor(env, e, network, process_time=0.0005) for e in all_acc_entities]

    acc_ids = [e.id for e in all_acc_entities]
    quorum = MajorityQuorum(acc_ids)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=0.300)

    results = {"before": [], "during": [], "after": []}

    def run():
        # Before partition: should work fine
        for slot in range(3):
            result = yield proposer.propose(slot=slot, value=f"pre-{slot}")
            results["before"].append(result)

        # Partition happens
        yield env.timeout(0.050)
        network.partition_locations("na-west", "europe")

        # During partition: 2 local + proposer, quorum = 2 of 3
        # With 2 in na-west, we still have quorum locally!
        for slot in range(3, 6):
            result = yield proposer.propose(slot=slot, value=f"during-{slot}")
            results["during"].append(result)

        # Heal
        network.heal_all()
        yield env.timeout(0.010)

        # After: back to normal
        for slot in range(6, 9):
            result = yield proposer.propose(slot=slot, value=f"post-{slot}")
            results["after"].append(result)

    env.process(run())
    env.run(until=60.0)

    if verbose:
        print("=" * 70)
        print("STEP 2c: DC Partition (2+1 split)")
        print("=" * 70)
        print()
        print("  Split: NA-West has 2 acceptors + proposer, Europe has 1")
        print("  Quorum: 2 of 3 - NA-West retains quorum alone!")
        print()

        for phase, label in [("before", "Before"), ("during", "During"), ("after", "After")]:
            print(f"  {label} partition:")
            for r in results[phase]:
                status = f"{r.total_time*1000:.1f}ms" if r.success else "FAILED"
                print(f"    Slot {r.slot}: {status}")

        print()
        print("  With 2-of-3 and 2 in NA-West, the partition is survivable.")
        print("  But what if the split were 1+2? Or 1+1+1 across 3 DCs?")
        print()

    return results


def run_bad_split(verbose: bool = True) -> list[ConsensusResult]:
    """1 acceptor per DC. Partition = no quorum = no progress."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = two_dc_topology(env)

    acc_na = registry.create(name="acc-nawest")
    acc_eu = registry.create(name="acc-europe")
    proposer_entity = registry.create(name="proposer-nawest")

    network.assign_entity(acc_na.id, "na-west")
    network.assign_entity(acc_eu.id, "europe")
    network.assign_entity(proposer_entity.id, "na-west")

    all_acc = [acc_na, acc_eu]
    for e in all_acc:
        Acceptor(env, e, network, process_time=0.0005)

    acc_ids = [e.id for e in all_acc]
    quorum = MajorityQuorum(acc_ids)  # 2 of 2 = need BOTH
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=0.300)

    results = {"before": [], "during": []}

    def run():
        # Before partition: works but slow (need Europe)
        for slot in range(3):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results["before"].append(result)

        # Partition
        network.partition_locations("na-west", "europe")

        # During partition: need 2 of 2, only have 1 = stuck
        for slot in range(3, 5):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results["during"].append(result)

    env.process(run())
    env.run(until=30.0)

    if verbose:
        print("=" * 70)
        print("STEP 2d: Bad Split - 1 Acceptor Per DC")
        print("=" * 70)
        print()
        print("  2 acceptors, 1 per DC. Majority = 2 of 2 = need BOTH.")
        print()

        print("  Before partition:")
        for r in results["before"]:
            print(f"    Slot {r.slot}: {r.total_time*1000:.1f}ms (must wait for Europe)")

        print()
        print("  During partition:")
        for r in results["during"]:
            status = f"{r.total_time*1000:.0f}ms" if r.success else "FAILED (no quorum)"
            print(f"    Slot {r.slot}: {status}")

        print()
        print("  With only 2 nodes, ANY failure = no progress.")
        print("  This is why 3 DCs matter. Step 3 fixes this.")
        print()

    return results


def side_by_side(verbose: bool = True):
    """The punchline: local vs degraded latency comparison."""
    if not verbose:
        return

    print()
    print("=" * 70)
    print("THE LATENCY CLIFF")
    print("=" * 70)
    print()

    healthy = run_two_dc_happy(verbose=False)
    degraded = run_one_local_down(verbose=False)

    avg_healthy = sum(r.total_time for r in healthy) / len(healthy)
    avg_degraded = sum(r.total_time for r in degraded) / len(degraded)

    p50_h = sorted(r.total_time for r in healthy)[len(healthy)//2]
    p50_d = sorted(r.total_time for r in degraded)[len(degraded)//2]
    p99_h = sorted(r.total_time for r in healthy)[-1]
    p99_d = sorted(r.total_time for r in degraded)[-1]

    print(f"  {'Metric':<20} {'All Healthy':>15} {'1 Local Down':>15} {'Ratio':>10}")
    print(f"  {'─'*20} {'─'*15} {'─'*15} {'─'*10}")
    print(f"  {'Average':.<20} {avg_healthy*1000:>14.1f}ms {avg_degraded*1000:>14.1f}ms {avg_degraded/avg_healthy:>9.0f}x")
    print(f"  {'P50':.<20} {p50_h*1000:>14.1f}ms {p50_d*1000:>14.1f}ms {p50_d/p50_h:>9.0f}x")
    print(f"  {'P99 (worst)':.<20} {p99_h*1000:>14.1f}ms {p99_d*1000:>14.1f}ms {p99_d/p99_h:>9.0f}x")
    print()
    print(f"  One failed node turns {avg_healthy*1000:.0f}ms consensus into {avg_degraded*1000:.0f}ms.")
    print(f"  That's not graceful degradation. That's a cliff.")
    print()
    print("  The problem: majority quorum treats all nodes equally.")
    print("  It doesn't know that Europe is 50x farther than the local rack.")
    print()
    print("  What we need: a way to keep the fast path fast,")
    print("  even when some nodes are far away.")
    print()
    print("  That's Flexible Paxos. Step 3.")
    print()


if __name__ == "__main__":
    run_two_dc_happy()
    run_one_local_down()
    run_partition()
    run_bad_split()
    side_by_side()
