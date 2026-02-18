"""Step 5: Add Orbit - Entity Migration During Consensus

LEO satellites move. Ground stations change. Latency varies.
A node that was 20ms away is now 40ms away, then hands off
to a different ground station entirely.

VMTP's entity migration: the node's address changes but its
identity doesn't. Paxos doesn't care that the acceptor moved.

TCP/QUIC: connection breaks on handoff. Must reconnect.
During reconnection, that acceptor is unavailable for consensus.

This is where the original VMTP story comes back:
not for steady-state performance, but for resilience
during topology changes.
"""

import simpy

from entity import EntityRegistry
from datacenter import DatacenterNetwork, five_dc_topology
from paxos import (
    Acceptor, Proposer, Learner,
    MajorityQuorum, FlexibleQuorum, ConsensusResult,
)


def _build_orbit_topology(env, seed=42):
    """5 DCs + LEO satellite with changing ground station."""
    network = five_dc_topology(env, seed=seed)

    # LEO satellite initially closest to NA-West
    network.add_location("leo-sat")
    network.add_link("na-west", "leo-sat", latency=0.020, jitter=0.005)  # 20ms
    network.add_link("europe", "leo-sat", latency=0.035, jitter=0.005)   # 35ms
    network.add_link("asia", "leo-sat", latency=0.045, jitter=0.005)     # 45ms

    return network


def run_stable_orbit(verbose: bool = True) -> list[ConsensusResult]:
    """Satellite in orbit, stable position. Just another node."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = _build_orbit_topology(env)

    # 5 ground acceptors + 1 satellite acceptor
    locations = ["na-west", "europe", "asia", "sa-east", "africa"]
    acc_entities = []
    for loc in locations:
        e = registry.create(name=f"acc-{loc}")
        network.assign_entity(e.id, loc)
        acc_entities.append(e)

    sat_entity = registry.create(name="acc-leo-sat")
    network.assign_entity(sat_entity.id, "leo-sat")
    acc_entities.append(sat_entity)

    proposer_entity = registry.create(name="proposer-nawest")
    network.assign_entity(proposer_entity.id, "na-west")

    acceptors = [Acceptor(env, e, network, process_time=0.0005) for e in acc_entities]
    acc_ids = [e.id for e in acc_entities]

    # Flexible Paxos: Phase 2 = 2 (local + satellite is fast)
    quorum = FlexibleQuorum(acc_ids, phase1_size=5, phase2_size=2)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=1.0)

    results = []

    def run():
        for slot in range(10):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=120.0)

    if verbose:
        print("=" * 70)
        print("STEP 5a: Stable Orbit - Satellite as Acceptor")
        print("=" * 70)
        print()
        print("  6 acceptors: 5 ground DCs + 1 LEO satellite")
        print("  Satellite RTT from NA-West: ~40ms")
        print("  Flex Paxos: Phase1=5, Phase2=2")
        print()
        _print_summary(results, quorum)

    return results


def run_satellite_handoff(verbose: bool = True):
    """Satellite moves: latency changes as it passes over different ground stations.

    Simulates a LEO satellite pass:
    - Phase 1: Close to NA-West (20ms)
    - Phase 2: Moving away, latency increasing
    - Phase 3: Handoff - now closer to Europe (25ms)
    - Phase 4: Moving away from Europe too
    """
    env = simpy.Environment()
    registry = EntityRegistry()
    network = _build_orbit_topology(env)

    locations = ["na-west", "europe", "asia", "sa-east", "africa"]
    acc_entities = []
    for loc in locations:
        e = registry.create(name=f"acc-{loc}")
        network.assign_entity(e.id, loc)
        acc_entities.append(e)

    sat_entity = registry.create(name="acc-leo-sat")
    network.assign_entity(sat_entity.id, "leo-sat")
    acc_entities.append(sat_entity)

    proposer_entity = registry.create(name="proposer-nawest")
    network.assign_entity(proposer_entity.id, "na-west")

    acceptors = [Acceptor(env, e, network, process_time=0.0005) for e in acc_entities]
    acc_ids = [e.id for e in acc_entities]

    quorum = FlexibleQuorum(acc_ids, phase1_size=5, phase2_size=2)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=1.0)

    results = []
    phases = []

    def orbital_dynamics():
        """Simulate satellite movement by updating link latencies."""
        # Phase 1: Close to NA-West
        yield env.timeout(2.0)

        # Phase 2: Moving away from NA-West, closer to Europe
        network.update_link("na-west", "leo-sat", latency=0.035)
        network.update_link("europe", "leo-sat", latency=0.025)
        yield env.timeout(2.0)

        # Phase 3: Handoff complete - satellite now serves Europe
        network.update_link("na-west", "leo-sat", latency=0.050)
        network.update_link("europe", "leo-sat", latency=0.020)
        yield env.timeout(2.0)

        # Phase 4: Moving away from Europe too
        network.update_link("na-west", "leo-sat", latency=0.065)
        network.update_link("europe", "leo-sat", latency=0.035)

    def workload():
        phase_labels = [
            "near NA-West (20ms)",
            "transitioning (35ms)",
            "near Europe (50ms from NA)",
            "moving away (65ms from NA)",
        ]

        slot = 0
        for phase_idx, label in enumerate(phase_labels):
            phase_results = []
            for i in range(5):
                result = yield proposer.propose(slot=slot, value=f"v-{slot}")
                results.append(result)
                phase_results.append(result)
                slot += 1

            phases.append((label, phase_results))
            yield env.timeout(1.5)  # Wait for orbital dynamics

    env.process(orbital_dynamics())
    env.process(workload())
    env.run(until=30.0)

    if verbose:
        print("=" * 70)
        print("STEP 5b: Satellite Handoff - Latency Changes Mid-Consensus")
        print("=" * 70)
        print()
        print("  LEO satellite moves: NA-West → transition → Europe")
        print("  Link latencies update dynamically.")
        print()
        print("  VMTP doesn't care. Entity ID is stable.")
        print("  Paxos doesn't care. It just sees varying response times.")
        print()

        for label, phase_results in phases:
            successful = [r for r in phase_results if r.success]
            if successful:
                avg = sum(r.total_time for r in successful) / len(successful)
                print(f"  Satellite {label}:")
                print(f"    {len(successful)}/{len(phase_results)} succeeded, "
                      f"avg {avg*1000:.1f}ms")
            else:
                print(f"  Satellite {label}: all failed")

        print()
        print("  Consensus continues through orbital handoff.")
        print("  No reconnection. No identity change. Just physics.")
        print()

    return results


def run_migration_during_consensus(verbose: bool = True):
    """An acceptor migrates between DCs during active consensus.

    This is VMTP's unique capability. The acceptor's entity ID
    stays the same. The network routing updates. Paxos continues.

    With TCP/QUIC, migration = connection break = that acceptor is
    temporarily unavailable = potential quorum loss.
    """
    env = simpy.Environment()
    registry = EntityRegistry()
    network = five_dc_topology(env)

    locations = ["na-west", "europe", "asia", "sa-east", "africa"]
    acc_entities = []
    for loc in locations:
        e = registry.create(name=f"acc-{loc}")
        network.assign_entity(e.id, loc)
        acc_entities.append(e)

    proposer_entity = registry.create(name="proposer-nawest")
    network.assign_entity(proposer_entity.id, "na-west")

    acceptors = [Acceptor(env, e, network, process_time=0.0005) for e in acc_entities]
    acc_ids = [e.id for e in acc_entities]

    # Flexible Paxos: Phase 2 needs 2 nodes.
    # Before migration: fastest 2 = local (~2ms) + Europe (~100ms) → ~100ms
    # After migration:  fastest 2 = local (~2ms) + ex-Europe (~2ms) → ~2ms
    quorum = FlexibleQuorum(acc_ids, phase1_size=4, phase2_size=2)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=1.0)

    results_before = []
    results_after = []
    migration_time = None

    def workload():
        nonlocal migration_time

        # Before migration: Europe acceptor at 100ms RTT
        for slot in range(10):
            result = yield proposer.propose(slot=slot, value=f"pre-{slot}")
            results_before.append(result)

        # Migrate Europe acceptor to NA-West (service migration)
        migration_time = env.now
        network.migrate_entity(acc_entities[1].id, "na-west")  # Europe → NA-West

        # After migration: that acceptor is now local (~2ms RTT)
        for slot in range(10, 20):
            result = yield proposer.propose(slot=slot, value=f"post-{slot}")
            results_after.append(result)

    env.process(workload())
    env.run(until=300.0)

    if verbose:
        print("=" * 70)
        print("STEP 5c: Acceptor Migration During Consensus")
        print("=" * 70)
        print()
        print("  Europe acceptor migrates to NA-West mid-session.")
        print("  Entity ID unchanged. Network routing updated.")
        print("  Flexible Paxos: Phase2=2 (wait for 2nd-fastest)")
        print()

        avg_before = sum(r.total_time for r in results_before if r.success) / max(1, len([r for r in results_before if r.success]))
        avg_after = sum(r.total_time for r in results_after if r.success) / max(1, len([r for r in results_after if r.success]))

        print(f"  Before migration (2nd fastest = Europe at 100ms RTT):")
        print(f"    {sum(1 for r in results_before if r.success)}/{len(results_before)} succeeded")
        print(f"    Avg: {avg_before*1000:.1f}ms")
        print()
        print(f"  After migration (2nd fastest = ex-Europe, now local at ~2ms):")
        print(f"    {sum(1 for r in results_after if r.success)}/{len(results_after)} succeeded")
        print(f"    Avg: {avg_after*1000:.1f}ms")
        print()
        if avg_after > 0 and avg_before > 0:
            print(f"  Speedup: {avg_before/avg_after:.1f}x (one migration, zero packets)")
        print()
        print("  VMTP migration cost: 0 packets, 0 reconnections.")
        print("  TCP equivalent: tear down + re-establish connections")
        print("  to that acceptor from every other node.")
        print()

    return results_before, results_after


def _print_summary(results, quorum):
    successful = [r for r in results if r.success]
    if not successful:
        print("  No successful decisions!")
        return
    times = [r.total_time for r in successful]
    avg = sum(times) / len(times)
    print(f"  Phase 1: {quorum.phase1_quorum_size()}, Phase 2: {quorum.phase2_quorum_size()}")
    print(f"  Decisions: {len(successful)}/{len(results)}")
    print(f"  Avg: {avg*1000:.1f}ms")
    print()


if __name__ == "__main__":
    run_stable_orbit()
    run_satellite_handoff()
    run_migration_during_consensus()
