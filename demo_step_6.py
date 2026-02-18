"""Step 6: Add Moon - The Latency Wall

Earth-Moon distance: ~384,400 km
Speed of light: ~299,792 km/s
One-way latency: ~1.28 seconds
Round-trip: ~2.56 seconds

With a lunar acceptor in majority quorum:
- Phase 1: wait for Moon's Promise = 2.56+ seconds
- Phase 2: wait for Moon's Accepted = 2.56+ seconds
- Total: 5.12+ seconds for one consensus round

With Flexible Paxos: keep the Moon OUT of Phase 2.
- Phase 1: include Moon (rare elections) = slow but acceptable
- Phase 2: Earth-only quorum = normal latency

This is where Flexible Paxos transitions from
"nice optimization" to "existential requirement."

Do you even WANT the Moon in your quorum?
The answer: yes, for durability. No, for speed.
Flexible Paxos lets you have both.
"""

import simpy

from entity import EntityRegistry
from datacenter import DatacenterNetwork, five_dc_topology
from paxos import (
    Acceptor, Proposer, Learner,
    MajorityQuorum, FlexibleQuorum, ConsensusResult,
)


MOON_LATENCY = 1.28  # seconds, one-way


def _build_lunar_topology(env, seed=42):
    """5 Earth DCs + Moon base."""
    network = five_dc_topology(env, seed=seed)

    network.add_location("moon")
    # Moon has similar latency to all Earth DCs (light-speed dominates)
    for loc in ["na-west", "europe", "asia", "sa-east", "africa"]:
        network.add_link(loc, "moon", latency=MOON_LATENCY, jitter=0.010)

    return network


def _setup_lunar(env, registry, network, include_moon=True):
    """Setup acceptors across Earth + Moon."""
    locations = ["na-west", "europe", "asia", "sa-east", "africa"]
    acc_entities = []
    for loc in locations:
        e = registry.create(name=f"acc-{loc}")
        network.assign_entity(e.id, loc)
        acc_entities.append(e)

    if include_moon:
        moon_entity = registry.create(name="acc-moon")
        network.assign_entity(moon_entity.id, "moon")
        acc_entities.append(moon_entity)

    proposer_entity = registry.create(name="proposer-nawest")
    network.assign_entity(proposer_entity.id, "na-west")

    acceptors = [Acceptor(env, e, network, process_time=0.0005) for e in acc_entities]
    acc_ids = [e.id for e in acc_entities]

    return acc_entities, acc_ids, proposer_entity


def run_majority_with_moon(verbose: bool = True) -> list[ConsensusResult]:
    """6 nodes (5 Earth + Moon), majority quorum. Feel the pain."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = _build_lunar_topology(env)

    acc_entities, acc_ids, proposer_entity = _setup_lunar(env, registry, network)

    # Majority of 6 = 4. Must include Moon if ANY Earth node is slow/down.
    # Actually with 5 Earth + 1 Moon, majority = 4.
    # If all 5 Earth respond, we don't need Moon. But with any failure...
    quorum = MajorityQuorum(acc_ids)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=10.0)

    results = []

    def run():
        for slot in range(5):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=120.0)

    if verbose:
        print("=" * 70)
        print("STEP 6a: Majority Quorum With Moon (6 nodes, need 4)")
        print("=" * 70)
        print()
        print(f"  Moon one-way latency: {MOON_LATENCY}s ({MOON_LATENCY*2:.2f}s RTT)")
        print(f"  Majority of 6 = 4")
        print()
        print(f"  If all 5 Earth nodes respond: don't need Moon. Fast.")
        print(f"  If 1 Earth node fails: MUST include Moon. 2.56s per phase.")
        print()
        _print_results(results)

    return results


def run_majority_earth_failure(verbose: bool = True) -> list[ConsensusResult]:
    """One Earth node down. Majority must include Moon."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = _build_lunar_topology(env)

    locations = ["na-west", "europe", "asia", "sa-east", "africa"]
    acc_entities = []
    for loc in locations:
        e = registry.create(name=f"acc-{loc}")
        network.assign_entity(e.id, loc)
        acc_entities.append(e)

    moon_entity = registry.create(name="acc-moon")
    network.assign_entity(moon_entity.id, "moon")
    acc_entities.append(moon_entity)

    proposer_entity = registry.create(name="proposer-nawest")
    network.assign_entity(proposer_entity.id, "na-west")

    # Start all except Africa (simulates failure)
    for e in acc_entities:
        if "africa" not in e.name:
            Acceptor(env, e, network, process_time=0.0005)
        else:
            network.register(e.id)

    acc_ids = [e.id for e in acc_entities]
    quorum = MajorityQuorum(acc_ids)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=10.0)

    results = []

    def run():
        for slot in range(3):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=120.0)

    if verbose:
        print("=" * 70)
        print("STEP 6b: One Earth Node Down - Must Include Moon")
        print("=" * 70)
        print()
        print("  Africa is down. 4 Earth + 1 Moon. Majority = 4.")
        print("  Only 4 Earth nodes responding, so quorum = 4.")
        print("  ...but wait, does the Moon response arrive faster")
        print("  than the 4th-fastest Earth node?")
        print()

        if results:
            # Show the cliff
            print("  The answer:")
            for r in results:
                if r.success:
                    print(f"    Slot {r.slot}: {r.total_time*1000:.0f}ms ({r.total_time:.2f}s)")
                else:
                    print(f"    Slot {r.slot}: FAILED after {r.total_time:.2f}s")
            print()

            # 4th fastest Earth from NA-West: Asia (~150ms RTT)
            # Moon: ~2560ms RTT
            # So we don't actually need Moon here - 4 Earth nodes suffice
            avg = sum(r.total_time for r in results if r.success) / max(1, len([r for r in results if r.success]))
            if avg < 1.0:
                print("  4 remaining Earth nodes still form a majority of 6.")
                print("  Moon wasn't needed! But lose TWO Earth nodes...")
            else:
                print("  The Moon's RTT dominates. 2.56 seconds per phase.")

        print()

    return results


def run_two_earth_down(verbose: bool = True) -> list[ConsensusResult]:
    """Two Earth nodes down. NOW the Moon matters."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = _build_lunar_topology(env)

    locations = ["na-west", "europe", "asia", "sa-east", "africa"]
    acc_entities = []
    for loc in locations:
        e = registry.create(name=f"acc-{loc}")
        network.assign_entity(e.id, loc)
        acc_entities.append(e)

    moon_entity = registry.create(name="acc-moon")
    network.assign_entity(moon_entity.id, "moon")
    acc_entities.append(moon_entity)

    proposer_entity = registry.create(name="proposer-nawest")
    network.assign_entity(proposer_entity.id, "na-west")

    # Only start NA-West, Europe, Asia, Moon (Africa + SA down)
    for e in acc_entities:
        if "africa" not in e.name and "sa-east" not in e.name:
            Acceptor(env, e, network, process_time=0.0005)
        else:
            network.register(e.id)

    acc_ids = [e.id for e in acc_entities]
    quorum = MajorityQuorum(acc_ids)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=10.0)

    results = []

    def run():
        for slot in range(3):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=120.0)

    if verbose:
        print("=" * 70)
        print("STEP 6c: Two Earth Nodes Down - Moon IS the Quorum")
        print("=" * 70)
        print()
        print("  Africa + SA-East down. 3 Earth + 1 Moon alive.")
        print("  Majority of 6 = 4. Must include Moon.")
        print()
        for r in results:
            if r.success:
                print(f"  Slot {r.slot}: {r.total_time:.2f}s "
                      f"({r.total_time*1000:.0f}ms)")
            else:
                print(f"  Slot {r.slot}: FAILED")
        print()
        if results and results[0].success:
            print(f"  Each decision: ~{results[0].total_time:.1f} seconds.")
            print(f"  That's the Moon's RTT. Physics. Non-negotiable.")
        print()

    return results


def run_flexible_lunar(verbose: bool = True) -> list[ConsensusResult]:
    """Flexible Paxos: Phase 1 includes Moon, Phase 2 stays on Earth."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = _build_lunar_topology(env)

    acc_entities, acc_ids, proposer_entity = _setup_lunar(env, registry, network)

    # Phase 1: all 6 (including Moon) - for election durability
    # Phase 2: 1 node (local only) - for commit speed
    # 6 + 1 = 7 > 6 ✓
    quorum = FlexibleQuorum(acc_ids, phase1_size=6, phase2_size=1)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=10.0)

    results = []

    def run():
        for slot in range(5):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=120.0)

    if verbose:
        print("=" * 70)
        print("STEP 6d: Flexible Paxos - Moon in Phase 1 Only")
        print("=" * 70)
        print()
        print("  Phase 1: ALL 6 (including Moon) - elections include lunar vote")
        print("  Phase 2: just 1 (local) - commits are Earth-speed")
        print()
        _print_results(results)
        if results:
            avg = sum(r.total_time for r in results if r.success) / max(1, len([r for r in results if r.success]))
            print(f"  Phase 1 waits for Moon (~2.56s). Phase 2 is local (~2ms).")
            print(f"  Total: ~{avg:.1f}s per full round.")
            print()
            print(f"  In Multi-Paxos: Phase 1 happens ONCE (leader election).")
            print(f"  After that, every commit is Phase 2 only = ~2ms.")
            print(f"  The Moon votes in elections but doesn't slow commits.")

        print()

    return results


def run_flexible_balanced_lunar(verbose: bool = True) -> list[ConsensusResult]:
    """Flexible Paxos: Phase 1 = 5, Phase 2 = 2. Practical balance."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = _build_lunar_topology(env)

    acc_entities, acc_ids, proposer_entity = _setup_lunar(env, registry, network)

    # Phase 1: 5 of 6 (can afford 1 failure, including Moon failure)
    # Phase 2: 2 of 6 (fast: local + nearest Earth DC)
    # 5 + 2 = 7 > 6 ✓
    quorum = FlexibleQuorum(acc_ids, phase1_size=5, phase2_size=2)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=10.0)

    results = []

    def run():
        for slot in range(5):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=120.0)

    if verbose:
        print("=" * 70)
        print("STEP 6e: Flexible Paxos - Balanced (Phase1=5, Phase2=2)")
        print("=" * 70)
        print()
        print("  Phase 1: 5 of 6 (Moon failure doesn't block elections)")
        print("  Phase 2: 2 of 6 (local + Europe ≈ 100ms)")
        print()
        _print_results(results)
        if results:
            print(f"  Phase 1 needs 5: waits for 5th fastest.")
            print(f"  If Moon is 6th, Phase 1 doesn't wait for it!")
            print(f"  The Moon adds durability without adding latency.")

        print()

    return results


def the_punchline(verbose: bool = True):
    """Side-by-side: the absurdity of including Moon in steady-state."""
    if not verbose:
        return

    print()
    print("=" * 70)
    print("THE LUNAR PUNCHLINE")
    print("=" * 70)
    print()
    print("  Multi-Paxos steady-state (Phase 2 only):")
    print()
    print(f"  {'Strategy':<30} {'Phase 2 latency':>18} {'Per commit':>12}")
    print(f"  {'─'*30} {'─'*18} {'─'*12}")
    print(f"  {'Majority (4/6)':.<30} {'150ms (Asia)':>18} {'~150ms':>12}")
    print(f"  {'Flex(6,1) - local only':.<30} {'2ms (local)':>18} {'~2ms':>12}")
    print(f"  {'Flex(5,2) - balanced':.<30} {'100ms (Europe)':>18} {'~100ms':>12}")
    print(f"  {'Include Moon in Phase 2':.<30} {'2,560ms (Moon)':>18} {'~2.6s':>12}")
    print()
    print(f"  Majority with Moon in Phase 2: 2.6 SECONDS per commit.")
    print(f"  Flexible Paxos, Phase 2 local: 2 MILLISECONDS per commit.")
    print(f"  Ratio: 1,280x")
    print()
    print(f"  The Moon adds geographic fault tolerance:")
    print(f"    If ALL of Earth goes offline, Moon has the last election state.")
    print(f"    Worth including in Phase 1 (rare elections).")
    print(f"    Catastrophic in Phase 2 (every commit).")
    print()
    print(f"  Flexible Paxos isn't an optimization here.")
    print(f"  It's the difference between a system that works and one that doesn't.")
    print()
    print(f"  Next: Mars. Where 'slow' means 4-24 MINUTES.")
    print()


def _print_results(results):
    for r in results:
        if r.success:
            if r.total_time > 1.0:
                print(f"  Slot {r.slot}: {r.total_time:.2f}s")
            else:
                print(f"  Slot {r.slot}: {r.total_time*1000:.1f}ms")
        else:
            print(f"  Slot {r.slot}: FAILED ({r.rounds} rounds, {r.total_time:.2f}s)")
    print()


if __name__ == "__main__":
    run_majority_with_moon()
    run_majority_earth_failure()
    run_two_earth_down()
    run_flexible_lunar()
    run_flexible_balanced_lunar()
    the_punchline()
