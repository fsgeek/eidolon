"""Step 8: All Together - "The Crumbs of Consensus"

Full topology: 5 Earth DCs + LEO satellite + Moon + Mars.
Heterogeneous latencies from 1ms to 22 minutes.
Each idea earns its place.

The crumbling walls quorum encodes the physical topology:
  Row 0 (Mars):    3 nodes  [slowest, minutes]
  Row 1 (Moon):    1 node   [slow, seconds]
  Row 2 (LEO):     1 node   [medium, tens of ms]
  Row 3 (Earth):   5 nodes  [fast, ms]

Phase 1 (elections): span all tiers. Geographic diversity.
Phase 2 (commits):   Earth only. Speed.

The quorum system knows about physics.
"""

import simpy

from entity import EntityRegistry
from datacenter import DatacenterNetwork, five_dc_topology
from paxos import (
    Acceptor, Proposer, Learner,
    MajorityQuorum, FlexibleQuorum, ConsensusResult,
)
from quorums import CrumblingWallQuorum


def build_full_topology(env, mars_latency=186.0, seed=42):
    """The complete topology: Earth + LEO + Moon + Mars."""
    network = five_dc_topology(env, seed=seed)

    # LEO satellite
    network.add_location("leo-sat")
    network.add_link("na-west", "leo-sat", latency=0.020, jitter=0.005)
    network.add_link("europe", "leo-sat", latency=0.030, jitter=0.005)
    network.add_link("asia", "leo-sat", latency=0.035, jitter=0.005)

    # Moon
    network.add_location("moon")
    for loc in ["na-west", "europe", "asia", "sa-east", "africa"]:
        network.add_link(loc, "moon", latency=1.28, jitter=0.01)
    network.add_link("leo-sat", "moon", latency=1.28, jitter=0.01)

    # Mars (3 nodes)
    for i in range(3):
        network.add_location(f"mars-{i}")
    network.add_link("mars-0", "mars-1", latency=0.005, jitter=0.001)
    network.add_link("mars-0", "mars-2", latency=0.005, jitter=0.001)
    network.add_link("mars-1", "mars-2", latency=0.005, jitter=0.001)

    for earth_loc in ["na-west", "europe"]:
        for i in range(3):
            network.add_link(earth_loc, f"mars-{i}", latency=mars_latency, jitter=5.0)
    for i in range(3):
        network.add_link("moon", f"mars-{i}", latency=mars_latency + 1.28, jitter=5.0)

    return network


def run_crumbling_wall_system(verbose: bool = True):
    """Run the full system with crumbling wall quorum for global consensus.

    The topology IS the quorum structure:
    - Tiers = geographic distance from fastest to slowest
    - Phase 2 (commits) = Earth tier only
    - Phase 1 (elections) = spans all tiers
    """
    env = simpy.Environment()
    registry = EntityRegistry()
    network = build_full_topology(env, mars_latency=186.0)

    # --- Create all nodes ---

    # Earth tier (fast)
    earth_locs = ["na-west", "europe", "asia", "sa-east", "africa"]
    earth_entities = []
    for loc in earth_locs:
        e = registry.create(name=f"earth-{loc}")
        network.assign_entity(e.id, loc)
        earth_entities.append(e)

    # LEO tier (medium)
    leo_entity = registry.create(name="leo-sat")
    network.assign_entity(leo_entity.id, "leo-sat")

    # Moon tier (slow)
    moon_entity = registry.create(name="moon")
    network.assign_entity(moon_entity.id, "moon")

    # Mars tier (slowest)
    mars_entities = []
    for i in range(3):
        e = registry.create(name=f"mars-{i}")
        network.assign_entity(e.id, f"mars-{i}")
        mars_entities.append(e)

    # Start all acceptors
    all_entities = earth_entities + [leo_entity, moon_entity] + mars_entities
    for e in all_entities:
        pt = 0.0005 if "earth" in e.name or "leo" in e.name else 0.001
        Acceptor(env, e, network, process_time=pt)

    all_ids = [e.id for e in all_entities]
    earth_ids = [e.id for e in earth_entities]
    mars_ids = [e.id for e in mars_entities]

    # --- Build the crumbling wall ---
    # Tiers ordered slow → fast (Mars, Moon, LEO, Earth)
    wall = CrumblingWallQuorum([
        mars_ids,                        # Tier 0: Mars (3 nodes, minutes away)
        [moon_entity.id],                # Tier 1: Moon (1 node, seconds away)
        [leo_entity.id],                 # Tier 2: LEO (1 node, tens of ms)
        earth_ids,                       # Tier 3: Earth (5 nodes, ms)
    ])

    # --- Earth-local proposer (uses crumbling wall for global) ---
    earth_proposer = registry.create(name="earth-proposer")
    network.assign_entity(earth_proposer.id, "na-west")
    global_prop = Proposer(
        env, earth_proposer, network, all_ids, wall,
        timeout=10.0,  # Long enough for Phase 1 to reach Moon
    )
    global_learner = Learner()

    # --- Mars-local proposer (local cluster only, autonomous) ---
    mars_proposer = registry.create(name="mars-proposer")
    network.assign_entity(mars_proposer.id, "mars-0")
    mars_quorum = MajorityQuorum(mars_ids)
    mars_prop = Proposer(env, mars_proposer, network, mars_ids, mars_quorum, timeout=1.0)
    mars_learner = Learner()

    # Results
    global_results = []
    mars_results = []

    def global_workload():
        """Earth proposer runs global consensus using crumbling wall.
        Phase 2 only needs Earth (fast). Phase 1 spans all tiers (rare)."""
        for slot in range(20):
            result = yield global_prop.propose(slot=slot, value=f"global-{slot}")
            global_results.append(result)
            if result.success:
                global_learner.learn(result.slot, result.value)

    def mars_workload():
        """Mars runs local consensus autonomously."""
        for slot in range(1000, 1020):
            result = yield mars_prop.propose(slot=slot, value=f"mars-{slot}")
            mars_results.append(result)
            if result.success:
                mars_learner.learn(result.slot, result.value)

    env.process(global_workload())
    env.process(mars_workload())
    env.run(until=300.0)

    if verbose:
        print("=" * 70)
        print("THE CRUMBS OF CONSENSUS: Crumbling Wall Quorum")
        print("=" * 70)
        print()
        print("  The topology IS the quorum structure:")
        print()
        print(wall.describe_tiers(["Mars", "Moon", "LEO", "Earth"]))
        print()
        print(f"  Total nodes: {wall.n}")
        print(f"  Majority would need: {wall.n // 2 + 1}")
        print(f"  Crumbling wall Phase 1: {wall.phase1_quorum_size()} "
              f"(spans all tiers)")
        print(f"  Crumbling wall Phase 2: {wall.phase2_quorum_size()} "
              f"(Earth only)")
        print()

        # Global results
        global_success = [r for r in global_results if r.success]
        if global_success:
            times = [r.total_time for r in global_success]
            print(f"  GLOBAL CONSENSUS (crumbling wall, {wall.n} nodes):")
            print(f"    Decisions: {len(global_success)}/{len(global_results)}")
            print(f"    Avg:  {sum(times)/len(times)*1000:.1f}ms")
            print(f"    P50:  {sorted(times)[len(times)//2]*1000:.1f}ms")
            print(f"    P99:  {sorted(times)[-1]*1000:.1f}ms")
            print(f"    Pkts: {sum(r.packets_sent for r in global_results)}")
            print()
            print(f"    Phase 1 reaches Moon (~2.6s RTT) but Phase 2")
            print(f"    stays on Earth (~100-200ms). In Multi-Paxos,")
            print(f"    Phase 1 is amortized across all commits.")
            print()

        # Mars results
        mars_success = [r for r in mars_results if r.success]
        if mars_success:
            times = [r.total_time for r in mars_success]
            print(f"  MARS LOCAL (3 nodes, majority, autonomous):")
            print(f"    Decisions: {len(mars_success)}/{len(mars_results)}")
            print(f"    Avg:  {sum(times)/len(times)*1000:.1f}ms")
            print()

        # Comparison
        print(f"  COMPARISON: What if we used majority instead?")
        print(f"    Majority of {wall.n} = {wall.n // 2 + 1}")
        print(f"    Phase 2 would need {wall.n // 2 + 1} nodes")
        print(f"    Must include Moon or Mars → seconds or minutes per commit")
        print()
        print(f"    Crumbling wall Phase 2 = {wall.phase2_quorum_size()} (Earth only)")
        print(f"    Commits never leave Earth. Elections span the solar system.")
        print()

    return global_results, mars_results


def run_majority_comparison(verbose: bool = True):
    """Run the same topology with flat majority for comparison."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = build_full_topology(env, mars_latency=186.0)

    earth_locs = ["na-west", "europe", "asia", "sa-east", "africa"]
    all_entities = []
    for loc in earth_locs:
        e = registry.create(name=f"earth-{loc}")
        network.assign_entity(e.id, loc)
        all_entities.append(e)

    leo = registry.create(name="leo-sat")
    network.assign_entity(leo.id, "leo-sat")
    all_entities.append(leo)

    moon = registry.create(name="moon")
    network.assign_entity(moon.id, "moon")
    all_entities.append(moon)

    for i in range(3):
        e = registry.create(name=f"mars-{i}")
        network.assign_entity(e.id, f"mars-{i}")
        all_entities.append(e)

    for e in all_entities:
        Acceptor(env, e, network, process_time=0.001)

    all_ids = [e.id for e in all_entities]
    quorum = MajorityQuorum(all_ids)  # 6 of 10

    proposer_entity = registry.create(name="proposer")
    network.assign_entity(proposer_entity.id, "na-west")
    proposer = Proposer(env, proposer_entity, network, all_ids, quorum, timeout=10.0)

    results = []

    def run():
        for slot in range(5):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=120.0)

    if verbose:
        print("=" * 70)
        print("COMPARISON: Flat Majority (6 of 10) - All Tiers Equal")
        print("=" * 70)
        print()
        print(f"  Majority of 10 = 6. Must include at least 1 non-Earth node.")
        print(f"  (Only 5 Earth nodes, need 6 → forced to include LEO/Moon/Mars)")
        print()

        for r in results:
            if r.success:
                if r.total_time > 1.0:
                    print(f"  Slot {r.slot}: {r.total_time:.2f}s")
                else:
                    print(f"  Slot {r.slot}: {r.total_time*1000:.1f}ms")
            else:
                print(f"  Slot {r.slot}: FAILED ({r.total_time:.2f}s)")

        print()
        print("  Majority doesn't know that Earth is fast and Mars is slow.")
        print("  It treats all nodes equally. Physics doesn't.")
        print()

    return results


def the_punchline(verbose: bool = True):
    """The full comparison table."""
    if not verbose:
        return

    print()
    print("=" * 70)
    print("THE CRUMBLING WALL: Quorum Meets Topology")
    print("=" * 70)
    print()
    print("  10 nodes across 4 tiers: Mars(3) / Moon(1) / LEO(1) / Earth(5)")
    print()
    print(f"  {'Strategy':<28} {'Phase 1':>10} {'Phase 2':>10} {'Phase 2 speed':>14}")
    print(f"  {'─'*28} {'─'*10} {'─'*10} {'─'*14}")
    print(f"  {'Majority (flat)':.<28} {'6':>10} {'6':>10} {'seconds+':>14}")
    print(f"  {'Flex Paxos (10,1)':.<28} {'10':>10} {'1':>10} {'~2ms':>14}")
    print(f"  {'Crumbling Wall':.<28} {'6':>10} {'5':>10} {'~200ms':>14}")
    print()
    print("  Majority:        Blind to topology. Forces slow nodes into commits.")
    print("  Flex(10,1):      Fast commits but fragile (1 failure = no Phase 2).")
    print("  Crumbling Wall:  Phase 2 = ALL of Earth. Tolerates any single")
    print("                   Earth failure AND keeps commits Earth-speed.")
    print("                   Phase 1 spans all tiers for election durability.")
    print()
    print("  The crumbling wall construction doesn't just minimize quorum SIZE.")
    print("  It encodes the topology into the quorum SHAPE.")
    print("  Fast tiers handle commits. Slow tiers add durability.")
    print("  The wall crumbles from the top (Mars, Moon) downward -")
    print("  losing far nodes doesn't affect commit speed.")
    print()


def the_arc(verbose: bool = True):
    """The full teaching arc."""
    if not verbose:
        return

    print()
    print("=" * 70)
    print("THE ARC: Eight Steps, Each Earns an Idea")
    print("=" * 70)
    print()

    steps = [
        ("1", "Single DC",
         "3 nodes, ~5ms consensus",
         "Everything works. Consensus is easy when everyone is close."),
        ("2", "Two DCs",
         "5ms → 200ms. One failure = 38x latency cliff.",
         "Geography has a cost. Majority quorum is blind to topology."),
        ("3", "Three DCs",
         "Flexible Paxos: Phase 2 = 1 node, ~2ms commits",
         "Not all phases are equal. Steady-state matters more."),
        ("4", "Five DCs",
         "Grid quorums marginal at n=5, powerful at n=100",
         "Quorum structure > quorum construction at small n."),
        ("5", "LEO Orbit",
         "Satellite handoff, entity migration mid-consensus",
         "Address-independent identity. Topology changes, Paxos doesn't care."),
        ("6", "Moon",
         "1,280x latency ratio. Flex Paxos: optimization → survival.",
         "Do you want the Moon in your quorum? Phase 1 yes, Phase 2 no."),
        ("7", "Mars",
         "12.3 min flat consensus. Hierarchical is the only option.",
         "Local autonomy + global reconciliation. Consensus about consensus."),
        ("8", "Crumbling Wall",
         "Topology-aware quorums. Earth commits, solar system elects.",
         "The wall crumbles from the top. Physics shapes the quorum."),
    ]

    for num, title, metric, lesson in steps:
        print(f"  Step {num}: {title}")
        print(f"    {metric}")
        print(f"    → {lesson}")
        print()

    print()
    print("  The Crumbs:")
    print()
    print("    VMTP (Cheriton, 1986)")
    print("      Transaction-as-primitive. Entity migration.")
    print("      Natural fit for consensus rounds.")
    print()
    print("    Flexible Paxos (Howard, 2016)")
    print("      Phase 1 and Phase 2 only need to intersect.")
    print("      At planetary scale: existential, not optional.")
    print()
    print("    Crumbling Walls (Peleg & Wool, 1995)")
    print("      The quorum system encodes the physical topology.")
    print("      Fast tiers handle commits. Slow tiers add durability.")
    print("      The wall crumbles from the top down.")
    print()
    print("    Entity Migration (V System, 1984)")
    print("      Address-independent identity. Nodes move.")
    print("      Consensus continues through topology changes.")
    print()
    print("    Hierarchical Consensus")
    print("      Local autonomy + global reconciliation.")
    print("      Physics forces the design.")
    print()

    print("=" * 70)
    print()
    print("  These ideas span 40 years.")
    print("  None of them won.")
    print("  All of them were right.")
    print()
    print("=" * 70)


if __name__ == "__main__":
    run_crumbling_wall_system()
    run_majority_comparison()
    the_punchline()
    the_arc()
