"""Step 7: Add Mars - Hierarchical Consensus

Mars-Earth distance: 55M km (closest) to 401M km (farthest)
Speed of light: 299,792 km/s
One-way latency: 3.1 minutes (closest) to 22.3 minutes (farthest)
Round-trip: 6.2 to 44.6 minutes

You cannot include Mars in a flat quorum for interactive consensus.
Even Flexible Paxos can't save you: Phase 1 with Mars takes 45 minutes
at opposition. No election should take 45 minutes.

The answer: hierarchical consensus.
- Local Paxos within Earth (fast, normal)
- Local Paxos within Mars (fast, autonomous)
- Cross-planet Paxos for global state reconciliation (slow, batched)

Mars MUST operate autonomously. It reconciles when it can.
This is consensus about consensus.
"""

import simpy

from entity import EntityRegistry
from datacenter import DatacenterNetwork, five_dc_topology
from paxos import (
    Acceptor, Proposer, Learner,
    MajorityQuorum, FlexibleQuorum, ConsensusResult,
)


# Mars orbital positions
MARS_CLOSEST = 186.0    # ~3.1 minutes one-way at closest approach
MARS_AVERAGE = 750.0    # ~12.5 minutes one-way average
MARS_FARTHEST = 1342.0  # ~22.4 minutes one-way at opposition


def _build_mars_topology(env, mars_latency=MARS_CLOSEST, seed=42):
    """5 Earth DCs + Moon + 3 Mars nodes."""
    network = five_dc_topology(env, seed=seed)

    # Moon
    network.add_location("moon")
    for loc in ["na-west", "europe", "asia", "sa-east", "africa"]:
        network.add_link(loc, "moon", latency=1.28, jitter=0.01)

    # Mars cluster (3 nodes, local to each other)
    for i in range(3):
        network.add_location(f"mars-{i}")
    # Mars internal links: fast (same planet, ~10ms)
    network.add_link("mars-0", "mars-1", latency=0.005, jitter=0.001)
    network.add_link("mars-0", "mars-2", latency=0.005, jitter=0.001)
    network.add_link("mars-1", "mars-2", latency=0.005, jitter=0.001)

    # Earth-Mars links (speed of light)
    for earth_loc in ["na-west", "europe"]:
        for i in range(3):
            network.add_link(earth_loc, f"mars-{i}",
                           latency=mars_latency, jitter=mars_latency * 0.01)

    # Moon-Mars
    for i in range(3):
        network.add_link("moon", f"mars-{i}",
                       latency=mars_latency + 1.28, jitter=mars_latency * 0.01)

    return network


def run_flat_paxos_mars(verbose: bool = True) -> list[ConsensusResult]:
    """Try flat Paxos with Mars. Watch it fail to be useful."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = _build_mars_topology(env, mars_latency=MARS_CLOSEST)

    # 5 Earth + 1 Moon + 3 Mars = 9 nodes
    earth_locs = ["na-west", "europe", "asia", "sa-east", "africa"]
    acc_entities = []
    for loc in earth_locs:
        e = registry.create(name=f"acc-{loc}")
        network.assign_entity(e.id, loc)
        acc_entities.append(e)

    moon_e = registry.create(name="acc-moon")
    network.assign_entity(moon_e.id, "moon")
    acc_entities.append(moon_e)

    for i in range(3):
        e = registry.create(name=f"acc-mars-{i}")
        network.assign_entity(e.id, f"mars-{i}")
        acc_entities.append(e)

    proposer_entity = registry.create(name="proposer-nawest")
    network.assign_entity(proposer_entity.id, "na-west")

    acceptors = [Acceptor(env, e, network, process_time=0.001) for e in acc_entities]
    acc_ids = [e.id for e in acc_entities]

    # Majority of 9 = 5. With 5 Earth + 1 Moon, Earth alone can do it.
    # So kill 2 Earth nodes to force Mars into the quorum.
    quorum = MajorityQuorum(acc_ids)
    proposer = Proposer(
        env, proposer_entity, network, acc_ids, quorum,
        timeout=MARS_CLOSEST * 3,  # Very long timeout
    )

    results = []

    def run():
        # Kill 2 Earth nodes to force Mars participation
        # (Africa and SA-East go down)
        for e in acc_entities:
            if "africa" in e.name or "sa-east" in e.name:
                # Drain their mailbox so they never respond
                pass  # They're running but unreachable:
        # Partition them out
        network = proposer.network
        for loc in ["africa", "sa-east"]:
            for other in ["na-west", "europe", "asia", "moon",
                          "mars-0", "mars-1", "mars-2"]:
                try:
                    network.partition_locations(loc, other)
                except:
                    pass

        result = yield proposer.propose(slot=0, value="flat-paxos-with-mars")
        results.append(result)

    env.process(run())
    env.run(until=MARS_CLOSEST * 8)  # Run long enough for Mars RTT

    if verbose:
        print("=" * 70)
        print("STEP 7a: Flat Paxos With Mars (DON'T DO THIS)")
        print("=" * 70)
        print()
        print(f"  Mars one-way latency: {MARS_CLOSEST:.0f}s "
              f"({MARS_CLOSEST/60:.1f} min) at closest approach")
        print(f"  Mars RTT: {MARS_CLOSEST*2:.0f}s ({MARS_CLOSEST*2/60:.1f} min)")
        print(f"  9 nodes, majority = 5. Two Earth nodes down → must include Mars.")
        print()

        if results and results[0].success:
            r = results[0]
            print(f"  Result: consensus in {r.total_time:.1f}s "
                  f"({r.total_time/60:.1f} minutes)")
        elif results:
            r = results[0]
            print(f"  Result: FAILED after {r.total_time:.1f}s "
                  f"({r.total_time/60:.1f} minutes)")
        print()
        print(f"  Even at CLOSEST approach, one consensus round takes minutes.")
        print(f"  At opposition ({MARS_FARTHEST/60:.0f} min one-way): "
              f"~{MARS_FARTHEST*2*2/60:.0f} minutes per round.")
        print()
        print(f"  Flat Paxos across planets is not viable for anything interactive.")
        print()

    return results


def run_earth_local_paxos(verbose: bool = True) -> list[ConsensusResult]:
    """Earth-only Paxos. Fast. Normal. This is the baseline."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = five_dc_topology(env)

    earth_locs = ["na-west", "europe", "asia", "sa-east", "africa"]
    acc_entities = []
    for loc in earth_locs:
        e = registry.create(name=f"acc-{loc}")
        network.assign_entity(e.id, loc)
        acc_entities.append(e)

    proposer_entity = registry.create(name="proposer-nawest")
    network.assign_entity(proposer_entity.id, "na-west")

    acceptors = [Acceptor(env, e, network, process_time=0.0005) for e in acc_entities]
    acc_ids = [e.id for e in acc_entities]

    quorum = FlexibleQuorum(acc_ids, phase1_size=4, phase2_size=2)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=1.0)

    results = []

    def run():
        for slot in range(10):
            result = yield proposer.propose(slot=slot, value=f"earth-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=60.0)

    if verbose:
        print("=" * 70)
        print("STEP 7b: Earth-Local Paxos (baseline)")
        print("=" * 70)
        print()
        print("  5 Earth DCs, Flex(4,2)")
        print()
        avg = sum(r.total_time for r in results if r.success) / max(1, len([r for r in results if r.success]))
        print(f"  {len([r for r in results if r.success])}/{len(results)} succeeded")
        print(f"  Avg: {avg*1000:.1f}ms per decision")
        print()

    return results


def run_mars_local_paxos(verbose: bool = True) -> list[ConsensusResult]:
    """Mars-only Paxos. Also fast - it's all local to Mars."""
    env = simpy.Environment()
    registry = EntityRegistry()
    # Minimal topology: just Mars internal
    from datacenter import DatacenterNetwork
    from network import NetworkConfig
    network = DatacenterNetwork(env, NetworkConfig(seed=42))

    for i in range(3):
        network.add_location(f"mars-{i}")
    network.add_link("mars-0", "mars-1", latency=0.005, jitter=0.001)
    network.add_link("mars-0", "mars-2", latency=0.005, jitter=0.001)
    network.add_link("mars-1", "mars-2", latency=0.005, jitter=0.001)

    acc_entities = []
    for i in range(3):
        e = registry.create(name=f"acc-mars-{i}")
        network.assign_entity(e.id, f"mars-{i}")
        acc_entities.append(e)

    proposer_entity = registry.create(name="proposer-mars")
    network.assign_entity(proposer_entity.id, "mars-0")

    acceptors = [Acceptor(env, e, network, process_time=0.001) for e in acc_entities]
    acc_ids = [e.id for e in acc_entities]

    quorum = MajorityQuorum(acc_ids)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=1.0)

    results = []

    def run():
        for slot in range(10):
            result = yield proposer.propose(slot=slot, value=f"mars-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=60.0)

    if verbose:
        print("=" * 70)
        print("STEP 7c: Mars-Local Paxos (autonomous)")
        print("=" * 70)
        print()
        print("  3 Mars nodes, ~10ms RTT between them. Majority = 2 of 3.")
        print()
        avg = sum(r.total_time for r in results if r.success) / max(1, len([r for r in results if r.success]))
        print(f"  {len([r for r in results if r.success])}/{len(results)} succeeded")
        print(f"  Avg: {avg*1000:.1f}ms per decision")
        print()
        print("  Mars runs its own Paxos. Fast. Autonomous.")
        print("  Doesn't need Earth. Doesn't wait for Earth.")
        print()

    return results


def run_hierarchical_consensus(verbose: bool = True):
    """The answer: hierarchical consensus.

    Earth runs Paxos for Earth decisions.
    Mars runs Paxos for Mars decisions.
    Cross-planet Paxos handles global state reconciliation
    when the communication window allows.
    """
    env = simpy.Environment()
    registry = EntityRegistry()
    network = _build_mars_topology(env, mars_latency=MARS_CLOSEST)

    # --- Earth cluster ---
    earth_locs = ["na-west", "europe", "asia"]
    earth_acc = []
    for loc in earth_locs:
        e = registry.create(name=f"earth-{loc}")
        network.assign_entity(e.id, loc)
        earth_acc.append(e)

    earth_proposer = registry.create(name="earth-proposer")
    network.assign_entity(earth_proposer.id, "na-west")

    for e in earth_acc:
        Acceptor(env, e, network, process_time=0.0005)
    earth_ids = [e.id for e in earth_acc]
    earth_quorum = MajorityQuorum(earth_ids)
    earth_prop = Proposer(env, earth_proposer, network, earth_ids, earth_quorum, timeout=1.0)

    # --- Mars cluster ---
    mars_acc = []
    for i in range(3):
        e = registry.create(name=f"mars-node-{i}")
        network.assign_entity(e.id, f"mars-{i}")
        mars_acc.append(e)

    mars_proposer = registry.create(name="mars-proposer")
    network.assign_entity(mars_proposer.id, "mars-0")

    for e in mars_acc:
        Acceptor(env, e, network, process_time=0.001)
    mars_ids = [e.id for e in mars_acc]
    mars_quorum = MajorityQuorum(mars_ids)
    mars_prop = Proposer(env, mars_proposer, network, mars_ids, mars_quorum, timeout=1.0)

    # --- Cross-planet reconciliation ---
    # Use ALL nodes for global consensus (rare, batched)
    all_ids = earth_ids + mars_ids
    global_quorum = FlexibleQuorum(all_ids, phase1_size=5, phase2_size=2)
    global_proposer_entity = registry.create(name="global-proposer")
    network.assign_entity(global_proposer_entity.id, "na-west")
    global_prop = Proposer(
        env, global_proposer_entity, network, all_ids, global_quorum,
        timeout=MARS_CLOSEST * 3,
    )

    earth_results = []
    mars_results = []
    global_results = []
    earth_learner = Learner()
    mars_learner = Learner()
    global_learner = Learner()

    def earth_workload():
        """Earth makes local decisions fast."""
        for slot in range(20):
            result = yield earth_prop.propose(slot=slot, value=f"earth-decision-{slot}")
            earth_results.append(result)
            if result.success:
                earth_learner.learn(result.slot, result.value)

    def mars_workload():
        """Mars makes local decisions fast, autonomously."""
        for slot in range(100, 120):  # Different slot range
            result = yield mars_prop.propose(slot=slot, value=f"mars-decision-{slot}")
            mars_results.append(result)
            if result.success:
                mars_learner.learn(result.slot, result.value)

    def global_reconciliation():
        """Periodically reconcile global state. Slow but necessary."""
        # Wait for local decisions to accumulate
        yield env.timeout(5.0)

        # Global consensus: agree on "Earth has decided slots 0-19,
        # Mars has decided slots 100-119"
        result = yield global_prop.propose(
            slot=1000,
            value="reconcile: Earth=0-19, Mars=100-119"
        )
        global_results.append(result)
        if result.success:
            global_learner.learn(result.slot, result.value)

    env.process(earth_workload())
    env.process(mars_workload())
    env.process(global_reconciliation())
    env.run(until=MARS_CLOSEST * 8)

    if verbose:
        print("=" * 70)
        print("STEP 7d: Hierarchical Consensus - Earth + Mars")
        print("=" * 70)
        print()
        print("  Architecture:")
        print("    Earth cluster: 3 nodes, local Paxos, ~milliseconds")
        print("    Mars cluster:  3 nodes, local Paxos, ~milliseconds")
        print("    Global:        6 nodes, Flex(5,2), ~minutes")
        print()

        earth_success = [r for r in earth_results if r.success]
        mars_success = [r for r in mars_results if r.success]

        if earth_success:
            earth_avg = sum(r.total_time for r in earth_success) / len(earth_success)
            print(f"  Earth local decisions:")
            print(f"    {len(earth_success)}/{len(earth_results)} succeeded")
            print(f"    Avg: {earth_avg*1000:.1f}ms per decision")
            print()

        if mars_success:
            mars_avg = sum(r.total_time for r in mars_success) / len(mars_success)
            print(f"  Mars local decisions:")
            print(f"    {len(mars_success)}/{len(mars_results)} succeeded")
            print(f"    Avg: {mars_avg*1000:.1f}ms per decision")
            print()

        if global_results:
            r = global_results[0]
            if r.success:
                print(f"  Global reconciliation:")
                print(f"    Time: {r.total_time:.1f}s ({r.total_time/60:.1f} minutes)")
                print(f"    Value: {r.value}")
            else:
                print(f"  Global reconciliation: FAILED after {r.total_time/60:.1f} min")
            print()

        print("  The pattern:")
        print("    Local decisions: fast, autonomous, milliseconds")
        print("    Global reconciliation: slow, batched, minutes")
        print("    Each planet is sovereign for its own operations.")
        print("    They sync when physics allows.")
        print()

    return earth_results, mars_results, global_results


def orbital_mechanics(verbose: bool = True):
    """Show how Mars latency varies with orbital position."""
    if not verbose:
        return

    print()
    print("=" * 70)
    print("MARS ORBITAL MECHANICS")
    print("=" * 70)
    print()
    print("  Mars-Earth distance varies by 7x over a 2-year cycle:")
    print()
    print(f"  {'Position':<20} {'Distance':>12} {'One-way':>10} {'RTT':>10} {'Status':>15}")
    print(f"  {'─'*20} {'─'*12} {'─'*10} {'─'*10} {'─'*15}")

    positions = [
        ("Closest approach", 55, MARS_CLOSEST),
        ("Average", 225, MARS_AVERAGE),
        ("Opposition", 401, MARS_FARTHEST),
    ]

    for name, dist_Mkm, latency in positions:
        rtt = latency * 2
        status = "difficult" if latency > 600 else "painful"
        if latency < 300:
            status = "bad"
        print(f"  {name:<20} {dist_Mkm:>9}M km {latency/60:>9.1f}m {rtt/60:>9.1f}m {status:>15}")

    print()
    print("  Even at CLOSEST approach: 6.2 minute round-trip.")
    print("  At opposition: 44.7 minute round-trip.")
    print()
    print("  Flat consensus is impossible. Hierarchical is necessary.")
    print("  Mars must be autonomous. Global sync is episodic.")
    print()
    print("  This isn't a computer science problem.")
    print("  It's a physics problem. And physics wins.")
    print()


if __name__ == "__main__":
    # Show the absurdity
    run_flat_paxos_mars()

    # Show the baselines
    run_earth_local_paxos()
    run_mars_local_paxos()

    # Show the solution
    run_hierarchical_consensus()

    # The physics
    orbital_mechanics()
