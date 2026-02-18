"""Step 3: Three Datacenters - Flexible Paxos

NA-West + Europe + Asia. One acceptor per DC.
Majority quorum = 2 of 3. Can survive one DC failure.

Problem: consensus speed = speed of 2nd-fastest responder.
With heterogeneous latencies, the "2nd fastest" varies by
which two DCs are reachable.

Enter Flexible Paxos (Howard et al., 2016):
  Phase 1 and Phase 2 quorums don't both need to be majorities.
  They only need to INTERSECT.
  Requirement: q1 + q2 > n

In Multi-Paxos, Phase 1 (leader election) is rare.
Phase 2 (every commit) is the hot path.
So: make Phase 2 quorum SMALL (fast) and Phase 1 quorum LARGE (rare).

With 3 nodes: Phase 2 = 1, Phase 1 = 3 (all).
  Steady-state commits only need the LOCAL acceptor. Sub-millisecond.
  Leader election needs everyone. Slow but rare.

This is the idea that changes everything at geographic scale.
"""

import simpy

from entity import EntityRegistry
from datacenter import DatacenterNetwork, three_dc_topology
from paxos import (
    Acceptor, Proposer, Learner,
    MajorityQuorum, FlexibleQuorum, ConsensusResult,
)


def _setup_three_dc(env, registry, network):
    """Common setup: 1 acceptor per DC, proposer in NA-West."""
    acc_na = registry.create(name="acc-nawest")
    acc_eu = registry.create(name="acc-europe")
    acc_asia = registry.create(name="acc-asia")
    proposer_entity = registry.create(name="proposer-nawest")

    network.assign_entity(acc_na.id, "na-west")
    network.assign_entity(acc_eu.id, "europe")
    network.assign_entity(acc_asia.id, "asia")
    network.assign_entity(proposer_entity.id, "na-west")

    all_acc = [acc_na, acc_eu, acc_asia]
    acceptors = [Acceptor(env, e, network, process_time=0.0005) for e in all_acc]
    acc_ids = [e.id for e in all_acc]

    return all_acc, acc_ids, proposer_entity, acceptors


def run_majority_healthy(verbose: bool = True) -> list[ConsensusResult]:
    """3 DCs, majority quorum, all healthy."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = three_dc_topology(env)

    all_acc, acc_ids, proposer_entity, acceptors = _setup_three_dc(env, registry, network)

    quorum = MajorityQuorum(acc_ids)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=0.500)

    results = []

    def run():
        for slot in range(10):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=60.0)

    if verbose:
        print("=" * 70)
        print("STEP 3a: Three DCs, Majority Quorum, All Healthy")
        print("=" * 70)
        print()
        print("  Topology:  NA-West + Europe (50ms) + Asia (75ms)")
        print("  Quorum:    2 of 3 (majority)")
        print("  Proposer:  NA-West")
        print()
        print("  Quorum = 2nd-fastest response = Europe (~100ms RTT)")
        print()
        _print_results(results, quorum)

    return results


def run_majority_one_down(verbose: bool = True) -> list[ConsensusResult]:
    """3 DCs, majority quorum, Europe down. Must include Asia."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = three_dc_topology(env)

    acc_na = registry.create(name="acc-nawest")
    acc_eu = registry.create(name="acc-europe-DOWN")
    acc_asia = registry.create(name="acc-asia")
    proposer_entity = registry.create(name="proposer-nawest")

    network.assign_entity(acc_na.id, "na-west")
    network.assign_entity(acc_eu.id, "europe")
    network.assign_entity(acc_asia.id, "asia")
    network.assign_entity(proposer_entity.id, "na-west")

    # Only start NA and Asia acceptors
    Acceptor(env, acc_na, network, process_time=0.0005)
    network.register(acc_eu.id)  # Register but don't start
    Acceptor(env, acc_asia, network, process_time=0.0005)

    acc_ids = [acc_na.id, acc_eu.id, acc_asia.id]
    quorum = MajorityQuorum(acc_ids)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=0.500)

    results = []

    def run():
        for slot in range(10):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=60.0)

    if verbose:
        print("=" * 70)
        print("STEP 3b: Europe Down - Must Include Asia")
        print("=" * 70)
        print()
        print("  Europe is down. Quorum needs NA-West + Asia.")
        print("  Asia is 75ms away = ~150ms RTT per phase.")
        print()
        _print_results(results, quorum)

    return results


def run_flexible_paxos(verbose: bool = True) -> list[ConsensusResult]:
    """3 DCs, Flexible Paxos: Phase 2 = 1 node, Phase 1 = 3 nodes."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = three_dc_topology(env)

    all_acc, acc_ids, proposer_entity, acceptors = _setup_three_dc(env, registry, network)

    # Flexible Paxos: Phase 1 needs ALL 3, Phase 2 needs only 1
    # Intersection guaranteed: any single node from Phase 2
    # must be in the Phase 1 set of 3 (which is everyone)
    quorum = FlexibleQuorum(acc_ids, phase1_size=3, phase2_size=1)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=0.500)

    results = []

    def run():
        for slot in range(10):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=60.0)

    if verbose:
        print("=" * 70)
        print("STEP 3c: Flexible Paxos (Phase1=3, Phase2=1)")
        print("=" * 70)
        print()
        print("  Howard's insight: Phase 1 and Phase 2 quorums only need")
        print("  to INTERSECT, not each be a majority.")
        print()
        print("  Phase 1 (Prepare): need ALL 3 - slow, but only for elections")
        print("  Phase 2 (Accept):  need only 1 - FAST, every commit")
        print()
        print("  For us: Phase 1 waits for Asia (150ms RTT).")
        print("  But Phase 2 completes with just the local acceptor (~2ms).")
        print()
        _print_results(results, quorum)

    return results


def run_flexible_22(verbose: bool = True) -> list[ConsensusResult]:
    """Flexible Paxos: Phase 1 = 2, Phase 2 = 2. Same as majority but explicit."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = three_dc_topology(env)

    all_acc, acc_ids, proposer_entity, acceptors = _setup_three_dc(env, registry, network)

    # 2+2 > 3, so intersection guaranteed. Same as majority.
    quorum = FlexibleQuorum(acc_ids, phase1_size=2, phase2_size=2)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=0.500)

    results = []

    def run():
        for slot in range(10):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=60.0)

    if verbose:
        print("=" * 70)
        print("STEP 3d: Flexible Paxos (Phase1=2, Phase2=2) = Majority")
        print("=" * 70)
        print()
        print("  For comparison: 2+2 > 3, same as majority quorum.")
        print()
        _print_results(results, quorum)

    return results


def run_multi_paxos_simulation(verbose: bool = True):
    """Simulate Multi-Paxos: one Phase 1 (election), many Phase 2 (commits).

    This is where Flexible Paxos really shines. In Multi-Paxos:
    - Phase 1 happens ONCE when a leader is elected
    - Phase 2 happens for EVERY value
    - With Flexible Paxos, Phase 2 can be tiny
    """
    env = simpy.Environment()
    registry = EntityRegistry()
    network = three_dc_topology(env)

    all_acc, acc_ids, proposer_entity, acceptors = _setup_three_dc(env, registry, network)

    num_commits = 50

    # --- Majority quorum ---
    quorum_maj = MajorityQuorum(acc_ids)
    proposer_maj = Proposer(env, proposer_entity, network, acc_ids, quorum_maj, timeout=0.500)

    majority_results = []

    def run_majority():
        for slot in range(num_commits):
            result = yield proposer_maj.propose(slot=slot, value=f"maj-{slot}")
            majority_results.append(result)

    env.process(run_majority())
    env.run(until=300.0)

    # --- Flexible quorum (Phase1=3, Phase2=1) - fresh env ---
    env2 = simpy.Environment()
    registry2 = EntityRegistry()
    network2 = three_dc_topology(env2)

    all_acc2, acc_ids2, proposer_entity2, acceptors2 = _setup_three_dc(env2, registry2, network2)

    quorum_flex = FlexibleQuorum(acc_ids2, phase1_size=3, phase2_size=1)
    proposer_flex = Proposer(env2, proposer_entity2, network2, acc_ids2, quorum_flex, timeout=0.500)

    flex_results = []

    def run_flex():
        for slot in range(num_commits):
            result = yield proposer_flex.propose(slot=slot, value=f"flex-{slot}")
            flex_results.append(result)

    env2.process(run_flex())
    env2.run(until=300.0)

    if verbose:
        print()
        print("=" * 70)
        print("MULTI-PAXOS SIMULATION: 50 Commits")
        print("=" * 70)
        print()
        print("  In Multi-Paxos, a stable leader skips Phase 1 after election.")
        print("  Our simulator still runs both phases per slot (basic Paxos),")
        print("  but the TIMES reveal what Multi-Paxos would look like:")
        print()

        # Extract Phase 1 and Phase 2 times
        # Phase 2 time ≈ total_time - Phase 1 time
        # For Flexible(3,1): Phase 1 is slow (wait for all), Phase 2 is fast
        # For Majority(2,2): Both phases wait for 2nd responder

        maj_times = [r.total_time for r in majority_results if r.success]
        flex_times = [r.total_time for r in flex_results if r.success]

        maj_avg = sum(maj_times) / len(maj_times) if maj_times else 0
        flex_avg = sum(flex_times) / len(flex_times) if flex_times else 0

        # In Multi-Paxos, Phase 1 happens once. So amortized cost:
        # Majority: every commit ≈ avg/2 (both phases similar)
        # Flexible: Phase 1 once ≈ slow, Phase 2 every commit ≈ fast
        #
        # Approximate: Phase 2 for majority ≈ half of total time
        # Phase 2 for flex(3,1) ≈ local RTT only

        # Better: compute from actual network latencies
        # Local RTT ≈ 2ms, Europe RTT ≈ 100ms, Asia RTT ≈ 150ms
        # Majority Phase 2: wait for 2nd = Europe ≈ 100ms
        # Flex Phase 2: wait for 1st = local ≈ 2ms

        print(f"  {'Metric':<30} {'Majority':>12} {'Flex(3,1)':>12}")
        print(f"  {'─'*30} {'─'*12} {'─'*12}")
        print(f"  {'Avg full round (both phases)':<30} {maj_avg*1000:>11.1f}ms {flex_avg*1000:>11.1f}ms")
        print()
        print(f"  In Multi-Paxos (Phase 1 = once, Phase 2 = every commit):")
        print()

        # Estimate Phase 2 only time
        # For majority: quorum=2, wait for 2nd fastest = Europe RTT
        local_rtt = 0.002  # ~2ms
        europe_rtt = 0.100  # ~100ms
        asia_rtt = 0.150  # ~150ms

        maj_phase2_est = europe_rtt + 0.001  # 2nd fastest + processing
        flex_phase2_est = local_rtt + 0.001  # fastest only + processing

        total_maj = maj_phase2_est * num_commits
        total_flex = flex_phase2_est * num_commits

        print(f"  {'Est. Phase 2 per commit':<30} {'~101ms':>12} {'~3ms':>12}")
        print(f"  {'Est. {0} commits'.format(num_commits):<30} {total_maj:>11.1f}s {total_flex:>11.1f}s")
        print(f"  {'Speedup':<30} {'':>12} {total_maj/total_flex:>11.0f}x")
        print()

        total_maj_pkts = sum(r.packets_sent for r in majority_results)
        total_flex_pkts = sum(r.packets_sent for r in flex_results)
        print(f"  {'Total proposer packets':<30} {total_maj_pkts:>12} {total_flex_pkts:>12}")
        print()
        print("  The packets are the same. The LATENCY is radically different.")
        print("  Flexible Paxos doesn't save messages. It saves TIME.")
        print()
        print("  Trade-off: Phase 1 (election) is slower with Flex(3,1).")
        print("  All 3 nodes must respond, including Asia (150ms RTT).")
        print("  But elections are rare. Commits happen every millisecond.")
        print()


def side_by_side(verbose: bool = True):
    """Compare all quorum strategies."""
    if not verbose:
        return

    configs = [
        ("Majority (2/3)", lambda ids: MajorityQuorum(ids)),
        ("Flex (3,1)", lambda ids: FlexibleQuorum(ids, 3, 1)),
        ("Flex (2,2)", lambda ids: FlexibleQuorum(ids, 2, 2)),
    ]

    all_results = {}

    for label, make_quorum in configs:
        env = simpy.Environment()
        registry = EntityRegistry()
        network = three_dc_topology(env)
        all_acc, acc_ids, proposer_entity, acceptors = _setup_three_dc(env, registry, network)

        quorum = make_quorum(acc_ids)
        proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=0.500)

        results = []

        def run(p=proposer, r=results):
            for slot in range(20):
                result = yield p.propose(slot=slot, value=f"v-{slot}")
                r.append(result)

        env.process(run())
        env.run(until=300.0)
        all_results[label] = results

    print()
    print("=" * 70)
    print("QUORUM STRATEGY COMPARISON: 3 DCs, 20 Decisions")
    print("=" * 70)
    print()
    print(f"  Latencies from proposer (NA-West):")
    print(f"    Local:  ~2ms RTT")
    print(f"    Europe: ~100ms RTT")
    print(f"    Asia:   ~150ms RTT")
    print()

    header = f"  {'Strategy':<20}"
    for label in configs:
        header += f" {label[0]:>15}"
    print(header)
    print(f"  {'─'*20}" + f" {'─'*15}" * len(configs))

    # Average time
    row = f"  {'Avg time':<20}"
    for label, _ in configs:
        results = all_results[label]
        avg = sum(r.total_time for r in results) / len(results)
        row += f" {avg*1000:>14.1f}ms"
    print(row)

    # P50
    row = f"  {'P50':<20}"
    for label, _ in configs:
        results = all_results[label]
        times = sorted(r.total_time for r in results)
        p50 = times[len(times)//2]
        row += f" {p50*1000:>14.1f}ms"
    print(row)

    # Total packets
    row = f"  {'Total packets':<20}"
    for label, _ in configs:
        results = all_results[label]
        total = sum(r.packets_sent for r in results)
        row += f" {total:>15}"
    print(row)

    # Successes
    row = f"  {'Success rate':<20}"
    for label, _ in configs:
        results = all_results[label]
        rate = sum(1 for r in results if r.success) / len(results)
        row += f" {rate*100:>14.0f}%"
    print(row)

    print()
    print("  Flex(3,1) is slower OVERALL because Phase 1 waits for all 3.")
    print("  But in Multi-Paxos, Phase 1 happens once. Phase 2 is the win.")
    print()
    print("  The question for Step 4: with 5 DCs, majority = 3 of 5.")
    print("  Can set quorums do better than O(n/2)?")
    print()


def _print_results(results: list[ConsensusResult], quorum):
    """Helper to print a result table."""
    print(f"  Quorum sizes: Phase1={quorum.phase1_quorum_size()}, "
          f"Phase2={quorum.phase2_quorum_size()}")
    print()
    print(f"  {'Slot':>4}  {'Time':>8}  {'Rounds':>6}  {'Pkts':>5}")
    print(f"  {'─'*4}  {'─'*8}  {'─'*6}  {'─'*5}")
    for r in results:
        if r.success:
            print(f"  {r.slot:>4}  {r.total_time*1000:>7.1f}ms  {r.rounds:>6}  {r.packets_sent:>5}")
        else:
            print(f"  {r.slot:>4}  {'FAILED':>8}  {r.rounds:>6}  {r.packets_sent:>5}")

    successful = [r for r in results if r.success]
    if successful:
        avg = sum(r.total_time for r in successful) / len(successful)
        total_pkts = sum(r.packets_sent for r in results)
        print()
        print(f"  Average: {avg*1000:.1f}ms, Total packets: {total_pkts}")
    print()


if __name__ == "__main__":
    run_majority_healthy()
    run_majority_one_down()
    run_flexible_paxos()
    run_flexible_22()
    run_multi_paxos_simulation()
    side_by_side()
