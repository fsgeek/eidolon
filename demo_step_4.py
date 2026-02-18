"""Step 4: Five Datacenters - Set Quorums

NA-West + Europe + Asia + South America + Africa.
Heterogeneous latencies. Africa close to Europe, far from Americas.

Majority = 3 of 5. Grid quorum = 4 of 5 (worse for 5!).
At small n, majority already IS nearly optimal.

But the REAL lesson: when you combine grid quorums with
Flexible Paxos, the Phase 2 quorum can be very small.
And with 5 nodes, you have more room to tune which
nodes participate in the fast path.

Also: first time we see heterogeneous latency topology
really matter. The "closest 3" vs "any 3" distinction.
"""

import simpy

from entity import EntityRegistry
from datacenter import DatacenterNetwork, five_dc_topology
from paxos import (
    Acceptor, Proposer, Learner,
    MajorityQuorum, FlexibleQuorum, ConsensusResult,
)
from quorums import GridQuorum, FlexibleGridQuorum


# Latency table from proposer in NA-West (one-way, approximate):
#   NA-West:  ~1ms (local)
#   Europe:   ~50ms
#   SA-East:  ~60ms
#   Asia:     ~75ms
#   Africa:   ~90ms

LOCATIONS = ["na-west", "europe", "asia", "sa-east", "africa"]

LATENCY_LABELS = {
    "na-west": "~2ms RTT",
    "europe": "~100ms RTT",
    "sa-east": "~120ms RTT",
    "asia": "~150ms RTT",
    "africa": "~180ms RTT",
}


def _setup_five_dc(env, registry, network):
    """One acceptor per DC, proposer in NA-West."""
    acc_entities = []
    for loc in LOCATIONS:
        e = registry.create(name=f"acc-{loc}")
        network.assign_entity(e.id, loc)
        acc_entities.append(e)

    proposer_entity = registry.create(name="proposer-nawest")
    network.assign_entity(proposer_entity.id, "na-west")

    acceptors = [Acceptor(env, e, network, process_time=0.0005) for e in acc_entities]
    acc_ids = [e.id for e in acc_entities]

    return acc_entities, acc_ids, proposer_entity


def _run_scenario(quorum_factory, label, num_slots=20, verbose=True):
    """Run a scenario with a given quorum system."""
    env = simpy.Environment()
    registry = EntityRegistry()
    network = five_dc_topology(env)

    acc_entities, acc_ids, proposer_entity = _setup_five_dc(env, registry, network)
    quorum = quorum_factory(acc_ids)
    proposer = Proposer(env, proposer_entity, network, acc_ids, quorum, timeout=1.0)

    results = []

    def run():
        for slot in range(num_slots):
            result = yield proposer.propose(slot=slot, value=f"v-{slot}")
            results.append(result)

    env.process(run())
    env.run(until=600.0)

    return results, quorum


def run_majority(verbose: bool = True):
    """5 DCs, majority quorum (3 of 5)."""
    results, quorum = _run_scenario(
        lambda ids: MajorityQuorum(ids),
        "Majority (3/5)",
    )

    if verbose:
        print("=" * 70)
        print("STEP 4a: Five DCs, Majority Quorum (3 of 5)")
        print("=" * 70)
        print()
        print("  Proposer in NA-West. Latencies:")
        for loc, lat in LATENCY_LABELS.items():
            print(f"    {loc:<12} {lat}")
        print()
        print("  Majority = 3 of 5. Must wait for 3rd-fastest response.")
        print("  3rd fastest from NA-West: SA-East (~120ms RTT)")
        print()
        _print_summary(results, quorum)

    return results


def run_flexible_fast(verbose: bool = True):
    """5 DCs, Flexible Paxos: Phase1=5, Phase2=1."""
    results, quorum = _run_scenario(
        lambda ids: FlexibleQuorum(ids, phase1_size=5, phase2_size=1),
        "Flex (5,1)",
    )

    if verbose:
        print("=" * 70)
        print("STEP 4b: Flexible Paxos (Phase1=ALL, Phase2=1)")
        print("=" * 70)
        print()
        print("  Phase 1: ALL 5 nodes (elections only)")
        print("  Phase 2: just 1 node (every commit)")
        print()
        _print_summary(results, quorum)

    return results


def run_flexible_balanced(verbose: bool = True):
    """5 DCs, Flexible Paxos: Phase1=4, Phase2=2."""
    results, quorum = _run_scenario(
        lambda ids: FlexibleQuorum(ids, phase1_size=4, phase2_size=2),
        "Flex (4,2)",
    )

    if verbose:
        print("=" * 70)
        print("STEP 4c: Flexible Paxos (Phase1=4, Phase2=2)")
        print("=" * 70)
        print()
        print("  Phase 1: 4 of 5 (can tolerate 1 failure during election)")
        print("  Phase 2: 2 of 5 (fast commits with 2 closest nodes)")
        print("  4 + 2 = 6 > 5, intersection guaranteed.")
        print()
        _print_summary(results, quorum)

    return results


def run_grid(verbose: bool = True):
    """5 DCs, grid quorum."""
    results, quorum = _run_scenario(
        lambda ids: GridQuorum(ids),
        "Grid",
    )

    if verbose:
        print("=" * 70)
        print("STEP 4d: Grid Quorum")
        print("=" * 70)
        print()
        grid = GridQuorum([0]*5)  # Just for dimensions
        print(f"  Grid: {grid.rows}x{grid.cols}, quorum size = {grid.quorum_size()} of 5")
        print(f"  (Worse than majority at n=5! Grid shines at larger n.)")
        print()
        _print_summary(results, quorum)

    return results


def comparison(verbose: bool = True):
    """Side-by-side comparison of all strategies."""
    configs = [
        ("Majority (3/5)", lambda ids: MajorityQuorum(ids)),
        ("Flex (5,1)", lambda ids: FlexibleQuorum(ids, 5, 1)),
        ("Flex (4,2)", lambda ids: FlexibleQuorum(ids, 4, 2)),
        ("Flex (3,3)", lambda ids: FlexibleQuorum(ids, 3, 3)),
    ]

    all_results = {}
    all_quorums = {}
    for label, factory in configs:
        results, quorum = _run_scenario(factory, label, num_slots=30, verbose=False)
        all_results[label] = results
        all_quorums[label] = quorum

    if verbose:
        print()
        print("=" * 70)
        print("QUORUM STRATEGY COMPARISON: 5 DCs, 30 Decisions")
        print("=" * 70)
        print()
        print("  Latencies from NA-West:")
        for loc, lat in LATENCY_LABELS.items():
            marker = " <-- proposer" if loc == "na-west" else ""
            print(f"    {loc:<12} {lat}{marker}")
        print()

        # Header
        labels = [c[0] for c in configs]
        header = f"  {'Metric':<24}"
        for l in labels:
            header += f" {l:>14}"
        print(header)
        print(f"  {'─'*24}" + f" {'─'*14}" * len(labels))

        # Quorum sizes
        row = f"  {'Phase 1 quorum':<24}"
        for l in labels:
            q = all_quorums[l]
            row += f" {q.phase1_quorum_size():>14}"
        print(row)

        row = f"  {'Phase 2 quorum':<24}"
        for l in labels:
            q = all_quorums[l]
            row += f" {q.phase2_quorum_size():>14}"
        print(row)
        print()

        # Average time
        row = f"  {'Avg time (both phases)':<24}"
        for l in labels:
            rs = all_results[l]
            s = [r for r in rs if r.success]
            avg = sum(r.total_time for r in s) / len(s) if s else 0
            row += f" {avg*1000:>13.1f}ms"
        print(row)

        # P50
        row = f"  {'P50':<24}"
        for l in labels:
            rs = all_results[l]
            s = sorted(r.total_time for r in rs if r.success)
            p50 = s[len(s)//2] if s else 0
            row += f" {p50*1000:>13.1f}ms"
        print(row)

        # Success rate
        row = f"  {'Success rate':<24}"
        for l in labels:
            rs = all_results[l]
            rate = sum(1 for r in rs if r.success) / len(rs) if rs else 0
            row += f" {rate*100:>13.0f}%"
        print(row)

        # Total packets
        row = f"  {'Total packets':<24}"
        for l in labels:
            rs = all_results[l]
            total = sum(r.packets_sent for r in rs)
            row += f" {total:>14}"
        print(row)

        print()

        # Multi-Paxos estimates
        print("  Multi-Paxos steady-state estimate (Phase 2 only):")
        print()

        # Phase 2 latency depends on how many responses needed
        # and which are fastest from NA-West
        latencies_from_nawest = {
            "na-west": 0.002,   # ~2ms RTT
            "europe": 0.100,    # ~100ms RTT
            "sa-east": 0.120,   # ~120ms RTT
            "asia": 0.150,      # ~150ms RTT
            "africa": 0.180,    # ~180ms RTT
        }
        sorted_latencies = sorted(latencies_from_nawest.values())

        row = f"  {'Phase 2 est. latency':<24}"
        for l in labels:
            q = all_quorums[l]
            p2_size = q.phase2_quorum_size()
            # Wait for p2_size-th fastest
            est = sorted_latencies[p2_size - 1]
            row += f" {est*1000:>13.0f}ms"
        print(row)

        print()
        print("  Flex(5,1): Phase 2 = 1 → wait for LOCAL only → ~2ms")
        print("  Flex(4,2): Phase 2 = 2 → wait for 2nd fastest → ~100ms (Europe)")
        print("  Flex(3,3): Phase 2 = 3 → wait for 3rd fastest → ~120ms (SA-East)")
        print("  Majority:  Phase 2 = 3 → same as Flex(3,3)")
        print()
        print("  The lesson: at 5 DCs, the quorum STRUCTURE matters more than")
        print("  the quorum CONSTRUCTION. Grid quorums don't help yet.")
        print("  Flexible Paxos is the win.")
        print()
        print("  Grid quorums earn their keep at n=25+ where majority = 13")
        print("  but grid = 9. Combined with Flexible Paxos at that scale,")
        print("  the savings compound.")
        print()


def _print_summary(results, quorum):
    successful = [r for r in results if r.success]
    if not successful:
        print("  No successful decisions!")
        return

    times = [r.total_time for r in successful]
    avg = sum(times) / len(times)
    p50 = sorted(times)[len(times)//2]
    total_pkts = sum(r.packets_sent for r in results)

    print(f"  Phase 1 quorum: {quorum.phase1_quorum_size()}, "
          f"Phase 2 quorum: {quorum.phase2_quorum_size()}")
    print()
    print(f"  Decisions: {len(successful)}/{len(results)} succeeded")
    print(f"  Avg time:  {avg*1000:.1f}ms")
    print(f"  P50:       {p50*1000:.1f}ms")
    print(f"  Packets:   {total_pkts}")
    print()


if __name__ == "__main__":
    run_majority()
    run_flexible_fast()
    run_flexible_balanced()
    comparison()
