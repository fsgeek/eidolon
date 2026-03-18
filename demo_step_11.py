"""Step 11: MRDT-based reconciliation vs Paxos-based reconciliation.

The question: what if we stop fighting the blackout?

Instead of global Paxos reconciliation (which requires Phase 1 quorums
spanning all tiers), use MRDTs for cross-tier state. Each tier runs
strong local consensus. Cross-tier state is eventually consistent via
automatic merge when connectivity returns.

We compare three regimes:
1) Paxos-only: global reconciliation via Paxos (from Step 9)
2) MRDT-only: no Paxos reconciliation, just MRDT merge
3) Hybrid: local Paxos + MRDT cross-tier merge

Metrics:
- Local consensus: same as before (should be identical)
- Cross-tier convergence time: how long until all tiers agree
- Staleness: version vector distance between tiers during blackout
- Merge count: how many merges occur post-blackout
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from statistics import mean

import simpy

from datacenter import five_dc_topology
from entity import EntityRegistry
from paxos import Acceptor, FlexibleQuorum, MajorityQuorum, Proposer
from quorums import CrumblingWallQuorum
from mrdt import GCounter, VersionVector, LWWRegister, MergeEvent
from demo_step_10 import build_topology, ReconciliationStats, _p95


@dataclass
class MRDTMetrics:
    """Metrics for MRDT-based reconciliation."""
    # Staleness measurements (sampled periodically)
    staleness_samples: list[dict[str, dict[str, int]]] = field(default_factory=list)
    staleness_times: list[float] = field(default_factory=list)
    # Merge events
    merges: list[MergeEvent] = field(default_factory=list)
    # Convergence: time after blackout when all tiers agree
    convergence_time_after_blackout: float | None = None
    # Operations during blackout
    earth_ops_during_blackout: int = 0
    mars_ops_during_blackout: int = 0
    # Total operations
    earth_ops_total: int = 0
    mars_ops_total: int = 0


@dataclass(frozen=True)
class Step11Config:
    mars_base_latency_s: float = 186.0
    blackout_start_s: float = 600.0
    blackout_duration_s: float = 900.0
    sim_end_s: float = 3000.0
    merge_interval_s: float = 30.0  # How often to attempt MRDT sync
    local_op_interval_s: float = 2.0  # How often each tier does local ops
    seed: int = 42


def run_mrdt_experiment(
    with_repeater: bool,
    cfg: Step11Config,
    verbose: bool = True,
) -> MRDTMetrics:
    env = simpy.Environment()
    network = build_topology(env, cfg.mars_base_latency_s, seed=cfg.seed)
    registry = EntityRegistry()

    # Set up entities (same topology as Step 9/10)
    earth_locs = ["na-west", "europe", "asia", "sa-east", "africa"]
    earth_entities = []
    for loc in earth_locs:
        entity = registry.create(name=f"earth-{loc}")
        network.assign_entity(entity.id, loc)
        earth_entities.append(entity)

    moon_entity = registry.create(name="moon")
    network.assign_entity(moon_entity.id, "moon")
    leo_entity = registry.create(name="leo")
    network.assign_entity(leo_entity.id, "leo-sat")

    mars_entities = []
    for i in range(3):
        entity = registry.create(name=f"mars-{i}")
        network.assign_entity(entity.id, f"mars-{i}")
        mars_entities.append(entity)

    # Set up local Paxos (same as before)
    earth_ids = [e.id for e in earth_entities]
    mars_ids = [e.id for e in mars_entities]

    for entity in earth_entities + [moon_entity, leo_entity] + mars_entities:
        pt = 0.0005 if "earth" in entity.name or "leo" in entity.name else 0.001
        Acceptor(env, entity, network, process_time=pt)

    ep = registry.create(name="earth-proposer")
    network.assign_entity(ep.id, "na-west")
    earth_prop = Proposer(env, ep, network, earth_ids,
        FlexibleQuorum(earth_ids, phase1_size=4, phase2_size=2), timeout=1.0)

    mp = registry.create(name="mars-proposer")
    network.assign_entity(mp.id, "mars-0")
    mars_prop = Proposer(env, mp, network, mars_ids,
        MajorityQuorum(mars_ids), timeout=1.0)

    # MRDT state: each tier maintains its own counter and version vector
    tier_ids = ["earth", "leo", "moon", "mars"]
    earth_counter = GCounter("earth", tier_ids)
    mars_counter = GCounter("mars", tier_ids)
    leo_counter = GCounter("leo", tier_ids)
    moon_counter = GCounter("moon", tier_ids)

    earth_vv = VersionVector("earth", tier_ids)
    mars_vv = VersionVector("mars", tier_ids)
    leo_vv = VersionVector("leo", tier_ids)
    moon_vv = VersionVector("moon", tier_ids)

    # LWW register for "current leader" state
    earth_reg = LWWRegister("earth")
    mars_reg = LWWRegister("mars")

    tier_counters = {"earth": earth_counter, "mars": mars_counter,
                     "leo": leo_counter, "moon": moon_counter}
    tier_vvs = {"earth": earth_vv, "mars": mars_vv,
                "leo": leo_vv, "moon": moon_vv}
    tier_regs = {"earth": earth_reg, "mars": mars_reg}

    metrics = MRDTMetrics()
    blackout_end = cfg.blackout_start_s + cfg.blackout_duration_s
    blackout_active = [False]

    def earth_local_ops():
        slot = 0
        while env.now < cfg.sim_end_s:
            result = yield earth_prop.propose(slot=slot, value=f"earth-{slot}")
            if result.success:
                earth_counter.increment()
                earth_vv.tick()
                earth_reg.write(f"earth-decided-{slot}", env.now)
                metrics.earth_ops_total += 1
                if blackout_active[0]:
                    metrics.earth_ops_during_blackout += 1
            slot += 1
            yield env.timeout(cfg.local_op_interval_s)

    def mars_local_ops():
        slot = 10000
        while env.now < cfg.sim_end_s:
            result = yield mars_prop.propose(slot=slot, value=f"mars-{slot}")
            if result.success:
                mars_counter.increment()
                mars_vv.tick()
                mars_reg.write(f"mars-decided-{slot}", env.now)
                metrics.mars_ops_total += 1
                if blackout_active[0]:
                    metrics.mars_ops_during_blackout += 1
            slot += 1
            yield env.timeout(cfg.local_op_interval_s)

    def mrdt_sync():
        """Periodically sync MRDT state between tiers.

        During blackout, cross-tier sync fails (network partitioned).
        When connectivity returns, merge happens automatically.
        """
        while env.now < cfg.sim_end_s:
            yield env.timeout(cfg.merge_interval_s)

            # Try to sync between each pair of tiers
            sync_pairs = [
                ("earth", "leo", 0.020),    # ~20ms
                ("earth", "moon", 1.28),    # ~1.3s
                ("earth", "mars", cfg.mars_base_latency_s if not blackout_active[0] else None),
                ("leo", "moon", 1.28),
                ("mars", "moon", cfg.mars_base_latency_s + 1.28 if not blackout_active[0] else None),
            ]

            if with_repeater and blackout_active[0]:
                # Repeater provides degraded Earth-Mars connectivity
                sync_pairs = [
                    ("earth", "leo", 0.020),
                    ("earth", "moon", 1.28),
                    ("earth", "mars", 220.0 + 0.350),  # via Lagrange relay
                    ("leo", "moon", 1.28),
                    ("mars", "moon", 220.0 + 1.28),
                ]

            for src, dst, latency in sync_pairs:
                if latency is None:
                    continue  # Partitioned

                # Record staleness before merge
                staleness = tier_vvs[dst].staleness(tier_vvs[src])

                # Merge counters
                tier_counters[dst].merge(tier_counters[src].clone())
                tier_counters[src].merge(tier_counters[dst].clone())

                # Merge version vectors
                tier_vvs[dst].merge(tier_vvs[src].clone())
                tier_vvs[src].merge(tier_vvs[dst].clone())

                # Merge LWW registers if both tiers have them
                if src in tier_regs and dst in tier_regs:
                    tier_regs[dst].merge(tier_regs[src].clone())
                    tier_regs[src].merge(tier_regs[dst].clone())

                metrics.merges.append(MergeEvent(
                    time=env.now,
                    src_tier=src, dst_tier=dst,
                    staleness=staleness,
                    merge_latency=latency,
                ))

    def staleness_monitor():
        """Sample staleness between Earth and Mars periodically."""
        while env.now < cfg.sim_end_s:
            yield env.timeout(10.0)
            sample = {}
            for t1 in tier_ids:
                for t2 in tier_ids:
                    if t1 != t2:
                        s = tier_vvs[t1].staleness(tier_vvs[t2])
                        total = sum(s.values())
                        if total > 0:
                            sample[f"{t1}<-{t2}"] = s
            metrics.staleness_samples.append(sample)
            metrics.staleness_times.append(env.now)

    def convergence_detector():
        """Detect when blackout-era divergence is absorbed.

        During active operation, staleness is never zero (ops happen
        faster than merges). So we measure the steady-state staleness
        pre-blackout, then detect when post-blackout staleness returns
        to that level. This tells us how long the blackout's accumulated
        divergence takes to resolve.
        """
        # Measure steady-state staleness right before blackout
        yield env.timeout(cfg.blackout_start_s - 1.0)
        steady_state_max = 0
        for t1 in tier_ids:
            for t2 in tier_ids:
                if t1 == t2:
                    continue
                s = tier_vvs[t1].staleness(tier_vvs[t2])
                steady_state_max = max(steady_state_max, sum(s.values()))

        # Wait until after blackout
        yield env.timeout(cfg.blackout_duration_s + 1.0)

        # Allow 2x steady-state as the "converged" threshold
        threshold = max(steady_state_max * 2, 5)

        while env.now < cfg.sim_end_s:
            yield env.timeout(1.0)
            max_staleness = 0
            for t1 in tier_ids:
                for t2 in tier_ids:
                    if t1 == t2:
                        continue
                    s = tier_vvs[t1].staleness(tier_vvs[t2])
                    max_staleness = max(max_staleness, sum(s.values()))
            if max_staleness <= threshold:
                metrics.convergence_time_after_blackout = env.now - blackout_end
                break

    def conjunction_controller():
        mars_locs = [f"mars-{i}" for i in range(3)]
        earth_path_locs = ["na-west", "europe", "moon"]

        yield env.timeout(cfg.blackout_start_s)
        blackout_active[0] = True

        if with_repeater:
            for src in earth_path_locs:
                for dst in mars_locs:
                    network.update_link(src, dst, latency=240.0, jitter=12.0)
        else:
            for src in earth_path_locs:
                for dst in mars_locs:
                    network.partition_locations(src, dst)

        yield env.timeout(cfg.blackout_duration_s)
        blackout_active[0] = False

        if with_repeater:
            for src in earth_path_locs:
                for dst in mars_locs:
                    base = cfg.mars_base_latency_s + (1.28 if src == "moon" else 0.0)
                    network.update_link(src, dst, latency=base, jitter=5.0)
        else:
            network.heal_all()

    env.process(earth_local_ops())
    env.process(mars_local_ops())
    env.process(mrdt_sync())
    env.process(staleness_monitor())
    env.process(convergence_detector())
    env.process(conjunction_controller())
    env.run(until=cfg.sim_end_s)

    if verbose:
        label = "MRDT " + ("WITH REPEATER" if with_repeater else "HARD BLACKOUT")
        print("=" * 74)
        print(label)
        print("=" * 74)
        print()
        print(f"  Earth ops: {metrics.earth_ops_total} total, "
              f"{metrics.earth_ops_during_blackout} during blackout")
        print(f"  Mars ops:  {metrics.mars_ops_total} total, "
              f"{metrics.mars_ops_during_blackout} during blackout")
        print()

        # Counter values at end
        print("  Final counter values:")
        for tid, counter in sorted(tier_counters.items()):
            print(f"    {tid}: {counter.value()}")

        # Convergence
        if metrics.convergence_time_after_blackout is not None:
            print(f"\n  Convergence after blackout: {metrics.convergence_time_after_blackout:.1f}s")
        else:
            print("\n  Convergence after blackout: NOT REACHED")

        # Staleness during blackout
        blackout_samples = [
            (t, s) for t, s in zip(metrics.staleness_times, metrics.staleness_samples)
            if cfg.blackout_start_s <= t <= blackout_end
        ]
        if blackout_samples:
            # Find max Earth<-Mars staleness during blackout
            max_staleness = 0
            for _, sample in blackout_samples:
                for key, s in sample.items():
                    if "earth" in key and "mars" in key:
                        max_staleness = max(max_staleness, sum(s.values()))
            print(f"  Max Earth/Mars staleness during blackout: {max_staleness} ops behind")

        # Merge count
        pre_merges = sum(1 for m in metrics.merges if m.time < cfg.blackout_start_s)
        during_merges = sum(1 for m in metrics.merges
                          if cfg.blackout_start_s <= m.time < blackout_end)
        post_merges = sum(1 for m in metrics.merges if m.time >= blackout_end)
        print(f"\n  Merges: {pre_merges} pre, {during_merges} during, {post_merges} post blackout")

        # LWW register state
        print("\n  LWW Register (latest decision):")
        for tid, reg in sorted(tier_regs.items()):
            print(f"    {tid}: {reg}")
        print()

    return metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mars-latency-s", type=float, default=186.0)
    parser.add_argument("--blackout-duration-s", type=float, default=900.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    cfg = Step11Config(
        mars_base_latency_s=args.mars_latency_s,
        blackout_duration_s=args.blackout_duration_s,
        seed=args.seed,
    )

    print("\n" + "=" * 74)
    print("STEP 11: MRDT vs PAXOS RECONCILIATION")
    print("=" * 74 + "\n")

    # Run MRDT under hard blackout
    m_blackout = run_mrdt_experiment(with_repeater=False, cfg=cfg, verbose=not args.quiet)

    # Run MRDT with repeater
    m_repeater = run_mrdt_experiment(with_repeater=True, cfg=cfg, verbose=not args.quiet)

    if not args.quiet:
        print("=" * 74)
        print("COMPARISON: PAXOS vs MRDT RECONCILIATION")
        print("=" * 74)
        print()
        print("  Paxos reconciliation (from Step 9):")
        print("    Hard blackout: 0% during-blackout success (by construction)")
        print("    With repeater: 100% during-blackout success (at 186s Mars delay)")
        print("    Recovery lag:  ~490s")
        print()
        print("  MRDT reconciliation:")
        print(f"    Hard blackout: both tiers continue independently")
        print(f"      Earth ops during blackout: {m_blackout.earth_ops_during_blackout}")
        print(f"      Mars ops during blackout:  {m_blackout.mars_ops_during_blackout}")
        if m_blackout.convergence_time_after_blackout is not None:
            print(f"      Convergence after blackout: {m_blackout.convergence_time_after_blackout:.1f}s")
        else:
            print(f"      Convergence after blackout: NOT REACHED in sim window")
        print()
        print(f"    With repeater: tiers merge continuously via relay")
        print(f"      Earth ops during blackout: {m_repeater.earth_ops_during_blackout}")
        print(f"      Mars ops during blackout:  {m_repeater.mars_ops_during_blackout}")
        if m_repeater.convergence_time_after_blackout is not None:
            print(f"      Convergence after blackout: {m_repeater.convergence_time_after_blackout:.1f}s")
        else:
            print(f"      Convergence after blackout: NOT REACHED in sim window")
        print()
        print("  Key insight: MRDT reconciliation has NO blackout liveness problem.")
        print("  Both tiers make progress independently during blackout.")
        print("  Convergence is automatic when connectivity returns.")
        print("  The cost: eventual consistency instead of strong consistency")
        print("  across tiers during the merge window.")
        print()


if __name__ == "__main__":
    main()
