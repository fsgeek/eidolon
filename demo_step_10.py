"""Step 10: Relaxed Phase 2 — availability under Earth-node crash.

Compares three regimes during conjunction blackout:
1) Strict Q2 = {E} (all 5 Earth) — one Earth crash kills global Phase 2
2) Relaxed Q2 = 4-of-5 Earth — tolerates one Earth crash
3) Relaxed Q2 = 4-of-5 Earth with repeater — tolerates crash + blackout

The interesting question: relaxed Q2 requires a larger Phase 1 quorum
(7 vs 6). Does this help or hurt liveness when both a blackout and a
crash are happening simultaneously?
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from statistics import mean

import simpy

from datacenter import five_dc_topology
from entity import EntityRegistry
from paxos import Acceptor, FlexibleQuorum, MajorityQuorum, Proposer
from quorums import CrumblingWallQuorum


@dataclass(frozen=True)
class ExperimentConfig:
    mars_base_latency_s: float = 186.0
    blackout_start_s: float = 600.0
    blackout_duration_s: float = 900.0
    sim_end_s: float = 3000.0
    reconcile_interval_s: float = 120.0
    global_timeout_s: float = 500.0
    global_max_rounds: int = 1
    seed: int = 42
    phase2_threshold: int | None = None  # None = strict (all Earth)
    crash_earth_nodes: int = 0  # Number of Earth nodes to crash during blackout
    crash_time_offset_s: float = 0.0  # When to crash relative to blackout start


@dataclass
class ReconciliationStats:
    total: int = 0
    success: int = 0


@dataclass
class StepTenResult:
    name: str
    phase2_mode: str  # "strict" or "relaxed-4of5"
    crash_count: int
    earth_success: int
    earth_total: int
    mars_success: int
    mars_total: int
    pre_blackout: ReconciliationStats
    during_blackout: ReconciliationStats
    post_blackout: ReconciliationStats
    first_success_after_blackout_s: float | None
    avg_global_latency_s: float | None
    earth_local_avg_latency_s: float | None
    earth_local_p95_latency_s: float | None
    mars_local_avg_latency_s: float | None
    mars_local_p95_latency_s: float | None


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = max(0, int(0.95 * (len(ordered) - 1)))
    return ordered[idx]


def build_topology(env: simpy.Environment, mars_base_latency_s: float, seed: int = 42):
    """5 Earth DCs + LEO + Moon + 3 Mars sites + Lagrange relay."""
    network = five_dc_topology(env, seed=seed)

    network.add_location("leo-sat")
    network.add_link("na-west", "leo-sat", latency=0.020, jitter=0.005)
    network.add_link("europe", "leo-sat", latency=0.030, jitter=0.005)
    network.add_link("asia", "leo-sat", latency=0.035, jitter=0.005)

    network.add_location("moon")
    for loc in ["na-west", "europe", "asia", "sa-east", "africa"]:
        network.add_link(loc, "moon", latency=1.28, jitter=0.01)
    network.add_link("leo-sat", "moon", latency=1.28, jitter=0.01)

    for i in range(3):
        network.add_location(f"mars-{i}")
    network.add_link("mars-0", "mars-1", latency=0.005, jitter=0.001)
    network.add_link("mars-0", "mars-2", latency=0.005, jitter=0.001)
    network.add_link("mars-1", "mars-2", latency=0.005, jitter=0.001)

    for earth_loc in ["na-west", "europe"]:
        for i in range(3):
            network.add_link(
                earth_loc, f"mars-{i}",
                latency=mars_base_latency_s, jitter=5.0,
            )
    for i in range(3):
        network.add_link("moon", f"mars-{i}", latency=mars_base_latency_s + 1.28, jitter=5.0)

    network.add_location("lagrange-relay")
    network.add_link("na-west", "lagrange-relay", latency=0.350, jitter=0.01)
    network.add_link("europe", "lagrange-relay", latency=0.360, jitter=0.01)
    for i in range(3):
        network.add_link("lagrange-relay", f"mars-{i}", latency=220.0, jitter=10.0)

    return network


def _wire_system(env: simpy.Environment, cfg: ExperimentConfig):
    registry = EntityRegistry()
    network = build_topology(env, cfg.mars_base_latency_s, seed=cfg.seed)

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

    for entity in earth_entities + [moon_entity, leo_entity] + mars_entities:
        process_time = 0.0005 if "earth" in entity.name or "leo" in entity.name else 0.001
        Acceptor(env, entity, network, process_time=process_time)

    earth_ids = [e.id for e in earth_entities]
    mars_ids = [e.id for e in mars_entities]
    all_ids = earth_ids + [leo_entity.id, moon_entity.id] + mars_ids

    earth_prop_entity = registry.create(name="earth-proposer")
    network.assign_entity(earth_prop_entity.id, "na-west")
    earth_prop = Proposer(
        env, earth_prop_entity, network, earth_ids,
        FlexibleQuorum(earth_ids, phase1_size=4, phase2_size=2),
        timeout=1.0,
    )

    mars_prop_entity = registry.create(name="mars-proposer")
    network.assign_entity(mars_prop_entity.id, "mars-0")
    mars_prop = Proposer(
        env, mars_prop_entity, network, mars_ids,
        MajorityQuorum(mars_ids),
        timeout=1.0,
    )

    global_prop_entity = registry.create(name="global-proposer")
    network.assign_entity(global_prop_entity.id, "na-west")
    wall = CrumblingWallQuorum(
        [mars_ids, [moon_entity.id], [leo_entity.id], earth_ids],
        phase2_threshold=cfg.phase2_threshold,
    )
    global_prop = Proposer(
        env, global_prop_entity, network, all_ids, wall,
        timeout=cfg.global_timeout_s,
        max_rounds=cfg.global_max_rounds,
        initiator_tier=3,  # Earth = bottom of wall
    )

    # Earth nodes to crash, from least to most connected
    crash_target_locs = ["africa", "sa-east", "asia", "europe", "na-west"]

    return network, earth_prop, mars_prop, global_prop, crash_target_locs, earth_locs


def run_step10_experiment(
    with_repeater: bool,
    cfg: ExperimentConfig,
    verbose: bool = True,
) -> StepTenResult:
    env = simpy.Environment()
    network, earth_prop, mars_prop, global_prop, crash_locs, earth_locs = _wire_system(env, cfg)

    earth_total = 0
    earth_success = 0
    mars_total = 0
    mars_success = 0
    pre = ReconciliationStats()
    during = ReconciliationStats()
    post = ReconciliationStats()
    global_latencies = []
    earth_latencies = []
    mars_latencies = []
    first_success_after_blackout = None
    blackout_end = cfg.blackout_start_s + cfg.blackout_duration_s

    def earth_local():
        nonlocal earth_total, earth_success
        slot = 0
        while env.now < cfg.sim_end_s:
            result = yield earth_prop.propose(slot=slot, value=f"earth-{slot}")
            earth_total += 1
            if result.success:
                earth_success += 1
                earth_latencies.append(result.total_time)
            slot += 1
            yield env.timeout(2.0)

    def mars_local():
        nonlocal mars_total, mars_success
        slot = 10_000
        while env.now < cfg.sim_end_s:
            result = yield mars_prop.propose(slot=slot, value=f"mars-{slot}")
            mars_total += 1
            if result.success:
                mars_success += 1
                mars_latencies.append(result.total_time)
            slot += 1
            yield env.timeout(2.0)

    def global_reconcile():
        nonlocal first_success_after_blackout
        slot = 20_000
        while env.now < cfg.sim_end_s:
            started = env.now
            result = yield global_prop.propose(slot=slot, value=f"reconcile-{slot}")
            slot += 1

            if started < cfg.blackout_start_s:
                bucket = pre
            elif started < blackout_end:
                bucket = during
            else:
                bucket = post
            bucket.total += 1
            if result.success:
                bucket.success += 1
                global_latencies.append(result.total_time)
                if env.now >= blackout_end and first_success_after_blackout is None:
                    first_success_after_blackout = env.now - blackout_end

            yield env.timeout(cfg.reconcile_interval_s)

    def conjunction_controller():
        mars_locs = [f"mars-{i}" for i in range(3)]
        earth_path_locs = ["na-west", "europe", "moon"]

        yield env.timeout(cfg.blackout_start_s)

        if with_repeater:
            for src in earth_path_locs:
                for dst in mars_locs:
                    network.update_link(src, dst, latency=240.0, jitter=12.0)
        else:
            for src in earth_path_locs:
                for dst in mars_locs:
                    network.partition_locations(src, dst)

        # Crash Earth nodes if configured
        nodes_to_crash = crash_locs[:cfg.crash_earth_nodes]
        if nodes_to_crash:
            yield env.timeout(cfg.crash_time_offset_s)
            # Crash-stop: partition each crashed location from everything
            all_locs = list(network._locations.keys())
            for cloc in nodes_to_crash:
                for other_loc in all_locs:
                    if other_loc != cloc:
                        network.partition_locations(cloc, other_loc)

        yield env.timeout(cfg.blackout_duration_s - (cfg.crash_time_offset_s if nodes_to_crash else 0.0))

        # Heal blackout
        if with_repeater:
            for src in earth_path_locs:
                for dst in mars_locs:
                    base = cfg.mars_base_latency_s + (1.28 if src == "moon" else 0.0)
                    network.update_link(src, dst, latency=base, jitter=5.0)
        else:
            network.heal_all()

        # Crashed nodes stay crashed (crash-stop model).
        # If we healed all, re-crash them.
        if nodes_to_crash:
            all_locs = list(network._locations.keys())
            for cloc in nodes_to_crash:
                for other_loc in all_locs:
                    if other_loc != cloc:
                        network.partition_locations(cloc, other_loc)

    env.process(earth_local())
    env.process(mars_local())
    env.process(global_reconcile())
    env.process(conjunction_controller())
    env.run(until=cfg.sim_end_s)

    phase2_mode = "strict" if cfg.phase2_threshold is None else f"relaxed-{cfg.phase2_threshold}of5"
    crash_suffix = f"+crash{cfg.crash_earth_nodes}" if cfg.crash_earth_nodes > 0 else ""

    result = StepTenResult(
        name=("with_repeater" if with_repeater else "blackout_only") + crash_suffix,
        phase2_mode=phase2_mode,
        crash_count=cfg.crash_earth_nodes,
        earth_success=earth_success,
        earth_total=earth_total,
        mars_success=mars_success,
        mars_total=mars_total,
        pre_blackout=pre,
        during_blackout=during,
        post_blackout=post,
        first_success_after_blackout_s=first_success_after_blackout,
        avg_global_latency_s=(
            sum(global_latencies) / len(global_latencies) if global_latencies else None
        ),
        earth_local_avg_latency_s=(mean(earth_latencies) if earth_latencies else None),
        earth_local_p95_latency_s=_p95(earth_latencies),
        mars_local_avg_latency_s=(mean(mars_latencies) if mars_latencies else None),
        mars_local_p95_latency_s=_p95(mars_latencies),
    )

    if verbose:
        label = f"{result.name} [{phase2_mode}]"
        print("=" * 74)
        print(label)
        print("=" * 74)
        print()
        print(
            f"  Earth local decisions: {earth_success}/{earth_total} "
            f"({(100.0 * earth_success / max(1, earth_total)):.1f}%)"
        )
        print(
            f"  Mars local decisions:  {mars_success}/{mars_total} "
            f"({(100.0 * mars_success / max(1, mars_total)):.1f}%)"
        )
        print()
        print("  Global reconciliation:")
        print(f"    Pre-blackout:    {pre.success}/{pre.total}")
        print(f"    During blackout: {during.success}/{during.total}")
        print(f"    Post-blackout:   {post.success}/{post.total}")
        if result.avg_global_latency_s is not None:
            print(f"    Avg latency:     {result.avg_global_latency_s:.1f}s")
        if result.first_success_after_blackout_s is not None:
            print(f"    Recovery lag:    {result.first_success_after_blackout_s:.1f}s")
        else:
            print("    Recovery lag:    n/a")
        print()

    return result


def run_comparison(cfg_base: ExperimentConfig, verbose: bool = True):
    """Run all four combinations: strict/relaxed x no-crash/crash, with repeater."""
    results = []

    configs = [
        ("Strict Q2, no crash", ExperimentConfig(
            **{**cfg_base.__dict__, "phase2_threshold": None, "crash_earth_nodes": 0})),
        ("Strict Q2, 1 crash", ExperimentConfig(
            **{**cfg_base.__dict__, "phase2_threshold": None, "crash_earth_nodes": 1})),
        ("Relaxed Q2 (4/5), no crash", ExperimentConfig(
            **{**cfg_base.__dict__, "phase2_threshold": 4, "crash_earth_nodes": 0})),
        ("Relaxed Q2 (4/5), 1 crash", ExperimentConfig(
            **{**cfg_base.__dict__, "phase2_threshold": 4, "crash_earth_nodes": 1})),
        ("Relaxed Q2 (3/5), 2 crashes", ExperimentConfig(
            **{**cfg_base.__dict__, "phase2_threshold": 3, "crash_earth_nodes": 2})),
    ]

    for label, cfg in configs:
        if verbose:
            print(f"\n{'='*74}")
            print(f"  SCENARIO: {label}")
            print(f"{'='*74}\n")
        r = run_step10_experiment(with_repeater=True, cfg=cfg, verbose=verbose)
        results.append((label, r))

    if verbose:
        print("\n" + "=" * 74)
        print("STEP 10 SUMMARY: RELAXED PHASE 2 UNDER EARTH-NODE CRASH")
        print("=" * 74)
        print()
        print(f"  {'Scenario':<42} {'During BL':>10} {'Post BL':>10} {'Recovery':>10}")
        print(f"  {'-'*42} {'-'*10} {'-'*10} {'-'*10}")
        for label, r in results:
            d_rate = f"{100*r.during_blackout.success/max(1,r.during_blackout.total):.0f}%" if r.during_blackout.total else "n/a"
            p_rate = f"{100*r.post_blackout.success/max(1,r.post_blackout.total):.0f}%" if r.post_blackout.total else "n/a"
            rec = f"{r.first_success_after_blackout_s:.1f}s" if r.first_success_after_blackout_s is not None else "n/a"
            print(f"  {label:<42} {d_rate:>10} {p_rate:>10} {rec:>10}")
        print()

    return results


def write_csv(output_path: str | Path, results: list[tuple[str, StepTenResult]], cfg: ExperimentConfig):
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "scenario", "phase2_mode", "crash_active", "seed",
            "mars_latency_s", "blackout_duration_s",
            "earth_success", "earth_total",
            "mars_success", "mars_total",
            "global_pre_success", "global_pre_total",
            "global_during_success", "global_during_total",
            "global_post_success", "global_post_total",
            "first_success_after_blackout_s",
            "avg_global_latency_s",
            "earth_local_avg_latency_s", "earth_local_p95_latency_s",
        ])
        for label, r in results:
            writer.writerow([
                r.name, r.phase2_mode, r.crash_count, cfg.seed,
                cfg.mars_base_latency_s, cfg.blackout_duration_s,
                r.earth_success, r.earth_total,
                r.mars_success, r.mars_total,
                r.pre_blackout.success, r.pre_blackout.total,
                r.during_blackout.success, r.during_blackout.total,
                r.post_blackout.success, r.post_blackout.total,
                f"{r.first_success_after_blackout_s:.6f}" if r.first_success_after_blackout_s is not None else "",
                f"{r.avg_global_latency_s:.6f}" if r.avg_global_latency_s is not None else "",
                f"{r.earth_local_avg_latency_s:.6f}" if r.earth_local_avg_latency_s is not None else "",
                f"{r.earth_local_p95_latency_s:.6f}" if r.earth_local_p95_latency_s is not None else "",
            ])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mars-latency-s", type=float, default=186.0)
    parser.add_argument("--blackout-start-s", type=float, default=600.0)
    parser.add_argument("--blackout-duration-s", type=float, default=900.0)
    parser.add_argument("--sim-end-s", type=float, default=3000.0)
    parser.add_argument("--reconcile-interval-s", type=float, default=120.0)
    parser.add_argument("--global-timeout-s", type=float, default=500.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--csv", type=str, default="")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    cfg = ExperimentConfig(
        mars_base_latency_s=args.mars_latency_s,
        blackout_start_s=args.blackout_start_s,
        blackout_duration_s=args.blackout_duration_s,
        sim_end_s=args.sim_end_s,
        reconcile_interval_s=args.reconcile_interval_s,
        global_timeout_s=args.global_timeout_s,
        seed=args.seed,
    )

    results = run_comparison(cfg, verbose=not args.quiet)

    if args.csv:
        write_csv(args.csv, results, cfg)
        if not args.quiet:
            print(f"Wrote CSV: {args.csv}")


if __name__ == "__main__":
    main()
