"""Per-tier liveness sweep: which initiating tiers retain global consensus during blackout?

The crumbling wall predicts that during Mars conjunction blackout:
  - Mars-initiated Phase 1: BLOCKED (needs all tiers below)
  - Moon-initiated Phase 1:  WORKS  (needs Moon + LEO + Earth, all reachable)
  - LEO-initiated Phase 1:   WORKS  (needs LEO + Earth)
  - Earth-initiated Phase 1:  WORKS  (needs Earth only)

This experiment tests that prediction by running a global proposer at each tier
and measuring during-blackout success, latency, and post-blackout recovery.

Each proposer is physically located at its tier, so latency reflects the real
network cost of initiating from that position in the wall.
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, stdev

import simpy

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from demo_step_9 import (
    ExperimentConfig,
    ReconciliationStats,
    build_topology,
    _p95,
)
from entity import EntityRegistry
from paxos import Acceptor, FlexibleQuorum, MajorityQuorum, Proposer
from quorums import CrumblingWallQuorum


# Tier definitions: index, name, and the network location for the proposer.
TIERS = [
    (0, "mars", "mars-0"),
    (1, "moon", "moon"),
    (2, "leo", "leo-sat"),
    (3, "earth", "na-west"),
]


@dataclass
class TierResult:
    """Results for one tier's global proposer in one scenario."""
    tier_index: int
    tier_name: str
    scenario: str  # "blackout_only" or "with_repeater"
    topology: str  # "sparse" or "full_coverage"
    pre_success: int
    pre_total: int
    during_success: int
    during_total: int
    post_success: int
    post_total: int
    avg_latency_s: float | None
    first_post_blackout_s: float | None
    earth_local_success: int
    earth_local_total: int
    mars_local_success: int
    mars_local_total: int


def _add_full_coverage_links(network, mars_base_latency_s: float):
    """Add missing links so every tier can reach all Earth ground stations.

    Sparse topology: LEO sees 3/5 Earth DCs, Mars sees 2/5.
    Full coverage: LEO sees 5/5, Mars sees 5/5 (via all ground stations).

    This models a LEO constellation with global coverage and Mars deep-space
    network with antennas at all ground station locations.
    """
    # LEO missing: sa-east, africa
    network.add_link("sa-east", "leo-sat", latency=0.040, jitter=0.005)
    network.add_link("africa", "leo-sat", latency=0.045, jitter=0.005)

    # Mars missing: asia, sa-east, africa
    for loc in ["asia", "sa-east", "africa"]:
        for i in range(3):
            network.add_link(loc, f"mars-{i}", latency=mars_base_latency_s, jitter=5.0)


def _wire_system_multitier(env: simpy.Environment, cfg: ExperimentConfig,
                           full_coverage: bool = False):
    """Build topology with one global proposer per tier."""
    registry = EntityRegistry()
    network = build_topology(env, cfg.mars_base_latency_s, seed=cfg.seed)

    if full_coverage:
        _add_full_coverage_links(network, cfg.mars_base_latency_s)

    # Create acceptor nodes (same as demo_step_9)
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

    # Earth-local and Mars-local proposers (same as demo_step_9)
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

    # One global proposer per tier, each physically at its tier location.
    wall = CrumblingWallQuorum([
        mars_ids,
        [moon_entity.id],
        [leo_entity.id],
        earth_ids,
    ])

    global_proposers = {}
    for tier_idx, tier_name, tier_loc in TIERS:
        prop_entity = registry.create(name=f"global-{tier_name}")
        network.assign_entity(prop_entity.id, tier_loc)
        prop = Proposer(
            env, prop_entity, network, all_ids, wall,
            timeout=cfg.global_timeout_s,
            max_rounds=cfg.global_max_rounds,
            initiator_tier=tier_idx,
        )
        global_proposers[tier_idx] = prop

    return network, earth_prop, mars_prop, global_proposers


def run_tier_experiment(
    with_repeater: bool,
    cfg: ExperimentConfig,
    full_coverage: bool = False,
    verbose: bool = True,
) -> list[TierResult]:
    """Run one scenario, measuring all four tiers' global proposers."""
    env = simpy.Environment()
    network, earth_prop, mars_prop, global_proposers = _wire_system_multitier(
        env, cfg, full_coverage=full_coverage,
    )

    earth_total = 0
    earth_success = 0
    mars_total = 0
    mars_success = 0

    blackout_end = cfg.blackout_start_s + cfg.blackout_duration_s

    # Per-tier tracking
    tier_stats: dict[int, dict] = {}
    for tier_idx, _, _ in TIERS:
        tier_stats[tier_idx] = {
            "pre": ReconciliationStats(),
            "during": ReconciliationStats(),
            "post": ReconciliationStats(),
            "latencies": [],
            "first_post": None,
        }

    def earth_local():
        nonlocal earth_total, earth_success
        slot = 0
        while env.now < cfg.sim_end_s:
            result = yield earth_prop.propose(slot=slot, value=f"earth-{slot}")
            earth_total += 1
            if result.success:
                earth_success += 1
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
            slot += 1
            yield env.timeout(2.0)

    def global_reconcile_for_tier(tier_idx: int):
        """Global reconciliation process for one tier's proposer."""
        prop = global_proposers[tier_idx]
        stats = tier_stats[tier_idx]
        # Each tier uses a different slot range to avoid conflicts.
        slot_base = 20_000 + tier_idx * 10_000
        slot = slot_base
        while env.now < cfg.sim_end_s:
            started = env.now
            result = yield prop.propose(slot=slot, value=f"reconcile-t{tier_idx}-{slot}")
            slot += 1

            if started < cfg.blackout_start_s:
                bucket = stats["pre"]
            elif started < blackout_end:
                bucket = stats["during"]
            else:
                bucket = stats["post"]
            bucket.total += 1
            if result.success:
                bucket.success += 1
                stats["latencies"].append(result.total_time)
                if env.now >= blackout_end and stats["first_post"] is None:
                    stats["first_post"] = env.now - blackout_end

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

        yield env.timeout(cfg.blackout_duration_s)

        if with_repeater:
            for src in earth_path_locs:
                for dst in mars_locs:
                    base = cfg.mars_base_latency_s + (1.28 if src == "moon" else 0.0)
                    network.update_link(src, dst, latency=base, jitter=5.0)
        else:
            network.heal_all()

    env.process(earth_local())
    env.process(mars_local())
    for tier_idx, _, _ in TIERS:
        env.process(global_reconcile_for_tier(tier_idx))
    env.process(conjunction_controller())
    env.run(until=cfg.sim_end_s)

    scenario_name = "with_repeater" if with_repeater else "blackout_only"
    topo_name = "full_coverage" if full_coverage else "sparse"
    results = []
    for tier_idx, tier_name, _ in TIERS:
        stats = tier_stats[tier_idx]
        lats = stats["latencies"]
        results.append(TierResult(
            tier_index=tier_idx,
            tier_name=tier_name,
            scenario=scenario_name,
            topology=topo_name,
            pre_success=stats["pre"].success,
            pre_total=stats["pre"].total,
            during_success=stats["during"].success,
            during_total=stats["during"].total,
            post_success=stats["post"].success,
            post_total=stats["post"].total,
            avg_latency_s=mean(lats) if lats else None,
            first_post_blackout_s=stats["first_post"],
            earth_local_success=earth_success,
            earth_local_total=earth_total,
            mars_local_success=mars_success,
            mars_local_total=mars_total,
        ))

    if verbose:
        label = "WITH REPEATER" if with_repeater else "HARD BLACKOUT"
        topo_label = " [full coverage]" if full_coverage else " [sparse]"
        print(f"\n{'='*74}")
        print(f"  {label}{topo_label}")
        print(f"{'='*74}")
        print(f"\n  {'Tier':<8} {'Pre':>8} {'During':>10} {'Post':>8} {'Avg lat':>10} {'Recovery':>10}")
        print(f"  {'-'*8} {'-'*8} {'-'*10} {'-'*8} {'-'*10} {'-'*10}")
        for r in results:
            pre_str = f"{r.pre_success}/{r.pre_total}"
            dur_str = f"{r.during_success}/{r.during_total}"
            post_str = f"{r.post_success}/{r.post_total}"
            lat_str = f"{r.avg_latency_s:.2f}s" if r.avg_latency_s is not None else "n/a"
            rec_str = f"{r.first_post_blackout_s:.1f}s" if r.first_post_blackout_s is not None else "n/a"
            print(f"  {r.tier_name:<8} {pre_str:>8} {dur_str:>10} {post_str:>8} {lat_str:>10} {rec_str:>10}")
        print()

    return results


CSV_HEADER = [
    "scenario", "topology", "tier_index", "tier_name",
    "mars_base_latency_s", "blackout_duration_s",
    "seed",
    "pre_success", "pre_total",
    "during_success", "during_total",
    "post_success", "post_total",
    "avg_latency_s", "first_post_blackout_s",
    "earth_local_success", "earth_local_total",
    "mars_local_success", "mars_local_total",
]


def _mean_ci95(values: list[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    if len(values) == 1:
        return values[0], 0.0
    mu = mean(values)
    sigma = stdev(values)
    ci95 = 1.96 * sigma / (len(values) ** 0.5)
    return mu, ci95


def run_sweep(args):
    seeds = [int(v.strip()) for v in args.seeds.split(",") if v.strip()]
    mars_latencies = [float(v.strip()) for v in args.mars_latencies_s.split(",") if v.strip()]
    blackout_durations = [float(v.strip()) for v in args.blackout_durations_s.split(",") if v.strip()]

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    raw_rows = []
    topologies = [False, True]  # sparse, full_coverage
    total_runs = len(mars_latencies) * len(blackout_durations) * len(seeds) * 2 * len(topologies)
    run_count = 0

    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)

        for full_cov in topologies:
            for mars_lat in mars_latencies:
                for bo_dur in blackout_durations:
                    for seed in seeds:
                        cfg = ExperimentConfig(
                            mars_base_latency_s=mars_lat,
                            blackout_duration_s=bo_dur,
                            seed=seed,
                            global_timeout_s=args.global_timeout_s,
                            global_max_rounds=args.global_max_rounds,
                            reconcile_interval_s=args.reconcile_interval_s,
                            blackout_start_s=args.blackout_start_s,
                            sim_end_s=args.sim_end_s,
                        )
                        for with_rep in [False, True]:
                            results = run_tier_experiment(
                                with_rep, cfg, full_coverage=full_cov, verbose=False,
                            )
                            run_count += 1
                            for r in results:
                                row = [
                                    r.scenario, r.topology, r.tier_index, r.tier_name,
                                    mars_lat, bo_dur, seed,
                                    r.pre_success, r.pre_total,
                                    r.during_success, r.during_total,
                                    r.post_success, r.post_total,
                                    f"{r.avg_latency_s:.6f}" if r.avg_latency_s is not None else "",
                                    f"{r.first_post_blackout_s:.6f}" if r.first_post_blackout_s is not None else "",
                                    r.earth_local_success, r.earth_local_total,
                                    r.mars_local_success, r.mars_local_total,
                                ]
                                writer.writerow(row)
                                raw_rows.append(r)
                            if run_count % 20 == 0:
                                print(f"  [{run_count}/{total_runs}] topo={'full' if full_cov else 'sparse'} mars={mars_lat}s bo={bo_dur}s seed={seed}")
                            f.flush()

    print(f"\nWrote {len(raw_rows)} rows to {output}")

    # Aggregate: mean ± 95% CI per (scenario, tier, mars_lat, bo_dur)
    from collections import defaultdict
    groups = defaultdict(lambda: {
        "during_rates": [],
        "post_rates": [],
        "latencies": [],
        "recovery": [],
    })
    for r in raw_rows:
        key = (r.scenario, r.tier_index, r.tier_name)
        # We need mars_lat and bo_dur... reconstruct from position
        # Actually let's just re-read the CSV. Simpler to aggregate from raw_rows
        # with the config info. But raw_rows don't carry mars_lat/bo_dur.
        pass

    # Re-read and aggregate from CSV
    _aggregate_csv(output, output.with_name(output.stem + "_ci.csv"))


def _aggregate_csv(raw_path: Path, ci_path: Path):
    """Aggregate raw per-seed rows into mean ± 95% CI."""
    from collections import defaultdict

    groups = defaultdict(lambda: {
        "during_rates": [],
        "post_rates": [],
        "latencies": [],
        "recovery": [],
    })

    with raw_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (
                row["scenario"],
                row["topology"],
                int(row["tier_index"]),
                row["tier_name"],
                float(row["mars_base_latency_s"]),
                float(row["blackout_duration_s"]),
            )
            dt = int(row["during_total"])
            ds = int(row["during_success"])
            pt = int(row["post_total"])
            ps = int(row["post_success"])
            groups[key]["during_rates"].append(ds / dt if dt > 0 else None)
            groups[key]["post_rates"].append(ps / pt if pt > 0 else None)
            if row["avg_latency_s"]:
                groups[key]["latencies"].append(float(row["avg_latency_s"]))
            if row["first_post_blackout_s"]:
                groups[key]["recovery"].append(float(row["first_post_blackout_s"]))

    with ci_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "scenario", "topology", "tier_index", "tier_name",
            "mars_base_latency_s", "blackout_duration_s",
            "during_rate_mean", "during_rate_ci95",
            "post_rate_mean", "post_rate_ci95",
            "avg_latency_s_mean", "avg_latency_s_ci95",
            "recovery_s_mean", "recovery_s_ci95",
            "n_seeds",
        ])
        for key in sorted(groups.keys()):
            scenario, topology, tier_idx, tier_name, mars_lat, bo_dur = key
            g = groups[key]
            dr = [v for v in g["during_rates"] if v is not None]
            pr = [v for v in g["post_rates"] if v is not None]
            dr_mean, dr_ci = _mean_ci95(dr)
            pr_mean, pr_ci = _mean_ci95(pr)
            lat_mean, lat_ci = _mean_ci95(g["latencies"])
            rec_mean, rec_ci = _mean_ci95(g["recovery"])

            def _fmt(v):
                return f"{v:.6f}" if v is not None else ""

            writer.writerow([
                scenario, topology, tier_idx, tier_name,
                mars_lat, bo_dur,
                _fmt(dr_mean), _fmt(dr_ci),
                _fmt(pr_mean), _fmt(pr_ci),
                _fmt(lat_mean), _fmt(lat_ci),
                _fmt(rec_mean), _fmt(rec_ci),
                len(dr),
            ])

    print(f"Wrote aggregated results to {ci_path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mars-latencies-s", type=str, default="186")
    parser.add_argument("--blackout-durations-s", type=str, default="900")
    parser.add_argument("--blackout-start-s", type=float, default=600.0)
    parser.add_argument("--sim-end-s", type=float, default=4000.0)
    parser.add_argument("--reconcile-interval-s", type=float, default=120.0)
    parser.add_argument("--global-timeout-s", type=float, default=500.0)
    parser.add_argument("--global-max-rounds", type=int, default=1)
    parser.add_argument("--seeds", type=str, default="42")
    parser.add_argument("--output", type=str, default="results/tier_liveness/tier_sweep.csv")
    args = parser.parse_args()

    if "," not in args.seeds and args.seeds.strip() == "42":
        # Single-run mode for quick testing — run both topologies
        cfg = ExperimentConfig(
            mars_base_latency_s=float(args.mars_latencies_s.split(",")[0]),
            blackout_duration_s=float(args.blackout_durations_s.split(",")[0]),
            seed=42,
            global_timeout_s=args.global_timeout_s,
            global_max_rounds=args.global_max_rounds,
            reconcile_interval_s=args.reconcile_interval_s,
            blackout_start_s=args.blackout_start_s,
            sim_end_s=args.sim_end_s,
        )
        for full_cov in [False, True]:
            label = "FULL COVERAGE" if full_cov else "SPARSE TOPOLOGY"
            print(f"\n{'#'*74}")
            print(f"  {label}")
            print(f"{'#'*74}")
            run_tier_experiment(False, cfg, full_coverage=full_cov, verbose=True)
            run_tier_experiment(True, cfg, full_coverage=full_cov, verbose=True)
        return

    run_sweep(args)


if __name__ == "__main__":
    main()
