"""Step 9: Conjunction window and Lagrange repeater refinement.

This scenario compares two regimes:
1) Baseline hard blackout during conjunction.
2) Repeater-assisted degraded continuity during conjunction.
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


@dataclass
class ReconciliationStats:
    total: int = 0
    success: int = 0


@dataclass
class ExperimentResult:
    name: str
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
    global_phase1_responses_earth: int
    global_phase1_responses_leo: int
    global_phase1_responses_moon: int
    global_phase1_responses_mars: int
    global_phase2_responses_earth: int
    global_phase2_responses_leo: int
    global_phase2_responses_moon: int
    global_phase2_responses_mars: int
    network_src_sent_earth: int
    network_src_sent_leo: int
    network_src_sent_moon: int
    network_src_sent_mars: int
    network_src_avg_oneway_s_earth: float | None
    network_src_avg_oneway_s_leo: float | None
    network_src_avg_oneway_s_moon: float | None
    network_src_avg_oneway_s_mars: float | None


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = max(0, int(0.95 * (len(ordered) - 1)))
    return ordered[idx]


def _tier_from_location(location: str | None) -> str:
    if location is None:
        return "unknown"
    if location.startswith("mars-"):
        return "mars"
    if location == "moon":
        return "moon"
    if location == "leo-sat":
        return "leo"
    return "earth"


def _extract_tier_metrics(network, global_proposer_id: int) -> dict[str, float | int | None]:
    packet_send_time = {}

    src_sent = {"earth": 0, "leo": 0, "moon": 0, "mars": 0}
    src_delay_sum = {"earth": 0.0, "leo": 0.0, "moon": 0.0, "mars": 0.0}
    src_delay_count = {"earth": 0, "leo": 0, "moon": 0, "mars": 0}

    g_p1 = {"earth": 0, "leo": 0, "moon": 0, "mars": 0}
    g_p2 = {"earth": 0, "leo": 0, "moon": 0, "mars": 0}

    for ev in network.packet_log:
        packet = ev["packet"]
        event = ev["event"]
        packet_key = id(packet)

        if event == "send":
            packet_send_time[packet_key] = ev["time"]
            src_id = getattr(packet, "client_id", None) if packet.__class__.__name__ == "Request" else getattr(packet, "server_id", None)
            src_loc = network._entity_location.get(src_id)
            src_tier = _tier_from_location(src_loc)
            if src_tier in src_sent:
                src_sent[src_tier] += 1
            continue

        if event == "deliver":
            send_t = packet_send_time.get(packet_key)
            if send_t is not None:
                src_id = getattr(packet, "client_id", None) if packet.__class__.__name__ == "Request" else getattr(packet, "server_id", None)
                src_loc = network._entity_location.get(src_id)
                src_tier = _tier_from_location(src_loc)
                if src_tier in src_delay_sum:
                    src_delay_sum[src_tier] += (ev["time"] - send_t)
                    src_delay_count[src_tier] += 1

            # Count global proposer response burden by source tier and phase.
            if (
                packet.__class__.__name__ == "Response"
                and getattr(packet, "client_id", None) == global_proposer_id
                and hasattr(packet, "payload")
                and packet.payload.__class__.__name__ == "PaxosPayload"
            ):
                src_loc = network._entity_location.get(packet.server_id)
                src_tier = _tier_from_location(src_loc)
                phase_name = getattr(packet.payload.phase, "name", "")
                if src_tier in g_p1 and phase_name == "PROMISE":
                    g_p1[src_tier] += 1
                if src_tier in g_p2 and phase_name == "ACCEPTED":
                    g_p2[src_tier] += 1

    def _avg(sum_v, cnt_v):
        return (sum_v / cnt_v) if cnt_v > 0 else None

    return {
        "global_phase1_responses_earth": g_p1["earth"],
        "global_phase1_responses_leo": g_p1["leo"],
        "global_phase1_responses_moon": g_p1["moon"],
        "global_phase1_responses_mars": g_p1["mars"],
        "global_phase2_responses_earth": g_p2["earth"],
        "global_phase2_responses_leo": g_p2["leo"],
        "global_phase2_responses_moon": g_p2["moon"],
        "global_phase2_responses_mars": g_p2["mars"],
        "network_src_sent_earth": src_sent["earth"],
        "network_src_sent_leo": src_sent["leo"],
        "network_src_sent_moon": src_sent["moon"],
        "network_src_sent_mars": src_sent["mars"],
        "network_src_avg_oneway_s_earth": _avg(src_delay_sum["earth"], src_delay_count["earth"]),
        "network_src_avg_oneway_s_leo": _avg(src_delay_sum["leo"], src_delay_count["leo"]),
        "network_src_avg_oneway_s_moon": _avg(src_delay_sum["moon"], src_delay_count["moon"]),
        "network_src_avg_oneway_s_mars": _avg(src_delay_sum["mars"], src_delay_count["mars"]),
    }


def build_topology(env: simpy.Environment, mars_base_latency_s: float, seed: int = 42):
    """5 Earth DCs + LEO + Moon + 3 Mars sites."""
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
                earth_loc,
                f"mars-{i}",
                latency=mars_base_latency_s,
                jitter=5.0,
            )
    for i in range(3):
        network.add_link("moon", f"mars-{i}", latency=mars_base_latency_s + 1.28, jitter=5.0)

    # Control-plane abstraction for the refinement scenario.
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
        env,
        earth_prop_entity,
        network,
        earth_ids,
        FlexibleQuorum(earth_ids, phase1_size=4, phase2_size=2),
        timeout=1.0,
    )

    mars_prop_entity = registry.create(name="mars-proposer")
    network.assign_entity(mars_prop_entity.id, "mars-0")
    mars_prop = Proposer(
        env,
        mars_prop_entity,
        network,
        mars_ids,
        MajorityQuorum(mars_ids),
        timeout=1.0,
    )

    global_prop_entity = registry.create(name="global-proposer")
    network.assign_entity(global_prop_entity.id, "na-west")
    wall = CrumblingWallQuorum([
        mars_ids,
        [moon_entity.id],
        [leo_entity.id],
        earth_ids,
    ])
    global_prop = Proposer(
        env,
        global_prop_entity,
        network,
        all_ids,
        wall,
        timeout=cfg.global_timeout_s,
        max_rounds=cfg.global_max_rounds,
    )

    return network, earth_prop, mars_prop, global_prop


def run_conjunction_experiment(
    with_repeater: bool,
    cfg: ExperimentConfig,
    verbose: bool = True,
) -> ExperimentResult:
    env = simpy.Environment()
    network, earth_prop, mars_prop, global_prop = _wire_system(env, cfg)

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
            # Refinement model: link remains available but degraded.
            for src in earth_path_locs:
                for dst in mars_locs:
                    network.update_link(src, dst, latency=240.0, jitter=12.0)
        else:
            # Baseline model: hard communication blackout.
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
    env.process(global_reconcile())
    env.process(conjunction_controller())
    env.run(until=cfg.sim_end_s)
    tier_metrics = _extract_tier_metrics(network, global_prop.entity.id)

    result = ExperimentResult(
        name="with_repeater" if with_repeater else "blackout_only",
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
        global_phase1_responses_earth=tier_metrics["global_phase1_responses_earth"],
        global_phase1_responses_leo=tier_metrics["global_phase1_responses_leo"],
        global_phase1_responses_moon=tier_metrics["global_phase1_responses_moon"],
        global_phase1_responses_mars=tier_metrics["global_phase1_responses_mars"],
        global_phase2_responses_earth=tier_metrics["global_phase2_responses_earth"],
        global_phase2_responses_leo=tier_metrics["global_phase2_responses_leo"],
        global_phase2_responses_moon=tier_metrics["global_phase2_responses_moon"],
        global_phase2_responses_mars=tier_metrics["global_phase2_responses_mars"],
        network_src_sent_earth=tier_metrics["network_src_sent_earth"],
        network_src_sent_leo=tier_metrics["network_src_sent_leo"],
        network_src_sent_moon=tier_metrics["network_src_sent_moon"],
        network_src_sent_mars=tier_metrics["network_src_sent_mars"],
        network_src_avg_oneway_s_earth=tier_metrics["network_src_avg_oneway_s_earth"],
        network_src_avg_oneway_s_leo=tier_metrics["network_src_avg_oneway_s_leo"],
        network_src_avg_oneway_s_moon=tier_metrics["network_src_avg_oneway_s_moon"],
        network_src_avg_oneway_s_mars=tier_metrics["network_src_avg_oneway_s_mars"],
    )

    if verbose:
        label = "WITH LAGRANGE REPEATER" if with_repeater else "BASELINE: HARD BLACKOUT"
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
            print(
                "    First success after blackout end: "
                f"{result.first_success_after_blackout_s:.1f}s"
            )
        else:
            print("    First success after blackout end: none observed")
        if result.earth_local_avg_latency_s is not None:
            print(f"    Earth local avg/p95: {result.earth_local_avg_latency_s*1000:.1f}ms / "
                  f"{result.earth_local_p95_latency_s*1000:.1f}ms")
        if result.mars_local_avg_latency_s is not None:
            print(f"    Mars local avg/p95:  {result.mars_local_avg_latency_s*1000:.1f}ms / "
                  f"{result.mars_local_p95_latency_s*1000:.1f}ms")
        print()

    return result


def compare_blackout_vs_repeater(cfg: ExperimentConfig, verbose: bool = True):
    baseline = run_conjunction_experiment(with_repeater=False, cfg=cfg, verbose=verbose)
    repeater = run_conjunction_experiment(with_repeater=True, cfg=cfg, verbose=verbose)

    if verbose:
        print("=" * 74)
        print("STEP 9 SUMMARY: CONJUNCTION MODEL")
        print("=" * 74)
        print()
        print(f"  {'Metric':<36} {'Blackout only':>16} {'With repeater':>16}")
        print(f"  {'-'*36} {'-'*16} {'-'*16}")
        print(
            f"  {'Earth local success':<36} "
            f"{baseline.earth_success}/{baseline.earth_total:>9} "
            f"{repeater.earth_success}/{repeater.earth_total:>9}"
        )
        print(
            f"  {'Mars local success':<36} "
            f"{baseline.mars_success}/{baseline.mars_total:>9} "
            f"{repeater.mars_success}/{repeater.mars_total:>9}"
        )
        print(
            f"  {'Global success during blackout':<36} "
            f"{baseline.during_blackout.success}/{baseline.during_blackout.total:>9} "
            f"{repeater.during_blackout.success}/{repeater.during_blackout.total:>9}"
        )

        def _fmt(x):
            return f"{x:.1f}s" if x is not None else "n/a"

        print(
            f"  {'Global avg latency':<36} "
            f"{_fmt(baseline.avg_global_latency_s):>16} "
            f"{_fmt(repeater.avg_global_latency_s):>16}"
        )
        print(
            f"  {'First success after blackout end':<36} "
            f"{_fmt(baseline.first_success_after_blackout_s):>16} "
            f"{_fmt(repeater.first_success_after_blackout_s):>16}"
        )
        print()
        print("  Interpretation:")
        print("    Local autonomy remains intact in both regimes.")
        print("    Repeater model converts hard outage into degraded continuity.")
        print("    This gives a concrete engineering path from prototype to refinement.")
        print()

    return baseline, repeater


def write_summary_csv(
    output_path: str | Path,
    cfg: ExperimentConfig,
    baseline: ExperimentResult,
    repeater: ExperimentResult,
):
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "scenario",
                "mars_base_latency_s",
                "blackout_start_s",
                "blackout_duration_s",
                "sim_end_s",
                "reconcile_interval_s",
                "global_timeout_s",
                "global_max_rounds",
                "earth_success",
                "earth_total",
                "mars_success",
                "mars_total",
                "global_pre_success",
                "global_pre_total",
                "global_during_success",
                "global_during_total",
                "global_post_success",
                "global_post_total",
                "first_success_after_blackout_s",
                "avg_global_latency_s",
                "earth_local_avg_latency_s",
                "earth_local_p95_latency_s",
                "mars_local_avg_latency_s",
                "mars_local_p95_latency_s",
                "global_phase1_responses_earth",
                "global_phase1_responses_leo",
                "global_phase1_responses_moon",
                "global_phase1_responses_mars",
                "global_phase2_responses_earth",
                "global_phase2_responses_leo",
                "global_phase2_responses_moon",
                "global_phase2_responses_mars",
                "network_src_sent_earth",
                "network_src_sent_leo",
                "network_src_sent_moon",
                "network_src_sent_mars",
                "network_src_avg_oneway_s_earth",
                "network_src_avg_oneway_s_leo",
                "network_src_avg_oneway_s_moon",
                "network_src_avg_oneway_s_mars",
            ]
        )
        for r in (baseline, repeater):
            writer.writerow(
                [
                    r.name,
                    cfg.mars_base_latency_s,
                    cfg.blackout_start_s,
                    cfg.blackout_duration_s,
                    cfg.sim_end_s,
                    cfg.reconcile_interval_s,
                    cfg.global_timeout_s,
                    cfg.global_max_rounds,
                    r.earth_success,
                    r.earth_total,
                    r.mars_success,
                    r.mars_total,
                    r.pre_blackout.success,
                    r.pre_blackout.total,
                    r.during_blackout.success,
                    r.during_blackout.total,
                    r.post_blackout.success,
                    r.post_blackout.total,
                    (
                        f"{r.first_success_after_blackout_s:.6f}"
                        if r.first_success_after_blackout_s is not None
                        else ""
                    ),
                    f"{r.avg_global_latency_s:.6f}" if r.avg_global_latency_s is not None else "",
                    f"{r.earth_local_avg_latency_s:.6f}" if r.earth_local_avg_latency_s is not None else "",
                    f"{r.earth_local_p95_latency_s:.6f}" if r.earth_local_p95_latency_s is not None else "",
                    f"{r.mars_local_avg_latency_s:.6f}" if r.mars_local_avg_latency_s is not None else "",
                    f"{r.mars_local_p95_latency_s:.6f}" if r.mars_local_p95_latency_s is not None else "",
                    r.global_phase1_responses_earth,
                    r.global_phase1_responses_leo,
                    r.global_phase1_responses_moon,
                    r.global_phase1_responses_mars,
                    r.global_phase2_responses_earth,
                    r.global_phase2_responses_leo,
                    r.global_phase2_responses_moon,
                    r.global_phase2_responses_mars,
                    r.network_src_sent_earth,
                    r.network_src_sent_leo,
                    r.network_src_sent_moon,
                    r.network_src_sent_mars,
                    f"{r.network_src_avg_oneway_s_earth:.6f}" if r.network_src_avg_oneway_s_earth is not None else "",
                    f"{r.network_src_avg_oneway_s_leo:.6f}" if r.network_src_avg_oneway_s_leo is not None else "",
                    f"{r.network_src_avg_oneway_s_moon:.6f}" if r.network_src_avg_oneway_s_moon is not None else "",
                    f"{r.network_src_avg_oneway_s_mars:.6f}" if r.network_src_avg_oneway_s_mars is not None else "",
                ]
            )


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mars-latency-s", type=float, default=186.0)
    parser.add_argument("--blackout-start-s", type=float, default=600.0)
    parser.add_argument("--blackout-duration-s", type=float, default=900.0)
    parser.add_argument("--sim-end-s", type=float, default=3000.0)
    parser.add_argument("--reconcile-interval-s", type=float, default=120.0)
    parser.add_argument("--global-timeout-s", type=float, default=500.0)
    parser.add_argument("--global-max-rounds", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--csv", type=str, default="")
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def main():
    args = _parse_args()
    cfg = ExperimentConfig(
        mars_base_latency_s=args.mars_latency_s,
        blackout_start_s=args.blackout_start_s,
        blackout_duration_s=args.blackout_duration_s,
        sim_end_s=args.sim_end_s,
        reconcile_interval_s=args.reconcile_interval_s,
        global_timeout_s=args.global_timeout_s,
        global_max_rounds=args.global_max_rounds,
        seed=args.seed,
    )
    baseline, repeater = compare_blackout_vs_repeater(cfg, verbose=not args.quiet)
    if args.csv:
        write_summary_csv(args.csv, cfg, baseline, repeater)
        if not args.quiet:
            print(f"Wrote CSV summary: {args.csv}")


if __name__ == "__main__":
    main()
