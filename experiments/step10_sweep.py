"""Step 10 sweep: coordinated relaxation under Earth-node crashes.

Sweeps crash count x quorum configuration across multiple seeds.
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path

import simpy

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from datacenter import five_dc_topology
from entity import EntityRegistry
from paxos import Acceptor, FlexibleQuorum, MajorityQuorum, Proposer
from quorums import CrumblingWallQuorum
from demo_step_10 import build_topology, ReconciliationStats
from demo_step_9 import mars_blackout_pairs
from time_budget import classify_attempt, scaled_window


def run_single(seed, global_q2_thresh, earth_q1, earth_q2, crash_count,
               mars_latency=186.0, blackout_dur=900.0, with_repeater=True):
    # Temporal budget: validate (and, if necessary, scale) the window
    # before anything else is built, so the proposer's timeout and the
    # blackout schedule below use the effective, guaranteed-valid values.
    window, temporally_scaled = scaled_window(
        d_max=mars_latency, p_max=0.0,
        blackout_duration=blackout_dur,
        phase_timeout=500.0,
        pre_window=600.0,
        post_window=max(4000.0 - 600.0 - blackout_dur, 0.0),
        reconciliation_cadence=120.0,
    )

    env = simpy.Environment()
    network = build_topology(env, mars_base_latency_s=mars_latency, seed=seed)
    registry = EntityRegistry()

    earth_locs = ["na-west", "europe", "asia", "sa-east", "africa"]
    earth_entities = []
    for loc in earth_locs:
        entity = registry.create(name=f"earth-{loc}")
        network.assign_entity(entity.id, loc)
        earth_entities.append(entity)

    moon = registry.create(name="moon")
    network.assign_entity(moon.id, "moon")
    leo = registry.create(name="leo")
    network.assign_entity(leo.id, "leo-sat")

    mars_entities = []
    for i in range(3):
        entity = registry.create(name=f"mars-{i}")
        network.assign_entity(entity.id, f"mars-{i}")
        mars_entities.append(entity)

    for entity in earth_entities + [moon, leo] + mars_entities:
        pt = 0.0005 if "earth" in entity.name or "leo" in entity.name else 0.001
        Acceptor(env, entity, network, process_time=pt)

    earth_ids = [e.id for e in earth_entities]
    mars_ids = [e.id for e in mars_entities]
    all_ids = earth_ids + [leo.id, moon.id] + mars_ids

    ep = registry.create(name="earth-proposer")
    network.assign_entity(ep.id, "na-west")
    earth_prop = Proposer(env, ep, network, earth_ids,
        FlexibleQuorum(earth_ids, phase1_size=earth_q1, phase2_size=earth_q2),
        timeout=1.0)

    mp = registry.create(name="mars-proposer")
    network.assign_entity(mp.id, "mars-0")
    mars_prop = Proposer(env, mp, network, mars_ids,
        MajorityQuorum(mars_ids), timeout=1.0)

    gp = registry.create(name="global-proposer")
    network.assign_entity(gp.id, "na-west")
    wall = CrumblingWallQuorum(
        [mars_ids, [moon.id], [leo.id], earth_ids],
        phase2_threshold=global_q2_thresh)
    global_prop = Proposer(env, gp, network, all_ids, wall,
        timeout=window.phase_timeout, max_rounds=1, initiator_tier=3)

    earth_ok, earth_n = 0, 0
    mars_ok, mars_n = 0, 0
    g_pre = ReconciliationStats()
    g_during = ReconciliationStats()
    g_post = ReconciliationStats()
    g_transition = ReconciliationStats()
    first_recovery = None
    global_latencies = []
    earth_latencies = []

    blackout_start = window.pre_window
    blackout_end = blackout_start + blackout_dur
    sim_end = window.horizon

    def earth_local():
        nonlocal earth_ok, earth_n
        slot = 0
        while env.now < sim_end:
            r = yield earth_prop.propose(slot=slot, value=f"e{slot}")
            earth_n += 1
            if r.success:
                earth_ok += 1
                earth_latencies.append(r.total_time)
            slot += 1
            yield env.timeout(2.0)

    def mars_local():
        nonlocal mars_ok, mars_n
        slot = 10000
        while env.now < sim_end:
            r = yield mars_prop.propose(slot=slot, value=f"m{slot}")
            mars_n += 1
            if r.success: mars_ok += 1
            slot += 1
            yield env.timeout(2.0)

    def global_reconcile():
        nonlocal first_recovery
        slot = 20000
        while env.now < sim_end:
            started = env.now
            r = yield global_prop.propose(slot=slot, value=f"g{slot}")
            slot += 1
            ended = env.now
            bucket = {"pre": g_pre, "during": g_during, "post": g_post,
                      "transition": g_transition}[
                classify_attempt(started, ended, blackout_start, blackout_end)]
            bucket.total += 1
            if r.success:
                bucket.success += 1
                global_latencies.append(r.total_time)
                if started >= blackout_end and first_recovery is None:
                    first_recovery = env.now - blackout_end
            yield env.timeout(120.0)

    def controller():
        pairs = mars_blackout_pairs(network)
        yield env.timeout(blackout_start)

        if with_repeater:
            for s, d in pairs:
                network.update_link(s, d, latency=240.0, jitter=12.0)
        else:
            for s, d in pairs:
                network.partition_locations(s, d)

        crash_targets = ["africa", "sa-east", "asia", "europe", "na-west"][:crash_count]
        if crash_targets:
            all_locs = list(network._locations.keys())
            for cl in crash_targets:
                for ol in all_locs:
                    if ol != cl:
                        network.partition_locations(cl, ol)

        yield env.timeout(blackout_dur)

        if with_repeater:
            for s, d in pairs:
                base = mars_latency + (1.28 if s == "moon" else 0)
                network.update_link(s, d, latency=base, jitter=5.0)
        else:
            network.heal_all()

        if crash_targets:
            all_locs = list(network._locations.keys())
            for cl in crash_targets:
                for ol in all_locs:
                    if ol != cl:
                        network.partition_locations(cl, ol)

    env.process(earth_local())
    env.process(mars_local())
    env.process(global_reconcile())
    env.process(controller())
    env.run(until=sim_end)

    e_rate = earth_ok / max(1, earth_n)
    g_d_rate = g_during.success / max(1, g_during.total) if g_during.total else None
    g_p_rate = g_post.success / max(1, g_post.total) if g_post.total else None
    g_pre_rate = g_pre.success / max(1, g_pre.total) if g_pre.total else None
    avg_glat = sum(global_latencies) / len(global_latencies) if global_latencies else None
    avg_elat = sum(earth_latencies) / len(earth_latencies) if earth_latencies else None

    return {
        "earth_local_rate": e_rate,
        "global_pre_rate": g_pre_rate,
        "global_during_rate": g_d_rate,
        "global_post_rate": g_p_rate,
        "global_transition_success": g_transition.success,
        "global_transition_total": g_transition.total,
        "phase_timeout_s": window.phase_timeout,
        "pre_window_s": window.pre_window,
        "post_window_s": window.post_window,
        "temporally_scaled": int(temporally_scaled),
        "recovery_lag_s": first_recovery,
        "avg_global_latency_s": avg_glat,
        "avg_earth_latency_s": avg_elat,
    }


def ci95(values):
    n = len(values)
    if n < 2:
        return 0.0
    m = sum(values) / n
    var = sum((x - m) ** 2 for x in values) / (n - 1)
    return 1.96 * math.sqrt(var / n)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seeds", type=str, default="40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89")
    parser.add_argument("--output", type=str, default="results/step10/step10_sweep.csv")
    parser.add_argument("--aggregate-output", type=str, default="results/step10/step10_sweep_ci.csv")
    args = parser.parse_args()

    seeds = [int(s) for s in args.seeds.split(",")]

    # Scenarios: (label, global_q2_thresh, earth_q1, earth_q2, crash_count)
    scenarios = [
        ("strict_std_0crash",    None, 4, 2, 0),
        ("strict_std_1crash",    None, 4, 2, 1),
        ("relax4_std_0crash",       4, 4, 2, 0),
        ("relax4_std_1crash",       4, 4, 2, 1),
        ("relax3_std_2crash",       3, 4, 2, 2),
        ("relax3_maj_2crash",       3, 3, 3, 2),
    ]

    raw_path = Path(args.output)
    raw_path.parent.mkdir(parents=True, exist_ok=True)

    raw_rows = []
    for label, gq2, eq1, eq2, crashes in scenarios:
        print(f"  {label}: ", end="", flush=True)
        for seed in seeds:
            r = run_single(seed, gq2, eq1, eq2, crashes)
            raw_rows.append({
                "scenario": label,
                "global_q2_thresh": gq2 if gq2 is not None else "strict",
                "earth_q1": eq1, "earth_q2": eq2,
                "crash_count": crashes, "seed": seed,
                **r,
            })
            print(".", end="", flush=True)
        print()

    # Write raw CSV
    with raw_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=raw_rows[0].keys())
        writer.writeheader()
        writer.writerows(raw_rows)
    print(f"Wrote raw: {raw_path}")

    # Aggregate
    agg_path = Path(args.aggregate_output)
    agg_rows = []
    for label, gq2, eq1, eq2, crashes in scenarios:
        rows = [r for r in raw_rows if r["scenario"] == label]
        metrics = {}
        for key in ["earth_local_rate", "global_during_rate", "global_post_rate",
                     "recovery_lag_s", "avg_global_latency_s", "avg_earth_latency_s"]:
            vals = [r[key] for r in rows if r[key] is not None]
            if vals:
                m = sum(vals) / len(vals)
                c = ci95(vals)
                metrics[f"{key}_mean"] = m
                metrics[f"{key}_ci95"] = c
            else:
                metrics[f"{key}_mean"] = None
                metrics[f"{key}_ci95"] = None
        agg_rows.append({
            "scenario": label,
            "global_q2_thresh": gq2 if gq2 is not None else "strict",
            "earth_q1": eq1, "earth_q2": eq2,
            "crash_count": crashes,
            "n_seeds": len(seeds),
            **metrics,
        })

    with agg_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=agg_rows[0].keys())
        writer.writeheader()
        writer.writerows(agg_rows)
    print(f"Wrote aggregate: {agg_path}")


if __name__ == "__main__":
    main()
