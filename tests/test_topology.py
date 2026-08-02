"""Topology invariants the experiments and the paper's claims rely on."""

import simpy

from demo_step_9 import build_topology


def test_mars_has_effective_route_to_every_phase1_tier():
    """Mars-initiated Phase 1 needs one respondent from every tier below:
    Moon, LEO, and >= 1 Earth. Each must be directly linked (the network
    model has no multi-hop routing)."""
    env = simpy.Environment()
    network = build_topology(env, mars_base_latency_s=186.0, seed=42)
    for i in range(3):
        mars = f"mars-{i}"
        assert network.has_link(mars, "moon"), mars
        assert network.has_link(mars, "leo-sat"), mars
        assert network.has_link(mars, "na-west") or network.has_link(mars, "europe"), mars


def test_demo_step_10_uses_the_same_builder():
    import demo_step_10
    from demo_step_9 import build_topology as canonical
    assert demo_step_10.build_topology is canonical


def test_blackout_pairs_cover_every_non_mars_location():
    from demo_step_9 import mars_blackout_pairs
    env = simpy.Environment()
    network = build_topology(env, mars_base_latency_s=186.0, seed=42)
    pairs = mars_blackout_pairs(network)
    non_mars_side = {src for src, _ in pairs}
    # Every route into Mars is severed: Earth, Moon, LEO, and the relay.
    assert {"leo-sat", "moon", "na-west", "europe", "lagrange-relay"} <= non_mars_side
    assert all(dst.startswith("mars-") for _, dst in pairs)
    assert not any(src.startswith("mars-") for src, _ in pairs)


def test_blackout_pairs_include_full_coverage_links():
    from demo_step_9 import mars_blackout_pairs
    from experiments.tier_liveness_sweep import _add_full_coverage_links
    env = simpy.Environment()
    network = build_topology(env, mars_base_latency_s=186.0, seed=42)
    _add_full_coverage_links(network, mars_base_latency_s=186.0)
    non_mars_side = {src for src, _ in mars_blackout_pairs(network)}
    assert {"asia", "sa-east", "africa"} <= non_mars_side
