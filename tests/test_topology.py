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
