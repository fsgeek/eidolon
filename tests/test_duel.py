"""Tests for the dueling-proposer experiment harness (duel.py) and its
supporting upgrades (per-link RNG, proposer instrumentation).

Design contract: docs/superpowers/notes/2026-07-22-dueling-proposer-premortem.md
"""
import random

import simpy

from datacenter import DatacenterNetwork, five_dc_topology
from network import NetworkConfig


def _mk_net(seed=7):
    env = simpy.Environment()
    net = DatacenterNetwork(env, NetworkConfig(seed=seed))
    return net


def test_legacy_rng_is_module_global():
    net = _mk_net()
    assert net._rng_for(1, 2) is random


def test_per_link_rng_isolated_from_other_traffic():
    # Same seed, two networks. Consume DIFFERENT amounts of module-global
    # randomness in each; the per-link stream for (1, 2) must be identical.
    net_a = _mk_net(seed=7)
    net_a.enable_per_link_rng()
    net_b = _mk_net(seed=7)
    net_b.enable_per_link_rng()

    random.random()  # perturb global stream (only before net_b's draws)
    _ = [net_b._rng_for(3, 4).random() for _ in range(5)]  # other-link traffic

    draws_a = [net_a._rng_for(1, 2).random() for _ in range(8)]
    draws_b = [net_b._rng_for(1, 2).random() for _ in range(8)]
    assert draws_a == draws_b


def test_per_link_rng_directional_and_distinct():
    net = _mk_net(seed=7)
    net.enable_per_link_rng()
    a = [net._rng_for(1, 2).random() for _ in range(4)]
    net2 = _mk_net(seed=7)
    net2.enable_per_link_rng()
    b = [net2._rng_for(2, 1).random() for _ in range(4)]
    assert a != b  # ordered pairs get distinct streams
