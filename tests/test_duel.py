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


from entity import EntityRegistry
from paxos import Acceptor, MajorityQuorum, Proposer


def _tiny_consensus(max_rounds=3):
    env = simpy.Environment()
    net = DatacenterNetwork(env, NetworkConfig(seed=1))
    net.add_location("dc")
    reg = EntityRegistry()
    accs = []
    for i in range(3):
        e = reg.create(name=f"a{i}")
        net.assign_entity(e.id, "dc")
        accs.append(Acceptor(env, e, net))
    pe = reg.create(name="prop")
    net.assign_entity(pe.id, "dc")
    prop = Proposer(env, pe, net, [a.entity.id for a in accs],
                    MajorityQuorum([a.entity.id for a in accs]),
                    timeout=0.5, max_rounds=max_rounds)
    holder = {}

    def go():
        holder["r"] = yield prop.propose(slot=0, value="v")
    env.process(go())
    env.run(until=10.0)
    return holder["r"], prop


def test_consensus_result_round_log_and_counters():
    r, prop = _tiny_consensus()
    assert r.success
    assert len(r.round_log) == r.rounds
    first = r.round_log[0]
    for key in ("round", "proposal_number", "p1_start", "p1_end",
                "p1_quorum", "p1_nacks", "p2_start", "p2_end",
                "p2_quorum", "p2_nacks"):
        assert key in first
    assert first["p1_quorum"] is True
    assert first["p2_quorum"] is True
    assert r.phase1_quorums == 1
    assert r.phase2_failures == 0
    assert r.phase1_nacks == 0 and r.phase2_nacks == 0
    assert "late_responses" in prop.stats and "late_nacks" in prop.stats
