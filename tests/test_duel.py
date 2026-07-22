"""Tests for the dueling-proposer experiment harness (duel.py) and its
supporting upgrades (per-link RNG, proposer instrumentation).

Design contract: docs/superpowers/notes/2026-07-22-dueling-proposer-premortem.md
"""
import random

import pytest
import simpy

from capability import Hazard, classify
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


def test_priority_proposer_ballot_uses_rank():
    from duel import PriorityProposer
    env = simpy.Environment()
    net = DatacenterNetwork(env, NetworkConfig(seed=1))
    net.add_location("dc")
    reg = EntityRegistry()
    e = reg.create(name="p")
    net.assign_entity(e.id, "dc")
    p = PriorityProposer(env, e, net, [e.id], MajorityQuorum([e.id]),
                         ballot_rank=501)
    assert p._next_proposal_number() == 1 * 1000 + 501
    assert p._next_proposal_number() == 2 * 1000 + 501


def test_priority_proposer_rejects_bad_rank():
    from duel import PriorityProposer
    env = simpy.Environment()
    net = DatacenterNetwork(env, NetworkConfig(seed=1))
    reg = EntityRegistry()
    e = reg.create(name="p")
    with pytest.raises(AssertionError):
        PriorityProposer(env, e, net, [e.id], MajorityQuorum([e.id]),
                         ballot_rank=1000)


def test_scale_jitter_zeroes_all_links():
    from duel import scale_jitter
    env = simpy.Environment()
    net = five_dc_topology(env, seed=3)
    scale_jitter(net, 0.0)
    assert all(l.jitter == 0.0 for l in net._links.values())
    assert net._default_local_link.jitter == 0.0
    assert net.config.delay_jitter == 0.0


def test_wire_duel_gates_and_shared_wall():
    from duel import wire_duel
    env = simpy.Environment()
    sys_ = wire_duel(env, k=5, polarity="leo_high", earth_max_rounds=1,
                     leo_max_rounds=8, jitter_scale=0.0, seed=0)
    # A1: literally the same quorum object.
    assert sys_.earth_prop.quorum is sys_.leo_prop.quorum
    # A2: explicit distinct ranks; leo_high means LEO wins equal-counter ties.
    assert sys_.leo_prop.ballot_rank > sys_.earth_prop.ballot_rank
    # A7: LEO really is in the hazard state, derived from actual links.
    leo_reach = {a for a in sys_.all_ids
                 if sys_.network.get_link(sys_.leo_prop.entity.id, a) is not None}
    rep = classify(sys_.wall, 2, leo_reach)
    assert rep.r1 and not rep.r2
    assert Hazard.DISRUPTIVE_ELECTION in rep.hazards


def test_wire_duel_k3_is_failover_regime():
    from duel import wire_duel
    env = simpy.Environment()
    sys_ = wire_duel(env, k=3, polarity="earth_high", earth_max_rounds=1,
                     leo_max_rounds=8, jitter_scale=0.0, seed=0)
    leo_reach = {a for a in sys_.all_ids
                 if sys_.network.get_link(sys_.leo_prop.entity.id, a) is not None}
    rep = classify(sys_.wall, 2, leo_reach)
    assert rep.r1 and rep.r2 and not rep.hazards
    assert sys_.earth_prop.ballot_rank > sys_.leo_prop.ballot_rank


def test_far_offset_earth_commits_clean():
    from duel import run_duel_trial
    t = run_duel_trial(offset=30.0, polarity="leo_high", k=5)
    assert t.outcome == "earth_commit"
    assert t.decided_by == "earth"
    assert t.earth_result.phase2_nacks == 0
    assert not t.rounds_overlapped
    # LEO completed elections but could never commit (R2=0).
    assert t.leo_result.phase1_quorums >= 1
    assert t.leo_result.phase2_failures == t.leo_result.phase1_quorums
    assert not t.leo_result.success


def test_overlap_offset_leo_high_single_shot_preempts_earth():
    # The lemma-predicted regime: Earth starts 0.5s into LEO's ~9s span
    # of repeated election attempts; LEO's higher ballot has already
    # poisoned its 3 reachable Earth nodes, so single-shot Earth cannot
    # commit (k=5 needs all five). The offset sits deep inside the
    # collision band, robust to link-latency tweaks — the map sweep
    # charts the razor edges; unit tests must not sit on them. (A +0.05
    # offset misses the window by ~7.5ms given concrete link latencies.)
    from duel import run_duel_trial
    t = run_duel_trial(offset=-0.5, polarity="leo_high", k=5,
                       earth_max_rounds=1)
    assert t.rounds_overlapped
    assert t.outcome in ("no_decision", "leo_blocked")
    assert not t.earth_result.success
    assert (t.earth_result.phase1_nacks + t.earth_result.phase2_nacks
            + t.earth_late_nacks) >= 1


def test_baseline_arm_has_no_leo_interference():
    from duel import run_duel_trial
    t = run_duel_trial(offset=0.05, polarity="leo_high", k=5,
                       leo_enabled=False)
    assert t.outcome == "earth_commit"
    assert t.leo_result is None


def test_zero_offset_rejected():
    from duel import run_duel_trial
    import pytest as _pytest
    with _pytest.raises(AssertionError):
        run_duel_trial(offset=0.0, polarity="leo_high", k=5)


def test_k3_someone_commits_and_safety_holds():
    from duel import run_duel_trial
    t = run_duel_trial(offset=-0.5, polarity="leo_high", k=3,
                       earth_max_rounds=5)
    assert t.outcome in ("earth_commit", "leo_commit")
    assert t.decided_value is not None


def test_censored_outcome_on_short_horizon():
    from duel import run_duel_trial
    t = run_duel_trial(offset=-0.5, polarity="leo_high", k=5, tail=0.05)
    assert t.outcome == "censored"


def test_classify_outcome_branches():
    # Pure-function coverage of the full A8 taxonomy (as amended: mutual
    # livelock, total-not-consecutive counts) without razor-edge dynamics.
    from duel import classify_outcome
    from paxos import ConsensusResult
    done_ok = ConsensusResult(success=True, slot=1)
    done_fail = ConsensusResult(success=False, slot=1)
    assert classify_outcome(None, None, done_ok, True, 0, 0, None) == "censored"
    assert classify_outcome(None, done_fail, None, True, 0, 0, None) == "censored"
    assert classify_outcome((5502, "leo-1"), done_fail, done_ok, True, 1, 0,
                            "leo") == "leo_commit"
    assert classify_outcome(None, done_fail, done_fail, True, 3, 1,
                            None) == "livelock"
    assert classify_outcome(None, done_fail, done_fail, True, 3, 0,
                            None) == "leo_blocked"
    assert classify_outcome(None, done_fail, None, False, 0, 0,
                            None) == "no_decision"


def test_offset_grid_shape():
    import sys as _sys
    from pathlib import Path as _P
    _sys.path.insert(0, str(_P(__file__).resolve().parents[1] / "experiments"))
    from duel_sweep import offset_grid
    grid = offset_grid()
    assert 0.0 not in grid
    assert min(grid) == -12.0
    assert max(grid) <= 118.0
    fine = [o for o in grid if -12.0 <= o <= 6.0]
    diffs = {round(b - a, 3) for a, b in zip(fine, fine[1:])}
    assert diffs == {0.05} or diffs == {0.05, 0.1}  # 0.1 gap where 0.0 was removed
    assert all(o > 6.0 for o in grid if o not in fine)


def test_wilson_ci_known_values():
    from duel_sweep import wilson_ci
    lo, hi = wilson_ci(0, 0)
    assert lo is None and hi is None
    lo, hi = wilson_ci(50, 50)
    assert hi == 1.0 and lo > 0.9   # no fake ±0.0 certainty
    lo, hi = wilson_ci(0, 50)
    assert lo == 0.0 and hi < 0.1
    lo, hi = wilson_ci(25, 50)
    assert 0.36 < lo < 0.5 < hi < 0.64


def test_trial_row_carries_full_config():
    from duel import run_duel_trial
    from duel_sweep import trial_row
    t = run_duel_trial(offset=30.0, polarity="leo_high", k=5)
    row = trial_row(t)
    for col in ("offset", "polarity", "k", "earth_max_rounds",
                "leo_max_rounds", "jitter_scale", "seed", "leo_enabled",
                "earth_start", "tail",
                "livelock_min_preempted_rounds", "outcome", "decided_by",
                "rounds_overlapped", "earth_success", "earth_rounds",
                "earth_p1_nacks", "earth_p2_nacks", "earth_late_nacks",
                "leo_p1_quorums", "leo_p2_failures",
                "earth_commit_latency_s"):
        assert col in row, col
