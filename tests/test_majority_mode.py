"""Majority global-quorum mode: the competitive baseline (review ADV-A-001).

`--global-quorum majority` builds the global proposer over an
AnchoredMajorityQuorum: Phase 1 accepts any majority (6-of-10) of the
full node set with no tier structure, Phase 2 requires all five Earth
nodes -- the same strict fast-tier Phase 2 the wall uses. The two
constructions therefore differ only in Phase 1 shape, which is what
makes the comparison apples-to-apples.

Safety: every 6-subset of the 10 nodes contains at least one Earth node
(only 5 non-Earth nodes exist), so every Q1 intersects the all-Earth Q2.
"""

from itertools import combinations

import pytest
import simpy

from quorums import AnchoredMajorityQuorum

from demo_step_9 import ExperimentConfig, _wire_system


def test_majority_mode_constructs_anchored_majority_quorum():
    env = simpy.Environment()
    cfg = ExperimentConfig(global_quorum="majority")
    network, earth_prop, mars_prop, global_prop = _wire_system(env, cfg)

    assert isinstance(global_prop.quorum, AnchoredMajorityQuorum)
    # Tier structure is irrelevant to majority Phase 1.
    assert global_prop.initiator_tier is None
    assert global_prop.quorum.n == 10
    assert global_prop.quorum.phase1_quorum_size() == 6
    assert len(global_prop.quorum.anchor) == 5


def test_majority_phase1_is_shape_free():
    q = AnchoredMajorityQuorum(list(range(10)), anchor=[0, 1, 2, 3, 4])
    # Any 6 nodes form a Phase 1 quorum, regardless of composition.
    assert q.is_phase1_quorum({0, 1, 2, 3, 4, 5}) is True
    assert q.is_phase1_quorum({4, 5, 6, 7, 8, 9}) is True
    # 5 respondents never suffice.
    assert q.is_phase1_quorum({5, 6, 7, 8, 9}) is False
    # initiator_tier is accepted and ignored.
    assert q.is_phase1_quorum({0, 1, 2, 3, 4, 5}, initiator_tier=3) is True


def test_majority_phase2_requires_full_anchor():
    q = AnchoredMajorityQuorum(list(range(10)), anchor=[0, 1, 2, 3, 4])
    assert q.is_phase2_quorum({0, 1, 2, 3, 4}) is True
    assert q.is_phase2_quorum({0, 1, 2, 3, 4, 7}) is True
    # Missing one anchor node fails even with extra respondents.
    assert q.is_phase2_quorum({0, 1, 2, 3, 5, 6, 7, 8, 9}) is False


def test_every_majority_intersects_the_anchor_exhaustively():
    q = AnchoredMajorityQuorum(list(range(10)), anchor=[0, 1, 2, 3, 4])
    anchor = set(q.anchor)
    for q1 in combinations(range(10), 6):
        assert set(q1) & anchor, f"Q1 {q1} misses the anchor Q2"


def test_insufficient_anchor_is_rejected_at_construction():
    # With a 4-node anchor in n=10, the 6 non-anchor nodes form a
    # majority that misses Phase 2 entirely -- must be rejected.
    with pytest.raises(ValueError):
        AnchoredMajorityQuorum(list(range(10)), anchor=[0, 1, 2, 3])


def test_anchor_must_be_subset_of_nodes():
    with pytest.raises(ValueError):
        AnchoredMajorityQuorum(list(range(10)), anchor=[0, 1, 2, 3, 99])
