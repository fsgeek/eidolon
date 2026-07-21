"""Spec scenario basis as executable acceptance tests.

These encode the paper's Evaluation claims: the relaxation transition
10 -> 11 and coverage of the complete R1 x R2 matrix.
"""

from capability import Hazard, classify
from quorums import CrumblingWallQuorum

MARS = [100, 101, 102]
MOON = [200]
LEO = [300]
EARTH = [1, 2, 3, 4, 5]
ALL_NODES = set(MARS + MOON + LEO + EARTH)

MARS_TIER, MOON_TIER, LEO_TIER, EARTH_TIER = 0, 1, 2, 3

# LEO reaches its own satellite plus three of five Earth ground
# stations (the sparse topology of experiments/tier_liveness_sweep.py).
SPARSE_LEO_REACH = {300, 1, 2, 3}


def make_wall(k=None):
    return CrumblingWallQuorum([MARS, MOON, LEO, EARTH], phase2_threshold=k)


def test_sparse_leo_relaxation_sequence():
    """k=5,4,3 -> (1,0), (1,0), (1,1): relaxation converts futile
    disruption into ordinary failover capability."""
    expected = {5: (True, False), 4: (True, False), 3: (True, True)}
    for k, state in expected.items():
        report = classify(make_wall(k), LEO_TIER, SPARSE_LEO_REACH)
        assert (report.r1, report.r2) == state, f"k={k}"


def test_relaxation_clears_the_disruption_hazard():
    assert classify(make_wall(5), LEO_TIER, SPARSE_LEO_REACH).hazards \
        == (Hazard.DISRUPTIVE_ELECTION,)
    assert classify(make_wall(3), LEO_TIER, SPARSE_LEO_REACH).hazards == ()


def test_scenario_basis_realizes_all_four_matrix_states():
    wall = make_wall()
    reports = [
        classify(wall, EARTH_TIER, ALL_NODES),               # full wall
        classify(wall, LEO_TIER, SPARSE_LEO_REACH),          # sparse LEO
        classify(wall, MOON_TIER, set(MOON) | set(EARTH)),   # broken LEO row
        classify(wall, MARS_TIER, set(MARS)),                # hard cut
    ]
    assert {(r.r1, r.r2) for r in reports} == {
        (True, True), (True, False), (False, True), (False, False)}
