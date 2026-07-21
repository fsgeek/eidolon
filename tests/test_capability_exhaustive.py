"""Exhaustive agreement between the classifier and the quorum predicates.

2^10 connectivity states x 4 initiator tiers x 6 thresholds. This is
the paper's 'checked exhaustively over the finite topology' claim for
the classifier, distinct from Paxos safety verification.
"""

from itertools import combinations

from capability import Hazard, classify
from quorums import CrumblingWallQuorum

MARS = [100, 101, 102]
MOON = [200]
LEO = [300]
EARTH = [1, 2, 3, 4, 5]
ALL_NODES = sorted(MARS + MOON + LEO + EARTH)


def all_subsets(nodes):
    for r in range(len(nodes) + 1):
        for combo in combinations(nodes, r):
            yield set(combo)


def test_classifier_agrees_with_quorum_predicates_exhaustively():
    for k in (None, 5, 4, 3, 2, 1):
        wall = CrumblingWallQuorum([MARS, MOON, LEO, EARTH],
                                   phase2_threshold=k)
        for subset in all_subsets(ALL_NODES):
            for tier in range(wall.num_tiers):
                report = classify(wall, tier, subset)
                assert report.r1 == wall.is_phase1_quorum(subset, tier), \
                    f"R1 disagrees: k={k} tier={tier} reach={sorted(subset)}"
                assert report.r2 == wall.is_phase2_quorum(subset), \
                    f"R2 disagrees: k={k} tier={tier} reach={sorted(subset)}"
                if report.r1:
                    # Minimal witness matches the corrected size formula.
                    assert len(report.r1_witness) \
                        == wall.phase1_quorum_size(tier)


def test_hazard_flags_follow_matrix_exhaustively():
    for k in (None, 5, 4, 3):
        wall = CrumblingWallQuorum([MARS, MOON, LEO, EARTH],
                                   phase2_threshold=k)
        for subset in all_subsets(ALL_NODES):
            for tier in range(wall.num_tiers):
                report = classify(wall, tier, subset)
                expects_disruptive = report.r1 and not report.r2
                expects_incumbent = report.r2 and not report.r1
                assert (Hazard.DISRUPTIVE_ELECTION in report.hazards) \
                    == expects_disruptive
                assert (Hazard.INCUMBENT_ONLY in report.hazards) \
                    == expects_incumbent
                assert report.requires_preexisting_authority \
                    == expects_incumbent
