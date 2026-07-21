"""Public accessors and size reporting on CrumblingWallQuorum."""

from quorums import CrumblingWallQuorum

MARS = [100, 101, 102]
MOON = [200]
LEO = [300]
EARTH = [1, 2, 3, 4, 5]


def make_wall(k=None):
    return CrumblingWallQuorum([MARS, MOON, LEO, EARTH], phase2_threshold=k)


def test_strict_wall_exposes_threshold_and_hitting_set():
    wall = make_wall()
    assert wall.phase2_threshold == 5
    assert wall.min_earth_in_q1 == 1


def test_relaxed_wall_exposes_threshold_and_hitting_set():
    # k=3: hitting set |E|-k+1 = 3 (comment near quorums.py:229)
    wall = make_wall(3)
    assert wall.phase2_threshold == 3
    assert wall.min_earth_in_q1 == 3


def test_phase1_quorum_size_strict_matches_tier_count():
    # Strict k=5: hitting set is 1, so sizes equal the tier count.
    wall = make_wall()
    assert [wall.phase1_quorum_size(t) for t in range(4)] == [4, 3, 2, 1]


def test_phase1_quorum_size_relaxed_includes_hitting_set():
    # k=3: every Q1 needs |E|-k+1 = 3 Earth nodes, so the true minima
    # are Mars 6, Moon 5, LEO 4, Earth 3 (paper tradeoff table: 3/6).
    wall = make_wall(3)
    assert [wall.phase1_quorum_size(t) for t in range(4)] == [6, 5, 4, 3]


def test_describe_reports_relaxed_phase1_minima():
    text = make_wall(3).describe()
    assert "top needs 6" in text
    assert "bottom needs 3" in text
