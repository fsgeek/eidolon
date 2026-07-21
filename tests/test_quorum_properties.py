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
