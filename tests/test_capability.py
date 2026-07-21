"""Core R1/R2 envelope of the crumbling-wall capability classifier."""

import pytest

from capability import classify
from quorums import CrumblingWallQuorum

MARS = [100, 101, 102]
MOON = [200]
LEO = [300]
EARTH = [1, 2, 3, 4, 5]
ALL_NODES = set(MARS + MOON + LEO + EARTH)

MARS_TIER, MOON_TIER, LEO_TIER, EARTH_TIER = 0, 1, 2, 3


def make_wall(k=None):
    return CrumblingWallQuorum([MARS, MOON, LEO, EARTH], phase2_threshold=k)


def test_full_reachability_gives_11_from_every_tier():
    wall = make_wall()
    for tier in (MARS_TIER, MOON_TIER, LEO_TIER, EARTH_TIER):
        report = classify(wall, tier, ALL_NODES)
        assert (report.r1, report.r2) == (True, True)


def test_mars_blackout_gives_00_from_mars():
    wall = make_wall()
    report = classify(wall, MARS_TIER, set(MARS))
    assert (report.r1, report.r2) == (False, False)


def test_mars_blackout_leaves_earth_at_11():
    wall = make_wall()
    report = classify(wall, EARTH_TIER, ALL_NODES - set(MARS))
    assert (report.r1, report.r2) == (True, True)


def test_sparse_leo_strict_phase2_gives_10():
    # LEO reaches its own satellite and three of five Earth nodes.
    wall = make_wall()
    report = classify(wall, LEO_TIER, {300, 1, 2, 3})
    assert (report.r1, report.r2) == (True, False)


def test_obligations_report_each_wall_row():
    wall = make_wall()
    report = classify(wall, MARS_TIER, set(MARS))
    assert [o.tier_index for o in report.r1_obligations] == [0, 1, 2, 3]
    assert [o.satisfied for o in report.r1_obligations] == [True, False, False, False]
    assert all(o.phase == 1 for o in report.r1_obligations)
    assert report.r2_obligation.phase == 2
    # Unreachable candidates are carried as data, not prose.
    assert report.r1_obligations[1].unreachable == frozenset({200})


def test_out_of_range_tier_raises():
    with pytest.raises(ValueError):
        classify(make_wall(), 4, ALL_NODES)
