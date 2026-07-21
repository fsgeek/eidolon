"""Core R1/R2 envelope of the crumbling-wall capability classifier."""

import pytest

from capability import EvidenceChannel, Hazard, classify, format_missing
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


def test_witnesses_are_valid_quorums():
    wall = make_wall(3)
    report = classify(wall, MARS_TIER, ALL_NODES)
    assert wall.is_phase1_quorum(set(report.r1_witness), MARS_TIER)
    assert wall.is_phase2_quorum(set(report.r2_witness))


def test_witness_sizes_match_paper_tradeoff_table():
    # arXiv/NINeS tradeoff table Phase 1 minima (Earth-init / Mars-init):
    # k=5 -> 1/4, k=4 -> 2/5, k=3 -> 3/6
    for k, earth_min, mars_min in [(5, 1, 4), (4, 2, 5), (3, 3, 6)]:
        wall = make_wall(k)
        assert len(classify(wall, EARTH_TIER, ALL_NODES).r1_witness) == earth_min
        assert len(classify(wall, MARS_TIER, ALL_NODES).r1_witness) == mars_min


def test_unreachable_phases_have_no_witness():
    report = classify(make_wall(), MARS_TIER, set(MARS))
    assert report.r1_witness is None
    assert report.r2_witness is None


def test_missing_is_typed_and_identifies_each_obligation():
    report = classify(make_wall(), MARS_TIER, set(MARS))
    # Blocked at Moon (tier 1), LEO (tier 2), Earth (tier 3) for
    # Phase 1, plus the Phase 2 obligation.
    assert [(o.phase, o.tier_index) for o in report.missing] \
        == [(1, 1), (1, 2), (1, 3), (2, 3)]
    moon_row = report.missing[0]
    assert moon_row.required == 1
    assert moon_row.witnesses == frozenset()
    assert moon_row.unreachable == frozenset({200})


def test_format_missing_renders_cli_text():
    report = classify(make_wall(), MARS_TIER, set(MARS))
    lines = format_missing(report)
    assert len(lines) == 4
    assert any("tier 1" in line and "200" in line for line in lines)
    assert any(line.startswith("Phase 2") for line in lines)


def test_fully_capable_report_has_empty_missing():
    report = classify(make_wall(), EARTH_TIER, ALL_NODES)
    assert report.missing == ()
    assert format_missing(report) == ()


def test_10_state_flags_disruptive_election():
    report = classify(make_wall(), LEO_TIER, {300, 1, 2, 3})
    assert report.hazards == (Hazard.DISRUPTIVE_ELECTION,)
    assert report.requires_preexisting_authority is False


def test_01_state_isolates_broken_intermediate_row():
    # Moon and the Earth anchor reachable; ONLY the LEO row is broken
    # (spec: "broken intermediate Phase-1 obligation with anchor
    # reachable"). Reaching only Earth would break two rows at once
    # and not isolate the intermediate obligation.
    report = classify(make_wall(), MOON_TIER, set(MOON) | set(EARTH))
    assert (report.r1, report.r2) == (False, True)
    assert report.hazards == (Hazard.INCUMBENT_ONLY,)
    assert report.requires_preexisting_authority is True
    assert report.can_acquire_or_recover_authority is False
    assert report.can_exercise_existing_authority is True
    assert [(o.phase, o.tier_index) for o in report.missing] == [(1, 2)]


def test_11_and_00_states_have_no_hazards():
    wall = make_wall()
    full = classify(wall, EARTH_TIER, ALL_NODES)
    cut = classify(wall, MARS_TIER, set(MARS))
    assert full.hazards == () and cut.hazards == ()
    assert full.requires_preexisting_authority is False
    assert cut.requires_preexisting_authority is False


def test_provenance_gives_joint_evidence_channels():
    prov = classify(make_wall(), EARTH_TIER, ALL_NODES).provenance
    assert prov["quorum_families"] == {EvidenceChannel.CONFIGURATION}
    structural = {EvidenceChannel.CONFIGURATION, EvidenceChannel.CONNECTIVITY}
    for key in ("r1", "r2", "r1_witness", "r2_witness", "missing",
                "hazards", "requires_preexisting_authority"):
        assert prov[key] == structural, key
    assert prov["operational_progress"] == {
        EvidenceChannel.CONFIGURATION, EvidenceChannel.CONNECTIVITY,
        EvidenceChannel.RUNTIME}
    assert prov["service_contract"] == {EvidenceChannel.POLICY}
