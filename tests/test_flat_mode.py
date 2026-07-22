"""Flat global-quorum mode: apples-to-apples baseline under the repaired
harness.

The historical construction (git show aae70f7:demo_step_9.py) built the
global proposer as:

    wall = CrumblingWallQuorum([mars_ids, [moon_id], [leo_id], earth_ids])
    global_prop = Proposer(env, entity, network, all_ids, wall,
                            timeout=cfg.global_timeout_s,
                            max_rounds=cfg.global_max_rounds)

i.e. no `initiator_tier` kwarg at all. `Proposer.initiator_tier` then
defaults to `None`, and `CrumblingWallQuorum.is_phase1_quorum` treats
`initiator_tier=None` as tier 0 (top of wall / Mars, "worst case") --
requiring a respondent from *every* tier. That is exactly the paper's
Q1^flat definition (Section "Flat versus Crumbling Wall"): a Phase 1
family that must intersect every tier, including a disconnected Mars.

`--global-quorum flat` reproduces this verbatim: same quorum class, same
tier construction, `initiator_tier` simply omitted. `--global-quorum wall`
(the default) preserves current behavior: `initiator_tier=3` (Earth is
the bottom of the wall), so an Earth-initiated Phase 1 only needs Earth.
"""

from quorums import CrumblingWallQuorum

from demo_step_9 import ExperimentConfig, _wire_system, run_conjunction_experiment

import simpy


def test_flat_mode_constructs_historical_quorum_type_and_params():
    """flat mode: CrumblingWallQuorum over the same four tiers, but with
    no initiator_tier -- matching the historical (aae70f7) call site."""
    env = simpy.Environment()
    cfg = ExperimentConfig(global_quorum="flat")
    network, earth_prop, mars_prop, global_prop = _wire_system(env, cfg)

    assert isinstance(global_prop.quorum, CrumblingWallQuorum)
    # Historical call never passed initiator_tier -> Proposer default None.
    assert global_prop.initiator_tier is None

    # Same tier construction as the historical (and current wall) call:
    # [mars_ids, [moon_id], [leo_id], earth_ids].
    tiers = global_prop.quorum.tiers
    assert len(tiers) == 4
    assert len(tiers[0]) == 3   # mars_ids
    assert len(tiers[1]) == 1   # moon
    assert len(tiers[2]) == 1   # leo
    assert len(tiers[3]) == 5   # earth_ids

    # With initiator_tier=None, is_phase1_quorum requires every tier --
    # the flat semantics (Q1^flat intersects every T_j).
    earth_only = set(tiers[3])
    assert global_prop.quorum.is_phase1_quorum(earth_only, global_prop.initiator_tier) is False
    all_tiers_one_each = {tiers[0][0], tiers[1][0], tiers[2][0], tiers[3][0]}
    assert global_prop.quorum.is_phase1_quorum(all_tiers_one_each, global_prop.initiator_tier) is True


def test_wall_mode_default_preserves_current_behavior():
    """wall is the default and keeps initiator_tier=3 (Earth-initiated,
    reads down from Earth only)."""
    env = simpy.Environment()
    cfg = ExperimentConfig()
    assert cfg.global_quorum == "wall"
    network, earth_prop, mars_prop, global_prop = _wire_system(env, cfg)

    assert isinstance(global_prop.quorum, CrumblingWallQuorum)
    assert global_prop.initiator_tier == 3


def test_flat_mode_smoke_run_completes_and_buckets_account_for_all_attempts():
    """Scaled-down run_conjunction_experiment in flat mode: completes and
    every global-reconcile attempt lands in exactly one bucket."""
    cfg = ExperimentConfig(
        mars_base_latency_s=5.0,
        blackout_start_s=60.0,
        blackout_duration_s=120.0,
        sim_end_s=400.0,
        reconcile_interval_s=30.0,
        global_timeout_s=25.0,
        global_quorum="flat",
        seed=42,
    )
    result = run_conjunction_experiment(with_repeater=False, cfg=cfg, verbose=False)
    buckets = [result.pre_blackout, result.during_blackout,
               result.post_blackout, result.transition]
    assert all(b is not None for b in buckets)
    total_attempts = sum(b.total for b in buckets)
    assert total_attempts > 0

    # Flat semantics: during blackout, Mars is unreachable so Phase 1
    # cannot get a Mars respondent -> during-blackout global success is 0.
    assert result.during_blackout.success == 0
