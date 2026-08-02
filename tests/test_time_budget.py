"""Time-budget validity and regime classification (spec section
'Time-budget validity'). Numeric cases come from the spec's Mars
examples: close approach ~186 s one-way, so a fresh two-phase round is
~744 s before margin and a 600 s pre-window cannot contain one."""

import pytest

from time_budget import (
    ExperimentWindow,
    classify_attempt,
    phase_time,
    round_time,
    validate_time_budget,
)


def test_phase_and_round_bounds():
    assert phase_time(186, 0) == 372
    assert round_time(186, 0) == 744


def test_close_approach_mars_with_short_pre_window_is_rejected():
    window = ExperimentWindow(phase_timeout=500, pre_window=600,
                              blackout_duration=900, post_window=900,
                              horizon=2400)
    violations = validate_time_budget(window, d_max=186, p_max=0)
    assert any("pre-blackout window" in v for v in violations)
    # The 500 s per-phase timeout DOES exceed the 372 s round trip:
    # the old paper claim to the contrary was wrong.
    assert not any("phase timeout" in v for v in violations)


def test_far_mars_delay_exceeds_phase_timeout():
    window = ExperimentWindow(phase_timeout=500, pre_window=6000,
                              blackout_duration=900, post_window=6000,
                              horizon=12900)
    violations = validate_time_budget(window, d_max=1342, p_max=0)
    assert any("phase timeout" in v for v in violations)


def test_valid_configuration_returns_no_violations():
    window = ExperimentWindow(phase_timeout=500, pre_window=900,
                              blackout_duration=900, post_window=900,
                              horizon=2700)
    assert validate_time_budget(window, d_max=186, p_max=1) == ()


def test_horizon_must_contain_all_windows():
    window = ExperimentWindow(phase_timeout=500, pre_window=900,
                              blackout_duration=900, post_window=900,
                              horizon=2000)
    violations = validate_time_budget(window, d_max=186, p_max=1)
    assert any("horizon" in v for v in violations)


def test_post_window_accounts_for_reconciliation_cadence():
    window = ExperimentWindow(phase_timeout=500, pre_window=900,
                              blackout_duration=900, post_window=900,
                              horizon=2700, reconciliation_cadence=300)
    violations = validate_time_budget(window, d_max=186, p_max=1)
    assert any("post-blackout window" in v for v in violations)


def test_attempt_regimes_by_full_containment():
    # Blackout spans [1000, 1900].
    assert classify_attempt(0, 999, 1000, 1900) == "pre"
    assert classify_attempt(1100, 1500, 1000, 1900) == "during"
    assert classify_attempt(1950, 2400, 1000, 1900) == "post"
    # A packet sent before the boundary may arrive after it.
    assert classify_attempt(900, 1100, 1000, 1900) == "transition"
    assert classify_attempt(1800, 2000, 1000, 1900) == "transition"


def test_classify_attempt_rejects_inverted_intervals():
    with pytest.raises(ValueError):
        classify_attempt(1100, 1000, 1000, 1900)
    with pytest.raises(ValueError):
        classify_attempt(0, 999, 1900, 1000)


def test_scaled_window_passes_validation_by_construction():
    from time_budget import scaled_window
    window, scaled = scaled_window(
        d_max=186.0, p_max=0.0, blackout_duration=900.0,
        phase_timeout=500.0, pre_window=600.0, post_window=900.0,
        reconciliation_cadence=120.0)
    assert scaled is True                      # 600 < 1.25 * 744
    assert window.pre_window == 930.0          # 1.25 * round_time(186, 0)
    assert validate_time_budget(window, d_max=186.0, p_max=0.0) == ()


def test_scaled_window_leaves_valid_configs_untouched():
    from time_budget import scaled_window
    window, scaled = scaled_window(
        d_max=0.15, p_max=0.0, blackout_duration=900.0,
        phase_timeout=500.0, pre_window=600.0, post_window=900.0,
        reconciliation_cadence=120.0)
    assert scaled is False
    assert (window.phase_timeout, window.pre_window, window.post_window) \
        == (500.0, 600.0, 900.0)


def test_scaled_window_scales_far_mars_timeout():
    from time_budget import scaled_window
    window, scaled = scaled_window(
        d_max=1342.0, p_max=0.0, blackout_duration=900.0,
        phase_timeout=500.0, pre_window=600.0, post_window=900.0)
    assert scaled is True
    assert window.phase_timeout > 2 * 1342.0   # exceeds phase_time
    assert validate_time_budget(window, d_max=1342.0, p_max=0.0) == ()
