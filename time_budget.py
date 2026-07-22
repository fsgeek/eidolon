"""Time-budget validity checks for experiment configurations.

A capability claim is only evidence if the experiment gave it enough
time: the per-phase timeout must exceed the slowest required
request-response path, and each observation window must be able to
contain at least one completed round. Configurations that fail these
checks must be rejected or labeled temporally unavailable — they must
not silently become evidence of a topological capability loss.

Spec: docs/superpowers/specs/2026-07-19-four-layer-capability-model-design.md
(section "Time-budget validity")
"""

from __future__ import annotations

from dataclasses import dataclass


def phase_time(d_max: float, p_max: float) -> float:
    """Worst-case two-message (request-response) Paxos phase."""
    return 2 * d_max + p_max


def round_time(d_max: float, p_max: float) -> float:
    """Worst-case fresh two-phase round."""
    return 4 * d_max + 2 * p_max


@dataclass(frozen=True)
class ExperimentWindow:
    """Temporal layout of a blackout experiment, in seconds."""

    phase_timeout: float
    pre_window: float
    blackout_duration: float
    post_window: float
    horizon: float
    reconciliation_cadence: float = 0.0


def validate_time_budget(window: ExperimentWindow, d_max: float,
                         p_max: float) -> tuple[str, ...]:
    """Return violations; an empty tuple means temporally valid."""
    violations = []
    if window.phase_timeout <= phase_time(d_max, p_max):
        violations.append(
            f"phase timeout {window.phase_timeout}s does not exceed the "
            f"worst request-response path {phase_time(d_max, p_max)}s")
    if window.pre_window < round_time(d_max, p_max):
        violations.append(
            f"pre-blackout window {window.pre_window}s cannot contain one "
            f"full two-phase round {round_time(d_max, p_max)}s")
    needed_post = round_time(d_max, p_max) + window.reconciliation_cadence
    if window.post_window < needed_post:
        violations.append(
            f"post-blackout window {window.post_window}s cannot contain one "
            f"full round plus reconciliation cadence ({needed_post}s)")
    needed_horizon = (window.pre_window + window.blackout_duration
                      + window.post_window)
    if window.horizon < needed_horizon:
        violations.append(
            f"horizon {window.horizon}s shorter than pre + blackout + post "
            f"({needed_horizon}s)")
    return tuple(violations)


def classify_attempt(start: float, end: float, blackout_start: float,
                     blackout_end: float) -> str:
    """Regime by FULL containment; boundary-crossers are 'transition'.

    The network tests a partition when a packet is sent, so a packet
    sent before a boundary may arrive after it; attempts that cross a
    boundary must be excluded from steady-regime success rates.

    Boundary convention: boundaries are exclusive of the blackout, so
    an attempt with end == blackout_start is "pre" and an attempt with
    start == blackout_end is "post".

    Raises:
        ValueError: if start > end or blackout_start > blackout_end.
    """
    if start > end:
        raise ValueError(f"start ({start}) must not exceed end ({end})")
    if blackout_start > blackout_end:
        raise ValueError(
            f"blackout_start ({blackout_start}) must not exceed "
            f"blackout_end ({blackout_end})")
    if end <= blackout_start:
        return "pre"
    if start >= blackout_end:
        return "post"
    if start >= blackout_start and end <= blackout_end:
        return "during"
    return "transition"


def scaled_window(*, d_max: float, p_max: float, blackout_duration: float,
                  phase_timeout: float, pre_window: float,
                  post_window: float, reconciliation_cadence: float = 0.0,
                  margin: float = 1.25) -> tuple[ExperimentWindow, bool]:
    """Return a temporally valid window, scaling insufficient fields.

    Spec: configurations that cannot contain their claimed capabilities
    must be rejected or scaled — this implements the scaling arm and
    reports whether scaling occurred so results can be labeled. The
    margin covers jitter and processing slack beyond the analytic bound.
    """
    pt = phase_time(d_max, p_max)
    rt = round_time(d_max, p_max)
    eff_timeout = max(phase_timeout, margin * pt)
    pre = max(pre_window, margin * rt)
    post = max(post_window, margin * rt + reconciliation_cadence)
    horizon = pre + blackout_duration + post
    window = ExperimentWindow(eff_timeout, pre, blackout_duration, post,
                              horizon, reconciliation_cadence)
    assert validate_time_budget(window, d_max, p_max) == ()
    scaled = (eff_timeout, pre, post) != (phase_timeout, pre_window, post_window)
    return window, scaled
