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
    """
    if end <= blackout_start:
        return "pre"
    if start >= blackout_end:
        return "post"
    if start >= blackout_start and end <= blackout_end:
        return "during"
    return "transition"
