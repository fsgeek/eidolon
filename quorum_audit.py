"""Audit explicit finite quorum families by their formability predicates.

The generic auditor is deliberately independent of the simulator's quorum
classes.  Callers supply a finite universe and explicit Phase 1 and Phase 2
families.  The library treats the universe's sequence order as the canonical
order for arbitrary hashable node identifiers; the JSON CLI supplies strings
in lexical order at its boundary.

Pre-registration:
docs/superpowers/notes/2026-07-30-quorum-auditor-preregistration.md
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from itertools import combinations
from typing import Generic, Hashable, Iterable, Sequence, TypeVar


NodeT = TypeVar("NodeT", bound=Hashable)


class PredicateRelation(StrEnum):
    """The four mutually exclusive orderings of two phase predicates."""

    EQUAL = "equal"
    R1_STRICTLY_IMPLIES_R2 = "r1-strictly-implies-r2"
    R2_STRICTLY_IMPLIES_R1 = "r2-strictly-implies-r1"
    INCOMPARABLE = "incomparable"


@dataclass(frozen=True)
class QuorumAudit(Generic[NodeT]):
    """Canonical families and their structural audit result."""

    universe: tuple[NodeT, ...]
    pinned: frozenset[NodeT]
    phase1_minimal: tuple[frozenset[NodeT], ...]
    phase2_minimal: tuple[frozenset[NodeT], ...]
    phase1_effective: tuple[frozenset[NodeT], ...]
    phase2_effective: tuple[frozenset[NodeT], ...]
    safe: bool
    unsafe_witness: (
        tuple[frozenset[NodeT], frozenset[NodeT]] | None
    )
    relation: PredicateRelation
    gap_10_witness: frozenset[NodeT] | None
    gap_01_witness: frozenset[NodeT] | None
    self_check_passed: bool | None = None


def _quorum_key(
    quorum: frozenset[NodeT], rank: dict[NodeT, int]
) -> tuple[int, tuple[int, ...]]:
    return len(quorum), tuple(sorted(rank[node] for node in quorum))


def _minimal_antichain(
    family: Iterable[frozenset[NodeT]], rank: dict[NodeT, int]
) -> tuple[frozenset[NodeT], ...]:
    """Remove duplicates and every strict superset, then sort canonically."""
    unique = frozenset(family)
    minimal = [
        quorum
        for quorum in unique
        if not any(other < quorum for other in unique)
    ]
    return tuple(sorted(minimal, key=lambda quorum: _quorum_key(quorum, rank)))


def _normalize_family(
    label: str,
    family: Iterable[Iterable[NodeT]],
    allowed: frozenset[NodeT],
) -> tuple[frozenset[NodeT], ...]:
    try:
        normalized = tuple(frozenset(quorum) for quorum in family)
    except TypeError as exc:
        raise ValueError(f"{label} contains an unhashable node") from exc
    if not normalized:
        raise ValueError(f"{label} must be nonempty")
    for quorum in normalized:
        if not quorum:
            raise ValueError(f"{label} contains an empty quorum")
        if not quorum <= allowed:
            unknown = quorum - allowed
            raise ValueError(f"{label} contains nodes outside N: {unknown!r}")
    return normalized


def _normalize_inputs(
    universe: Sequence[NodeT],
    phase1: Iterable[Iterable[NodeT]],
    phase2: Iterable[Iterable[NodeT]],
    pinned: Iterable[NodeT],
) -> tuple[
    tuple[NodeT, ...],
    tuple[frozenset[NodeT], ...],
    tuple[frozenset[NodeT], ...],
    frozenset[NodeT],
]:
    nodes = tuple(universe)
    if not nodes:
        raise ValueError("N must be nonempty")
    try:
        allowed = frozenset(nodes)
    except TypeError as exc:
        raise ValueError("N contains an unhashable node") from exc
    if len(allowed) != len(nodes):
        raise ValueError("N contains duplicate nodes")
    try:
        pinned_set = frozenset(pinned)
    except TypeError as exc:
        raise ValueError("P contains an unhashable node") from exc
    if not pinned_set <= allowed:
        unknown = pinned_set - allowed
        raise ValueError(f"P contains nodes outside N: {unknown!r}")
    return (
        nodes,
        _normalize_family("Q1", phase1, allowed),
        _normalize_family("Q2", phase2, allowed),
        pinned_set,
    )


def _first_gap_witness(
    source: tuple[frozenset[NodeT], ...],
    target: tuple[frozenset[NodeT], ...],
    rank: dict[NodeT, int],
) -> frozenset[NodeT] | None:
    """First source minimum that contains no target minimum."""
    candidates = [
        quorum
        for quorum in source
        if not any(other <= quorum for other in target)
    ]
    return min(
        candidates,
        key=lambda quorum: _quorum_key(quorum, rank),
        default=None,
    )


def verify_report_exhaustively(
    universe: Sequence[NodeT],
    phase1: Iterable[Iterable[NodeT]],
    phase2: Iterable[Iterable[NodeT]],
    pinned: Iterable[NodeT],
    report: QuorumAudit[NodeT],
) -> None:
    """Check a report by independently enumerating its reachable states.

    This oracle evaluates the supplied predicates directly over every
    connectivity set C such that P is a subset of C. It intentionally does
    not use antichain minimization or the containment-based classifier.
    """
    nodes, q1, q2, pinned_set = _normalize_inputs(
        universe, phase1, phase2, pinned
    )
    gap_10: frozenset[NodeT] | None = None
    gap_01: frozenset[NodeT] | None = None

    for size in range(len(nodes) + 1):
        for members in combinations(nodes, size):
            connected = frozenset(members)
            if not pinned_set <= connected:
                continue
            phase1_formable = any(quorum <= connected for quorum in q1)
            phase2_formable = any(quorum <= connected for quorum in q2)
            if phase1_formable and not phase2_formable and gap_10 is None:
                gap_10 = connected
            if phase2_formable and not phase1_formable and gap_01 is None:
                gap_01 = connected

    if gap_10 is None and gap_01 is None:
        relation = PredicateRelation.EQUAL
    elif gap_10 is None:
        relation = PredicateRelation.R1_STRICTLY_IMPLIES_R2
    elif gap_01 is None:
        relation = PredicateRelation.R2_STRICTLY_IMPLIES_R1
    else:
        relation = PredicateRelation.INCOMPARABLE

    expected = (relation, gap_10, gap_01)
    observed = (
        report.relation,
        report.gap_10_witness,
        report.gap_01_witness,
    )
    if observed != expected:
        raise AssertionError(
            "containment classification disagrees with exhaustive states: "
            f"observed={observed!r}, expected={expected!r}"
        )


def audit_quorum_families(
    universe: Sequence[NodeT],
    phase1: Iterable[Iterable[NodeT]],
    phase2: Iterable[Iterable[NodeT]],
    *,
    pinned: Iterable[NodeT] = (),
    exhaustive: bool = False,
) -> QuorumAudit[NodeT]:
    """Audit safety and phase-predicate ordering for explicit families.

    After antichain minimization, classification takes
    O(|min(Q1)| * |min(Q2)| * |N|) time, excluding parsing,
    deterministic sorting, and the separate quadratic-in-family-size
    minimization pass.

    Exhaustive verification is added by the next test-driven task.
    """
    nodes, q1, q2, pinned_set = _normalize_inputs(
        universe, phase1, phase2, pinned)
    rank = {node: index for index, node in enumerate(nodes)}
    q1_min = _minimal_antichain(q1, rank)
    q2_min = _minimal_antichain(q2, rank)
    q1_effective = _minimal_antichain(
        (quorum | pinned_set for quorum in q1_min), rank
    )
    q2_effective = _minimal_antichain(
        (quorum | pinned_set for quorum in q2_min), rank
    )
    gap_10 = _first_gap_witness(q1_effective, q2_effective, rank)
    gap_01 = _first_gap_witness(q2_effective, q1_effective, rank)
    if gap_10 is None and gap_01 is None:
        relation = PredicateRelation.EQUAL
    elif gap_10 is None:
        relation = PredicateRelation.R1_STRICTLY_IMPLIES_R2
    elif gap_01 is None:
        relation = PredicateRelation.R2_STRICTLY_IMPLIES_R1
    else:
        relation = PredicateRelation.INCOMPARABLE

    unsafe_pairs = [
        (phase1_quorum, phase2_quorum)
        for phase1_quorum in q1_min
        for phase2_quorum in q2_min
        if phase1_quorum.isdisjoint(phase2_quorum)
    ]
    unsafe_witness = min(
        unsafe_pairs,
        key=lambda pair: (
            _quorum_key(pair[0], rank),
            _quorum_key(pair[1], rank),
        ),
        default=None,
    )
    report = QuorumAudit(
        universe=nodes,
        pinned=pinned_set,
        phase1_minimal=q1_min,
        phase2_minimal=q2_min,
        phase1_effective=q1_effective,
        phase2_effective=q2_effective,
        safe=unsafe_witness is None,
        unsafe_witness=unsafe_witness,
        relation=relation,
        gap_10_witness=gap_10,
        gap_01_witness=gap_01,
    )
    if exhaustive:
        verify_report_exhaustively(nodes, q1, q2, pinned_set, report)
        report = replace(report, self_check_passed=True)
    return report
