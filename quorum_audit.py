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

from dataclasses import dataclass
from enum import StrEnum
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


def audit_quorum_families(
    universe: Sequence[NodeT],
    phase1: Iterable[Iterable[NodeT]],
    phase2: Iterable[Iterable[NodeT]],
    *,
    pinned: Iterable[NodeT] = (),
    exhaustive: bool = False,
) -> QuorumAudit[NodeT]:
    """Validate and canonicalize explicit phase families.

    Directional classification, pinned-domain lifting, and exhaustive
    verification are added by the next test-driven tasks.
    """
    nodes, q1, q2, pinned_set = _normalize_inputs(
        universe, phase1, phase2, pinned)
    rank = {node: index for index, node in enumerate(nodes)}
    q1_min = _minimal_antichain(q1, rank)
    q2_min = _minimal_antichain(q2, rank)
    if q1_min != q2_min:
        raise NotImplementedError("directional classification is not implemented")
    if pinned_set:
        raise NotImplementedError("pinned-domain lifting is not implemented")
    if exhaustive:
        raise NotImplementedError("exhaustive self-checking is not implemented")
    return QuorumAudit(
        universe=nodes,
        pinned=pinned_set,
        phase1_minimal=q1_min,
        phase2_minimal=q2_min,
        phase1_effective=q1_min,
        phase2_effective=q2_min,
        safe=True,
        unsafe_witness=None,
        relation=PredicateRelation.EQUAL,
        gap_10_witness=None,
        gap_01_witness=None,
    )
