"""Generic quorum-family auditor behavior.

The fixtures are hand-derived from the pre-registration. Tests exercise the
public audit boundary; they do not reconstruct its containment algorithm.
"""

from dataclasses import replace
from itertools import combinations

import pytest

from quorum_audit import (
    PredicateRelation,
    audit_quorum_families,
    verify_report_exhaustively,
)


@pytest.mark.parametrize(
    "universe,phase1,phase2,pinned",
    [
        ([], [["a"]], [["a"]], []),
        (["a"], [], [["a"]], []),
        (["a"], [[]], [["a"]], []),
        (["a"], [["b"]], [["a"]], []),
        (["a"], [["a"]], [["a"]], ["b"]),
    ],
)
def test_invalid_inputs_raise_value_error(universe, phase1, phase2, pinned):
    """Removing any structural input check must expose malformed families."""
    with pytest.raises(ValueError):
        audit_quorum_families(
            universe, phase1, phase2, pinned=pinned)


def test_supersets_are_removed_without_changing_semantics():
    """Keeping a strict superset must not create syntactic phase asymmetry."""
    report = audit_quorum_families(
        ["a", "b", "c"],
        [["a", "b"], ["a", "b", "c"]],
        [["a", "b"]],
    )

    expected = (frozenset({"a", "b"}),)
    assert report.phase1_minimal == expected
    assert report.phase2_minimal == expected
    assert report.relation is PredicateRelation.EQUAL


@pytest.mark.parametrize(
    "phase1,phase2,relation,witness_10,witness_01",
    [
        (
            [["a", "b"]],
            [["a"]],
            PredicateRelation.R1_STRICTLY_IMPLIES_R2,
            None,
            frozenset({"a"}),
        ),
        (
            [["a"]],
            [["a", "b"]],
            PredicateRelation.R2_STRICTLY_IMPLIES_R1,
            frozenset({"a"}),
            None,
        ),
        (
            [["a", "b"]],
            [["a", "c"]],
            PredicateRelation.INCOMPARABLE,
            frozenset({"a", "b"}),
            frozenset({"a", "c"}),
        ),
    ],
)
def test_registered_predicate_classes(
    phase1, phase2, relation, witness_10, witness_01
):
    """Reversing either containment test must change a registered profile."""
    report = audit_quorum_families(
        ["a", "b", "c"], phase1, phase2)

    assert report.safe is True
    assert report.relation is relation
    assert report.gap_10_witness == witness_10
    assert report.gap_01_witness == witness_01


def test_unsafe_configuration_is_classified_and_flagged():
    """Rejecting or hiding a disjoint cross-phase pair breaks linter use."""
    report = audit_quorum_families(
        ["a", "b", "c"], [["a"]], [["b"]])

    assert report.safe is False
    assert report.unsafe_witness == (
        frozenset({"a"}), frozenset({"b"}))
    assert report.relation is PredicateRelation.INCOMPARABLE
    assert report.gap_10_witness == frozenset({"a"})
    assert report.gap_01_witness == frozenset({"b"})


def _threshold_family(nodes, size):
    return [list(quorum) for quorum in combinations(nodes, size)]


@pytest.mark.parametrize(
    "q1,q2,relation,witness_10,witness_01",
    [
        (
            1,
            3,
            PredicateRelation.R2_STRICTLY_IMPLIES_R1,
            frozenset({"a"}),
            None,
        ),
        (2, 2, PredicateRelation.EQUAL, None, None),
        (
            3,
            1,
            PredicateRelation.R1_STRICTLY_IMPLIES_R2,
            None,
            frozenset({"a"}),
        ),
    ],
)
def test_registered_uniform_threshold_profiles(
    q1, q2, relation, witness_10, witness_01
):
    """Swapping threshold order must swap the sole reachable gap."""
    nodes = ["a", "b", "c"]
    report = audit_quorum_families(
        nodes, _threshold_family(nodes, q1), _threshold_family(nodes, q2))

    assert report.safe is True
    assert report.relation is relation
    assert report.gap_10_witness == witness_10
    assert report.gap_01_witness == witness_01


def test_pinning_can_close_only_the_01_gap():
    """Ignoring P would leave an impossible pinned-domain state reachable."""
    unpinned = audit_quorum_families(
        ["a", "b", "c"], [["a"]], [["b"]])
    pinned = audit_quorum_families(
        ["a", "b", "c"], [["a"]], [["b"]], pinned=["a"])

    assert unpinned.relation is PredicateRelation.INCOMPARABLE
    assert pinned.safe is False  # Pinning restricts states, not Paxos safety.
    assert pinned.relation is PredicateRelation.R2_STRICTLY_IMPLIES_R1
    assert pinned.gap_10_witness == frozenset({"a"})
    assert pinned.gap_01_witness is None


def test_lifted_family_is_minimized_again():
    """Failing to re-minimize leaves redundant effective predicates."""
    report = audit_quorum_families(
        ["a", "b", "c"], [["a"], ["b"]], [["c"]], pinned=["a"])

    assert report.phase1_minimal == (
        frozenset({"a"}), frozenset({"b"}))
    assert report.phase1_effective == (frozenset({"a"}),)


def test_exhaustive_mode_marks_a_verified_report():
    """The public switch must run the oracle and expose that fact."""
    report = audit_quorum_families(
        ["a", "b", "c"],
        [["a", "b"]],
        [["a", "c"]],
        pinned=["a"],
        exhaustive=True,
    )

    assert report.self_check_passed is True


def test_exhaustive_oracle_rejects_a_mutated_classification():
    """An oracle that merely repeats the report cannot catch this mutation."""
    universe = ["a", "b", "c"]
    phase1 = [["a", "b"]]
    phase2 = [["a", "c"]]
    report = audit_quorum_families(universe, phase1, phase2)
    bad_report = replace(
        report,
        relation=PredicateRelation.EQUAL,
        gap_10_witness=None,
        gap_01_witness=None,
    )

    with pytest.raises(AssertionError):
        verify_report_exhaustively(
            universe, phase1, phase2, [], bad_report)


def _all_subsets(nodes):
    return [
        list(subset)
        for size in range(len(nodes) + 1)
        for subset in combinations(nodes, size)
    ]


def test_exhaustive_oracle_covers_every_three_node_family_pair_and_pin_set():
    """All 127^2 nonempty family pairs and eight pinned domains agree."""
    nodes = ["a", "b", "c"]
    nonempty_quorums = _all_subsets(nodes)[1:]
    families = [
        [nonempty_quorums[index] for index in range(len(nonempty_quorums))
         if mask & (1 << index)]
        for mask in range(1, 1 << len(nonempty_quorums))
    ]
    checked = 0

    for phase1 in families:
        for phase2 in families:
            for pinned in _all_subsets(nodes):
                report = audit_quorum_families(
                    nodes,
                    phase1,
                    phase2,
                    pinned=pinned,
                    exhaustive=True,
                )
                assert report.self_check_passed is True
                checked += 1

    assert checked == 129_032
