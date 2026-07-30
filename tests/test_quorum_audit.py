"""Generic quorum-family auditor behavior.

The fixtures are hand-derived from the pre-registration. Tests exercise the
public audit boundary; they do not reconstruct its containment algorithm.
"""

from itertools import combinations

import pytest

from quorum_audit import PredicateRelation, audit_quorum_families


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
