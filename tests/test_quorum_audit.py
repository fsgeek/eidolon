"""Generic quorum-family auditor behavior.

The fixtures are hand-derived from the pre-registration. Tests exercise the
public audit boundary; they do not reconstruct its containment algorithm.
"""

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
