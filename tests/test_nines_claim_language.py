"""Regression checks for the NINeS revision's claim boundaries."""

from pathlib import Path


PAPER = Path("docs/paper/nines/main.tex")


def test_approved_title_and_disallowed_claims():
    text = PAPER.read_text(encoding="utf-8")
    assert "Legible Consensus: Capability Gaps in Flexible Quorums" in text
    for banned in (
        "every departure from phase symmetry opens exactly one",
        "blocks liveness completely",
        "only available mitigation",
    ):
        assert banned not in text


def test_scope_and_edge_boundary_are_explicit():
    text = PAPER.read_text(encoding="utf-8")
    assert "N_e" in text and "C_t" in text
    assert "magnifying glass" in text
    assert "not an evaluated result" in text
