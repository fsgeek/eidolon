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


def test_body_uses_predicate_order_not_family_syntax_or_intrinsic_labels():
    text = PAPER.read_text(encoding="utf-8")
    for banned in (
        "which mixed state it admits is determined by the direction",
        "it is the only mitigation left",
        "the harmful one persists",
    ):
        assert banned not in text
    assert "which gaps are reachable is determined by how the predicates order" in text
    assert "state-aware mitigation requires distinguishing the state" in text


def test_exact_correspondence_and_auditor_are_in_the_body():
    text = PAPER.read_text(encoding="utf-8")
    assert "\\operatorname{Form}" in text
    assert "semantic equality" in text
    assert "complete invariant" in text
    assert "construction-independent auditor" in text
    assert "O(|\\min(\\mathcal{Q}_1)|" in text
    assert "results/capability/\\allowbreak quorum\\_audit\\_registered.json" in text


def test_prior_containment_results_are_distinguished_and_cited():
    text = PAPER.read_text(encoding="utf-8")
    bibliography = Path("docs/paper/nines/references.bib").read_text(
        encoding="utf-8"
    )
    assert "guerraoui2010rqs" in bibliography
    assert "\\cite{guerraoui2010rqs}" in text
    assert "member-indexed containment" in text
    assert "phase-formability ordering" in text


def test_relocated_corollary_and_operational_boundaries_are_present():
    text = PAPER.read_text(encoding="utf-8")
    assert "fault-tolerance-independent reason for the odd-cluster convention" in text
    assert "Demand for Phase~1 is correlated with its price" in text
    assert "fresh runtime authority" in text
    assert "chosen client contract" in text
