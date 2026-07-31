"""Regression checks for the NINeS revision's claim boundaries."""

from pathlib import Path


PAPER = Path("docs/paper/nines/main.tex")


def section(text: str, start: str, end: str) -> str:
    start_index = text.index(start)
    content_start = start_index + len(start)
    end_index = text.index(end, content_start)
    return text[start_index:end_index]


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
    assert "not an evaluated result" in text


def test_body_uses_predicate_order_not_family_syntax_or_intrinsic_labels():
    text = PAPER.read_text(encoding="utf-8")
    for banned in (
        "which mixed state it admits is determined by the direction",
        "it is the only mitigation left",
        "the harmful one persists",
    ):
        assert banned not in text
    assert "semantic equality" in text
    assert "complete invariant" in text


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
    assert "cost-minimal split" in text
    assert "odd $n$" in text
    assert "even $n$" in text
    assert "one additional participant" in text
    assert "fresh runtime authority" in text
    assert "chosen client contract" in text


def test_wall_readout_is_labeled_as_demonstration_not_evidence():
    text = PAPER.read_text(encoding="utf-8")
    assert "demonstration artifact, not empirical evidence" in text
    assert "experiments/\\allowbreak capability\\_readout.py" in text


def test_introduction_states_the_two_questions_and_defines_legibility():
    text = PAPER.read_text(encoding="utf-8")
    introduction = section(text, r"\section{Introduction}", r"\section{")
    assert "acquire proposal authority" in introduction
    assert "complete a commit" in introduction
    assert "legible with respect to a supplied connectivity summary" in introduction
    assert "without enumerating candidate quorum subsets" in introduction
    assert "attempting protocol execution" in introduction
    assert r"O(\text{tiers})" in introduction
    assert "A node-health dashboard answers neither" in introduction
    for boundary in (
        "does not detect connectivity",
        "establish current authority",
        "select recovery policy",
        "guarantee that an operator consults",
    ):
        assert boundary in introduction


def test_introduction_distinguishes_symmetry_majority_and_vantage():
    text = PAPER.read_text(encoding="utf-8")
    introduction = section(text, r"\section{Introduction}", r"\section{")
    assert "phase symmetry" in introduction.lower()
    assert "majority" in introduction.lower()
    assert "threshold" in introduction.lower()
    assert "vantage" in introduction.lower()
    assert "coincid" in introduction.lower()
    assert "unconstrained reading" in introduction.lower()
    assert "self-reachable reading" in introduction.lower()
    assert "colocated acceptor" in introduction.lower()
    assert "The count worked because" not in introduction
    assert "for so long" not in introduction
    if "spend" in introduction.lower():
        assert introduction.lower().index("coincid") < introduction.lower().index(
            "spend"
        )


def test_formal_investigation_sections_appear_in_order():
    text = PAPER.read_text(encoding="utf-8")
    headings = (
        r"\section{Why One Count Once Worked}",
        r"\section{Capability Gaps}",
        r"\section{Putting the Gaps on a Wall}",
        r"\section{Where the Wall Works, and Where It Stops}",
    )
    positions = [text.index(heading) for heading in headings]
    assert positions == sorted(positions)
