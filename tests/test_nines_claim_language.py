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


def test_readout_is_a_bounded_demonstration():
    text = PAPER.read_text(encoding="utf-8")
    reading = section(text, r"\section{Look Through the Instrument}", r"\section{")
    assert "demonstration artifact, not empirical evidence" in reading
    assert "configuration and connectivity" in reading
    assert "runtime authority" in reading
    assert "service policy" in reading
    assert "missing Phase 1 LEO obligation" in reading
    assert "experiments/\\allowbreak capability\\_readout.py" in text
    assert "not an evaluated result" in text


def test_readout_precedes_construction_and_calibration():
    text = PAPER.read_text(encoding="utf-8")
    headings = (
        r"\section{Look Through the Instrument}",
        r"\section{Putting the Gaps on a Wall}",
        r"\section{Where the Wall Works, and Where It Stops}",
        r"\section{Prevention Has a Cost}",
        r"\section{Supplemental Wall Calibration}",
    )
    positions = [text.index(heading) for heading in headings]
    assert positions == sorted(positions)


def test_supplement_preserves_the_calibration_results():
    text = PAPER.read_text(encoding="utf-8")
    reading = section(
        text,
        r"\section{Supplemental Wall Calibration}",
        r"\end{document}",
    )
    for required in (
        "majority Phase~1 leaves the fast tier",
        "LEO's 131~ms is \\emph{faster} than Earth",
        "Liveness requires both",
        "These point values are cadence-alignment artifacts",
    ):
        assert required in reading
    prevention = section(text, r"\section{Prevention Has a Cost}", r"\section{")
    assert "The weakest link migrates" in prevention


def test_ending_opens_the_unsolved_problem_before_bounding_claims():
    text = PAPER.read_text(encoding="utf-8")
    headings = (
        r"\section{Look Through the Instrument}",
        r"\section{What Remains Unsolved}",
        r"\section{Threats to Validity and Limitations}",
        r"\section{Conclusion}",
    )
    positions = [text.index(heading) for heading in headings]
    assert positions == sorted(positions)
    assert r"\section{Discussion}" not in text


def test_open_problem_and_conclusion_keep_their_boundaries():
    text = PAPER.read_text(encoding="utf-8")
    open_problem = section(
        text,
        r"\section{What Remains Unsolved}",
        r"\section{",
    )
    for required in (
        "Mars makes one temporal regime",
        "fixed while visibility changes",
        "authority-bearing membership",
        "fixed-epoch formability",
        "transferable evidence of authority and accepted state",
    ):
        assert required in open_problem

    conclusion = section(text, r"\section{Conclusion}", r"\bibliographystyle")
    assert "phase-capability coincidence" in conclusion
    assert "prevention boundary" in conclusion
    assert "support detection" in conclusion
    assert "fresh runtime authority" in conclusion
    assert "which actions the system can presently complete" in conclusion
    assert "changing visibility and changing membership" in conclusion


def test_cross_cutting_limits_are_explicit():
    text = PAPER.read_text(encoding="utf-8")
    limits = section(
        text,
        r"\section{Threats to Validity and Limitations}",
        r"\section{",
    )
    for required in (
        "single-decree",
        "Multi-Paxos",
        "supplied connectivity",
        "no deployment prevalence",
        "no terrestrial evaluation",
        "runtime authority",
        "service policy",
    ):
        assert required in limits


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
    assert "unconstrained reading" not in introduction.lower()
    assert "self-reachable reading" not in introduction.lower()
    assert "evaluated fixture rather than a proposed deployment" in introduction.lower()
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


def test_behavioral_reversal_immediately_follows_capability_boundary():
    text = PAPER.read_text(encoding="utf-8")
    headings = (
        r"\section{Capability Gaps}",
        r"\section{What Happens Inside the Gaps}",
        r"\section{Look Through the Instrument}",
    )
    positions = [text.index(heading) for heading in headings]
    assert positions == sorted(positions)
    assert r"\subsection{The Valence of the Two Capability States}" not in text


def test_behavioral_result_keeps_method_and_bounds_adjacent():
    text = PAPER.read_text(encoding="utf-8")
    behavior = section(
        text,
        r"\section{What Happens Inside the Gaps}",
        r"\section{",
    )
    for required in (
        "five arms",
        r"$\{1,2,4,8\}$",
        "50 seeds per cell",
        "single-decree Paxos",
        "healthy proposer's fixed eight-round budget",
        "all 50 seeds",
        "accepted value",
        "not evidence of livelock",
        "Under Multi-Paxos",
    ):
        assert required in behavior
    for overclaim in (
        "the cost is contention, not the capability state",
        "measured-harmful direction",
        "blocks the healthy proposer completely",
    ):
        assert overclaim not in text


def test_wall_positive_results_precede_the_boundary():
    text = PAPER.read_text(encoding="utf-8")
    wall = section(
        text,
        r"\section{Where the Wall Works, and Where It Stops}",
        r"\section{",
    )
    required = (
        r"At $k=3$, the $(1,0)$ gap is absent at every tier",
        r"both gaps are absent for Earth under both connectivity readings",
        r"both gaps are absent for LEO under the self-reachable reading",
        r"$(0,1)$ remains reachable for Moon and Mars",
        r"For $k \le 3$, $(1,0)$ is absent at every tier",
        r"For $k \ge 3$, $(0,1)$ is absent for Earth",
    )
    assert all(claim in wall for claim in required)
    assert wall.index(required[0]) < wall.index(required[3])


def test_central_per_tier_table_uses_one_observed_condition():
    text = PAPER.read_text(encoding="utf-8")
    table = section(text, r"\caption{Per-tier global consensus", r"\end{table}")
    assert "1800~s blackout" in table
    assert "900~s blackout" not in table
    assert r"Mars & top (3) & 0.0\%" in table
    assert r"---$^{\dagger}$" not in table


def test_superseded_wall_and_valence_claims_do_not_return():
    text = PAPER.read_text(encoding="utf-8")
    for banned in (
        "compensates every other tier with legibility",
        "the harmful gap",
        "the harmless gap",
        "intrinsically harmful",
        "universally benign",
    ):
        assert banned not in text
