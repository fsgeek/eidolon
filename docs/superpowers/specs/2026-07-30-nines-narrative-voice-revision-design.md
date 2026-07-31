# NINeS Narrative and Voice Revision Design

**Date:** 2026-07-30
**Status:** Approved in conversation
**Target:** `docs/paper/nines/main.tex`

## Purpose

Revise the NINeS manuscript so that it tells a rigorous, memorable story in
the human author's voice. The revision must preserve the paper's proofs,
registered measurements, evidence bounds, citations, and artifact
traceability.

The current draft contains valuable technical work and a stronger new thesis,
but recent machine-assisted editing introduced a recurrent rhetorical style:
frequent em-dashes, repeated contrast pivots, symmetrical formulations, and
paragraphs that announce their interpretation more than once. A surface
cleanup would leave much of that cadence in the argument's structure. The
revision will therefore rebuild the narrative around a layered investigation
and then restore the author's voice within that structure.

This work is the gate before the postponed terrestrial edge experiment. The
paper may make a structural applicability argument about edge systems and may
demonstrate the readout on an edge-shaped input. It will not present
terrestrial empirical validation.

The production-system census remains post-submission work because the
Cassandra evidence still needs a version pin. A dual-dashboard figure may be
reconsidered after the story, page count, and existing figures settle, but it
is not required to pass this revision gate.

## Narrative Contract

The paper begins with two operational questions:

1. Can a proposer acquire proposal authority?
2. Can it complete a commit?

The motivating puzzle is why a single reachable-replica count appeared to
answer both questions in familiar majority-based systems. This formulation
requires explicit scope. Phase symmetry makes the two phase-formability
predicates equal. A node count determines the shared predicate only for
threshold families such as majority, from a stated connectivity vantage
point. Node health, reachability, and phase-predicate equality are distinct
concepts.

The body reconstructs the investigation through the following discoveries:

1. **The anomaly.** Every node may be healthy while a service lacks a
   capability that a green dashboard appears to promise.
2. **The hidden pair.** Acquisition and commit are separate predicates over
   connectivity.
3. **The historical explanation.** Phase symmetry made those predicates
   identical. Majority quorums provided the familiar threshold instance in
   which one reachable-replica count answered both questions.
4. **The structural cause.** Flexible quorum designs allow the predicates to
   diverge while retaining the cross-phase intersection needed for safety.
5. **The exact characterization.** Containment between the minimal quorum
   families determines which capability gaps are reachable.
6. **The behavioral reversal.** The two mixed states have sharply different
   behavior in the registered single-decree experiment. These observations
   remain bounded to its retry policy, budget, and topology.
7. **The constructive response.** Crumbling-wall geometry can close selected
   gaps for selected tiers while keeping the hot path on the anchor tier.
8. **The boundary.** No Phase 2 threshold closes both gaps at every tier of the
   evaluated wall. Residual gaps arise from upper-tier participation
   obligations.
9. **The operational result.** Given a connectivity summary, the readout makes
   the two predicates and failed obligations interpretable. It does not detect
   failures, establish current authority, choose a response, or guarantee that
   an operator consults it.
10. **The open question.** Future work should ask whether coincidence can be
    extended farther up a structured topology without placing inter-tier
    latency back on the commit path.

The abstract discloses the answer. The body creates narrative energy through
successive reinterpretation and juxtaposition. It must not hide results solely
to create suspense.

The portable diagnostic left with the reader is:

> Can this system acquire authority, and can it commit?

## Legibility Definition

In this paper, a quorum construction is legible with respect to a supplied
connectivity summary when its phase capabilities and failed obligations can be
read from a compact structural representation without enumerating candidate
quorum subsets or attempting protocol execution. The wall supplies such a
representation: acquisition and commit formability and the unsatisfied tier
obligations can be determined in `O(tiers)` time.

The interface assumes that configuration and connectivity are already known.
It does not detect connectivity, establish current authority, select recovery
policy, or guarantee that an operator consults the result. Those are separate
operational problems.

This definition does not restore the superseded claim that the wall
"compensates every other tier with legibility." The registered gradient
falsified that framing: Moon and Mars retain `(0,1)` exposure at every `k`.
Legibility is load-bearing because it supplies interpretation where
construction reaches its limit.

## Constructive Role of the Wall

The wall is a useful design tool with a known boundary. The revision must
present its positive results before describing that boundary.

For the registered 5/1/1/3 wall:

- At `k=3`, the `(1,0)` gap is absent at every tier.
- At `k=3`, both gaps are absent for Earth under both connectivity readings.
- At `k=3`, both gaps are absent for LEO under the self-reachable reading.
- At `k=3`, `(0,1)` remains reachable for Moon and Mars.
- For `k <= 3`, `(1,0)` is absent at every tier.
- For `k >= 3`, `(0,1)` is absent for Earth and, under the self-reachable
  reading, for LEO.

Every statement that the wall "prevents" a gap must name the gap, threshold,
tier, and connectivity reading. The paper must explain the self-reachable
assumption and show what changes under unconstrained connectivity.

The design lesson is that wall geometry lets a designer choose where
coincidence matters most. The `k=3` configuration assigns it to the Earth
anchor and, under the colocated-acceptor model, adjacent LEO while keeping the
hot path on Earth. Moon and Mars retain exposure because their downward paths
include intermediate participation obligations. This provides a usable
design technique now and defines the next research problem.

## Manuscript Architecture

The target section sequence is:

1. **Introduction: the green-dashboard puzzle.** Present the symptom, the two
   operational questions, the answer, and the paper's contributions.
2. **Why one count once worked.** Introduce phase symmetry, majority as its
   familiar threshold instance, and the consequence of Flexible Paxos. Place
   the odd-cluster observation here: a cost-minimal threshold split can be
   phase-symmetric at odd `n`, while symmetry costs one additional participant
   across the phases at even `n`.
3. **Capability gaps.** Define `R1` and `R2`, prove the exact containment
   correspondence, and introduce the generic auditor.
4. **Putting the gaps on a wall.** Define the topology and construction,
   establish safety, and explain the participation-policy role of non-anchor
   witnesses.
5. **Where the wall works, and where it stops.** Present the positive `k=3`
   result, reusable threshold rules, the self-reachable sensitivity, and the
   residual Moon and Mars exposure. Before leaving the section, identify the
   residual `(0,1)` gap as the state associated with retry-budget exhaustion
   in the registered experiment that follows. Do not assign the state an
   intrinsic cost or valence.
6. **What happens inside the gaps.** Present the registered valence experiment
   and its bounded behavioral reversal.
7. **Reading the wall.** Present the readout and the supporting topology,
   baseline, liveness, connectivity, and crash-relaxation results.
8. **What remains unsolved.** Discuss performance-preserving prevention as an
   open problem. Scoped authority, gap-aware proposer behavior, and
   multi-anchor families may be mentioned as future directions without claims
   of efficacy.
9. **Related work, limitations, and conclusion.** Give foundational work
   attribution when first used and retain the full relationship discussion
   later. Close with the two-question diagnostic and the open problem.

This is a narrative ordering, not a claim about the chronology of discovery.
The prose must distinguish explanatory order from research chronology where
the difference could imply unsupported causation.

Detailed experimental parameters and claim-to-artifact traceability remain in
the appendix. Experimental method must stay close enough to each result for a
reader to evaluate it; storytelling does not justify separating a measurement
from its conditions.

## Authorial Voice Model

The local reference corpus consists of two primarily author-written published
papers, pre-2025 academic blog posts, and public expert declarations. The 2025
coauthored paper is a weak comparison signal because the author contributed to
it but is not its primary voice. The corpus is evidence of tendencies, not a
stylometric template.

The revision should:

- begin from observations, examples, and questions;
- show how one observation led to the next;
- use "we" for choices, measurements, and interpretations;
- gloss "acquire proposal authority" at first use as the leader-election step
  under Multi-Paxos while preserving the single-decree meaning used by the
  experiments;
- state uncertainty and evidentiary limits directly;
- place qualifications beside the claims they bound;
- prefer concrete mechanisms to compressed rhetorical labels;
- mix short conclusions with longer explanatory sentences;
- preserve appropriate differences between explanatory, formal, and
  experimental registers; and
- retain useful intellectual texture instead of making every sentence appear
  frictionlessly polished.

The revision should not reproduce incidental quirks, transcription artifacts,
blog informality, legal boilerplate, or the style of coauthors. The human
author's judgment is the final authority on whether a passage sounds like his
work.

## Narrative and Style Controls

- Let section order create the investigation. Do not describe the manuscript
  itself as a mystery.
- Reserve strong contrasts for genuine reversals supported by prior evidence.
- Give each surprise enough support before announcing it.
- Avoid repeating the thesis or interpretation after every result.
- End sections with the observation or question that makes the next section
  necessary.
- Attribute Flexible Paxos, crumbling walls, and other foundational ideas at
  first use even if the full related-work section appears later.
- Introduce coincidence as a named relationship before using language about
  spending it.
- Require new transitions to replace or remove existing exposition so that
  narrative connective tissue does not expand the manuscript.
- Check the rendered page count after each major section. The main paper must
  remain within the NINeS 12-page limit, excluding references and appendices.

Every em-dash and recurring contrast construction will be reviewed in context.
Counts are discovery aids only. There is no target count and there will be no
automatic replacement. The review also looks for slogan density, repeated
interpretive announcements, unnecessary personification, symmetrical lists
that add no information, and stock transitions such as repeated claims of a
"deeper" point.

The rewrite must preserve several insights at full prominence without
freezing their present wording: the significance of the proof's simplicity;
LEO's lower measured latency than Earth and the separation between obligation
and physical cost; the need to check both wall structure and network
reachability; the opposite-remediation result; and the odd-cluster
observation. Every current aphorism remains subject to the voice review.

## Epistemic Controls

The revision preserves these boundaries:

- `(0,1)` produced blocking under the registered single-decree experiment's
  retry policy and budget. The state is not intrinsically labeled harmful.
- `(1,0)` matched a healthy contender on every recorded metric in that
  experiment and injected an accepted value. This does not establish universal
  benignity.
- The paper makes no claim about how frequently either state occurs in
  deployed systems.
- The wall's non-anchor witnesses implement participation policy. They are not
  additional Paxos safety requirements.
- The readout interprets known connectivity. Detection, current authority, and
  service policy remain outside its interface.
- The edge mapping is a structural applicability argument and demonstration,
  not terrestrial evaluation.
- The revision may identify performance-preserving gap closure as an open
  problem. It must not imply that no such solution exists.

Claim preservation requires more than comparing individual sentences.
Reordering changes emphasis, implied causality, and the apparent scope of
evidence. Each revised section therefore receives an implication audit that
asks:

1. What would a careful reader infer from this ordering?
2. Which claims appear causal, general, novel, or empirically established?
3. Do those implications match the proofs, registrations, results, and prior
   work?

## Editing and Review Sequence

1. Build a paragraph-level narrative map of the current manuscript. Assign
   each paragraph a role such as question, observation, definition, proof,
   measurement, interpretation, qualification, transition, or repetition.
2. Identify material that must move, merge, remain fixed, or be removed.
3. Rewrite one representative narrative unit first. The introduction is the
   preferred pilot because it tests both voice and the layered investigation.
4. Obtain human review of the pilot before applying its voice and narrative
   choices to the remaining manuscript.
5. Revise the manuscript section by section. After each section, perform claim,
   implication, citation, and page-budget checks.
6. Audit the central per-tier liveness table. Test whether one common
   1800-second blackout condition produces a clearer table with a directly
   observed Mars result. Do not mix a Mars cell from 1800 seconds into a table
   whose other cells report the 900-second condition.
7. Perform the contextual AI-tell review after the narrative and voice pass.
8. Rebuild the traceability appendix against the final claim set.
9. Run the manuscript claim-language tests, full test suite, anonymization
   checks, and complete LaTeX build.
10. Conduct a final human cold read focused on voice, accessibility, narrative
   coherence, and places where the prose feels more certain than the evidence.

## Success Criteria

The revision is complete when:

- a networked-systems reader can state the two capability questions and explain
  why majority-style monitoring once collapsed them into one;
- the paper's sequence produces recognition and successive reinterpretation
  without withholding required scholarly information;
- the wall is presented as a concrete tool that closes named gaps in named
  configurations, followed by its exact boundary;
- legibility is defined in the introduction, instantiated by the wall's
  `O(tiers)` readout, and distinguished from detection, current authority, and
  policy;
- the readout's contribution and limits are both explicit;
- the open terrestrial experiment and performance-preserving prevention work
  remain clearly outside the paper's evidence;
- the human author recognizes the manuscript as his voice;
- no proof, measurement, evidence bound, attribution, citation, or artifact
  path has drifted;
- the main text remains within the venue page limit;
- all required tests and manuscript builds pass; and
- the final PDF remains double-blind and traceable to the approved artifacts.
