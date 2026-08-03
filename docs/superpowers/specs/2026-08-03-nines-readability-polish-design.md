# NINeS Readability Polish Design

**Date:** 2026-08-03  
**Status:** Approved in conversation; pending written-spec review  
**Manuscript:** `docs/paper/nines/main.tex`

## Objective

Make the completed NINeS narrative easier to enter, decode, skim, and retain
without reopening its structure or scientific design. A reader should reach the
same claims through a smoother path. No result, evidentiary boundary, section
role, formal term, or scope changes.

## Non-Negotiable Constraints

- Do not reorder, add, or remove major sections.
- Do not introduce a new claim, protocol, experiment, production-causality
  argument, terrestrial evaluation, or dynamic-membership result.
- Preserve every number, registered result, deviation, retry-policy bound,
  single-decree limitation, fixed-epoch boundary, citation, and
  claim-to-artifact mapping.
- Keep **phase-capability coincidence** as the sole formal name and **the smaller
  question** as narrative language.
- Keep GitHub and LogDevice in their distinct evidentiary roles.
- Keep 5/1/1/3 an analytical fixture rather than a deployment proposal.
- Permit sentence-level edits anywhere only when they remove a demonstrable
  decoding burden. Do not polish merely to substitute stylistic preference.
- Avoid net expansion where rewriting or deletion can solve the same problem.

## Pass 1: Conceptual Access

1. Rewrite the abstract's entrance around the broken operational assumption:
   one count happened to answer acquisition and commit together. Define the two
   questions before using `formability` as compressed terminology.
2. Remove detailed experimental parameters from the abstract unless required to
   keep the behavioral claim honest. Retain the registered, bounded nature of
   the result.
3. Promote the practitioner result into the introduction: a structural mixed
   state has no policy-independent valence, so detecting “a gap” is insufficient
   without direction, authority, and policy.
4. Before `\operatorname{Form}`, explain in ordinary language that the formal
   question is whether the nodes currently reachable contain any configured
   quorum for the phase.

## Pass 2: Continuity and Decoding

1. Gloss `(1,0)` and `(0,1)` at the first substantive use in each major section
   and in captions or compact tables where the tuple would otherwise require
   backward lookup.
2. Define the unconstrained and self-reachable readings in one crisp sentence
   before their first result.
3. Make the single-decree/Multi-Paxos distinction memorable: an authorized
   Multi-Paxos leader may continue Phase 2 in `(0,1)` until authority must be
   reacquired; the single-decree fixture exposes missing Phase 1 on every
   decree.
4. State that the construction generating the readout's tier obligations follows
   immediately after the readout.
5. Explain why the `(1,0)` and healthy arms have exactly identical recorded
   metrics: under the deterministic fixture, they induce the same recorded
   healthy-proposer trace.
6. Audit headings, section openings and endings, captions, and rhetorical
   questions as the paper's skim layer. Retain the reader's bet and the
   reversal-to-instrument question; convert weaker questions if repetition makes
   the device visible.

## Pass 3: Prose and Cadence

- Tighten sentences with excessive nesting, delayed subjects, ambiguous
  referents, or several qualifications before the main claim.
- Place bounds adjacent to the claim they limit without making the caveat the
  reader's first encounter with the result.
- Alternate dense formal explanation with short interpretive sentences where
  useful.
- Remove repeated caveats once one local statement and the limitations section
  preserve the boundary.
- Preserve the manuscript's present voice; do not normalize it into generic
  conference prose.

## Administrative Audit

- Confirm double-blind presentation and search for author identity leakage.
- Record that `Eidolon` is a permitted system name but flag its deanonymization
  risk for the authors' final judgment.
- Confirm that no concurrent peer-review conflict is discoverable from the
  repository; because external submission state is not locally provable, report
  this as an author attestation item rather than claiming it is cleared.
- Prepare a concise factual summary of LLM involvement suitable for the NINeS
  HotCRP disclosure; do not submit it externally.

## Verification

1. Compare every numeral and citation key before and after the polish.
2. Run focused claim-language and anonymization tests, then the full suite.
3. Build with `pdflatex`, BibTeX, and two final `pdflatex` passes.
4. Require no undefined citations/references, duplicate labels, or overfull
   boxes.
5. Inspect the rendered first four pages, every result table, the readout, and
   all section transitions.
6. Re-run prohibited-claim and terminology searches.
7. Update tests only when they encode superseded wording rather than a scientific
   boundary; document the reason in the final handoff.

## Acceptance Criteria

- The paper's structure and supported claims are unchanged.
- The abstract explains the broken assumption before specialized terminology.
- A non-specialist can interpret each tuple at its point of use.
- The policy-dependent valence result is visible in the introduction.
- Single-decree and Multi-Paxos consequences cannot reasonably be conflated.
- The manuscript compiles cleanly, all tests pass, and visual inspection finds
  no regression.
- The final handoff includes the polished PDF, a concise change summary,
  verification evidence, administrative attestation items, and an updated
  submit/defer recommendation.
