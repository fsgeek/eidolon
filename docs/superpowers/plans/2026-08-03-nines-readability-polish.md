# NINeS Readability Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Polish the completed NINeS manuscript through conceptual-access, continuity, and prose passes while preserving its structure, scientific claims, evidence, results, terminology, and scope.

**Architecture:** Treat the current clean manuscript at commit `53f8da1` as the scientific baseline and the approved design at `docs/superpowers/specs/2026-08-03-nines-readability-polish-design.md` as the editorial contract. Perform three independently reviewable prose passes, then audit administrative readiness and verify source, tests, claims, numbers, citations, and rendering before updating the submission recommendation.

**Tech Stack:** LaTeX (`pdflatex`, BibTeX, `pdftotext`, `pdftoppm`), Python 3.14 via `uv`, pytest, Git.

## Global Constraints

- Do not reorder, add, or remove major sections.
- Do not introduce a new claim, protocol, experiment, production-causality argument, terrestrial evaluation, or dynamic-membership result.
- Preserve every number, registered result, deviation, retry-policy bound, single-decree limitation, fixed-epoch boundary, citation, and claim-to-artifact mapping.
- Keep **phase-capability coincidence** as the sole formal name and **the smaller question** as narrative language.
- Keep GitHub and LogDevice in their distinct evidentiary roles.
- Keep 5/1/1/3 an analytical fixture rather than a deployment proposal.
- Permit sentence-level edits only when they remove a demonstrable decoding burden.
- Avoid net expansion where rewriting or deletion can solve the same problem.
- Use `apply_patch` for repository edits and `uv`, never `pip`.

---

## Task 1: Capture the Scientific and Structural Baseline

**Files:**
- Read: `docs/paper/nines/main.tex`
- Read: `docs/paper/nines/references.bib`
- Read: `tests/test_nines_claim_language.py`

**Interfaces:**
- Consumes: clean revision branch and approved polish specification.
- Produces: baseline hashes, section order, numerals, citation keys, and passing-test evidence used by every later audit.

- [ ] Record `sha256sum` for `main.tex` and `references.bib` and record the current commit.
- [ ] Extract section headings with `rg -n '^\\section'` and save the output in the execution log; later section order must match exactly.
- [ ] Extract all decimal/integer tokens and all `\cite{...}` occurrences from `main.tex` at `HEAD` for later comparison.
- [ ] Run `uv run pytest tests/test_nines_claim_language.py tests/test_anonymous_artifact.py -q` and require 27 passing tests.
- [ ] Run `uv run pytest` and require 130 passing tests.

## Task 2: Conceptual-Access Pass

**Files:**
- Modify: `docs/paper/nines/main.tex`, abstract, introduction, and opening of `Capability Gaps` only.
- Test: `tests/test_nines_claim_language.py` only if an existing wording assertion encodes prose rather than a claim boundary.

**Interfaces:**
- Consumes: the existing narrative and review synthesis.
- Produces: an abstract that explains the broken assumption before `formability`, an introduction that surfaces policy-dependent valence, and a plain-language entry to the formal predicate.

- [ ] Rewrite the abstract opening to identify the two operational questions and explain that one count happened to answer both before naming phase-capability coincidence.
- [ ] Compress the experimental abstract sentence to retain preregistration, reversal, single-decree scope, modeled policy/budget dependence, and direction without the seed count or repeated budget detail.
- [ ] Add one introduction sentence stating that the same structural direction can support availability under retained authority or impede progress under another policy; therefore direction, authority, and policy matter.
- [ ] Before `\operatorname{Form}`, add the ordinary-language question: whether the nodes currently reachable contain any configured quorum for the phase.
- [ ] Run the focused claim-language tests and the four-stage paper build.
- [ ] Inspect page 1 with `pdftotext -f 1 -l 1` and commit as `docs: ease entry into NINeS argument`.

## Task 3: Continuity and Decoding Pass

**Files:**
- Modify: `docs/paper/nines/main.tex`, first tuple uses by section, experiment table/caption, readout transition, and exact-boundary setup.
- Test: `tests/test_nines_claim_language.py` for durable decoding requirements where appropriate.

**Interfaces:**
- Consumes: the conceptually accessible front matter.
- Produces: locally recoverable tuple meanings, defined connectivity readings, memorable protocol-scope distinction, and explicit continuity between readout and wall.

- [ ] Gloss `(1,0)` as acquisition-without-commit and `(0,1)` as commit-without-acquisition at first substantive use in each relevant major section and compact result display.
- [ ] Define the unconstrained reading as ranging over all connectivity sets and the self-reachable reading as requiring the initiator's colocated acceptor before the boundary results.
- [ ] Tighten the single-decree/Multi-Paxos explanation so retained incumbent authority and per-decree Phase 1 exposure cannot be conflated.
- [ ] Add a forward pointer from the readout to the immediately following construction that generates its tier obligations.
- [ ] Explain that the identical `(1,0)` and healthy metrics arise because the deterministic fixture produces the same recorded healthy-proposer trace.
- [ ] Audit rhetorical questions; retain the reader's bet and reversal-to-readout question and convert any weaker repeated device into declarative momentum.
- [ ] Audit table and figure captions for locally recoverable tuple/fixture/scope meaning without expanding appendix calibration.
- [ ] Run focused tests, build, inspect pages 2--7, and commit as `docs: reduce decoding cost in NINeS paper`.

## Task 4: Prose and Cadence Pass

**Files:**
- Modify: `docs/paper/nines/main.tex` sentence-level prose only.

**Interfaces:**
- Consumes: semantically complete polished manuscript.
- Produces: reduced nesting, clearer referents, balanced cadence, and unchanged claims.

- [ ] Read the ten content pages from `pdftotext` without editing and mark only sentences with delayed subjects, ambiguous referents, repeated caveats, or three-plus nested qualifications.
- [ ] Rewrite marked sentences using shorter subjects and explicit referents; do not alter mathematical statements, numerical result sentences, quotations, or citations.
- [ ] Remove a caveat only when the same boundary remains adjacent to the claim or explicit in `Threats to Validity and Limitations`.
- [ ] Compare word count and require no material net expansion; investigate any increase above 1 percent.
- [ ] Run focused tests, build, inspect first four pages and all section endings, and commit as `docs: polish NINeS prose cadence`.

## Task 5: Administrative Readiness Audit

**Files:**
- Create: `docs/superpowers/notes/2026-08-03-nines-administrative-readiness.md`
- Read: repository identity and provenance files; do not alter scientific manuscript merely to hide a permitted system name.

**Interfaces:**
- Consumes: final polished manuscript and NINeS CFP requirements.
- Produces: anonymization findings, author-attestation items, and a factual LLM-use disclosure draft.

- [ ] Search the manuscript and packaged artifact for author names, usernames, email addresses, home paths, affiliations, acknowledgments, and self-identifying repository URLs.
- [ ] Record `Eidolon` as a permitted system name with a possible one-search deanonymization risk requiring author judgment.
- [ ] Record concurrent-review status as an author attestation item because repository evidence cannot prove external submission state.
- [ ] Draft an LLM-use disclosure stating the actual roles visible in repository provenance: collaborative framing, editorial revision, review synthesis, implementation assistance, and verification; authors retained claim and submission responsibility.
- [ ] Commit as `docs: audit NINeS administrative readiness`.

## Task 6: Completion and Submission Audit

**Files:**
- Verify: `docs/paper/nines/main.tex`
- Verify: `docs/paper/nines/references.bib`
- Verify: `docs/paper/nines/main.pdf`
- Verify: `tests/test_nines_claim_language.py`
- Modify: `docs/superpowers/notes/2026-08-02-nines-go-no-go.md`

**Interfaces:**
- Consumes: all three polish passes and administrative findings.
- Produces: authoritative evidence that polishing changed accessibility but not scientific content, plus an updated GO or NO-GO recommendation.

- [ ] Compare section-heading sequence against Task 1 and require exact equality.
- [ ] Compare citation-key multiset and all scientific numerals against `53f8da1`; manually classify every textual-number difference caused by ordinary prose rather than results.
- [ ] Run terminology and prohibited-claim searches for competing names, causal overreach, intrinsic valence, mobile-solution claims, and deployment recommendations.
- [ ] Run focused tests and require all pass; run the full suite and require all pass.
- [ ] Build with `pdflatex`, BibTeX, and two final `pdflatex` passes; require no undefined citation/reference, duplicate label, or overfull box.
- [ ] Render and inspect pages 1--4, the experiment table, readout, wall figure, boundary table, conclusion, references, and appendices.
- [ ] Confirm the conclusion remains within the 12 content-page venue limit.
- [ ] Update the go/no-go record with polish evidence, administrative attestations, and the final recommendation.
- [ ] Commit as `docs: verify final NINeS readability polish` and leave the worktree clean.
