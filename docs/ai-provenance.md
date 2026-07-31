# AI provenance

How AI was used in this project, written so a skeptical reader can check
rather than trust. Roles are named instead of people so this document can
travel with an anonymized artifact bundle; the git history resolves the
identities for anyone who needs them.

## Thesis

AI-assisted work is exactly as good as the adversarial pressure applied
to it — no better. That cuts both ways: this repository documents both
the pressure working and the failures that shipped when it lapsed. The
distinction between generative use and "AI slop" is not who typed; it is
whether provenance is legible. Slop is fluent assertion nobody can trace.
The remedy is not a quality claim (which a slop process would also emit,
fluently) but a record that makes every claim traceable to an artifact a
reader can rerun. Whether that standard is met here is decided per claim,
by checking — this document only tells you where to press.

## Roles

- **Human author** — research direction, arbitration, and acceptance.
  Every claim-affecting change is gated on explicit human acceptance
  (e.g., regenerated sweep data entered the record only at commit
  `c423435`, after a BLOCKED verdict was returned for a human decision).
  The human author also holds the submission, disclosure, and
  publication decisions.
- **Claude instances (Anthropic), across many sessions** — implementation,
  experiment design and execution, drafting, and repair campaigns. Each
  session leaves handoff records; successors treat predecessors' claims
  as testimony to verify, not facts to assume.
- **Codex instances (OpenAI)** — implementation, formal and artifact
  audits, and machine-drafting of the current NINeS revision from a
  human-approved design. Codex produced the first revised front matter;
  the human author reviewed the claim-affecting prose before the body
  was brought into alignment with it.
- **Claude as a cross-model reviewer** — independent editorial and
  technical review of the approved design, front matter, formal section,
  related-work boundary, and evidence language. Findings were checked
  against proofs, source papers, and repository artifacts before being
  applied; agreement alone was not treated as evidence.
- **A multi-judge review harness ("rikuy"), run by the research
  supervisor** — panels of independent reviewer personas whose findings
  are then adjudicated against repository ground truth, not accepted on
  authority. Verified findings are applied; unverified ones are recorded
  as rejected, with reasons.

## How to audit a claim

1. Pick any empirical claim in the paper (`docs/paper/nines/main.tex`).
2. Follow it to the traceability appendix, which names the producing
   artifact and script.
3. Open the named CSV under `results/`, or rerun it: reproduction
   commands are in `docs/step9-repro.md`.
4. In the public repository, check the chronology: research commits are
   GPG-signed and OpenTimestamps-stamped, so the order of claim, evidence,
   and correction cannot be quietly rewritten. Signing identities and
   timestamp receipts are intentionally absent from the anonymous bundle.

The formal claims follow the same rule: the quorum-intersection proof is
stated in the paper as primary, and the TLA+ specifications under `tla/`
check exactly what the paper says they check — no more. That sentence is
itself the product of a correction (see ledger).

## Process gates

- **Design before revision.** The capability-first narrative, exact
  four-way theorem, evidence bounds, and artifact interfaces were written
  into a design and execution plan before the current introduction or
  implementation was drafted. The introduction and subsequent body
  revision were machine-drafted from that approved contract, then reviewed
  by the human author and independently by Claude and Codex.
- **Editorial triangulation.** The narrative and voice revision used an
  approved editorial contract, a paragraph-level claim and implication map,
  and a human-reviewed introduction pilot. One model family reviewed a draft
  substantially written by that family; a different model family evaluated
  the review and led the rewrite. Agreement raised confidence but did not
  establish truth. Disagreements were adjudicated against proofs and
  artifacts, while the human author retained authority over voice and
  claim-affecting prose.
- **Expected outputs before implementation.** The generic auditor's
  semantic cases, witnesses, wall readings, and discrepancy rules were
  committed before its code or tests existed. The included pre-registration
  note is
  `docs/superpowers/notes/2026-07-30-quorum-auditor-preregistration.md`.
  Its optimized classifier is cross-checked by an independent connectivity-
  state enumerator and by 129,032 exhaustive small-universe cases.
- **Pre-registration.** The dueling-proposer experiment was designed by
  an adversarial pre-mortem before any code: four critics produced a
  design contract with predictions registered in advance. The included
  pre-registration and results notes preserve that boundary. Two central
  predictions were falsified by the data and are retained as findings,
  not rewritten.
- **Acceptance gates.** Regeneration of paper data under a repaired
  harness halted at a BLOCKED verdict
  (`docs/superpowers/notes/2026-07-21-regeneration-delta.md`: fifteen
  claim-affected rows) and resumed only on human acceptance
  (`c423435`).
- **Adversarial review, cross-model and multi-round**, with findings
  verified against the repository before application. Verification runs
  in both directions: reviewer findings have been rejected when the
  artifacts contradicted them, and review of the reviewers' own
  spot-checks caught a stale-artifact error there too.
- **Claim-language and package audits.** Regression tests prohibit the
  superseded exactly-one, unconditional-livelock, and only-mitigation
  formulations; the final manuscript is rebuilt through the full LaTeX
  citation cycle. The anonymous artifact is created from a positive
  allowlist, scanned for identity-bearing content, and checked against
  every path in the paper's traceability appendix.

## Corrections that cost something (selected)

The signature of a working process is corrections recorded
contemporaneously, against the work's own interest. Samples, each
checkable:

- **Two pre-registered predictions falsified** (durable promise state;
  retry-budget ceiling) — kept as findings; the falsifications are the
  result.
- **A BLOCKED verdict eleven days before a deadline** that stopped
  regenerated data from entering the paper until fifteen claim-affected
  rows were individually attributed and accepted (`c423435`, delta
  report above).
- **An attribution withdrawn.** A repair commit was initially credited
  with a 92%→100% shift; probe runs showed the values predated the
  ablation window, and the attribution was retracted in the delta
  report (§13) rather than left standing.
- **A verification claim deleted.** A 6.5-billion-state partial TLA+
  exploration was removed from the paper when checking revealed it had
  model-checked a superseded construction; the remaining claims were
  narrowed to what the specifications actually contain, then the
  tractable gap was closed by extending the spec (`3ef3b2c`: 27,921
  states, complete).
- **A baseline built against the paper's interest.** A reviewer called
  the flat-quorum comparison a strawman; instead of arguing, a
  competitive majority-quorum baseline was implemented and run
  (`20a64aa`). It matched the wall's headline liveness — and the paper
  now says so, resting its contribution on what the measurement actually
  supports.

## Failures that shipped

Pressure lapses; when it does, errors survive. Recording them is the
point:

- A results-table error in the arXiv version survived **five internal
  review rounds and months in public** before fresh outside review
  caught it (fixed in the draft at `aed529e` and in arXiv v2). Internal
  review saturates; independent pressure was the fix.
- The repository carried a **raw/aggregate CSV mismatch from March**
  (`results/step9/`, documented in the delta report §0): an aggregate
  file silently overwritten by a run with a defaulted output path,
  unnoticed because provenance checks covered the raw file only. The
  same default-path trap struck a second time during the baseline run
  before the default was fixed (`20a64aa`).
- The paper's own traceability appendix **twice cited a stale artifact**
  that a careful reader would have reproduced wrong numbers from — found
  once by a reviewer's failed spot-check, once by outside review.

## Limits of this document

This narrative is self-reported and was itself drafted by an AI
instance. The tamper-evident parts of the record are the signed,
timestamped commits and the artifacts they contain — not this file.
If this document and the repository ever disagree, believe the
repository, and count the disagreement as one more finding.
