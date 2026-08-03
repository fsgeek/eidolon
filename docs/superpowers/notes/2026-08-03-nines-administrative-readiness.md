# NINeS 2027 Administrative Readiness

Date: 2026-08-03

This audit checks the current manuscript against the administrative requirements in the official NINeS 2027 call for papers. It is not a substitute for the authors' attestations about external submission state or institutional policy.

## Verified from the CFP

- Deadline: August 6, 2026, AoE.
- Main-text limit: 12 pages; references and appendices do not count.
- Review is double-blind and requires a good-faith, non-destructive anonymization effort. A system name may remain when it matters to evaluation.
- Concurrent submission of substantially similar work is prohibited when review periods overlap.
- Authors must honestly disclose LLM use through the HotCRP questions.
- Authors must attest compliance with applicable ethical and institutional requirements.

Source: <https://nines-conference.org/cfp> (checked 2026-08-03).

## Manuscript and artifact checks

- The rendered paper identifies its authors only as `Paper #98`.
- The paper contains no author name, username, email address, affiliation, acknowledgment, local filesystem path, or author-controlled repository URL found by the identity scan.
- URLs in the bibliography point to cited third-party sources (GitHub's incident report and Meta's LogDevice account), not to an author identity.
- `Eidolon` remains as the simulator name in the limitations and appendix. The CFP permits non-destructive system names, but a reviewer could search a distinctive public system name. Retaining it is therefore an explicit author judgment: it improves artifact legibility at a possible deanonymization cost.
- The anonymous artifact is generated from a positive allowlist. Its automated tests reject known identity-bearing strings, exclude git/timestamp material, require deterministic packaging, and verify that the manifest covers every artifact named in the traceability appendix.
- The paper's content ends on page 10, within the 12-page main-text limit; references and appendices follow.

## Required author attestations before submission

The repository cannot establish these external facts. The submitting author should confirm each one in HotCRP:

1. No substantially similar work is under review at another peer-reviewed venue whose review period overlaps NINeS. If the relationship to an earlier or current Eidolon submission is uncertain, contact the PC chairs before submitting.
2. The work complies with applicable institutional ethical requirements. The manuscript reports simulations, formal models, and analysis of public production accounts; it does not report a human-participant experiment, but the institutional attestation remains the authors' responsibility.
3. The authors intentionally accept or remove the residual searchability risk from the `Eidolon` name.

## Draft LLM-use disclosure

Compact form:

> We used several LLM systems (including OpenAI Codex, Anthropic Claude, Google Gemini, DeepSeek, Grok, and Kimi) as collaborative tools for implementation, experiment and artifact review, narrative restructuring, editorial revision, and independent readability critiques. The human authors set the research direction, selected and revised the framing, adjudicated model disagreements, checked claims against proofs, source papers, code, and generated artifacts, and retain responsibility for every claim and for the submitted text. Pre-registered predictions that the experiments falsified remain reported as falsifications.

If the form permits more detail, add:

> LLM output was not treated as evidence. Claim-affecting suggestions were checked against repository ground truth; the paper's traceability appendix maps empirical and formal claims to reproducible artifacts. The repository also records material corrections, rejected suggestions, and the provenance of AI-assisted work in `docs/ai-provenance.md`.

The disclosure should be adjusted to match the exact HotCRP questions, but it should not be narrowed to “minor editorial help”: the documented use includes implementation, experimental design/review, drafting, and editorial synthesis.

## Readiness result

Administrative readiness is **conditional GO**. The manuscript and local artifact satisfy the checks available in the repository. Submission still depends on the three author attestations above, especially concurrent-submission status and the deliberate decision about retaining the `Eidolon` name.
