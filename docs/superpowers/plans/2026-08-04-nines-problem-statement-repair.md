# NINeS Problem-Statement Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the paper's problem understandable to a general networking reader through two approved prose replacements.

**Architecture:** Preserve the manuscript's narrative and technical structure. Replace only the abstract opening and the introduction's second paragraph, then regenerate and inspect the PDF.

**Tech Stack:** LaTeX, `latexmk`, `pdftotext`, pytest via `uv`.

## Global Constraints

- Do not change the GitHub narrative, LogDevice bridge, theorem, experiment, wall, section order, evidence, terminology after its first definition, claims, or scope.
- Treat `docs/superpowers/specs/2026-08-04-nines-problem-statement-repair-design.md` as authoritative, including the author's parenthetical wording.
- Preserve all unrelated working-tree changes.

---

### Task 1: Apply and verify the approved prose repair

**Files:**
- Modify: `docs/paper/nines/main.tex`
- Regenerate: `docs/paper/nines/main.pdf`
- Verify: `tests/test_nines_claim_language.py`
- Verify: `tests/test_anonymous_artifact.py`

**Interfaces:**
- Consumes: the exact approved prose in the design document.
- Produces: a manuscript whose abstract explains the operational shortcut without specialist terminology and whose introduction explicitly states the problem and research questions.

- [ ] **Step 1: Replace the abstract opening**

Replace the first four sentences through “even though the protocol remains safe” with the exact approved abstract opening. Continue with the existing containment sentence and leave the remainder unchanged.

- [ ] **Step 2: Replace the introduction's second paragraph**

Replace the paragraph beginning “We approach that mismatch” with the exact approved introduction text, preserving `(and does not)`.

- [ ] **Step 3: Audit the source diff**

Run:

```bash
git diff -- docs/paper/nines/main.tex
```

Expected: exactly the two approved prose locations differ.

- [ ] **Step 4: Run focused regression tests**

Run:

```bash
uv run pytest tests/test_nines_claim_language.py tests/test_anonymous_artifact.py -q
```

Expected: 27 tests pass.

- [ ] **Step 5: Rebuild and inspect**

Run:

```bash
cd docs/paper/nines
latexmk -g -pdf -interaction=nonstopmode -halt-on-error main.tex
```

Expected: exit status 0, with no undefined references, undefined citations, multiply defined labels, or overfull boxes. Render and inspect page 1; confirm the conclusion remains on content page 10.

- [ ] **Step 6: Verify final scope**

Run `git diff --check`, the focused tests again if verification changes any source, and inspect `git status --short`. Do not commit or discard unrelated author changes.
