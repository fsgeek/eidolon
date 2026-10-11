# CLAUDE.md

Stable facts only. Project status, decisions, and history live in memory
(qhaway / khipumaq), not here — this file rots when it narrates.

## What this is

Eidolon: a discrete-event simulator and analysis tooling for flexible-quorum
consensus. Current paper: "Legible Consensus: Capability Gaps in Flexible
Quorums" (NINeS '27 submission #98). Began as a VMTP (RFC 1045) simulator,
then Earth–Mars Flexible Paxos; the earlier arXiv paper is `docs/paper/main.tex`.

## Environment

Use `uv`, never `pip`. Python >= 3.14.

```bash
uv sync
uv run pytest
uv run python <script>
```

## Paper

- Live paper: `docs/paper/nines/main.tex` (+ `references.bib`). The appendix
  "Claim-to-Artifact Traceability" maps every claim to a script and result
  file — start there to find code behind a claim.
- `docs/paper/nines/nines-98.pdf` is the as-submitted (anonymized) copy.
- Build: `cd docs/paper/nines && pdflatex main && bibtex main && pdflatex main && pdflatex main`
- `\balance` must stay in the traceability appendix's prose, before its
  table float; placed earlier it corrupts the appendix layout.

## Code map

- `paxos.py`, `quorums.py` (`CrumblingWallQuorum`), `network.py`,
  `datacenter.py` — simulator core.
- `capability.py` — static capability classifier for the wall.
- `duel.py`, `flip.py` — contention experiments; `flip.py` is pre-registered
  (`docs/superpowers/notes/2026-07-29-midround-flip-preregistration.md`).
- `quorum_audit.py` — design-time containment audit for quorum families.
- `experiments/` — sweep drivers; `results/` — their CSVs; `tla/` — TLA+ specs.
- `demo_step_*.py` — the historical Earth–Mars progression.
- `artifact-manifest.txt` — files included in the reviewer artifact.

## Notation

- 5/1/1/3 topology: 5 Earth, 1 LEO, 1 Moon, 3 Mars (Earth = tier 0, Mars = tier 3).
- Capability states are written (R1, R2): R1 = can acquire (Phase 1),
  R2 = can commit (Phase 2). (1,0) and (0,1) are the mixed states.
- Wall: Phase 2 = k-of-|E| Earth nodes; a tier-i Phase 1 needs one node from
  each tier at or below i plus >= |E|-k+1 Earth nodes.
