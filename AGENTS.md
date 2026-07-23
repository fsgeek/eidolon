# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

Eidolon is a discrete-event simulation framework for studying quorum-based consensus under extreme latency and intermittent disconnection, motivated by Earth--Mars operations.

**Core thesis**: Paxos safety depends on quorum intersection properties, not majority voting. Quorum *shape* can encode physical topology, keeping the hot commit path local to the fast tier while the rarer election path spans all tiers.

**Current state**: Paper revision targeting arXiv submission. The simulator, experiments, and sweep data are complete. The paper (`docs/paper/main.tex`) is being refined for framing and scientific rigor.

**History**: This repository began as a VMTP (RFC 1045) transport protocol simulator ("VMTPsim") before evolving into its current focus on topology-aware Flexible Paxos.

## Development Environment

```bash
# Use uv, not pip
uv sync              # Install dependencies
uv run python ...    # Run scripts
uv run pytest        # Run tests
```

**Important**: This project uses `uv` for Python environment management. Do not use `pip` directly. Requires Python >= 3.14.

## Key Documents

- `docs/paper/main.tex` - The paper (compiles with pdflatex + bibtex)
- `docs/paper/references.bib` - Bibliography (15 entries)
- `docs/step9-repro.md` - Reproduction commands for all experiments
- `docs/workshop-paper-roadmap.md` - Paper development roadmap
- `docs/rfc1045.txt` - Original RFC 1045 specification (historical)

## Repository Layout

### Core simulation
- `paxos.py` - Acceptor, Proposer, QuorumSystem base class, FlexibleQuorum, MajorityQuorum
- `quorums.py` - GridQuorum, FlexibleGridQuorum, CrumblingWallQuorum (topology-aware)
- `datacenter.py` - Network topology builder (five_dc_topology)
- `network.py` - Asynchronous network model with delay, jitter, partitions
- `entity.py` - Entity registry for transport-level endpoints

### Interplanetary demos (progressive)
- `demo_step_1.py` through `demo_step_9.py` - Building from simple to conjunction blackout
- `demo_step_9.py` is the primary experiment driver

### Experiment tooling
- `experiments/step9_sweep.py` - Parameter sweep (Mars latency x blackout duration)
- `experiments/step9_liveness.py` - Timeout threshold sweep
- `experiments/plot_step9.py` - Sweep visualization
- `experiments/plot_step9_liveness.py` - Liveness envelope plots

### Results
- `results/step9/step9_sweep.csv` / `step9_sweep_ci.csv` - Sweep raw/aggregated
- `results/step9/step9_liveness.csv` / `step9_liveness_ci.csv` - Liveness raw/aggregated
- `results/step9/plots/` - SVG/PDF figures

## Key Architectural Concepts

**5/1/1/3 topology**: 5 Earth, 1 LEO, 1 Moon, 3 Mars acceptor nodes (10 total). Use this notation consistently.

**Three consensus scopes**:
- Earth-local: Flexible Paxos (q1=4, q2=2) over 5 Earth nodes
- Mars-local: Majority quorum over 3 Mars nodes
- Global reconciliation: CrumblingWallQuorum spanning all tiers

**Quorum families**:
- Phase 2 (hot path): Q2 = {E} (full Earth tier only)
- Phase 1 (elections): Must span all 4 tiers AND have |Q| >= 6
- Intersection guaranteed because Q1 contains an Earth node and Q2 = E

**CrumblingWallQuorum** (`quorums.py`): `is_phase1_quorum` enforces tier-spanning + minimum size. `is_phase2_quorum` checks that the full fast tier (Earth) is present. During blackout, Phase 1 fails because Mars nodes are unreachable — this is a liveness failure, not a safety violation.

## Building the Paper

```bash
cd docs/paper
pdflatex main && bibtex main && pdflatex main && pdflatex main
```

## Running Experiments

See `docs/step9-repro.md` for full reproduction commands, or:

```bash
# Single run
uv run python demo_step_9.py --mars-latency-s 186 --blackout-duration-s 900 --seed 42

# Full sweep (50 seeds x 18 points)
uv run python experiments/step9_sweep.py --seeds "40,41,...,89" --output results/step9/step9_sweep.csv
```
