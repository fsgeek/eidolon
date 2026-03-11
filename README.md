# Eidolon (formerly VMTPsim)

Discrete-event simulation framework for studying quorum-based consensus under
extreme latency and intermittent disconnection.

## Current Focus

This project now centers on Paxos and quorum-system design in topology-aware,
high-latency environments (Earth, Moon, LEO, Mars).

Core thesis:
Paxos safety depends on quorum intersection properties, not simplistic
"majority voting." Quorum *shape* can encode physical topology.

## What Is Implemented

- Paxos acceptor/proposer/learner simulation (`paxos.py`)
- Quorum system variants:
  - Majority
  - Flexible Paxos (`q1 + q2 > n`)
  - Grid quorum variants
  - Crumbling wall quorum (`quorums.py`)
- Topology-aware network model with location links and partitions (`datacenter.py`)
- Interplanetary progression demos (`demo_step_1.py` ... `demo_step_9.py`)
- Step 9 conjunction experiment:
  - Hard blackout model
  - Lagrange repeater refinement
  - CSV outputs and sweep/plot tooling

## Recent Status (March 10, 2026)

- Structured quorum semantics are enforced in the proposer path:
  - quorum checks use `is_phase1_quorum` / `is_phase2_quorum`
  - crumbling-wall phase constraints are active in execution
- Step 9 artifact pipeline is operational:
  - single-run CSV export
  - parameter sweep over Mars latency and blackout duration
  - SVG figure generation without external plotting dependencies

## Repository Layout

- Core simulation:
  - `paxos.py`
  - `quorums.py`
  - `datacenter.py`
  - `network.py`
- Interplanetary demos:
  - `demo_step_7.py` (hierarchical Earth/Mars)
  - `demo_step_8.py` (crumbling wall full topology)
  - `demo_step_9.py` (conjunction blackout vs repeater)
- Experiment tooling:
  - `experiments/step9_sweep.py`
  - `experiments/plot_step9.py`
  - `experiments/step9_liveness.py`
  - `experiments/plot_step9_liveness.py`
- Research docs:
  - `docs/workshop-paper-roadmap.md`
  - `docs/step9-repro.md`
  - `docs/papers/224964.224978.pdf` (Peleg & Wool)

## Quick Start

```bash
uv sync
```

Run Step 9 baseline/repeater comparison:

```bash
uv run python demo_step_9.py \
  --mars-latency-s 186 \
  --blackout-start-s 600 \
  --blackout-duration-s 900 \
  --sim-end-s 3000 \
  --reconcile-interval-s 120 \
  --seed 42 \
  --csv results/step9/single_run.csv
```

Run the sweep and generate plots:

```bash
uv run python experiments/step9_sweep.py \
  --mars-latencies-s "186,750,1342" \
  --blackout-durations-s "300,900,1800" \
  --seeds "40,41,42,43,44" \
  --output results/step9/step9_sweep.csv \
  --aggregate-output results/step9/step9_sweep_ci.csv

uv run python experiments/plot_step9.py \
  --input results/step9/step9_sweep.csv \
  --output-dir results/step9/plots

uv run python experiments/step9_liveness.py \
  --mars-latency-s 186 \
  --timeout-s "120,240,360,500,720" \
  --blackout-durations-s "300,900,1800" \
  --seeds "40,41,42,43,44" \
  --output results/step9/step9_liveness.csv \
  --aggregate-output results/step9/step9_liveness_ci.csv

uv run python experiments/plot_step9_liveness.py \
  --input results/step9/step9_liveness_ci.csv \
  --output-dir results/step9/plots
```

## Outputs

- Sweep CSV: `results/step9/step9_sweep.csv`
- Sweep CI CSV: `results/step9/step9_sweep_ci.csv`
- Liveness CSV: `results/step9/step9_liveness.csv`
- Liveness CI CSV: `results/step9/step9_liveness_ci.csv`
- Single-run CSV: `results/step9/single_run.csv`
- Plots: `results/step9/plots/*.svg`

## References

- RFC 1045: VMTP Specification (`docs/rfc1045.txt`)
- Peleg & Wool (1995): Crumbling Walls (`docs/papers/224964.224978.pdf`)
- Repro commands: `docs/step9-repro.md`
