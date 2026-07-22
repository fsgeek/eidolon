# Step 9 Reproducibility

This document captures exact commands for the conjunction/repeater experiments and related sweeps.

## Environment

```bash
uv sync
```

## Paper Build

Build the manuscript from `docs/paper/` with `latexmk`:

```bash
latexmk -pdf main.tex
```

## Single Run (Baseline + Repeater)

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

## Parameter Sweep (Global Proposer, Crumbling Wall)

```bash
uv run python experiments/step9_sweep.py \
  --mars-latencies-s "186,750,1342" \
  --blackout-durations-s "300,900,1800" \
  --blackout-start-s 600 \
  --sim-end-s 4000 \
  --reconcile-interval-s 120 \
  --seeds "40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89" \
  --output results/step9/step9_sweep.csv \
  --aggregate-output results/step9/step9_sweep_ci.csv
```

The temporal window is **automatically scaled** to ensure sufficient time for each phase and for recovery. Scaling information is captured in the CSV columns: `phase_timeout_s`, `pre_window_s`, `post_window_s`, `temporally_scaled`.

## Plot Generation (SVG)

```bash
uv run python experiments/plot_step9.py \
  --input results/step9/step9_sweep.csv \
  --output-dir results/step9/plots
```

## Liveness Envelope Sweep (Phase Timeout Sensitivity)

```bash
uv run python experiments/step9_liveness.py \
  --mars-latency-s 186 \
  --timeout-s "120,240,360,500,720" \
  --blackout-durations-s "300,900,1800" \
  --blackout-start-s 600 \
  --sim-end-s 4000 \
  --reconcile-interval-s 120 \
  --global-max-rounds 1 \
  --seeds "40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89" \
  --output results/step9/step9_liveness.csv \
  --aggregate-output results/step9/step9_liveness_ci.csv

uv run python experiments/plot_step9_liveness.py \
  --input results/step9/step9_liveness_ci.csv \
  --output-dir results/step9/plots
```

## Per-Tier Liveness Sweep (Sparse + Full Coverage)

Tests which initiating tiers retain global consensus during blackout. Runs both sparse (LEO sees 3/5 Earth DCs, Mars sees 2/5) and full-coverage (LEO and Mars see all Earth DCs) topologies, and both blackout scenarios.

```bash
uv run python experiments/tier_liveness_sweep.py \
  --mars-latencies-s "186,750,1342" \
  --blackout-durations-s "300,900,1800" \
  --blackout-start-s 600 \
  --sim-end-s 4000 \
  --reconcile-interval-s 120 \
  --seeds "40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89" \
  --output results/tier_liveness/tier_sweep.csv
```

The aggregated results (mean ± 95% CI per tier/topology/scenario/mars_latency/blackout_duration) are automatically written to `results/tier_liveness/tier_sweep_ci.csv`.

## Crash-Tolerance Sweep (Relaxed Quorum Configurations)

Sweeps crash count and quorum configuration under the crumbling wall. Evaluates strict (standard) and relaxed (k-of-5 Earth) Phase 2 quorums with varying numbers of crashed Earth nodes.

```bash
uv run python experiments/step10_sweep.py \
  --seeds "40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89" \
  --output results/step10/step10_sweep.csv \
  --aggregate-output results/step10/step10_sweep_ci.csv
```

Scenarios tested: strict (all 5 Earth) and relaxed Phase 2 quorums (4-of-5 or 3-of-5) with 0–2 crashed Earth nodes. The temporal window is automatically scaled to ensure sufficient time for recovery after crashes.

## Output Files

### Step 9 Crumbling Wall Sweep
- `results/step9/step9_sweep.csv` — raw per-seed results (45 columns)
- `results/step9/step9_sweep_ci.csv` — mean ± 95% CI aggregated per (scenario, mars_latency, blackout_duration)

### Step 9 Liveness Envelope
- `results/step9/step9_liveness.csv` — raw per-seed results (phase-timeout sweep)
- `results/step9/step9_liveness_ci.csv` — mean ± 95% CI aggregated per (scenario, timeout, blackout_duration)

### Per-Tier Liveness Sweep
- `results/tier_liveness/tier_sweep.csv` — raw per-seed results (25 columns, both topologies × 2 scenarios per run)
- `results/tier_liveness/tier_sweep_ci.csv` — mean ± 95% CI aggregated per (scenario, topology, tier, mars_latency, blackout_duration)

### Crash-Tolerance Sweep
- `results/step10/step10_sweep.csv` — raw per-seed results (19 columns, 6 scenarios)
- `results/step10/step10_sweep_ci.csv` — mean ± 95% CI aggregated per scenario

### Plots
- `results/step9/plots/during_success_latency_*.svg` — during-blackout success rates vs. latency
- `results/step9/plots/recovery_lag_latency_*.svg` — recovery latency vs. Mars latency
- `results/step9/plots/liveness_envelope_*.svg` — phase-timeout threshold envelope

## Notes

- All timings are in simulation seconds.
- `scenario=blackout_only` models hard conjunction blackout.
- `scenario=with_repeater` models degraded continuity via repeater.
- Aggregated CSV files (ending in `_ci.csv`) report means and 95% confidence intervals over the specified seed set.
- The **temporal window is automatically scaled** by each script to satisfy the time budget constraints (one complete round-trip plus buffer for processing). The columns `phase_timeout_s`, `pre_window_s`, `post_window_s`, and `temporally_scaled` in the raw output document whether scaling was applied.

## Dueling-proposer experiment (duel.py)

# Deterministic offset->outcome map (primary)
uv run python experiments/duel_sweep.py --mode map --output results/duel/duel_map.csv

# Jitter robustness sweep (secondary; per-link RNG, Wilson CIs)
uv run python experiments/duel_sweep.py --mode jitter \
  --output results/duel/duel_jitter.csv \
  --aggregate-output results/duel/duel_jitter_ci.csv \
  --seeds "40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59"
