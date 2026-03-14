# Step 9 Reproducibility

This document captures exact commands for the conjunction/repeater experiments.

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

## Parameter Sweep

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

## Plot Generation (SVG)

```bash
uv run python experiments/plot_step9.py \
  --input results/step9/step9_sweep.csv \
  --output-dir results/step9/plots
```

## Liveness Envelope Sweep

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

## Outputs

- CSV summary:
  - `results/step9/single_run.csv`
- Sweep data:
  - `results/step9/step9_sweep.csv`
  - `results/step9/step9_sweep_ci.csv`
- Liveness envelope:
  - `results/step9/step9_liveness.csv`
  - `results/step9/step9_liveness_ci.csv`
- Plots:
  - `results/step9/plots/during_success_latency_*.svg`
  - `results/step9/plots/recovery_lag_latency_*.svg`
  - `results/step9/plots/liveness_envelope_*.svg`

## Notes

- All timings are in simulation seconds.
- `scenario=blackout_only` models hard conjunction blackout.
- `scenario=with_repeater` models degraded continuity via repeater.
- `step9_sweep_ci.csv` reports means and 95% CI over the specified 50-seed set.
