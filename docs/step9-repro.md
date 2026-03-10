# Step 9 Reproducibility

This document captures exact commands for the conjunction/repeater experiments.

## Environment

```bash
uv sync
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
  --seed 42 \
  --output results/step9/step9_sweep.csv
```

## Plot Generation (SVG)

```bash
uv run python experiments/plot_step9.py \
  --input results/step9/step9_sweep.csv \
  --output-dir results/step9/plots
```

## Outputs

- CSV summary:
  - `results/step9/single_run.csv`
- Sweep data:
  - `results/step9/step9_sweep.csv`
- Plots:
  - `results/step9/plots/during_success_latency_*.svg`
  - `results/step9/plots/recovery_lag_latency_*.svg`

## Notes

- All timings are in simulation seconds.
- `scenario=blackout_only` models hard conjunction blackout.
- `scenario=with_repeater` models degraded continuity via repeater.
