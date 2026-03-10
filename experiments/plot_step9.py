"""Generate SVG plots from Step 9 sweep CSV (no external dependencies)."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DataPoint:
    blackout_duration_s: float
    value: float


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=str,
        default="results/step9/step9_sweep.csv",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="results/step9/plots",
    )
    return parser.parse_args()


def _load_rows(path: Path):
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _line_chart_svg(
    output: Path,
    title: str,
    x_label: str,
    y_label: str,
    series: list[tuple[str, str, list[DataPoint]]],
):
    width = 920
    height = 520
    ml = 90
    mr = 40
    mt = 55
    mb = 70

    all_points = [p for _, _, pts in series for p in pts]
    x_vals = [p.blackout_duration_s for p in all_points]
    y_vals = [p.value for p in all_points]

    x_min = min(x_vals)
    x_max = max(x_vals)
    y_min = 0.0
    y_max = max(y_vals) if y_vals else 1.0
    if y_max <= y_min:
        y_max = y_min + 1.0

    def sx(x):
        if x_max == x_min:
            return ml + (width - ml - mr) / 2
        return ml + (x - x_min) * (width - ml - mr) / (x_max - x_min)

    def sy(y):
        return mt + (y_max - y) * (height - mt - mb) / (y_max - y_min)

    parts = []
    parts.append(f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>")
    parts.append("<rect width='100%' height='100%' fill='white'/>")
    parts.append(f"<text x='{width/2:.1f}' y='28' text-anchor='middle' font-size='20' "
                 f"font-family='monospace'>{title}</text>")

    # Axes
    x0, y0 = ml, height - mb
    x1, y1 = width - mr, mt
    parts.append(f"<line x1='{x0}' y1='{y0}' x2='{x1}' y2='{y0}' stroke='black' stroke-width='2'/>")
    parts.append(f"<line x1='{x0}' y1='{y0}' x2='{x0}' y2='{y1}' stroke='black' stroke-width='2'/>")

    # Ticks
    x_ticks = sorted(set(x_vals))
    for xv in x_ticks:
        x = sx(xv)
        parts.append(f"<line x1='{x:.1f}' y1='{y0}' x2='{x:.1f}' y2='{y0+6}' stroke='black'/>")
        parts.append(f"<text x='{x:.1f}' y='{y0+24}' text-anchor='middle' font-size='12' "
                     f"font-family='monospace'>{xv:.0f}</text>")

    for i in range(6):
        yv = y_min + i * (y_max - y_min) / 5
        y = sy(yv)
        parts.append(f"<line x1='{x0-6}' y1='{y:.1f}' x2='{x0}' y2='{y:.1f}' stroke='black'/>")
        parts.append(f"<line x1='{x0}' y1='{y:.1f}' x2='{x1}' y2='{y:.1f}' stroke='#e6e6e6'/>")
        parts.append(f"<text x='{x0-10}' y='{y+4:.1f}' text-anchor='end' font-size='12' "
                     f"font-family='monospace'>{yv:.2f}</text>")

    parts.append(f"<text x='{(x0+x1)/2:.1f}' y='{height-20}' text-anchor='middle' font-size='14' "
                 f"font-family='monospace'>{x_label}</text>")
    parts.append(f"<text x='20' y='{(y0+y1)/2:.1f}' text-anchor='middle' font-size='14' "
                 f"font-family='monospace' transform='rotate(-90 20 {(y0+y1)/2:.1f})'>{y_label}</text>")

    # Series
    legend_x = width - mr - 250
    legend_y = mt + 10
    for idx, (name, color, pts) in enumerate(series):
        pts = sorted(pts, key=lambda p: p.blackout_duration_s)
        if not pts:
            continue
        path = " ".join(
            f"{'M' if i == 0 else 'L'} {sx(p.blackout_duration_s):.1f} {sy(p.value):.1f}"
            for i, p in enumerate(pts)
        )
        parts.append(f"<path d='{path}' fill='none' stroke='{color}' stroke-width='3'/>")
        for p in pts:
            px = sx(p.blackout_duration_s)
            py = sy(p.value)
            parts.append(f"<circle cx='{px:.1f}' cy='{py:.1f}' r='4' fill='{color}'/>")

        ly = legend_y + idx * 22
        parts.append(f"<line x1='{legend_x}' y1='{ly}' x2='{legend_x+22}' y2='{ly}' "
                     f"stroke='{color}' stroke-width='3'/>")
        parts.append(f"<text x='{legend_x+28}' y='{ly+4}' font-size='12' font-family='monospace'>{name}</text>")

    parts.append("</svg>")
    output.write_text("\n".join(parts), encoding="utf-8")


def main():
    args = _parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = _load_rows(input_path)
    latencies = sorted({float(r["mars_base_latency_s"]) for r in rows})

    for latency in latencies:
        latency_rows = [r for r in rows if float(r["mars_base_latency_s"]) == latency]
        baseline_rows = [r for r in latency_rows if r["scenario"] == "blackout_only"]
        repeater_rows = [r for r in latency_rows if r["scenario"] == "with_repeater"]

        def during_rate(r):
            total = int(r["global_during_total"])
            return int(r["global_during_success"]) / total if total > 0 else 0.0

        def recovery(r):
            v = r["first_success_after_blackout_s"].strip()
            return float(v) if v else 0.0

        baseline_during = [
            DataPoint(float(r["blackout_duration_s"]), during_rate(r)) for r in baseline_rows
        ]
        repeater_during = [
            DataPoint(float(r["blackout_duration_s"]), during_rate(r)) for r in repeater_rows
        ]
        _line_chart_svg(
            output_dir / f"during_success_latency_{int(latency)}s.svg",
            title=f"Step 9 During-Blackout Success Rate (Mars latency={latency:.0f}s)",
            x_label="Blackout duration (s)",
            y_label="Success rate",
            series=[
                ("Hard blackout", "#c0392b", baseline_during),
                ("With repeater", "#1f77b4", repeater_during),
            ],
        )

        baseline_recovery = [
            DataPoint(float(r["blackout_duration_s"]), recovery(r)) for r in baseline_rows
        ]
        repeater_recovery = [
            DataPoint(float(r["blackout_duration_s"]), recovery(r)) for r in repeater_rows
        ]
        _line_chart_svg(
            output_dir / f"recovery_lag_latency_{int(latency)}s.svg",
            title=f"Step 9 Recovery Lag (Mars latency={latency:.0f}s)",
            x_label="Blackout duration (s)",
            y_label="First-success-after-blackout (s)",
            series=[
                ("Hard blackout", "#c0392b", baseline_recovery),
                ("With repeater", "#1f77b4", repeater_recovery),
            ],
        )

    print(f"Wrote SVG plots to: {output_dir}")


if __name__ == "__main__":
    main()
