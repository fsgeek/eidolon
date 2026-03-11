"""Generate SVG liveness-envelope heatmaps from step9_liveness_ci.csv."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=str,
        default="results/step9/step9_liveness_ci.csv",
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


def _color(rate: float) -> str:
    # 0.0 -> red, 1.0 -> green
    rate = max(0.0, min(1.0, rate))
    r = int(220 * (1 - rate) + 30 * rate)
    g = int(60 * (1 - rate) + 170 * rate)
    b = int(60)
    return f"rgb({r},{g},{b})"


def _render_heatmap(rows, scenario: str, output: Path):
    srows = [r for r in rows if r["scenario"] == scenario]
    timeouts = sorted({float(r["global_timeout_s"]) for r in srows})
    blackouts = sorted({float(r["blackout_duration_s"]) for r in srows})

    data = {}
    for r in srows:
        key = (float(r["blackout_duration_s"]), float(r["global_timeout_s"]))
        data[key] = float(r["global_during_rate_mean"]) if r["global_during_rate_mean"] else 0.0

    cell_w = 120
    cell_h = 70
    ml = 170
    mt = 80
    width = ml + len(timeouts) * cell_w + 80
    height = mt + len(blackouts) * cell_h + 90

    parts = []
    parts.append(f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>")
    parts.append("<rect width='100%' height='100%' fill='white'/>")
    title = f"Step 9 Liveness Envelope ({scenario})"
    parts.append(f"<text x='{width/2:.1f}' y='30' text-anchor='middle' font-family='monospace' "
                 f"font-size='20'>{title}</text>")
    parts.append("<text x='20' y='60' font-family='monospace' font-size='12'>Cell value: during-blackout success rate</text>")

    for j, timeout in enumerate(timeouts):
        x = ml + j * cell_w + cell_w / 2
        parts.append(
            f"<text x='{x:.1f}' y='{mt-16}' text-anchor='middle' font-family='monospace' font-size='12'>{timeout:.0f}s</text>"
        )
    parts.append(
        f"<text x='{ml + (len(timeouts) * cell_w)/2:.1f}' y='{mt-38}' text-anchor='middle' "
        f"font-family='monospace' font-size='13'>Global timeout</text>"
    )

    for i, blackout in enumerate(blackouts):
        y = mt + i * cell_h + cell_h / 2
        parts.append(
            f"<text x='{ml-12}' y='{y+4:.1f}' text-anchor='end' font-family='monospace' font-size='12'>{blackout:.0f}s</text>"
        )
    parts.append(
        f"<text x='30' y='{mt + (len(blackouts)*cell_h)/2:.1f}' transform='rotate(-90 30 {mt + (len(blackouts)*cell_h)/2:.1f})' "
        f"text-anchor='middle' font-family='monospace' font-size='13'>Blackout duration</text>"
    )

    for i, blackout in enumerate(blackouts):
        for j, timeout in enumerate(timeouts):
            rate = data.get((blackout, timeout), 0.0)
            x = ml + j * cell_w
            y = mt + i * cell_h
            fill = _color(rate)
            parts.append(f"<rect x='{x}' y='{y}' width='{cell_w-2}' height='{cell_h-2}' fill='{fill}' stroke='white'/>")
            parts.append(
                f"<text x='{x + cell_w/2:.1f}' y='{y + cell_h/2 + 4:.1f}' text-anchor='middle' "
                f"font-family='monospace' font-size='14' fill='white'>{rate*100:.0f}%</text>"
            )

    parts.append("</svg>")
    output.write_text("\n".join(parts), encoding="utf-8")


def main():
    args = _parse_args()
    rows = _load_rows(Path(args.input))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for scenario in ("blackout_only", "with_repeater"):
        _render_heatmap(
            rows,
            scenario,
            output_dir / f"liveness_envelope_{scenario}.svg",
        )

    print(f"Wrote liveness heatmaps to: {output_dir}")


if __name__ == "__main__":
    main()
