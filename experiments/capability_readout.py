"""Render a wall configuration and connectivity summary as capabilities."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from capability import CapabilityReport, classify
from quorums import CrumblingWallQuorum


def _integer_list(value: Any, label: str, *, nonempty: bool = False) -> list[int]:
    if not isinstance(value, list) or not all(
        isinstance(item, int) and not isinstance(item, bool) for item in value
    ):
        raise ValueError(f"{label} must be a JSON array of integers")
    if nonempty and not value:
        raise ValueError(f"{label} must be nonempty")
    if len(set(value)) != len(value):
        raise ValueError(f"{label} contains duplicate node IDs")
    return value


def _source(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonempty string")
    return value


def parse_payload(payload: Any) -> tuple[
    list[str], list[list[int]], int, int, set[int], str, str
]:
    """Validate the readout schema and resolve the initiating tier name."""
    if not isinstance(payload, dict):
        raise ValueError("input must be a JSON object")
    required = {
        "tiers",
        "phase2_threshold",
        "initiating_tier",
        "reachable",
        "configuration_source",
        "connectivity_source",
    }
    missing = required - payload.keys()
    extra = payload.keys() - required
    if missing:
        raise ValueError(f"missing input field(s): {', '.join(sorted(missing))}")
    if extra:
        raise ValueError(f"unknown input field(s): {', '.join(sorted(extra))}")

    raw_tiers = payload["tiers"]
    if not isinstance(raw_tiers, list) or not raw_tiers:
        raise ValueError("tiers must be a nonempty JSON array")
    names: list[str] = []
    tiers: list[list[int]] = []
    for index, tier in enumerate(raw_tiers):
        if not isinstance(tier, dict) or set(tier) != {"name", "nodes"}:
            raise ValueError(
                f"tiers[{index}] must contain exactly name and nodes"
            )
        names.append(_source(tier["name"], f"tiers[{index}].name"))
        tiers.append(_integer_list(
            tier["nodes"], f"tiers[{index}].nodes", nonempty=True
        ))
    if len(set(names)) != len(names):
        raise ValueError("tier names must be unique")
    flat_nodes = [node for tier in tiers for node in tier]
    if len(set(flat_nodes)) != len(flat_nodes):
        raise ValueError("node IDs must be unique across tiers")

    threshold = payload["phase2_threshold"]
    if not isinstance(threshold, int) or isinstance(threshold, bool):
        raise ValueError("phase2_threshold must be an integer")
    initiating_name = _source(payload["initiating_tier"], "initiating_tier")
    if initiating_name not in names:
        raise ValueError(f"unknown initiating tier: {initiating_name}")
    reachable = set(_integer_list(payload["reachable"], "reachable"))
    unknown = reachable - set(flat_nodes)
    if unknown:
        raise ValueError(f"reachable contains unknown node IDs: {sorted(unknown)}")

    return (
        names,
        tiers,
        threshold,
        names.index(initiating_name),
        reachable,
        _source(payload["configuration_source"], "configuration_source"),
        _source(payload["connectivity_source"], "connectivity_source"),
    )


def _channels(report: CapabilityReport, key: str) -> list[str]:
    return sorted(channel.value for channel in report.provenance[key])


def report_to_dict(
    report: CapabilityReport,
    tier_names: list[str],
    reachable: set[int],
    configuration_source: str,
    connectivity_source: str,
) -> dict[str, Any]:
    """Translate typed classifier output without inferring runtime state."""
    missing = [
        {
            "phase": obligation.phase,
            "tier": tier_names[obligation.tier_index],
            "tier_index": obligation.tier_index,
            "required": obligation.required,
            "reachable": sorted(obligation.witnesses),
            "unreachable": sorted(obligation.unreachable),
        }
        for obligation in report.missing
    ]
    return {
        "R1": report.r1,
        "R2": report.r2,
        "state": f"({int(report.r1)},{int(report.r2)})",
        "initiating_tier": tier_names[report.initiator_tier],
        "reachable": sorted(reachable),
        "witnesses": {
            "phase1": (
                None if report.r1_witness is None
                else sorted(report.r1_witness)
            ),
            "phase2": (
                None if report.r2_witness is None
                else sorted(report.r2_witness)
            ),
        },
        "missing": missing,
        "hazards": [hazard.value for hazard in report.hazards],
        "requires_preexisting_authority": (
            report.requires_preexisting_authority
        ),
        "runtime_authority": "unknown",
        "service_policy": "not-inferred",
        "evidence_sources": {
            "configuration": configuration_source,
            "connectivity": connectivity_source,
        },
        "evidence_provenance": {
            "R1": _channels(report, "r1"),
            "R2": _channels(report, "r2"),
            "witnesses": _channels(report, "r1_witness"),
            "missing": _channels(report, "missing"),
            "requires_preexisting_authority": _channels(
                report, "requires_preexisting_authority"
            ),
            "operational_progress": _channels(
                report, "operational_progress"
            ),
            "service_policy": _channels(report, "service_contract"),
        },
    }


def evaluate(payload: Any) -> dict[str, Any]:
    names, tiers, threshold, initiator, reachable, config_src, connect_src = (
        parse_payload(payload)
    )
    wall = CrumblingWallQuorum(tiers, phase2_threshold=threshold)
    report = classify(wall, initiator, reachable)
    return report_to_dict(
        report, names, reachable, config_src, connect_src
    )


def _read_json(path: str) -> Any:
    if path == "-":
        return json.load(sys.stdin)
    with Path(path).open(encoding="utf-8") as stream:
        return json.load(stream)


def _render_text(result: dict[str, Any]) -> str:
    lines = [
        f"state: {result['state']}",
        f"initiating tier: {result['initiating_tier']}",
        f"R1 acquisition formable: {result['R1']}",
        f"R2 commit quorum formable: {result['R2']}",
        f"requires preexisting authority: "
        f"{result['requires_preexisting_authority']}",
        f"runtime authority: {result['runtime_authority']}",
        f"service policy: {result['service_policy']}",
    ]
    for obligation in result["missing"]:
        lines.append(
            f"missing Phase {obligation['phase']} {obligation['tier']} "
            f"obligation: require {obligation['required']}, "
            f"reachable {len(obligation['reachable'])}"
        )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read out wall capabilities from configuration and connectivity"
    )
    parser.add_argument("--input", required=True, help="JSON path, or - for stdin")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    args = parser.parse_args(argv)
    try:
        result = evaluate(_read_json(args.input))
    except (OSError, json.JSONDecodeError, ValueError, TypeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if args.format == "json":
        print(json.dumps(result, sort_keys=True, indent=2))
    else:
        sys.stdout.write(_render_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
