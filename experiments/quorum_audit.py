"""Command-line boundary for the generic explicit-family quorum auditor."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from quorum_audit import QuorumAudit, audit_quorum_families


def _require_string_list(value: Any, label: str) -> list[str]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) for item in value
    ):
        raise ValueError(f"{label} must be a JSON array of strings")
    return value


def _require_family(value: Any, label: str) -> list[list[str]]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a JSON array of string arrays")
    return [_require_string_list(quorum, f"{label}[{index}]")
            for index, quorum in enumerate(value)]


def parse_payload(payload: Any) -> tuple[
    list[str], list[list[str]], list[list[str]], list[str]
]:
    """Validate only the JSON shape; the library validates quorum semantics."""
    if not isinstance(payload, dict):
        raise ValueError("input must be a JSON object")
    required = {"universe", "phase1", "phase2", "pinned"}
    missing = required - payload.keys()
    extra = payload.keys() - required
    if missing:
        raise ValueError(f"missing input field(s): {', '.join(sorted(missing))}")
    if extra:
        raise ValueError(f"unknown input field(s): {', '.join(sorted(extra))}")
    universe = sorted(_require_string_list(payload["universe"], "universe"))
    phase1 = _require_family(payload["phase1"], "phase1")
    phase2 = _require_family(payload["phase2"], "phase2")
    pinned = _require_string_list(payload["pinned"], "pinned")
    return universe, phase1, phase2, pinned


def _members(quorum) -> list[str] | None:
    return None if quorum is None else sorted(quorum)


def _family(family) -> list[list[str]]:
    return [sorted(quorum) for quorum in family]


def report_to_dict(report: QuorumAudit[str]) -> dict[str, Any]:
    """Return the stable JSON representation shared by artifact drivers."""
    unsafe = None
    if report.unsafe_witness is not None:
        unsafe = [_members(quorum) for quorum in report.unsafe_witness]
    return {
        "universe": list(report.universe),
        "pinned": sorted(report.pinned),
        "safe": report.safe,
        "unsafe_witness": unsafe,
        "phase1_minimal": _family(report.phase1_minimal),
        "phase2_minimal": _family(report.phase2_minimal),
        "phase1_effective": _family(report.phase1_effective),
        "phase2_effective": _family(report.phase2_effective),
        "relation": report.relation.value,
        "gaps": {
            "(1,0)": _members(report.gap_10_witness),
            "(0,1)": _members(report.gap_01_witness),
        },
        "self_check_passed": report.self_check_passed,
    }


def _read_json(path: str) -> Any:
    if path == "-":
        return json.load(sys.stdin)
    with Path(path).open(encoding="utf-8") as stream:
        return json.load(stream)


def _render_text(result: dict[str, Any]) -> str:
    lines = ["SAFE" if result["safe"] else "UNSAFE"]
    if result["unsafe_witness"] is not None:
        lines.append(f"unsafe witness: {result['unsafe_witness']}")
    lines.extend(
        [
            f"relation: {result['relation']}",
            f"(1,0) witness: {result['gaps']['(1,0)']}",
            f"(0,1) witness: {result['gaps']['(0,1)']}",
            f"pinned: {result['pinned']}",
            f"self-check passed: {result['self_check_passed']}",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit explicit finite Phase 1 and Phase 2 quorum families"
    )
    parser.add_argument("--input", required=True, help="JSON path, or - for stdin")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--exhaustive", action="store_true")
    args = parser.parse_args(argv)

    try:
        universe, phase1, phase2, pinned = parse_payload(_read_json(args.input))
        report = audit_quorum_families(
            universe,
            phase1,
            phase2,
            pinned=pinned,
            exhaustive=args.exhaustive,
        )
    except (OSError, json.JSONDecodeError, ValueError, TypeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    result = report_to_dict(report)
    if args.format == "json":
        print(json.dumps(result, sort_keys=True, indent=2))
    else:
        sys.stdout.write(_render_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
