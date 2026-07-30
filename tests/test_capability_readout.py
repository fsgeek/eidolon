"""CLI contract for the wall-specific operational capability readout."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
CLI = REPO_ROOT / "experiments" / "capability_readout.py"
EXAMPLES = REPO_ROOT / "examples" / "capability"


def _run(input_path, *extra):
    return subprocess.run(
        [sys.executable, str(CLI), "--input", str(input_path), *extra],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize(
    "filename,missing_tier,phase2_witness",
    [
        ("planetary_moon_01.json", "LEO", [1, 2, 3, 4, 5]),
        ("edge_remote_01.json", "metro", [1, 2, 3]),
    ],
)
def test_registered_examples_expose_01_without_inventing_policy(
    filename, missing_tier, phase2_witness
):
    input_path = EXAMPLES / filename

    first = _run(input_path, "--format", "json")
    second = _run(input_path, "--format", "json")

    assert first.returncode == 0
    assert first.stderr == ""
    assert first.stdout == second.stdout
    decoded = json.loads(first.stdout)
    assert first.stdout == json.dumps(decoded, sort_keys=True, indent=2) + "\n"
    assert (decoded["R1"], decoded["R2"], decoded["state"]) == (
        False,
        True,
        "(0,1)",
    )
    assert decoded["witnesses"] == {
        "phase1": None,
        "phase2": phase2_witness,
    }
    assert [item["tier"] for item in decoded["missing"]] == [missing_tier]
    assert decoded["missing"][0]["phase"] == 1
    assert decoded["requires_preexisting_authority"] is True
    assert decoded["runtime_authority"] == "unknown"
    assert decoded["service_policy"] == "not-inferred"
    assert decoded["evidence_provenance"]["R1"] == [
        "configuration",
        "connectivity",
    ]


@pytest.mark.parametrize(
    "mutation",
    [
        {"initiating_tier": "Venus"},
        {"reachable": [999]},
    ],
)
def test_invalid_tier_or_node_exits_two_without_traceback(tmp_path, mutation):
    payload = json.loads(
        (EXAMPLES / "planetary_moon_01.json").read_text(encoding="utf-8")
    )
    payload.update(mutation)
    input_path = tmp_path / "invalid.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")

    result = _run(input_path, "--format", "json")

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr.startswith("error: ")
    assert result.stderr.count("\n") == 1
    assert "Traceback" not in result.stderr


def test_text_output_preserves_the_runtime_and_policy_boundaries():
    result = _run(EXAMPLES / "edge_remote_01.json", "--format", "text")

    assert result.returncode == 0
    assert result.stdout.splitlines()[0] == "state: (0,1)"
    assert "runtime authority: unknown" in result.stdout
    assert "service policy: not-inferred" in result.stdout
