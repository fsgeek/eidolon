"""Command-line contract for the generic quorum-family auditor."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CLI = REPO_ROOT / "experiments" / "quorum_audit.py"


def _run_cli(tmp_path, payload, *extra):
    input_path = tmp_path / "audit.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    return subprocess.run(
        [
            sys.executable,
            str(CLI),
            "--input",
            str(input_path),
            *extra,
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_json_output_is_canonical_complete_and_repeatable(tmp_path):
    payload = {
        "universe": ["c", "a", "b"],
        "phase1": [["b", "a"]],
        "phase2": [["c", "a"]],
        "pinned": [],
    }

    first = _run_cli(tmp_path, payload, "--format", "json", "--exhaustive")
    second = _run_cli(tmp_path, payload, "--format", "json", "--exhaustive")

    assert first.returncode == 0
    assert first.stderr == ""
    assert first.stdout == second.stdout
    decoded = json.loads(first.stdout)
    assert first.stdout == json.dumps(decoded, sort_keys=True, indent=2) + "\n"
    assert decoded == {
        "gaps": {"(0,1)": ["a", "c"], "(1,0)": ["a", "b"]},
        "phase1_effective": [["a", "b"]],
        "phase1_minimal": [["a", "b"]],
        "phase2_effective": [["a", "c"]],
        "phase2_minimal": [["a", "c"]],
        "pinned": [],
        "relation": "incomparable",
        "safe": True,
        "self_check_passed": True,
        "universe": ["a", "b", "c"],
        "unsafe_witness": None,
    }


def test_schema_error_is_one_line_without_traceback(tmp_path):
    result = _run_cli(
        tmp_path,
        {
            "universe": ["a"],
            "phase1": [["unknown"]],
            "phase2": [["a"]],
            "pinned": [],
        },
        "--format",
        "json",
    )

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr.startswith("error: ")
    assert result.stderr.count("\n") == 1
    assert "Traceback" not in result.stderr


def test_text_output_leads_with_unsafe_status(tmp_path):
    result = _run_cli(
        tmp_path,
        {
            "universe": ["a", "b"],
            "phase1": [["a"]],
            "phase2": [["b"]],
            "pinned": [],
        },
        "--format",
        "text",
    )

    assert result.returncode == 0
    assert result.stdout.splitlines()[0] == "UNSAFE"
