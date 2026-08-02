"""Safety and reproducibility checks for the anonymous artifact builder."""

from __future__ import annotations

import zipfile
from pathlib import Path, PurePosixPath

import pytest

from scripts.build_anonymous_artifact import (
    build_zip_from_members,
    manifest_covers,
    scan_members,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "identity",
    [
        "Tony",
        "wamason",
        "research@example.invalid",
        "/home/author/project",
        r"C:\Users\author\project",
    ],
)
def test_identity_scan_rejects_case_insensitive_matches(tmp_path, identity):
    (tmp_path / "bad.txt").write_text(
        f"prefix {identity.upper()} suffix", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="identifying content"):
        scan_members(tmp_path, [PurePosixPath("bad.txt")])


def test_clean_synthetic_tree_packages_deterministically(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "README.md").write_text("anonymous artifact\n", encoding="utf-8")
    (source / "data").mkdir()
    (source / "data" / "result.csv").write_text("x,y\n1,2\n", encoding="utf-8")
    members = [PurePosixPath("data/result.csv"), PurePosixPath("README.md")]
    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"

    first_result = build_zip_from_members(source, members, first)
    second_result = build_zip_from_members(source, members, second)

    assert first.read_bytes() == second.read_bytes()
    assert first_result.sha256 == second_result.sha256
    assert first_result.member_count == 2
    with zipfile.ZipFile(first) as archive:
        assert archive.namelist() == ["README.md", "data/result.csv"]
        assert all(info.date_time == (1980, 1, 1, 0, 0, 0)
                   for info in archive.infolist())
        assert all(not Path(name).is_absolute() for name in archive.namelist())
        assert all(".git" not in name and "timestamps" not in name
                   for name in archive.namelist())


def test_manifest_covers_every_traceability_artifact():
    rules = [
        line.strip()
        for line in (REPO_ROOT / "artifact-manifest.txt").read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip() and not line.startswith("#")
    ]
    cited = [
        "results/step9_flat/step9_sweep.csv",
        "results/step9_maj/step9_sweep.csv",
        "results/step9/step9_sweep.csv",
        "results/tier_liveness/tier_sweep_full_ci.csv",
        "results/step10/step10_sweep_ci.csv",
        "tla/QuorumIntersection.tla",
        "tla/ExhaustiveIntersection.tla",
        "tla/PaxosSmall.tla",
        "results/capability/capability_map.csv",
        "results/capability/dual_containment.csv",
        "results/capability/quorum_audit_registered.json",
        "results/capability/dual_uniform.csv",
        "results/flip/flip_map.csv",
        "results/flip/flip_sweep.csv",
        "flip.py",
        "experiments/flip_sweep.py",
        "experiments/flip_verdict.py",
        "results/capability/dual_gradient_map.csv",
        "experiments/capability_dual_sweep.py",
        "quorum_audit.py",
        "experiments/quorum_audit.py",
        "experiments/capability_readout.py",
        "examples/capability/planetary_moon_01.json",
        "examples/capability/edge_remote_01.json",
    ]

    assert all(manifest_covers(path, rules) for path in cited)
