"""Build a deterministic, identity-scanned artifact from a positive allowlist."""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXED_ZIP_TIME = (1980, 1, 1, 0, 0, 0)
IDENTITY_MARKERS = (
    "tony",
    "wamason",
    "research@",
    "/home/",
    "c:\\users\\",
)


@dataclass(frozen=True)
class BuildResult:
    sha256: str
    member_count: int


def manifest_covers(path: str, rules: Iterable[str]) -> bool:
    """Return whether a normalized repository path matches an allowlist rule."""
    candidate = PurePosixPath(path)
    return any(candidate.full_match(rule) for rule in rules)


def _reject_member_name(member: PurePosixPath) -> None:
    parts = tuple(part.casefold() for part in member.parts)
    if member.is_absolute() or ".." in parts:
        raise ValueError(f"unsafe archive member: {member.as_posix()}")
    if ".git" in parts or "timestamps" in parts:
        raise ValueError(f"prohibited archive member: {member.as_posix()}")
    if member.as_posix().casefold() == "docs/paper/main.tex":
        raise ValueError("legacy paper is prohibited from the anonymous artifact")
    if "marching" in member.name.casefold():
        raise ValueError(f"marching-orders file is prohibited: {member.as_posix()}")
    if len(parts) >= 3 and parts[:2] == ("docs", "superpowers") \
            and any(part in {"plans", "specs"} for part in parts[2:]):
        raise ValueError(f"planning file is prohibited: {member.as_posix()}")


def scan_members(root: Path, members: Iterable[PurePosixPath]) -> None:
    """Reject prohibited names and identity markers in UTF-8 text members."""
    resolved_root = root.resolve()
    for member in members:
        _reject_member_name(member)
        source = (root / Path(*member.parts)).resolve()
        if resolved_root not in source.parents:
            raise ValueError(f"archive member escapes source root: {member}")
        if not source.is_file():
            raise ValueError(f"archive member is missing or not a file: {member}")
        try:
            text = source.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        folded = text.casefold()
        for marker in IDENTITY_MARKERS:
            if marker in folded:
                raise ValueError(
                    f"identifying content in {member.as_posix()} "
                    f"(matched deny marker)"
                )


def build_zip_from_members(
    root: Path,
    members: Iterable[PurePosixPath],
    output: Path,
) -> BuildResult:
    """Scan and write sorted members with fixed ZIP metadata."""
    ordered = sorted(set(members), key=lambda item: item.as_posix())
    scan_members(root, ordered)
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(
        output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as archive:
        for member in ordered:
            source = root / Path(*member.parts)
            info = zipfile.ZipInfo(member.as_posix(), FIXED_ZIP_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, source.read_bytes(), compresslevel=9)
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    return BuildResult(sha256=digest, member_count=len(ordered))


def _tracked_files(repo_root: Path) -> set[str]:
    result = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files", "-z"],
        capture_output=True,
        check=True,
    )
    return {
        item.decode("utf-8")
        for item in result.stdout.split(b"\0")
        if item
    }


def expand_manifest(
    repo_root: Path, manifest_path: Path
) -> list[PurePosixPath]:
    """Expand every rule solely against Git-tracked files."""
    rules = [
        line.strip()
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not rules:
        raise ValueError("artifact manifest is empty")
    tracked = _tracked_files(repo_root)
    selected: set[str] = set()
    for rule in rules:
        matches = sorted(path for path in tracked if manifest_covers(path, [rule]))
        if not matches:
            raise ValueError(f"manifest rule matches no tracked files: {rule}")
        selected.update(matches)
    return [PurePosixPath(path) for path in sorted(selected)]


def require_clean_worktree(repo_root: Path) -> None:
    result = subprocess.run(
        [
            "git",
            "-C",
            str(repo_root),
            "status",
            "--porcelain",
            "--untracked-files=all",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    if result.stdout:
        raise ValueError("working tree must be clean before packaging")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the deterministic anonymous NINeS artifact"
    )
    parser.add_argument(
        "--manifest", type=Path, default=REPO_ROOT / "artifact-manifest.txt"
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        require_clean_worktree(REPO_ROOT)
        members = expand_manifest(REPO_ROOT, args.manifest)
        result = build_zip_from_members(REPO_ROOT, members, args.output)
    except (OSError, subprocess.CalledProcessError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(
        f"members={result.member_count} sha256={result.sha256} "
        f"output={args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
