"""Exercise the real secret-scan wrapper with a deterministic scanner transport."""

from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import tarfile
from pathlib import Path

import pytest

from scripts.ci_changes import git

SCANNER = """#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
assert '--ignore-gitleaks-allow' in args and '--redact' in args
assert 'GITLEAKS_CONFIG' not in os.environ
assert 'GITLEAKS_CONFIG_TOML' not in os.environ
ignore = pathlib.Path(args[args.index('--gitleaks-ignore-path') + 1])
assert ignore.name == 'reviewed.ignore'
assert ignore.read_text() == '', 'public history must not inherit private exceptions'
if args[0] == 'git':
    assert '--log-opts=--all -m' in args
    if os.environ.get('TEST_SCANNER_FAILURE') == '1': sys.exit(2)
else:
    root = pathlib.Path(args[1])
    files = {str(p.relative_to(root)): p.read_text() for p in root.rglob('*') if p.is_file()}
    pathlib.Path(os.environ['TEST_SCAN_LOG']).write_text(json.dumps(files))
    if any('synthetic-leak-marker' in text for text in files.values()): sys.exit(1)
"""


def prepare(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    root = tmp_path / "repo"
    root.mkdir()
    (root / "scripts").mkdir()
    git("init", "-q", root=root)
    (root / "tracked.txt").write_text("original\n")
    git("add", ".", root=root)
    git(
        "-c",
        "user.name=CI",
        "-c",
        "user.email=ci@example.invalid",
        "commit",
        "-qm",
        "fixture",
        root=root,
    )
    archive = tmp_path / "scanner.tar.gz"
    with tarfile.open(archive, "w:gz") as output:
        info = tarfile.TarInfo("gitleaks")
        payload = SCANNER.encode()
        info.size, info.mode = len(payload), 0o755
        output.addfile(info, io.BytesIO(payload))
    source = Path(__file__).resolve().parents[2]
    wrapper = (source / "scripts/verify-secrets").read_text()
    pin = "551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb"
    (root / "scripts/verify-secrets").write_text(
        wrapper.replace(pin, hashlib.sha256(archive.read_bytes()).hexdigest())
    )
    (root / ".gitleaks.toml").write_text((source / ".gitleaks.toml").read_text())
    (root / ".gitleaksignore").write_text("unreviewed ignore\n")
    tools = tmp_path / "bin"
    tools.mkdir()
    curl = tools / "curl"
    curl.write_text('#!/bin/bash\ncp "$TEST_ARCHIVE" "${@: -1}"\n')
    curl.chmod(0o755)
    env = {
        **os.environ,
        "PATH": str(tools) + os.pathsep + os.environ["PATH"],
        "TEST_ARCHIVE": str(archive),
        "TEST_SCAN_LOG": str(tmp_path / "scan.json"),
        "GITLEAKS_CONFIG": "untrusted",
        "GITLEAKS_CONFIG_TOML": "untrusted",
    }
    return root, env


@pytest.mark.parametrize("case", ["clean", "unstaged", "untracked", "scanner_failure", "checksum"])
def test_wrapper_checks_actual_candidate_and_propagates_failures(tmp_path: Path, case: str) -> None:
    root, env = prepare(tmp_path)
    if case == "unstaged":
        (root / "tracked.txt").write_text("synthetic-leak-marker")
    elif case == "untracked":
        (root / "new.txt").write_text("synthetic-leak-marker")
    elif case == "scanner_failure":
        env["TEST_SCANNER_FAILURE"] = "1"
    elif case == "checksum":
        Path(env["TEST_ARCHIVE"]).write_bytes(b"corrupt download")
    result = subprocess.run(
        ["/bin/bash", "scripts/verify-secrets"],
        cwd=root,
        env=env,
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    assert (result.returncode == 0) is (case == "clean"), result.stderr
    if case in {"clean", "unstaged", "untracked"}:
        files = json.loads(Path(env["TEST_SCAN_LOG"]).read_text())
        assert files["tracked.txt"] == (
            "synthetic-leak-marker" if case == "unstaged" else "original\n"
        )
        if case == "untracked":
            assert files["new.txt"] == "synthetic-leak-marker"
