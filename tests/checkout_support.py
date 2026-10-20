"""Disposable local Git repositories; no remote or user configuration is needed."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


def git(directory: Path, *arguments: str) -> None:
    executable = shutil.which("git")
    assert executable is not None
    subprocess.run(  # noqa: S603 -- local synthetic repository, explicit arguments
        [executable, "-C", str(directory), *arguments],
        check=True,
        capture_output=True,
        timeout=15,
    )


def init_repository(directory: Path) -> None:
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    git(directory, "init", "-q")


def commit_empty(directory: Path) -> None:
    git(
        directory,
        "-c",
        "user.name=Synthetic",
        "-c",
        "user.email=synthetic@example.invalid",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "-qm",
        "synthetic",
        "--allow-empty",
    )
