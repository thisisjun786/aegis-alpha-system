"""Repository recognition is structural, bounded and independent of Git at runtime."""

from __future__ import annotations

import errno
import os
from itertools import combinations
from pathlib import Path

import pytest

from aegis_alpha.storage.checkout import containing_checkout, git_marker, require_outside_checkout
from tests.checkout_support import commit_empty, git, init_repository


@pytest.mark.parametrize(
    "parts",
    [parts for size in range(4) for parts in combinations(("HEAD", "objects", "refs"), size)],
)
def test_directory_marker_requires_all_parts(tmp_path: Path, parts: tuple[str, ...]) -> None:
    marker = tmp_path / ".git"
    marker.mkdir()
    for part in parts:
        if part == "HEAD":
            (marker / part).write_text("ref: refs/heads/main\n")
        else:
            (marker / part).mkdir()
    assert git_marker(tmp_path) is (set(parts) == {"HEAD", "objects", "refs"})


def test_commondir_marker_and_wrong_types(tmp_path: Path) -> None:
    marker = tmp_path / ".git"
    marker.mkdir()
    (marker / "commondir").write_text("../shared\n")
    assert not git_marker(tmp_path)
    (marker / "HEAD").mkdir()
    assert not git_marker(tmp_path)
    (marker / "HEAD").rmdir()
    (marker / "HEAD").write_text("ref: refs/heads/main\n")
    assert git_marker(tmp_path)
    (marker / "commondir").unlink()
    (marker / "objects").write_text("not a directory")
    (marker / "refs").mkdir()
    assert not git_marker(tmp_path)


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        (b"", False),
        (b"synthetic", False),
        (b"\ngitdir: somewhere", False),
        (b" gitdir: somewhere", False),
        (b"gitdir: missing\n", True),
        (b"gitdir:\xff", True),
        (b"gitdir:" + b"x" * 100000, True),
    ],
)
def test_git_file_prefix(tmp_path: Path, content: bytes, *, expected: bool) -> None:
    (tmp_path / ".git").write_bytes(content)
    assert git_marker(tmp_path) is expected


def test_real_repositories_worktrees_and_submodules(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    init_repository(repository)
    commit_empty(repository)
    assert git_marker(repository)
    worktree = tmp_path / "worktree"
    git(repository, "worktree", "add", "--detach", str(worktree))
    assert git_marker(worktree)
    submodule = tmp_path / "source"
    init_repository(submodule)
    commit_empty(submodule)
    git(repository, "-c", "protocol.file.allow=always", "submodule", "add", str(submodule), "sub")
    assert git_marker(repository / "sub")
    assert (repository / "sub" / ".git").is_file()


@pytest.mark.parametrize("kind", ["directory", "file", "empty", "broken", "loop"])
def test_symlinked_markers(tmp_path: Path, kind: str) -> None:
    target = tmp_path / "metadata"
    if kind == "directory":
        init_repository(target)
        target = target / ".git"
    elif kind == "file":
        target.write_text("gitdir: external\n")
    elif kind == "empty":
        target.mkdir()
    elif kind == "loop":
        target.symlink_to(".git")
    (tmp_path / ".git").symlink_to(target.relative_to(tmp_path))
    assert git_marker(tmp_path) is (kind in {"directory", "file", "loop"})


@pytest.mark.parametrize("kind", ["directory", "file"])
def test_unreadable_marker_fails_closed(tmp_path: Path, kind: str) -> None:
    marker = tmp_path / ".git"
    if kind == "directory":
        marker.mkdir()
    else:
        marker.write_text("synthetic")
    marker.chmod(0)
    try:
        if os.geteuid() == 0:
            pytest.skip("root can read mode-000 paths; injected errors cover root runs")
        assert git_marker(tmp_path)
    finally:
        marker.chmod(0o700 if kind == "directory" else 0o600)


@pytest.mark.parametrize("failure", [errno.EACCES, errno.EIO, errno.ENOENT, errno.ENOTDIR])
def test_marker_lstat_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: int) -> None:
    original = Path.lstat

    def lstat(path: Path) -> os.stat_result:
        if path == tmp_path / ".git":
            raise OSError(failure, "synthetic filesystem fault")
        return original(path)

    monkeypatch.setattr(Path, "lstat", lstat)
    assert git_marker(tmp_path) is (failure not in {errno.ENOENT, errno.ENOTDIR})


def test_walk_includes_self_missing_children_and_files(tmp_path: Path) -> None:
    init_repository(tmp_path)
    (tmp_path / "file").write_bytes(b"")
    for path in (tmp_path, tmp_path / "missing" / "child", tmp_path / "file"):
        assert containing_checkout(path) == tmp_path
        with pytest.raises(RuntimeError, match=r"^custom refusal$"):
            require_outside_checkout(path, error=RuntimeError, message="custom refusal")


def test_walk_returns_nearest_and_ignores_synthetic_markers(tmp_path: Path) -> None:
    outer = tmp_path / ".git"
    outer.mkdir()
    assert containing_checkout(tmp_path / "missing") is None
    require_outside_checkout(tmp_path)
    init_repository(tmp_path)
    nested = tmp_path / "nested"
    init_repository(nested)
    assert containing_checkout(nested / "child") == nested
