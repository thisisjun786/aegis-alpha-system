"""Recognize Git checkout markers without invoking Git or trusting empty mount targets."""

from __future__ import annotations

import os
import stat
from pathlib import Path


def _mode(path: Path) -> int:
    try:
        return path.stat().st_mode
    except (FileNotFoundError, NotADirectoryError):
        return 0


def _git_file(marker: Path) -> bool:
    # Bound the read to the prefix. Nonblocking/no-follow also avoids hanging on a
    # FIFO or following a replacement symlink between lstat and open.
    descriptor = os.open(marker, os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK | os.O_NOFOLLOW)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            return True  # Changed during inspection: fail closed.
        return os.read(descriptor, len(b"gitdir:")) == b"gitdir:"
    finally:
        os.close(descriptor)


def git_marker(directory: Path) -> bool:
    """Recognize a gitdir file or HEAD plus objects/refs (or commondir).

    Resolve a symlinked marker once, then inspect it by the same rules. Missing
    paths, including a disappearing empty mount target, are not repositories;
    unreadable markers and other inspection failures are refused conservatively.
    """
    marker = directory / ".git"
    try:
        mode = marker.lstat().st_mode
        if stat.S_ISLNK(mode):
            marker = marker.resolve(strict=True)
            mode = marker.lstat().st_mode
        if stat.S_ISREG(mode):
            return _git_file(marker)
        if not stat.S_ISDIR(mode):
            return False
        return stat.S_ISREG(_mode(marker / "HEAD")) and (
            stat.S_ISREG(_mode(marker / "commondir"))
            or (stat.S_ISDIR(_mode(marker / "objects")) and stat.S_ISDIR(_mode(marker / "refs")))
        )
    except (FileNotFoundError, NotADirectoryError):
        return False
    except (OSError, RuntimeError):  # Path.resolve reports symlink loops as RuntimeError on 3.12.
        return True


def containing_checkout(path: Path) -> Path | None:
    """Return the nearest checkout containing path, including path itself."""
    return next((parent for parent in (path, *path.parents) if git_marker(parent)), None)


def require_outside_checkout(
    path: Path,
    *,
    error: type[Exception] = ValueError,
    message: str = "persistent storage cannot be inside a Git checkout",
) -> None:
    if containing_checkout(path) is not None:
        raise error(message)
