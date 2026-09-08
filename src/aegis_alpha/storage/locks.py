"""Private filesystem admission held for the entire database connection lifetime."""

from __future__ import annotations

import fcntl
import os
import stat
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path

from aegis_alpha.data.descriptor_tree import DescriptorTree

_REMOTE_FILESYSTEMS = frozenset({"nfs", "nfs4", "cifs", "smb3", "9p", "fuse.sshfs", "ceph", "afs"})


def require_outside_checkout(path: Path) -> None:
    if any((parent / ".git").exists() for parent in (path, *path.parents)):
        raise ValueError("persistent storage cannot be inside a Git checkout")


def private_directory(path: Path, *, create: bool = False) -> None:
    require_outside_checkout(path)
    if create:
        with DescriptorTree.open_path(Path(path.anchor)) as anchor:
            anchor.mkdir(path.relative_to(path.anchor), parents=True, exist_ok=True)
    with DescriptorTree.open_path(path) as tree:
        info = os.fstat(tree.descriptor)
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise ValueError("storage directory must be owned and private (mode 0700)")
    mount_file = Path("/proc/self/mountinfo")
    if mount_file.exists():
        mounts = []
        for line in mount_file.read_text().splitlines():
            left, right = line.split(" - ", 1)
            mount = Path(left.split()[4].replace("\\040", " ").replace("\\134", "\\"))
            if path.is_relative_to(mount):
                mounts.append((len(mount.parts), right.split()[0]))
        if mounts and max(mounts)[1] in _REMOTE_FILESYSTEMS:
            raise ValueError("embedded databases require a local filesystem")


def private_file(path: Path) -> os.stat_result:
    with DescriptorTree.open_path(path.parent) as tree:
        info = tree.stat(path.name)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise ValueError("storage file must be private, owned and not linked")
    return info


@contextmanager
def file_lock(path: Path) -> Iterator[None]:
    private_directory(path.parent)
    with (
        DescriptorTree.open_path(path.parent) as tree,
        tree.binary_writer(path.name, append=True, truncate=False) as handle,
    ):
        info = private_file(path)
        opened = os.fstat(handle.fileno())
        if (info.st_dev, info.st_ino) != (opened.st_dev, opened.st_ino):
            raise ValueError("storage lock was replaced")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("installation_busy: another AAS command owns this storage") from None
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def storage_locks(root: Path, stores: tuple[Path, ...]) -> Iterator[None]:
    with ExitStack() as stack:
        stack.enter_context(file_lock(root / ".storage.lock"))
        for path in sorted(stores):
            stack.enter_context(file_lock(path.with_name(path.name + ".lock")))
        yield
