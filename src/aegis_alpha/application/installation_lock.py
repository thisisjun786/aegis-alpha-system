from __future__ import annotations

import fcntl
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from aegis_alpha.data.descriptor_tree import DescriptorTree


@contextmanager
def installation_admission(root: Path, *, update: bool) -> Iterator[None]:
    """Hold a shared lease through a container run, or refuse a concurrent update."""
    with DescriptorTree.open_path(root) as tree:
        parent = os.fstat(tree.descriptor)
        if parent.st_uid != os.getuid() or stat.S_IMODE(parent.st_mode) & 0o077:
            raise ValueError("installation admission requires a private owned directory")
        with tree.binary_writer(".installation.lock", append=True, truncate=False) as handle:
            info = os.fstat(handle.fileno())
            if (
                info.st_uid != os.getuid()
                or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) & 0o077
            ):
                raise ValueError("installation admission lock is not privately owned")
            operation = fcntl.LOCK_EX | fcntl.LOCK_NB if update else fcntl.LOCK_SH
            try:
                fcntl.flock(handle.fileno(), operation)
            except BlockingIOError:
                raise RuntimeError("an AAS command or another installer is active") from None
            try:
                with DescriptorTree.open_path(root) as visible:
                    if visible.identity != tree.identity:
                        raise ValueError("installation admission directory was replaced")
                current = tree.stat(".installation.lock")
                if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
                    raise ValueError("installation admission lock was replaced")
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
