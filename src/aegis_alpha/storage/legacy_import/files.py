"""Byte access for legacy units, with every read bound to the hash the source ID uses.

A loader reads a unit's files only through a ``Bytes`` object. ``OriginalBytes`` reads the
admitted original where it lies (a plan) and records the SHA-256 and size of what it read;
``RetainedBytes`` reads the content-addressed copy in ``raw/`` that an apply retained and
refuses any byte that differs from it. Either way the rows a loader emits come from exactly
the bytes whose hashes name the source, and a file the unit lists but the loader never read,
or a read outside the unit, fails the unit.
"""

from __future__ import annotations

import hashlib
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, BinaryIO, Protocol

from aegis_alpha.data.descriptor_tree import DescriptorTree, DescriptorTreeError
from aegis_alpha.storage.paths import private_source_file, same_private_file
from aegis_alpha.storage.source_identity import SourceFile

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from contextlib import AbstractContextManager
    from pathlib import Path

_CHUNK = 1024 * 1024


class Bytes(Protocol):
    """Read access to one unit's original files."""

    seen: dict[Path, SourceFile]

    def read(self, path: Path, *, max_bytes: int) -> bytes:
        """Return a whole file of at most ``max_bytes``."""
        ...

    def stream(self, path: Path) -> AbstractContextManager[BinaryIO]:
        """Open a seekable handle on a file too large to hold in memory."""
        ...


def _hash_handle(handle: BinaryIO) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    for chunk in iter(lambda: handle.read(_CHUNK), b""):
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


class _Recorder:
    def __init__(self) -> None:
        self.seen: dict[Path, SourceFile] = {}

    def _record(self, path: Path, item: SourceFile) -> None:
        if self.seen.setdefault(path, item) != item:
            raise ValueError(f"legacy file changed while it was read: {path.name}")

    def files(self, paths: tuple[Path, ...]) -> tuple[SourceFile, ...]:
        """The hashes of exactly ``paths``; every one must have been read, and no other."""
        unread = [path.name for path in paths if path not in self.seen]
        if unread:
            raise ValueError(f"legacy unit lists a file its loader never read: {unread[0]}")
        extra = sorted(path.name for path in set(self.seen) - set(paths))
        if extra:
            raise ValueError(f"legacy loader read a file outside its unit: {extra[0]}")
        return tuple(self.seen[path] for path in paths)


class OriginalBytes(_Recorder):
    """Admitted private originals in place; each read records the hash it observed."""

    def read(self, path: Path, *, max_bytes: int) -> bytes:
        admitted = private_source_file(path)
        try:
            with (
                DescriptorTree.open_path(path.parent) as tree,
                tree.binary_reader(path.name, require_single_link=True) as handle,
            ):
                if not same_private_file(admitted, os.fstat(handle.fileno())):
                    raise ValueError(f"legacy file changed before reading: {path.name}")
                payload = handle.read(max_bytes + 1)
                if not same_private_file(admitted, os.fstat(handle.fileno())):
                    raise ValueError(f"legacy file changed while reading: {path.name}")
        except DescriptorTreeError as error:
            raise ValueError(f"cannot read legacy file {path.name}: {error}") from None
        if len(payload) > max_bytes:
            raise ValueError(f"legacy file exceeds its size bound: {path.name}")
        self._record(path, SourceFile(hashlib.sha256(payload).hexdigest(), len(payload)))
        return payload

    @contextmanager
    def stream(self, path: Path) -> Iterator[BinaryIO]:
        admitted = private_source_file(path)
        try:
            with (
                DescriptorTree.open_path(path.parent) as tree,
                tree.binary_reader(path.name, require_single_link=True) as handle,
            ):
                if not same_private_file(admitted, os.fstat(handle.fileno())):
                    raise ValueError(f"legacy file changed before reading: {path.name}")
                digest, size = _hash_handle(handle)
                self._record(path, SourceFile(digest, size))
                handle.seek(0)
                yield handle
                if not same_private_file(admitted, os.fstat(handle.fileno())):
                    raise ValueError(f"legacy file changed while reading: {path.name}")
        except DescriptorTreeError as error:
            raise ValueError(f"cannot read legacy file {path.name}: {error}") from None


class RetainedBytes(_Recorder):
    """The ``raw/`` copies an apply retained, addressed by the hash each original had."""

    def __init__(self, raw_root: Path, retained: Mapping[Path, SourceFile]) -> None:
        super().__init__()
        self._root = raw_root
        self._retained = dict(retained)

    def _item(self, path: Path) -> SourceFile:
        try:
            return self._retained[path]
        except KeyError:
            raise ValueError(f"legacy loader read a file outside its unit: {path.name}") from None

    def read(self, path: Path, *, max_bytes: int) -> bytes:
        item = self._item(path)
        if item.size_bytes > max_bytes:
            raise ValueError(f"legacy file exceeds its size bound: {path.name}")
        with DescriptorTree.open_path(self._root) as tree:
            payload = tree.read_bytes(item.relative_path, max_bytes=item.size_bytes)
        if hashlib.sha256(payload).hexdigest() != item.sha256:
            raise ValueError("retained raw object does not match its address")
        self._record(path, item)
        return payload

    @contextmanager
    def stream(self, path: Path) -> Iterator[BinaryIO]:
        item = self._item(path)
        with (
            DescriptorTree.open_path(self._root) as tree,
            tree.binary_reader(item.relative_path, require_single_link=True) as handle,
        ):
            if os.fstat(handle.fileno()).st_size != item.size_bytes:
                raise ValueError("retained raw object does not match its address")
            # ingest_arrow re-hashes every retained object before it reads the rows.
            self._record(path, item)
            yield handle
