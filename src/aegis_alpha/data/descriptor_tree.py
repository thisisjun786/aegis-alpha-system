"""Private descriptor-relative filesystem primitives for Norgate runtime I/O.

The logical paths carried by plans and receipts remain ordinary ``Path``
values.  This module owns execution-only directory descriptors so content I/O
cannot be redirected by replacing a visible ancestor after admission.
"""

from __future__ import annotations

import os
import secrets
import stat
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Self

if TYPE_CHECKING:
    from aegis_alpha.data.data_root import BoundDataPath, DataRootCapability


class DescriptorTreeError(ValueError):
    """A descriptor tree could not perform an alias-safe operation."""


_READ_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class DirectoryToken:
    device: int
    inode: int
    mode: int


def _directory_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )


def _leaf_flags(flags: int) -> int:
    return (
        flags
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )


def _token(value: os.stat_result, *, label: str) -> DirectoryToken:
    if not stat.S_ISDIR(value.st_mode):
        raise DescriptorTreeError(f"{label} must be a directory")
    return DirectoryToken(value.st_dev, value.st_ino, value.st_mode)


def _parts(value: str | os.PathLike[str], *, allow_root: bool = False) -> tuple[str, ...]:
    raw = os.fspath(value)
    if not isinstance(raw, str):
        raise DescriptorTreeError("relative descriptor path must be text without NUL")
    # A str subclass may answer split, startswith, endswith or __contains__ with something
    # its text never contained, which would let components reach os.open(dir_fd=...) that no
    # check here ever saw. Invoking the base descriptor cannot be interposed, so every check
    # below reads an exact str.
    text = str.__str__(raw)
    if "\x00" in text:
        raise DescriptorTreeError("relative descriptor path must be text without NUL")
    if text in {"", "."}:
        if allow_root:
            return ()
        raise DescriptorTreeError("relative descriptor path must name a child")
    # Split the relative path here instead of routing it through PurePosixPath.
    # pathlib interns every component it parses, and these components are
    # caller-supplied content identifiers: a content-addressed digest parsed this
    # way becomes an immortal interned string for the life of the process, and the
    # insertion that crosses the interned dictionary's next doubling threshold
    # charges the whole new keys table to whatever code happened to make it.
    if text.startswith(("/", "~")):
        raise DescriptorTreeError("descriptor path must be relative")
    parts = tuple(text.split("/"))
    # PurePosixPath used to drop "." components, so a path spelled with them
    # differed from its parsed form and was refused here rather than below.
    if "//" in text or text.endswith("/") or "." in parts:
        raise DescriptorTreeError("descriptor path must use its exact lexical spelling")
    if any(part in {"", "..", "~"} for part in parts):
        raise DescriptorTreeError("descriptor path contains an alias component")
    return parts


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError("short descriptor write")
        offset += written


class DescriptorTree(AbstractContextManager["DescriptorTree"]):
    """An owned directory FD paired with its logical, serializable path."""

    __slots__ = ("_closed", "_fd", "_identity", "logical_root")

    def __init__(self, logical_root: Path, descriptor: int, *, duplicate: bool = True) -> None:
        owned = os.dup(descriptor) if duplicate else descriptor
        try:
            identity = _token(os.fstat(owned), label="descriptor tree root")
        except BaseException:
            with suppress(OSError):
                os.close(owned)
            raise
        self.logical_root = logical_root
        self._fd = owned
        self._identity = identity
        self._closed = False

    @classmethod
    def open_path(cls, path: Path) -> Self:
        # Read the root's own text once and rebuild an ordinary Path from it: a Path
        # subclass can report an anchor and parts that disagree with the path it names,
        # and those components are what os.open walks below.
        raw = os.fspath(path)
        if not isinstance(raw, str):
            raise DescriptorTreeError("descriptor tree root must be text")
        root = Path(str.__str__(raw))
        if not root.is_absolute():
            raise DescriptorTreeError("descriptor tree root must be absolute")
        descriptor: int | None = None
        try:
            descriptor = os.open(root.anchor, _directory_flags())
            for component in root.parts[1:]:
                child = os.open(component, _directory_flags(), dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
            return cls(root, descriptor, duplicate=False)
        except OSError as error:
            if descriptor is not None:
                with suppress(OSError):
                    os.close(descriptor)
            raise DescriptorTreeError(
                "descriptor tree root cannot be opened without aliases"
            ) from error

    @property
    def descriptor(self) -> int:
        self._require_open()
        return self._fd

    @property
    def identity(self) -> DirectoryToken:
        self._require_open()
        return self._identity

    def __enter__(self) -> Self:
        self._require_open()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with suppress(OSError):
            os.close(self._fd)

    def _require_open(self) -> None:
        if self._closed:
            raise DescriptorTreeError("descriptor tree is closed")
        try:
            observed = _token(os.fstat(self._fd), label="descriptor tree root")
        except OSError as error:
            raise DescriptorTreeError("descriptor tree FD is invalid") from error
        if observed != self._identity:
            raise DescriptorTreeError("descriptor tree root identity changed")

    def _open_directory_fd(self, relative: str | os.PathLike[str] = ".") -> int:
        components = _parts(relative, allow_root=True)
        descriptor = os.dup(self.descriptor)
        try:
            for component in components:
                child = os.open(component, _directory_flags(), dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
        except BaseException:
            with suppress(OSError):
                os.close(descriptor)
            raise
        _token(os.fstat(descriptor), label="descriptor subtree")
        return descriptor

    @contextmanager
    def open_directory(self, relative: str | os.PathLike[str] = ".") -> Iterator[int]:
        try:
            descriptor = self._open_directory_fd(relative)
        except OSError as error:
            raise DescriptorTreeError("directory cannot be opened without aliases") from error
        try:
            yield descriptor
        finally:
            with suppress(OSError):
                os.close(descriptor)

    def subtree(self, relative: str | os.PathLike[str]) -> DescriptorTree:
        try:
            descriptor = self._open_directory_fd(relative)
        except OSError as error:
            raise DescriptorTreeError("subtree cannot be opened without aliases") from error
        return DescriptorTree(
            self.logical_root.joinpath(*_parts(relative)),
            descriptor,
            duplicate=False,
        )

    @contextmanager
    def _parent(self, relative: str | os.PathLike[str]) -> Iterator[tuple[int, str]]:
        components = _parts(relative)
        parent_parts = components[:-1]
        parent_text = "/".join(parent_parts) if parent_parts else "."
        with self.open_directory(parent_text) as descriptor:
            yield descriptor, components[-1]

    def stat(self, relative: str | os.PathLike[str] = ".") -> os.stat_result:
        components = _parts(relative, allow_root=True)
        if not components:
            return os.fstat(self.descriptor)
        with self._parent(relative) as (parent, name):
            try:
                return os.stat(name, dir_fd=parent, follow_symlinks=False)
            except OSError as error:
                raise DescriptorTreeError(
                    f"descriptor path cannot be stated: {relative}"
                ) from error

    def exists(self, relative: str | os.PathLike[str]) -> bool:
        try:
            self.stat(relative)
        except DescriptorTreeError as error:
            if isinstance(error.__cause__, FileNotFoundError):
                return False
            raise
        return True

    def listdir(self, relative: str | os.PathLike[str] = ".") -> tuple[str, ...]:
        with self.open_directory(relative) as descriptor:
            try:
                return tuple(sorted(os.listdir(descriptor), key=str.casefold))  # noqa: PTH208
            except OSError as error:
                raise DescriptorTreeError("descriptor directory cannot be listed") from error

    def mkdir(
        self,
        relative: str | os.PathLike[str],
        *,
        parents: bool = False,
        exist_ok: bool = False,
        mode: int = 0o700,
    ) -> None:
        components = _parts(relative)
        descriptor = os.dup(self.descriptor)
        try:
            for index, component in enumerate(components):
                final = index == len(components) - 1
                try:
                    child = os.open(component, _directory_flags(), dir_fd=descriptor)
                except FileNotFoundError:
                    if not final and not parents:
                        raise
                    os.mkdir(component, mode, dir_fd=descriptor)
                    child = os.open(component, _directory_flags(), dir_fd=descriptor)
                else:
                    if final and not exist_ok:
                        os.close(child)
                        raise FileExistsError(os.fspath(relative))
                os.close(descriptor)
                descriptor = child
        except OSError as error:
            raise DescriptorTreeError(
                f"descriptor directory cannot be created: {relative}"
            ) from error
        finally:
            with suppress(OSError):
                os.close(descriptor)

    def _open_regular_fd(
        self,
        relative: str | os.PathLike[str],
        flags: int,
        *,
        mode: int = 0o600,
        require_single_link: bool = False,
    ) -> int:
        with self._parent(relative) as (parent, name):
            try:
                descriptor = os.open(name, _leaf_flags(flags), mode, dir_fd=parent)
            except OSError as error:
                raise DescriptorTreeError(f"regular file cannot be opened: {relative}") from error
        try:
            information = os.fstat(descriptor)
        except OSError as error:
            with suppress(OSError):
                os.close(descriptor)
            raise DescriptorTreeError(f"regular file cannot be inspected: {relative}") from error
        if not stat.S_ISREG(information.st_mode):
            os.close(descriptor)
            raise DescriptorTreeError(f"descriptor leaf is not a regular file: {relative}")
        if require_single_link and information.st_nlink != 1:
            os.close(descriptor)
            raise DescriptorTreeError(f"descriptor leaf must not be hard-linked: {relative}")
        return descriptor

    @contextmanager
    def binary_reader(
        self,
        relative: str | os.PathLike[str],
        *,
        require_single_link: bool = False,
    ) -> Iterator[BinaryIO]:
        descriptor = self._open_regular_fd(
            relative,
            os.O_RDONLY,
            require_single_link=require_single_link,
        )
        with os.fdopen(descriptor, "rb", closefd=True) as handle:
            yield handle

    @contextmanager
    def binary_writer(
        self,
        relative: str | os.PathLike[str],
        *,
        exclusive: bool = False,
        append: bool = False,
        truncate: bool = True,
        mode: int = 0o600,
    ) -> Iterator[BinaryIO]:
        flags = os.O_WRONLY | os.O_CREAT
        if exclusive:
            flags |= os.O_EXCL
        if append:
            flags |= os.O_APPEND
        elif truncate:
            flags |= os.O_TRUNC
        descriptor = self._open_regular_fd(relative, flags, mode=mode)
        file_mode = "ab" if append else "wb"
        with os.fdopen(descriptor, file_mode, closefd=True) as handle:
            yield handle

    def read_bytes(
        self,
        relative: str | os.PathLike[str],
        *,
        max_bytes: int | None = None,
    ) -> bytes:
        if max_bytes is not None and max_bytes < 0:
            raise DescriptorTreeError("descriptor read cap must not be negative")
        with self.binary_reader(relative) as handle:
            chunks: list[bytes] = []
            total = 0
            while True:
                # Ask for at most what the cap still allows, plus the single byte
                # that proves the file is over it. A fixed megabyte request
                # allocates that megabyte whatever the cap says, so a caller that
                # approved 128 KiB was charged eight times the size it admitted.
                allowed = _READ_CHUNK_BYTES
                if max_bytes is not None:
                    allowed = min(allowed, max_bytes + 1 - total)
                chunk = handle.read(allowed)
                if not chunk:
                    break
                total += len(chunk)
                if max_bytes is not None and total > max_bytes:
                    raise DescriptorTreeError(f"descriptor file exceeds size cap: {relative}")
                chunks.append(chunk)
            return b"".join(chunks)

    def atomic_write_bytes(self, relative: str | os.PathLike[str], payload: bytes) -> None:
        with self._parent(relative) as (parent, name):
            temporary = f".{name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
            descriptor: int | None = None
            try:
                descriptor = os.open(
                    temporary,
                    _leaf_flags(os.O_WRONLY | os.O_CREAT | os.O_EXCL),
                    0o600,
                    dir_fd=parent,
                )
                _write_all(descriptor, payload)
                os.fsync(descriptor)
                os.close(descriptor)
                descriptor = None
                os.replace(temporary, name, src_dir_fd=parent, dst_dir_fd=parent)
                os.fsync(parent)
            except OSError as error:
                raise DescriptorTreeError(f"atomic descriptor write failed: {relative}") from error
            finally:
                if descriptor is not None:
                    with suppress(OSError):
                        os.close(descriptor)
                with suppress(FileNotFoundError):
                    os.unlink(temporary, dir_fd=parent)

    def fsync_file(self, relative: str | os.PathLike[str]) -> None:
        descriptor = self._open_regular_fd(relative, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def fsync_directory(self, relative: str | os.PathLike[str] = ".") -> None:
        with self.open_directory(relative) as descriptor:
            os.fsync(descriptor)

    def unlink(self, relative: str | os.PathLike[str], *, missing_ok: bool = False) -> None:
        with self._parent(relative) as (parent, name):
            try:
                os.unlink(name, dir_fd=parent)
            except FileNotFoundError:
                if not missing_ok:
                    raise
            except OSError as error:
                raise DescriptorTreeError(
                    f"descriptor leaf cannot be unlinked: {relative}"
                ) from error

    def rmdir(self, relative: str | os.PathLike[str]) -> None:
        with self._parent(relative) as (parent, name):
            try:
                os.rmdir(name, dir_fd=parent)
            except OSError as error:
                raise DescriptorTreeError(
                    f"descriptor directory cannot be removed: {relative}"
                ) from error

    def rename_to(
        self,
        source: str | os.PathLike[str],
        target_tree: DescriptorTree,
        target: str | os.PathLike[str],
        *,
        require_missing: bool = True,
    ) -> None:
        with (
            self._parent(source) as (source_parent, source_name),
            target_tree._parent(target) as (target_parent, target_name),
        ):
            if require_missing:
                try:
                    os.stat(target_name, dir_fd=target_parent, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise FileExistsError(os.fspath(target))
            try:
                os.rename(
                    source_name,
                    target_name,
                    src_dir_fd=source_parent,
                    dst_dir_fd=target_parent,
                )
                os.fsync(target_parent)
            except OSError as error:
                raise DescriptorTreeError("descriptor-relative rename failed") from error

    def directory_identity(self, relative: str | os.PathLike[str] = ".") -> DirectoryToken:
        with self.open_directory(relative) as descriptor:
            return _token(os.fstat(descriptor), label="descriptor directory")

    def assert_directory_identity(
        self,
        relative: str | os.PathLike[str],
        expected: DirectoryToken,
    ) -> None:
        if self.directory_identity(relative) != expected:
            raise DescriptorTreeError(f"descriptor directory identity changed: {relative}")

    def remove_tree(
        self,
        relative: str | os.PathLike[str],
        *,
        expected: DirectoryToken | None = None,
    ) -> None:
        with self._parent(relative) as (parent, name):
            try:
                target = os.open(name, _directory_flags(), dir_fd=parent)
            except OSError as error:
                raise DescriptorTreeError(f"owned tree cannot be opened: {relative}") from error
            try:
                identity = _token(os.fstat(target), label="owned tree")
                if expected is not None and identity != expected:
                    raise DescriptorTreeError(f"owned tree identity changed: {relative}")
                self._remove_contents(target)
                try:
                    visible = _token(
                        os.stat(name, dir_fd=parent, follow_symlinks=False),
                        label="visible owned tree",
                    )
                except OSError as error:
                    raise DescriptorTreeError(
                        f"visible owned tree disappeared during cleanup: {relative}"
                    ) from error
                if visible != identity:
                    raise DescriptorTreeError(
                        f"visible owned tree was replaced during cleanup: {relative}"
                    )
                os.rmdir(name, dir_fd=parent)
                os.fsync(parent)
            finally:
                os.close(target)

    @classmethod
    def _remove_contents(cls, descriptor: int) -> None:
        for name in tuple(os.listdir(descriptor)):
            information = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            if stat.S_ISDIR(information.st_mode):
                child = os.open(name, _directory_flags(), dir_fd=descriptor)
                try:
                    identity = _token(os.fstat(child), label="owned child directory")
                    cls._remove_contents(child)
                    visible = _token(
                        os.stat(name, dir_fd=descriptor, follow_symlinks=False),
                        label="visible owned child directory",
                    )
                    if visible != identity:
                        raise DescriptorTreeError(
                            "owned child directory was replaced during cleanup"
                        )
                    os.rmdir(name, dir_fd=descriptor)
                finally:
                    os.close(child)
            else:
                os.unlink(name, dir_fd=descriptor)
        os.fsync(descriptor)


class NorgateRuntimeIO(AbstractContextManager["NorgateRuntimeIO"]):
    """One production invocation's retained source and output-parent trees."""

    __slots__ = (
        "_capability",
        "_closed",
        "_published",
        "output_binding",
        "output_leaf",
        "output_parent",
        "source",
        "source_binding",
    )

    def __init__(
        self,
        capability: DataRootCapability,
        source_binding: BoundDataPath,
        output_binding: BoundDataPath,
    ) -> None:
        source = capability.open_bound_tree(source_binding)
        try:
            output_parent = capability.open_bound_parent_tree(output_binding)
        except BaseException:
            source.close()
            raise
        self._capability = capability
        self.source_binding = source_binding
        self.output_binding = output_binding
        self.source = source
        self.output_parent = output_parent
        self.output_leaf = output_binding.path.name
        self._published: DescriptorTree | None = None
        self._closed = False

    def __enter__(self) -> Self:
        self.revalidate()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def revalidate(self) -> None:
        if self._closed:
            raise DescriptorTreeError("Norgate runtime I/O context is closed")
        self._capability.revalidate_path(self.source_binding)
        self._capability.revalidate_path(self.output_binding)

    def bind_published_output(self, *, expected: DirectoryToken) -> DescriptorTree:
        """Bind a just-published leaf to its identity captured before rename."""

        if self._closed:
            raise DescriptorTreeError("Norgate runtime I/O context is closed")
        candidate = self.output_parent.subtree(self.output_leaf)
        try:
            self._assert_tree_matches_token(candidate, expected)
            binding = self._capability.bind_path(
                self.output_binding.path,
                namespace="normalized",
            )
            old_parent_components = (
                self.output_binding.components[:-1]
                if self.output_binding.final_exists
                else self.output_binding.components
            )
            self._assert_parent_components(binding, old_parent_components)
            self._assert_tree_matches_binding(candidate, binding)
            self._capability.revalidate_path(binding)
        except BaseException:
            candidate.close()
            raise
        if self._published is not None:
            self._published.close()
        self.output_binding = binding
        self._published = candidate
        return self._published

    def published_output(self) -> DescriptorTree:
        """Return the retained published tree, opening a pre-existing leaf once."""

        if self._closed:
            raise DescriptorTreeError("Norgate runtime I/O context is closed")
        if self._published is None:
            if not self.output_binding.final_exists:
                raise DescriptorTreeError(
                    "a newly published output requires its pre-rename identity"
                )
            self._capability.revalidate_path(self.output_binding)
            candidate = self.output_parent.subtree(self.output_leaf)
            try:
                self._assert_tree_matches_binding(candidate, self.output_binding)
                self._capability.revalidate_path(self.output_binding)
            except BaseException:
                candidate.close()
                raise
            self._published = candidate
        return self._published

    @staticmethod
    def _assert_tree_matches_token(
        tree: DescriptorTree,
        expected: DirectoryToken,
    ) -> None:
        observed = tree.identity
        if (observed.device, observed.inode) != (expected.device, expected.inode):
            raise DescriptorTreeError("published output identity differs from staged dataset")

    @staticmethod
    def _assert_parent_components(
        binding: BoundDataPath,
        expected: tuple[object, ...],
    ) -> None:
        if binding.components[:-1] != expected:
            raise DescriptorTreeError("published output parent was replaced")

    @staticmethod
    def _assert_tree_matches_binding(
        tree: DescriptorTree,
        binding: BoundDataPath,
    ) -> None:
        if not binding.final_exists:
            raise DescriptorTreeError("published output binding must exist")
        expected = binding.components[-1]
        observed = tree.identity
        if (observed.device, observed.inode) != (expected.device, expected.inode):
            raise DescriptorTreeError("published output was replaced while being pinned")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._published is not None:
            self._published.close()
        self.output_parent.close()
        self.source.close()


__all__ = [
    "DescriptorTree",
    "DescriptorTreeError",
    "DirectoryToken",
    "NorgateRuntimeIO",
]
