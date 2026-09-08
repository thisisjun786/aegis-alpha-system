"""Fail-closed resolution of the machine-local Aegis data root.

The data root is deployment configuration, not a repository default.  Every
production command that creates or consumes path-bound evidence must resolve
``AAS_DATA_ROOT`` through this module and pass the resulting immutable
:class:`DataRoot` through its call graph.  The resolver intentionally performs
lexical and filesystem admission together: a path which merely *looks* like a
directory through an alias is not a valid production root.

The Linux deployment path belongs in host configuration and documentation, not
in this runtime module.  The environment is always authoritative.
"""

from __future__ import annotations

import os
import stat
from contextlib import AbstractContextManager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final, Literal, Self

if TYPE_CHECKING:
    from aegis_alpha.data.descriptor_tree import DescriptorTree

DATA_ROOT_ENV_VAR: Final = "AAS_DATA_ROOT"

type PathLike = str | os.PathLike[str]


class DataRootError(ValueError):
    """The configured data root is absent, malformed, or not trustworthy."""


@dataclass(frozen=True, slots=True)
class DataRoot:
    """The one admitted root and its fixed top-level evidence namespaces.

    All fields are immutable ``Path`` values derived from the same validated
    root.  They intentionally preserve the configured spelling; the resolver
    rejects aliases and lexical normalization before constructing this object,
    so evidence paths remain path-sensitive and deterministic.
    """

    root: Path
    raw: Path
    normalized: Path
    canonical: Path
    identity: Path
    owner_receipts: Path
    backups: Path

    def __post_init__(self) -> None:
        """Reject hand-built instances whose namespaces are not root-derived.

        ``DataRoot`` is intentionally public and frozen, so callers can still
        construct one directly.  The capability boundary must not trust a
        plausible-looking object with a substituted ``raw`` or ``normalized``
        path; validate the derivation invariant at construction time as well
        as when opening a capability.
        """

        if not isinstance(self.root, Path):
            raise DataRootError("data root fields must be pathlib.Path values")
        try:
            root_text = os.fspath(self.root)
        except TypeError as error:  # pragma: no cover - Path is checked above
            raise DataRootError("data root must be a filesystem path") from error
        _validate_lexical_path(root_text)
        expected = {
            "raw": self.root / "raw",
            "normalized": self.root / "normalized",
            "canonical": self.root / "canonical",
            "identity": self.root / "identity",
            "owner_receipts": self.root / "owner-receipts",
            "backups": self.root / "backups",
        }
        for field_name, expected_path in expected.items():
            value = getattr(self, field_name)
            if not isinstance(value, Path) or value != expected_path:
                raise DataRootError(f"data root {field_name} must be derived exactly from root")

    @property
    def raw_root(self) -> Path:
        return self.raw

    @property
    def normalized_root(self) -> Path:
        return self.normalized

    @property
    def canonical_root(self) -> Path:
        return self.canonical

    @property
    def identity_root(self) -> Path:
        return self.identity

    @property
    def owner_receipts_root(self) -> Path:
        return self.owner_receipts

    @property
    def backups_root(self) -> Path:
        return self.backups

    @classmethod
    def from_env(cls) -> DataRoot:
        """Resolve the one environment variable used by production paths."""

        return resolve_data_root()

    def open_capability(self) -> DataRootCapability:
        """Open a scoped descriptor capability for this admitted root."""

        return open_data_root_capability(self)


@dataclass(frozen=True, slots=True)
class _DirectoryIdentity:
    path: Path
    device: int
    inode: int


@dataclass(frozen=True, slots=True)
class BoundDataPath:
    """Lexical path and component identities admitted below one namespace."""

    path: Path
    namespace: Literal["raw", "normalized"]
    components: tuple[_DirectoryIdentity, ...]
    final_exists: bool


class DataRootCapability(AbstractContextManager["DataRootCapability"]):
    """A short-lived FD/identity capability for the configured data root.

    The root and its ``raw``/``normalized`` namespaces are opened one
    component at a time with ``O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC``.  Descendant
    admission uses the corresponding namespace FD and records device/inode
    identities for later visible-path revalidation.  The object is deliberately
    not part of any plan or receipt identity; it is an execution-only guard.
    """

    __slots__ = (
        "_closed",
        "_identities",
        "_normalized_fd",
        "_raw_fd",
        "_root_fd",
        "data_root",
    )

    def __init__(self, data_root: DataRoot) -> None:
        _validate_data_root_shape(data_root)
        descriptors: dict[str, int] = {}
        identities: dict[str, tuple[_DirectoryIdentity, ...]] = {}
        try:
            for key, path in (
                ("root", data_root.root),
                ("raw", data_root.raw),
                ("normalized", data_root.normalized),
            ):
                descriptor, components = _open_directory_path(path, label=f"data root {key}")
                descriptors[key] = descriptor
                identities[key] = components
        except BaseException:
            for descriptor in descriptors.values():
                _close_quietly(descriptor)
            raise
        self.data_root = data_root
        self._root_fd = descriptors["root"]
        self._raw_fd = descriptors["raw"]
        self._normalized_fd = descriptors["normalized"]
        self._identities = identities
        self._closed = False

    @property
    def root_fd(self) -> int:
        return self._descriptor(self._root_fd, "root")

    @property
    def raw_fd(self) -> int:
        return self._descriptor(self._raw_fd, "raw")

    @property
    def normalized_fd(self) -> int:
        return self._descriptor(self._normalized_fd, "normalized")

    def __enter__(self) -> Self:
        self._require_open()
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for descriptor in (self._root_fd, self._raw_fd, self._normalized_fd):
            _close_quietly(descriptor)

    def revalidate(self) -> None:
        """Recheck the visible environment spelling and all three identities."""

        self._require_open()
        configured = os.environ.get(DATA_ROOT_ENV_VAR)
        expected = os.fspath(self.data_root.root)
        if configured != expected:
            raise DataRootError(
                f"{DATA_ROOT_ENV_VAR} changed while the data-root capability was active"
            )
        try:
            visible = resolve_data_root(configured)
            _validate_data_root_shape(visible)
            opened: dict[str, tuple[_DirectoryIdentity, ...]] = {}
            descriptors: list[int] = []
            try:
                for key, path in (
                    ("root", visible.root),
                    ("raw", visible.raw),
                    ("normalized", visible.normalized),
                ):
                    descriptor, components = _open_directory_path(
                        path, label=f"visible data root {key}"
                    )
                    descriptors.append(descriptor)
                    opened[key] = components
            finally:
                for descriptor in descriptors:
                    _close_quietly(descriptor)
        except DataRootError:
            raise
        except OSError as error:
            raise DataRootError("visible data root could not be revalidated") from error
        for key in ("root", "raw", "normalized"):
            if opened[key] != self._identities[key]:
                raise DataRootError(f"visible data root {key} was replaced during execution")

    def bind_path(
        self,
        path: Path,
        *,
        namespace: Literal["raw", "normalized"],
        allow_missing_final: bool = False,
        create_parents: bool = False,
    ) -> BoundDataPath:
        """Admit a lexical descendant by descriptor-relative traversal.

        ``create_parents`` is used only for the normal import output path.  It
        creates missing intermediate directories with ``mkdirat`` semantics and
        immediately reopens each one with no-follow flags.  The final output
        directory itself is never created by this helper.
        """

        self.revalidate()
        base_path, base_fd = self._namespace(namespace)
        _validate_descendant_path(path, base_path, namespace)
        relative = path.relative_to(base_path)
        parts = relative.parts
        identities: list[_DirectoryIdentity] = list(self._identities[namespace])
        descriptor = os.dup(base_fd)
        try:
            for index, component in enumerate(parts):
                is_final = index == len(parts) - 1
                try:
                    child = os.open(component, _directory_open_flags(), dir_fd=descriptor)
                except FileNotFoundError as error:
                    if is_final and allow_missing_final:
                        return BoundDataPath(
                            path=path,
                            namespace=namespace,
                            components=tuple(identities),
                            final_exists=False,
                        )
                    if not create_parents or is_final:
                        raise DataRootError(
                            f"{namespace} path is missing at component {component}"
                        ) from error
                    with suppress(FileExistsError):
                        os.mkdir(component, 0o700, dir_fd=descriptor)
                    try:
                        child = os.open(component, _directory_open_flags(), dir_fd=descriptor)
                    except OSError as open_error:
                        raise DataRootError(
                            f"{namespace} path component is not a safe directory: {component}"
                        ) from open_error
                except OSError as error:
                    raise DataRootError(
                        f"{namespace} path component must not be a symlink or "
                        f"non-directory: {component}"
                    ) from error
                status = os.fstat(child)
                if not stat.S_ISDIR(status.st_mode):
                    _close_quietly(child)
                    raise DataRootError(f"{namespace} path component is not a directory")
                identities.append(
                    _DirectoryIdentity(
                        path=base_path.joinpath(*parts[: index + 1]),
                        device=status.st_dev,
                        inode=status.st_ino,
                    )
                )
                _close_quietly(descriptor)
                descriptor = child
            return BoundDataPath(
                path=path,
                namespace=namespace,
                components=tuple(identities),
                final_exists=True,
            )
        finally:
            _close_quietly(descriptor)

    def revalidate_path(self, binding: BoundDataPath) -> None:
        """Reopen a previously admitted path and compare every component."""

        self._require_open()
        self.revalidate()
        current = self.bind_path(
            binding.path,
            namespace=binding.namespace,
            allow_missing_final=not binding.final_exists,
        )
        if current.final_exists != binding.final_exists or current.components != binding.components:
            raise DataRootError(f"visible {binding.namespace} path was replaced during execution")

    def open_bound_tree(self, binding: BoundDataPath) -> DescriptorTree:
        """Return an owned descriptor tree for one admitted existing directory.

        The import is local to avoid a module cycle: ``descriptor_tree`` uses
        the public capability protocol only for its execution-scoped context.
        """

        from aegis_alpha.data.descriptor_tree import DescriptorTree  # noqa: PLC0415

        if not binding.final_exists:
            raise DataRootError("an existing bound path is required")
        self.revalidate_path(binding)
        descriptor = self._open_bound_descriptor(binding, parent=False)
        return DescriptorTree(binding.path, descriptor, duplicate=False)

    def open_bound_parent_tree(self, binding: BoundDataPath) -> DescriptorTree:
        """Return an owned descriptor tree for an admitted leaf's parent."""

        from aegis_alpha.data.descriptor_tree import DescriptorTree  # noqa: PLC0415

        self.revalidate_path(binding)
        descriptor = self._open_bound_descriptor(binding, parent=True)
        return DescriptorTree(binding.path.parent, descriptor, duplicate=False)

    def _open_bound_descriptor(self, binding: BoundDataPath, *, parent: bool) -> int:
        base_path, base_fd = self._namespace(binding.namespace)
        _validate_descendant_path(binding.path, base_path, binding.namespace)
        parts = binding.path.relative_to(base_path).parts
        expected = binding.components[-1]
        if parent:
            if not parts:
                raise DataRootError("a namespace root has no bound leaf parent")
            parts = parts[:-1]
            if binding.final_exists:
                expected = binding.components[-2]
        descriptor = os.dup(base_fd)
        try:
            for component in parts:
                child = os.open(component, _directory_open_flags(), dir_fd=descriptor)
                _close_quietly(descriptor)
                descriptor = child
            status = os.fstat(descriptor)
        except OSError as error:
            _close_quietly(descriptor)
            raise DataRootError("bound directory cannot be reopened without aliases") from error
        if not stat.S_ISDIR(status.st_mode):
            _close_quietly(descriptor)
            raise DataRootError("bound descriptor must remain a directory")
        if (status.st_dev, status.st_ino) != (expected.device, expected.inode):
            _close_quietly(descriptor)
            raise DataRootError("bound directory was replaced while being reopened")
        return descriptor

    def _namespace(self, namespace: Literal["raw", "normalized"]) -> tuple[Path, int]:
        if namespace == "raw":
            return self.data_root.raw, self._raw_fd
        if namespace == "normalized":
            return self.data_root.normalized, self._normalized_fd
        raise DataRootError(f"unsupported data-root namespace: {namespace}")

    def _descriptor(self, descriptor: int, label: str) -> int:
        self._require_open()
        try:
            os.fstat(descriptor)
        except OSError as error:
            raise DataRootError(f"data-root {label} descriptor is not open") from error
        return descriptor

    def _require_open(self) -> None:
        if self._closed:
            raise DataRootError("data-root capability is closed")


def open_data_root_capability(data_root: DataRoot | None = None) -> DataRootCapability:
    """Open a capability for ``data_root`` or the current environment."""

    active = configured_data_root() if data_root is None else data_root
    return DataRootCapability(active)


def resolve_data_root(value: PathLike | None = None) -> DataRoot:
    """Resolve and admit ``AAS_DATA_ROOT`` (or an explicit test value).

    ``value`` exists for tests and for callers that have already captured the
    deployment configuration.  When omitted, the environment is read exactly
    once here; production modules do not inspect ``os.environ`` themselves.
    The returned object is safe to pass across module boundaries and cannot be
    mutated after construction.
    """

    raw_value: PathLike | None = value
    if raw_value is None:
        raw_value = os.environ.get(DATA_ROOT_ENV_VAR)
    if raw_value is None:
        raise DataRootError(f"{DATA_ROOT_ENV_VAR} is required for production data paths")
    if isinstance(raw_value, bytes):
        raise DataRootError(f"{DATA_ROOT_ENV_VAR} must be text, not bytes")
    text = os.fspath(raw_value)
    if not isinstance(text, str):  # pragma: no cover - os.fspath's type contract
        raise DataRootError(f"{DATA_ROOT_ENV_VAR} must be text")
    _validate_lexical_path(text)
    root = Path(text)
    _validate_filesystem_path(root, text)
    return DataRoot(
        root=root,
        raw=root / "raw",
        normalized=root / "normalized",
        canonical=root / "canonical",
        identity=root / "identity",
        owner_receipts=root / "owner-receipts",
        backups=root / "backups",
    )


def configured_data_root() -> DataRoot:
    """Named alias for production callers that resolve from the environment."""

    return resolve_data_root()


def _validate_data_root_shape(data_root: DataRoot) -> None:
    if not isinstance(data_root, DataRoot):
        raise DataRootError("data-root capability requires a DataRoot instance")
    # Re-run the invariant explicitly at the capability boundary.  This keeps
    # the guard correct even if a future deserializer or unsafe object bypasses
    # ``DataRoot.__post_init__``.
    try:
        root_text = os.fspath(data_root.root)
    except TypeError as error:
        raise DataRootError("data root must be a filesystem path") from error
    _validate_lexical_path(root_text)
    expected = {
        "raw": data_root.root / "raw",
        "normalized": data_root.root / "normalized",
        "canonical": data_root.root / "canonical",
        "identity": data_root.root / "identity",
        "owner_receipts": data_root.root / "owner-receipts",
        "backups": data_root.root / "backups",
    }
    for field_name, expected_path in expected.items():
        if getattr(data_root, field_name, None) != expected_path:
            raise DataRootError(f"data root {field_name} must be derived exactly from root")


def _validate_descendant_path(
    path: Path,
    base: Path,
    namespace: Literal["raw", "normalized"],
) -> None:
    if not isinstance(path, Path):
        raise DataRootError(f"{namespace} path must be a pathlib.Path")
    if not path.is_absolute():
        raise DataRootError(f"{namespace} path must be absolute")
    if any(part in {".", "..", "~"} for part in path.parts):
        raise DataRootError(f"{namespace} path must not use a path alias")
    if path.as_posix() != os.fspath(path):
        raise DataRootError(f"{namespace} path must use its exact lexical spelling")
    try:
        path.relative_to(base)
    except ValueError as error:
        raise DataRootError(
            f"{namespace} path must derive from AAS_DATA_ROOT/{namespace}"
        ) from error


def _open_directory_path(
    path: Path,
    *,
    label: str,
) -> tuple[int, tuple[_DirectoryIdentity, ...]]:
    """Open an absolute directory one component at a time with no-follow."""

    if not path.is_absolute() or any(part in {".", "..", "~"} for part in path.parts):
        raise DataRootError(f"{label} must be an absolute, non-aliased path")
    descriptor: int | None = None
    components: list[_DirectoryIdentity] = []
    try:
        descriptor = os.open(path.anchor, _directory_open_flags())
        root_status = os.fstat(descriptor)
        if not stat.S_ISDIR(root_status.st_mode):
            raise DataRootError(  # noqa: TRY301 - descriptor admission must fail closed
                f"{label} filesystem anchor is not a directory"
            )
        components.append(
            _DirectoryIdentity(
                path=Path(path.anchor),
                device=root_status.st_dev,
                inode=root_status.st_ino,
            )
        )
        for component in path.parts[1:]:
            try:
                child = os.open(component, _directory_open_flags(), dir_fd=descriptor)
            except OSError as error:
                raise DataRootError(
                    f"{label} must contain only existing non-symlink directories"
                ) from error
            status = os.fstat(child)
            if not stat.S_ISDIR(status.st_mode):
                _close_quietly(child)
                raise DataRootError(  # noqa: TRY301 - child FD is closed before refusal
                    f"{label} component is not a directory: {component}"
                )
            current = Path(path.anchor).joinpath(*path.parts[1 : len(components) + 1])
            components.append(
                _DirectoryIdentity(path=current, device=status.st_dev, inode=status.st_ino)
            )
            _close_quietly(descriptor)
            descriptor = child
        if descriptor is None:  # pragma: no cover - absolute paths always have an anchor
            raise DataRootError(  # noqa: TRY301 - defensive descriptor invariant
                f"{label} could not be opened"
            )
        return descriptor, tuple(components)
    except BaseException:
        if descriptor is not None:
            _close_quietly(descriptor)
        raise


def _directory_open_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )


def _close_quietly(descriptor: int) -> None:
    with suppress(OSError):
        os.close(descriptor)


def _validate_lexical_path(text: str) -> None:
    if not text or not text.strip():
        raise DataRootError(f"{DATA_ROOT_ENV_VAR} must be non-empty")
    if "\x00" in text:
        raise DataRootError(f"{DATA_ROOT_ENV_VAR} must not contain NUL")
    path = Path(text)
    if not path.is_absolute():
        raise DataRootError(f"{DATA_ROOT_ENV_VAR} must be an absolute path")
    # Path.parts retains dot components but collapses repeated separators.  The
    # raw spelling checks below therefore remain necessary for duplicate and
    # trailing separators, which would otherwise create aliases silently.
    if any(part in {".", "..", "~"} for part in path.parts):
        raise DataRootError(
            f"{DATA_ROOT_ENV_VAR} must not contain '.', '..', or '~' path components"
        )
    if text.startswith("~") or "/~/" in text:
        raise DataRootError(f"{DATA_ROOT_ENV_VAR} must not use '~' expansion")
    if "//" in text:
        raise DataRootError(f"{DATA_ROOT_ENV_VAR} must not contain duplicate separators")
    if text.endswith("/"):
        raise DataRootError(f"{DATA_ROOT_ENV_VAR} must not have a trailing separator")
    # ``Path`` may normalize a spelling without changing ``parts`` (for
    # example a platform-specific separator).  Compare the POSIX spelling
    # directly so the configured identity is never rewritten implicitly.
    if path.as_posix() != text:
        raise DataRootError(f"{DATA_ROOT_ENV_VAR} must use its exact normalized spelling")


def _validate_filesystem_path(root: Path, configured: str) -> None:
    """Reject missing, non-directory, aliased, or changed roots before use."""

    current = Path("/")
    try:
        relative = root.relative_to(Path("/"))
    except ValueError as error:  # pragma: no cover - guarded by is_absolute
        raise DataRootError(f"{DATA_ROOT_ENV_VAR} must be absolute") from error
    for component in relative.parts:
        current /= component
        try:
            status = current.lstat()
        except OSError as error:
            raise DataRootError(
                f"{DATA_ROOT_ENV_VAR} must name an existing directory: {configured}"
            ) from error
        if stat.S_ISLNK(status.st_mode):
            raise DataRootError(
                f"{DATA_ROOT_ENV_VAR} must not contain symlink components: {configured}"
            )
        if not stat.S_ISDIR(status.st_mode):
            raise DataRootError(f"{DATA_ROOT_ENV_VAR} must name a directory: {configured}")
    try:
        realpath = os.path.realpath(configured)
    except OSError as error:  # pragma: no cover - realpath normally cannot fail
        raise DataRootError(f"{DATA_ROOT_ENV_VAR} realpath check failed") from error
    if realpath != configured:
        raise DataRootError(
            f"{DATA_ROOT_ENV_VAR} realpath differs from configured path: {configured}"
        )


__all__ = [
    "DATA_ROOT_ENV_VAR",
    "BoundDataPath",
    "DataRoot",
    "DataRootCapability",
    "DataRootError",
    "configured_data_root",
    "open_data_root_capability",
    "resolve_data_root",
]
