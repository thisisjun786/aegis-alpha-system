"""Content-addressed source-library identities and their ``sl:`` state links.

A source ID names the original bytes a loader read, never the code that read them:
``<provider>-<shape>-<hex>`` where ``hex`` is the SHA-256 of the canonical JSON
``["aas-source-id-v1", schema_major, [[relative_path, size, sha256], ...]]``. The
loader's code and transform hashes live in the commit manifest's ``lineage``.

Every completed commit whose pinned bytes are retained in ``raw/`` is linked into
state as one ``source_snapshots`` row (``provider='source-library'``,
``snapshot_id='sl:' + source_id``) plus one ``source_files`` row per original file.
The link is derived only from the commit marker, its durable intent and ``raw/``, so
linking the same commit again changes nothing.

One ID names one file group, so the group is part of the identity: a loader commits
one source per complete original unit whose members the bytes themselves fix (for
example one collection job's ``complete.json`` and the files it lists), never per
batch of units sized by loader code. Regrouping the same files under such a rule
yields the same ID set; regrouping by batch size does not.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from aegis_alpha.data.descriptor_tree import DescriptorTreeError
from aegis_alpha.storage import source_library_schema as schema
from aegis_alpha.storage.raw import verify_raw
from aegis_alpha.storage.state import atomic, get_operation

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from aegis_alpha.storage.workspace import Workspace

SOURCE_ID_FORMAT = "aas-source-id-v1"
LINK_PROVIDER = "source-library"
LINK_PREFIX = "sl:"
_MAX_SOURCE_ID = 240
_PROVIDER = re.compile(r"[a-z0-9]+")
_SHAPE = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_RECORD_KEYS = frozenset({"format", "provider", "shape", "schema_major", "files"})

LinkStatus = Literal["linked", "unchanged", "pending", "unbacked", "corrupt", "incomplete"]
_STATUSES: tuple[LinkStatus, ...] = (
    "linked",
    "unchanged",
    "pending",
    "unbacked",
    "corrupt",
    "incomplete",
)


@dataclass(frozen=True, slots=True)
class SourceFile:
    """One original file retained in ``raw/`` at its content-addressed path."""

    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        if not isinstance(self.sha256, str) or _SHA256.fullmatch(self.sha256) is None:
            raise ValueError("source file hash must be lowercase SHA-256 hex")
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes < 0
        ):
            raise ValueError("source file size must be a nonnegative integer")

    @property
    def relative_path(self) -> str:
        return self.sha256[:2] + "/" + self.sha256

    def entry(self) -> list[object]:
        return [self.relative_path, self.size_bytes, self.sha256]


@dataclass(frozen=True, slots=True)
class SourceContent:
    """The content identity of one source-library commit.

    ``files`` is normalized to one entry per distinct object, ordered by relative
    path, so the caller's file order and repeated identical files never move the ID.
    ``schema_major`` rises only when the loader's output columns or types change.
    """

    provider: str
    shape: str
    schema_major: int
    files: tuple[SourceFile, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.provider, str) or _PROVIDER.fullmatch(self.provider) is None:
            raise ValueError("source provider must be lowercase letters and digits")
        if not isinstance(self.shape, str) or _SHAPE.fullmatch(self.shape) is None:
            raise ValueError("source shape must be lowercase words joined by hyphens")
        if (
            isinstance(self.schema_major, bool)
            or not isinstance(self.schema_major, int)
            or self.schema_major < 1
        ):
            raise ValueError("source schema major must be a positive integer")
        unique: dict[str, SourceFile] = {}
        for item in self.files:
            if not isinstance(item, SourceFile):
                raise TypeError("source files must be SourceFile values")
            if unique.setdefault(item.sha256, item) != item:
                raise ValueError("one source file hash has two sizes")
        if not unique:
            raise ValueError("a content source requires at least one original file")
        object.__setattr__(
            self, "files", tuple(sorted(unique.values(), key=lambda f: f.relative_path))
        )
        if len(self.source_id) > _MAX_SOURCE_ID:
            raise ValueError("source provider and shape make the source ID too long")

    def document(self) -> bytes:
        """Return the exact bytes whose SHA-256 is the ID's hex suffix."""
        return schema.encoded(
            [SOURCE_ID_FORMAT, self.schema_major, [item.entry() for item in self.files]]
        ).encode()

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.document()).hexdigest()

    @property
    def source_id(self) -> str:
        return f"{self.provider}-{self.shape}-{self.sha256}"

    def record(self) -> dict[str, object]:
        """Return the manifest record from which ``from_record`` rebuilds this identity."""
        return {
            "format": SOURCE_ID_FORMAT,
            "provider": self.provider,
            "shape": self.shape,
            "schema_major": self.schema_major,
            "files": [item.entry() for item in self.files],
        }

    @classmethod
    def from_record(cls, record: object) -> SourceContent:
        if not isinstance(record, dict) or set(record) != _RECORD_KEYS:
            raise ValueError("invalid content source record")
        if record["format"] != SOURCE_ID_FORMAT or not isinstance(record["files"], list):
            raise ValueError("invalid content source record")
        files = []
        for entry in record["files"]:
            if not isinstance(entry, list) or len(entry) != 3:  # noqa: PLR2004 -- path,size,hash
                raise ValueError("invalid content source file entry")
            path, size, digest = entry
            item = SourceFile(digest, size)
            if path != item.relative_path:
                raise ValueError("content source file path must be its raw address")
            files.append(item)
        return cls(record["provider"], record["shape"], record["schema_major"], tuple(files))


def is_content_record(metadata: object) -> bool:
    """Whether a commit manifest's metadata claims a content-addressed identity."""
    return (
        isinstance(metadata, dict)
        and isinstance(metadata.get("source"), dict)
        and metadata["source"].get("format") == SOURCE_ID_FORMAT
    )


@dataclass(frozen=True, slots=True)
class _Link:
    snapshot: tuple[object, ...]
    files: tuple[SourceFile, ...]


def _commits(workspace: Workspace) -> Iterator[str]:
    """Yield every committed source ID; manifests are fetched one at a time later."""
    for conn in schema.connections(workspace).values():
        rows = conn.execute("SELECT source_id FROM source_library_commits").fetchall()
        yield from sorted(str(row[0]) for row in rows)


def _marker(workspace: Workspace, source_id: str) -> tuple[object, ...] | None:
    from aegis_alpha.storage.source_library import _marker as marker  # noqa: PLC0415

    return marker(workspace, source_id)


def _derive(workspace: Workspace, source_id: str, *, check_raw: bool) -> _Link | LinkStatus:
    """Derive the link rows of one commit, or say why it cannot be linked yet."""
    marker = _marker(workspace, source_id)
    if marker is None:
        raise ValueError("source commit marker missing")
    operation_id, request_hash, source_sha256, _, manifest_json = marker
    operation = get_operation(workspace.state, str(operation_id))
    if operation is None:
        return "incomplete"
    if (
        operation["kind"],
        operation["request_hash"],
        operation["target_id"],
        operation["payload_hash"],
    ) != ("source_import", request_hash, source_id, source_sha256):
        raise ValueError("source marker/intent mismatch")
    if operation["phase"] != "COMPLETED":
        return "incomplete"
    metadata = json.loads(str(manifest_json)).get("metadata")
    files = _files(workspace, source_id, str(source_sha256), metadata, check_raw=check_raw)
    if isinstance(files, str):
        return files
    snapshot = (
        LINK_PREFIX + source_id,
        LINK_PROVIDER,
        operation["created_at_us"],
        operation["completed_at_us"],
        None,
        "raw_verified",
    )
    return _Link(snapshot, files)


def content_of(source_id: str, source_sha256: str, metadata: object) -> SourceContent | None:
    """Return a commit's content identity, or None for a commit under an explicit ID."""
    if not is_content_record(metadata):
        return None
    content = SourceContent.from_record(cast("dict[str, object]", metadata)["source"])
    if (content.source_id, content.sha256) != (source_id, source_sha256):
        raise ValueError("content source record does not match its source ID")
    return content


def _raw_status(workspace: Workspace, item: SourceFile) -> LinkStatus | None:
    """Re-hash one retained object: None when intact, otherwise why it cannot back a link."""
    try:
        verify_raw(workspace.paths.raw, item.relative_path, item.sha256, item.size_bytes)
    except (OSError, DescriptorTreeError):
        return "unbacked"
    except ValueError:
        return "corrupt"
    return None


def _files(
    workspace: Workspace, source_id: str, source_sha256: str, metadata: object, *, check_raw: bool
) -> tuple[SourceFile, ...] | LinkStatus:
    """Return the original files a commit links, or why raw cannot back them.

    A content commit also needs its ID document in raw, so ``source_sha256`` keeps a
    preimage; that document is provenance of the ID, not an original file.
    """
    content = content_of(source_id, source_sha256, metadata)
    if content is not None:
        files = content.files
        retained = (*files, SourceFile(content.sha256, len(content.document())))
    else:
        # A commit made under an explicit ID links the raw object its intent pins.
        size = _raw_size(workspace, source_sha256)
        if size is None:
            return "unbacked"
        files = retained = (SourceFile(source_sha256, size),)
    if check_raw:
        statuses = {_raw_status(workspace, item) for item in retained} - {None}
        if statuses:
            return "corrupt" if "corrupt" in statuses else "unbacked"
    return files


def _raw_size(workspace: Workspace, digest: str) -> int | None:
    path = workspace.paths.raw / digest[:2] / digest
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if not path.is_file() or path.is_symlink():
        return None
    return info.st_size


def _recorded(workspace: Workspace, snapshot_id: str) -> _Link | None:
    row = workspace.state.execute(
        "SELECT snapshot_id,provider,requested_at_us,retrieved_at_us,publication_at_us,status "
        "FROM source_snapshots WHERE snapshot_id=?",
        (snapshot_id,),
    ).fetchone()
    if row is None:
        return None
    files = []
    for path, digest, size in workspace.state.execute(
        "SELECT relative_path,byte_hash,size_bytes FROM source_files WHERE snapshot_id=? "
        "ORDER BY relative_path",
        (snapshot_id,),
    ):
        item = SourceFile(digest, size)
        if path != item.relative_path:
            raise ValueError("source link file is not addressed by its hash")
        files.append(item)
    return _Link(tuple(row), tuple(files))


def link_source(workspace: Workspace, source_id: str, *, apply: bool = True) -> LinkStatus:
    """Record one commit's ``sl:`` link, or report what applying it would do.

    ``unchanged`` means the identical link already exists. A recorded link that
    differs from the derivation is a conflict and raises; links are immutable.
    """
    derived = _derive(workspace, source_id, check_raw=True)
    if isinstance(derived, str):
        return derived
    recorded = _recorded(workspace, str(derived.snapshot[0]))
    if recorded is not None:
        if recorded != derived:
            raise ValueError("source link conflicts with the recorded snapshot")
        return "unchanged"
    if not apply:
        return "pending"
    with atomic(workspace.state):
        workspace.state.execute(
            "INSERT INTO source_snapshots(snapshot_id,provider,requested_at_us,"
            "retrieved_at_us,publication_at_us,status) VALUES (?,?,?,?,?,?)",
            derived.snapshot,
        )
        workspace.state.executemany(
            "INSERT INTO source_files(snapshot_id,relative_path,byte_hash,size_bytes) "
            "VALUES (?,?,?,?)",
            [
                (derived.snapshot[0], item.relative_path, item.sha256, item.size_bytes)
                for item in derived.files
            ],
        )
    return "linked"


def source_link(workspace: Workspace, *, apply: bool) -> dict[str, object]:
    """Plan or apply the ``sl:`` link of every committed source-library source.

    One commit that cannot be linked never stops the others: a commit whose raw
    bytes differ from its pins is listed under ``corrupt_sources`` and one whose
    records contradict each other (or an existing link) under ``invalid_sources``.
    """
    counts: dict[str, int] = dict.fromkeys((*_STATUSES, "invalid"), 0)
    listed: dict[str, list[str]] = {"unbacked": [], "corrupt": [], "incomplete": []}
    invalid: list[dict[str, str]] = []
    files = size = 0
    if schema.ensure(workspace):
        for source_id in _commits(workspace):
            try:
                status = link_source(workspace, source_id, apply=apply)
            except ValueError as error:
                counts["invalid"] += 1
                invalid.append({"source_id": source_id, "error": str(error)})
                continue
            counts[status] += 1
            if status in listed:
                listed[status].append(source_id)
            elif status in {"linked", "pending"}:
                derived = _derive(workspace, source_id, check_raw=False)
                if isinstance(derived, _Link):
                    files += len(derived.files)
                    size += sum(item.size_bytes for item in derived.files)
    return {
        "mode": "apply" if apply else "plan",
        "commits": sum(counts.values()),
        **counts,
        "new_files": files,
        "new_bytes": size,
        "unbacked_sources": listed["unbacked"],
        "corrupt_sources": listed["corrupt"],
        "incomplete_sources": listed["incomplete"],
        "invalid_sources": invalid,
    }


def link_content_sources(workspace: Workspace) -> list[str]:
    """Record the missing link of every completed content commit; return their IDs.

    A content commit is linked at commit time; this finishes one interrupted between
    completing its intent and recording its link. Explicit-ID commits are linked by
    ``source_link`` instead, because their pinned bytes may not be in raw yet.
    """
    linked: list[str] = []
    if not schema.ensure(workspace):
        return linked
    for source_id in _commits(workspace):
        if _recorded(workspace, LINK_PREFIX + source_id) is not None:
            continue
        marker = _marker(workspace, source_id)
        if marker is None or not is_content_record(json.loads(str(marker[4])).get("metadata")):
            continue
        if link_source(workspace, source_id) == "linked":
            linked.append(source_id)
    return linked


def verify_links(workspace: Workspace, source_ids: Iterable[str]) -> int:
    """Check every recorded ``sl:`` link of a live commit against its derivation.

    A completed content commit must be linked and keep its ID document in raw. An
    explicit-ID commit may stay unlinked until ``source_link`` backfills it. Original
    file bytes are re-hashed by the workspace verifier, which reads every
    ``source_files`` row; this check covers the link rows themselves.
    """
    linked = 0
    for source_id in source_ids:
        recorded = _recorded(workspace, LINK_PREFIX + source_id)
        marker = _marker(workspace, source_id)
        if marker is None:
            raise ValueError("source commit marker missing")
        content = content_of(source_id, str(marker[2]), json.loads(str(marker[4])).get("metadata"))
        if content is not None:
            document = SourceFile(content.sha256, len(content.document()))
            if _raw_status(workspace, document) is not None:
                raise ValueError(f"content source {source_id} lacks its ID document in raw")
            if recorded is None:
                raise ValueError(f"content source {source_id} lacks its link; run aas db recover")
        if recorded is None:
            continue
        derived = _derive(workspace, source_id, check_raw=False)
        if not isinstance(derived, _Link):
            raise ValueError("linked source commit is no longer linkable")  # noqa: TRY004
        if recorded.snapshot != derived.snapshot or {item.sha256 for item in recorded.files} != {
            item.sha256 for item in derived.files
        }:
            raise ValueError("source link conflicts with its commit")
        linked += 1
    return linked
