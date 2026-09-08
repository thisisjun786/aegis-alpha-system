"""Independently administered public authority for FMP owner approvals.

This boundary is meaningful only when the collector cannot rewrite application
code, obtain the provider credential directly, or write the authority directory.
Those deployment controls, POSIX ACLs, mount namespaces, and credential release
remain outside the application boundary.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, cast

from aegis_alpha.collection.usage_checkpoint_crypto import (
    Ed25519PublicKeyring,
    TrustedEd25519PublicKey,
)
from aegis_alpha.data.fmp_owner_approval import OwnerApprovalAuthority

_SCHEMA: Final = "aegis-alpha/fmp-owner-approval-authority"
_VERSION: Final = 1
_MAX_BYTES: Final = 4_096
_KEY = re.compile(r"[0-9a-f]{64}")
_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._:/-]{0,254}")
_UTC_TEXT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z")
_FIELDS: Final = {
    "schema",
    "version",
    "authority_id",
    "key_id",
    "public_key_encoding",
    "public_key",
    "valid_from_utc",
    "valid_until_utc",
}
type OwnershipReader = Callable[[os.stat_result], int]


def load_owner_approval_authority(
    path: Path,
    now: datetime,
    *,
    collector_uid: int | None = None,
    ownership_reader: OwnershipReader | None = None,
) -> OwnerApprovalAuthority:
    """Load a public-only key owned outside the non-root collector principal."""

    effective_uid = os.geteuid() if collector_uid is None else collector_uid
    if effective_uid == 0:
        raise ValueError("owner approval collector must not run as root")
    owner = (lambda metadata: metadata.st_uid) if ownership_reader is None else ownership_reader
    payload = _read_external(path, effective_uid, owner)
    document = _strict_document(payload)
    if set(document) != _FIELDS:
        raise ValueError("owner approval authority fields are not exact")
    if document["schema"] != _SCHEMA or type(document["version"]) is not int:
        raise ValueError("owner approval authority contract is unsupported")
    if document["version"] != _VERSION:
        raise ValueError("owner approval authority contract is unsupported")
    authority_id = _identifier(document["authority_id"])
    key_id = _identifier(document["key_id"])
    if document["public_key_encoding"] != "raw-ed25519-hex":
        raise ValueError("owner approval authority key encoding is unsupported")
    encoded = document["public_key"]
    if not isinstance(encoded, str) or _KEY.fullmatch(encoded) is None:
        raise ValueError("owner approval authority public key is malformed")
    valid_from = _timestamp(document["valid_from_utc"])
    valid_until = _timestamp(document["valid_until_utc"])
    if valid_until <= valid_from or not valid_from <= now < valid_until:
        raise ValueError("owner approval authority key is not currently valid")
    trust = TrustedEd25519PublicKey(
        bytes.fromhex(encoded),
        valid_from_utc=valid_from,
        valid_until_utc=valid_until,
    )
    return OwnerApprovalAuthority(
        authority_id=authority_id,
        key_id=key_id,
        valid_from_utc=valid_from,
        valid_until_utc=valid_until,
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
        keyring=Ed25519PublicKeyring({(authority_id, key_id): trust}),
    )


def _read_external(path: Path, collector_uid: int, owner: OwnershipReader) -> bytes:
    if not path.is_absolute() or path.name in {"", ".", ".."}:
        raise ValueError("owner approval authority path must be absolute")
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError("owner approval authority path must not contain symlinks")
    if any((parent / ".git").exists() for parent in (path.parent, *path.parents)):
        raise ValueError("owner approval authority must be outside Git")
    directory_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY
    file_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK
    if hasattr(os, "O_NOFOLLOW"):
        directory_flags |= os.O_NOFOLLOW
        file_flags |= os.O_NOFOLLOW
    directory = os.open(path.parent, directory_flags)
    try:
        _require_boundary(os.fstat(directory), collector_uid, owner, stat.S_ISDIR)
        descriptor = os.open(path.name, file_flags, dir_fd=directory)
        try:
            metadata = os.fstat(descriptor)
            _require_boundary(metadata, collector_uid, owner, stat.S_ISREG)
            if metadata.st_size > _MAX_BYTES:
                raise ValueError("owner approval authority is too large")
            return _read_bounded(descriptor)
        finally:
            os.close(descriptor)
    finally:
        os.close(directory)


def _require_boundary(
    metadata: os.stat_result,
    collector_uid: int,
    owner: OwnershipReader,
    expected_type: Callable[[int], bool],
) -> None:
    if not expected_type(metadata.st_mode):
        raise ValueError("owner approval authority boundary has the wrong type")
    if owner(metadata) == collector_uid:
        raise ValueError("owner approval authority must be independently owned")
    if metadata.st_mode & 0o022:
        raise ValueError("owner approval authority boundary is group/world writable")


def _read_bounded(descriptor: int) -> bytes:
    chunks: list[bytes] = []
    remaining = _MAX_BYTES + 1
    while remaining:
        chunk = os.read(descriptor, remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    payload = b"".join(chunks)
    if len(payload) > _MAX_BYTES:
        raise ValueError("owner approval authority is too large")
    return payload


def _strict_document(payload: bytes) -> Mapping[str, object]:
    def from_pairs(pairs: Sequence[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("owner approval authority contains duplicate object keys")
            result[key] = value
        return result

    try:
        raw = json.loads(payload, object_pairs_hook=from_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("owner approval authority is malformed") from None
    if not isinstance(raw, Mapping):
        raise ValueError(  # noqa: TRY004 - untrusted JSON contract failure
            "owner approval authority must be a JSON object"
        )
    return cast("Mapping[str, object]", raw)


def _identifier(value: object) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError("owner approval authority identity is malformed")
    return value


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str) or _UTC_TEXT.fullmatch(value) is None:
        raise ValueError("owner approval authority timestamp is not canonical UTC")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError:
        raise ValueError("owner approval authority timestamp is not canonical UTC") from None
    if parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ") != value:
        raise ValueError("owner approval authority timestamp is not canonical UTC")
    return parsed
