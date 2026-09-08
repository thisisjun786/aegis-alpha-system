"""Strict external public authority artifact loader for FMP usage checkpoints.

The launcher must provide an absolute path whose ancestors are protected by its OS
and deployment policy. This loader opens the immediate parent and artifact with
``O_NOFOLLOW``, validates owner/mode/type from those open descriptors, and parses
only bytes read from the verified artifact descriptor. It cannot protect against a
compromised process or another actor with the same UID replacing or rewriting files;
the launcher must provide a separately administered path/ancestor boundary for that
threat model.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, cast

from aegis_alpha.collection.usage_checkpoint_crypto import (
    Ed25519PublicKeyring,
    TrustedEd25519PublicKey,
)

_AUTHORITY_SCHEMA: Final = "aegis-alpha/fmp-usage-authority"
_AUTHORITY_VERSION: Final = 1
_AUTHORITY_MAX_BYTES: Final = 4_096
_PUBLIC_KEY_HEX_LENGTH: Final = 64
_PUBLIC_KEY_PATTERN: Final = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_MAX_LENGTH: Final = 255
_IDENTIFIER_PATTERN: Final = re.compile(r"^[a-z0-9][a-z0-9._:/-]*$")
_UTC_TEXT_PATTERN: Final = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")
_UTC_TEXT_FORMAT: Final = "%Y-%m-%dT%H:%M:%S.%fZ"
_EXPECTED_FIELDS: Final = {
    "schema",
    "version",
    "authority_id",
    "key_id",
    "public_key_encoding",
    "public_key",
    "valid_from_utc",
    "valid_until_utc",
}


@dataclass(frozen=True, slots=True)
class FmpUsageAuthority:
    authority_id: str
    key_id: str
    valid_from_utc: datetime
    valid_until_utc: datetime
    artifact_sha256: str
    keyring: Ed25519PublicKeyring


def load_fmp_usage_authority(path: Path, now: datetime) -> FmpUsageAuthority:
    payload = _read_external_artifact(path)
    document = _strict_json_object(payload)
    if set(document) != _EXPECTED_FIELDS:
        raise ValueError("authority artifact fields are not exact")
    if (
        document["schema"] != _AUTHORITY_SCHEMA
        or type(document["version"]) is not int
        or document["version"] != _AUTHORITY_VERSION
    ):
        raise ValueError("authority artifact contract is unsupported")
    authority_id = _identifier(document["authority_id"])
    key_id = _identifier(document["key_id"])
    if document["public_key_encoding"] != "raw-ed25519-hex":
        raise ValueError("authority key encoding is unsupported")
    encoded = document["public_key"]
    if (
        not isinstance(encoded, str)
        or len(encoded) != _PUBLIC_KEY_HEX_LENGTH
        or not _PUBLIC_KEY_PATTERN.fullmatch(encoded)
    ):
        raise ValueError("authority public key is malformed")
    try:
        public_key = bytes.fromhex(encoded)
    except ValueError:
        raise ValueError("authority public key is malformed") from None
    valid_from = _parse_utc(document["valid_from_utc"])
    valid_until = _parse_utc(document["valid_until_utc"])
    if valid_until <= valid_from:
        raise ValueError("authority validity interval must be increasing")
    trust = TrustedEd25519PublicKey(
        public_key,
        valid_from_utc=valid_from,
        valid_until_utc=valid_until,
    )
    if not trust.valid_at(now):
        raise ValueError("authority key is not currently trusted")
    return FmpUsageAuthority(
        authority_id=authority_id,
        key_id=key_id,
        valid_from_utc=valid_from,
        valid_until_utc=valid_until,
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
        keyring=Ed25519PublicKeyring({(authority_id, key_id): trust}),
    )


def _strict_json_object(payload: bytes) -> Mapping[str, object]:
    def object_from_pairs(pairs: Sequence[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("authority artifact contains a duplicate object key")
            result[key] = value
        return result

    try:
        raw_document = json.loads(payload, object_pairs_hook=object_from_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("authority artifact is malformed") from None
    if not isinstance(raw_document, Mapping):
        raise ValueError(  # noqa: TRY004 - untrusted artifact validation
            "authority artifact must be an object"
        )
    return cast("Mapping[str, object]", raw_document)


def _read_external_artifact(path: Path) -> bytes:
    if not path.is_absolute() or path.name in {"", ".", ".."}:
        raise ValueError("authority artifact path is invalid")
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError("authority artifact path must not contain symlinks")
    if any((parent / ".git").exists() for parent in (path.parent, *path.parents)):
        raise ValueError("authority artifact must be outside a Git repository")
    directory_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY
    file_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK
    if hasattr(os, "O_NOFOLLOW"):
        directory_flags |= os.O_NOFOLLOW
        file_flags |= os.O_NOFOLLOW
    directory = os.open(path.parent, directory_flags)
    try:
        _require_private_owner(os.fstat(directory), expected_type=stat.S_ISDIR)
        descriptor = os.open(path.name, file_flags, dir_fd=directory)
        try:
            metadata = os.fstat(descriptor)
            _require_private_owner(metadata, expected_type=stat.S_ISREG)
            if metadata.st_size > _AUTHORITY_MAX_BYTES:
                raise ValueError("authority artifact is too large")
            return _read_bounded(descriptor)
        finally:
            os.close(descriptor)
    finally:
        os.close(directory)


def _require_private_owner(
    metadata: os.stat_result,
    *,
    expected_type: Callable[[int], bool],
) -> None:
    if not expected_type(metadata.st_mode):
        raise ValueError("authority artifact boundary has the wrong type")
    if metadata.st_uid != os.geteuid():
        raise ValueError("authority artifact boundary has the wrong owner")
    if metadata.st_mode & 0o022:
        raise ValueError("authority artifact boundary is group/world writable")


def _read_bounded(descriptor: int) -> bytes:
    chunks: list[bytes] = []
    remaining = _AUTHORITY_MAX_BYTES + 1
    while remaining:
        chunk = os.read(descriptor, remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    payload = b"".join(chunks)
    if len(payload) > _AUTHORITY_MAX_BYTES:
        raise ValueError("authority artifact is too large")
    return payload


def _identifier(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) > _IDENTIFIER_MAX_LENGTH
        or not _IDENTIFIER_PATTERN.fullmatch(value)
    ):
        raise ValueError("authority identity is malformed")
    return value


def _parse_utc(value: object) -> datetime:
    if not isinstance(value, str) or not _UTC_TEXT_PATTERN.fullmatch(value):
        raise ValueError("authority validity timestamp is not canonical UTC")
    try:
        parsed = datetime.strptime(value, _UTC_TEXT_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        raise ValueError("authority validity timestamp is not canonical UTC") from None
    if parsed.strftime(_UTC_TEXT_FORMAT) != value:
        raise ValueError("authority validity timestamp is not canonical UTC")
    return parsed
