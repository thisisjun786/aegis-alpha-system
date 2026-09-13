"""Immutable, whole-document convention pins on an admitted state connection."""

from __future__ import annotations

import hashlib
import re
import sqlite3
from dataclasses import dataclass

from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage.state import atomic

_MAX_BYTES = 1024 * 1024
_KINDS = frozenset({"calendar", "fx", "basis", "cost", "execution", "benchmark", "risk_free"})
_KEYS = frozenset({"schema", "hash_format", "kind", "id", "version", "payload"})
_SELECT = (
    "SELECT kind,convention_id,version,payload,content_hash FROM conventions "
    "WHERE kind=? AND convention_id=? AND version=?"
)


def _identity(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or re.search(r"[\x00-\x1f\x7f-\x9f]", value)
    ):
        raise ValueError("convention identity must be nonempty, trimmed and control-free")
    return value


def _kind(value: object) -> str:
    if not isinstance(value, str) or value not in _KINDS:
        raise ValueError("unsupported convention kind")
    return value


def _version(value: object) -> str:
    version = _identity(value)
    if version.casefold() == "latest":
        raise ValueError("convention version must be exact, not latest")
    return version


def _hash(value: object) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("convention hash must be lowercase SHA-256 hex")
    return value


@dataclass(frozen=True, slots=True)
class ConventionPin:
    kind: str
    id: str
    version: str
    hash: str

    def __post_init__(self) -> None:
        _kind(self.kind)
        _identity(self.id)
        _version(self.version)
        _hash(self.hash)


def _validate_document(document: object) -> tuple[str, str, str]:
    if not isinstance(document, dict) or document.keys() != _KEYS:
        raise ValueError("invalid convention envelope keys")
    if (
        document["schema"] != "aas-convention-v1"
        or document["hash_format"] != "aas-canonical-json-sha256-v1"
    ):
        raise ValueError("unsupported convention schema or hash format")
    kind = _kind(document["kind"])
    identity = _identity(document["id"])
    version = _version(document["version"])
    payload = document["payload"]
    if not isinstance(payload, dict):
        raise TypeError("convention payload must be an object")
    _identity(payload.get("schema"))
    if kind == "basis" and (
        payload.keys() != {"schema", "price_basis"}
        or payload["schema"] != "aas-basis-v1"
        or payload["price_basis"] not in ("capital", "total_return")
    ):
        raise ValueError("unsupported basis payload")
    return kind, identity, version


def _document(raw: bytes) -> tuple[ConventionPin, bytes]:
    if not isinstance(raw, bytes) or len(raw) > _MAX_BYTES:
        raise ValueError("convention must be bytes of at most 1 MiB")
    if raw.startswith(b"\xef\xbb\xbf"):
        raise ValueError("convention UTF-8 must not have a BOM")
    # Literal NUL is invalid JSON text and enables the decoder's UTF-16/32 detection.
    if b"\x00" in raw:
        raise ValueError("convention JSON text must not contain literal NUL bytes")
    try:
        raw.decode("utf-8", errors="strict")
        document = decode_json(raw)
        kind, identity, version = _validate_document(document)
        canonical = canonical_json_bytes(document)
    except (ValueError, TypeError, RecursionError) as error:
        raise ValueError("invalid convention document") from error
    if len(canonical) > _MAX_BYTES:
        raise ValueError("canonical convention exceeds 1 MiB")
    return ConventionPin(kind, identity, version, hashlib.sha256(canonical).hexdigest()), canonical


def _stored_document(row: sqlite3.Row) -> tuple[ConventionPin, bytes]:
    stored_pin = ConventionPin(row[0], row[1], row[2], row[4])
    try:
        raw = row[3].encode("utf-8")
    except UnicodeError as error:
        raise ValueError("invalid stored convention encoding") from error
    parsed_pin, canonical = _document(raw)
    if canonical != raw or parsed_pin != stored_pin:
        raise ValueError("stored convention is noncanonical or has mismatched identity/hash")
    return parsed_pin, canonical


def register_convention(
    connection: sqlite3.Connection, raw: bytes, *, expected_file_sha256: str
) -> ConventionPin:
    """Register one document; the file digest refers to exact incoming bytes."""
    _hash(expected_file_sha256)
    if not isinstance(raw, bytes) or len(raw) > _MAX_BYTES:
        raise ValueError("convention must be bytes of at most 1 MiB")
    if hashlib.sha256(raw).hexdigest() != expected_file_sha256:
        raise ValueError("convention file hash mismatch")
    pin, canonical = _document(raw)
    with atomic(connection):
        previous = connection.execute(_SELECT, (pin.kind, pin.id, pin.version)).fetchone()
        if previous is not None:
            if _stored_document(previous) != (pin, canonical):
                raise ValueError("convention ID/version already has different content")
        else:
            connection.execute(
                "INSERT INTO conventions(kind,convention_id,version,payload,content_hash) "
                "VALUES (?,?,?,?,?)",
                (pin.kind, pin.id, pin.version, canonical.decode("utf-8"), pin.hash),
            )
    return pin


def read_convention(connection: sqlite3.Connection, pin: ConventionPin) -> bytes:
    """Read canonical whole-document bytes using only the supplied state connection."""
    row = connection.execute(_SELECT, (pin.kind, pin.id, pin.version)).fetchone()
    if row is None:
        raise ValueError("convention ID/version is not registered")
    stored_pin, canonical = _stored_document(row)
    if stored_pin != pin:
        raise ValueError("convention hash does not match requested pin")
    return canonical
