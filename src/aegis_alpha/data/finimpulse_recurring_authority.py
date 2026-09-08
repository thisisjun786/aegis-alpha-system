"""Detached-signed standing probe scope for FinImpulse (ADR 0011 Tier 2).

One signed scope replaces per-probe owner-execution approval. It is bounded
by its validity window and the signing key's trust interval, the exact
FinImpulse host and storage roots, the observed 60 calls-per-minute rate cap,
and a positive ``max_spend_micros`` spend bound. Admission compares that
integer-microdollar ceiling against the run's ``--budget-usd`` because the
collector already meters Decimal USD (call + row prices), not call counts.

The signed spend bound is currently a per-invocation admission bound;
cumulative cross-process accounting does not exist and is a G-B entry
prerequisite. A local fail-closed revocation marker stops the scope at
request time. G-B gate artifacts and 007 identity evidence remain orthogonal
and are still required.
"""

from __future__ import annotations

import hashlib
import json
import re
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final, cast
from urllib.parse import urlparse

from cryptography.exceptions import InvalidSignature

from aegis_alpha.collection.usage_checkpoint_errors import (
    StaleVerificationKeyError,
    UnknownVerificationKeyError,
)
from aegis_alpha.data.finimpulse_collector import ENDPOINT
from aegis_alpha.data.finimpulse_owner_authority import OwnerAuthority
from aegis_alpha.data.finimpulse_recurring_errors import RecurringAuthorityError
from aegis_alpha.data.finimpulse_recurring_revocation import require_not_revoked
from aegis_alpha.data.serialization import canonical_json_bytes

RECURRING_AUTHORITY_CONTRACT: Final = "aegis-alpha/finimpulse-probe-authority"
RECURRING_AUTHORITY_VERSION: Final = 1
POLICY_ID: Final = "finimpulse-eps-revision-candidate-v1"
ALLOWED_HOST: Final = urlparse(ENDPOINT).hostname or "api.finimpulse.com"
MAX_CALLS_PER_MINUTE: Final = 60
USD_MICROS: Final = Decimal(1_000_000)
_MAX_AUTHORITY_BYTES: Final = 4096
_SIGNATURE_BYTES: Final = 64
_ALLOWED_DOMAINS: Final = (ALLOWED_HOST,)
_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9:._-]{2,127}")
_UTC_FORMAT: Final = "%Y-%m-%dT%H:%M:%S.%fZ"
_FIELDS: Final = (
    "contract",
    "version",
    "authority_id",
    "key_id",
    "approver",
    "policy_id",
    "allowed_domains",
    "raw_store_root",
    "dataset_root",
    "calls_per_minute",
    "max_spend_micros",
    "issued_at_utc",
    "valid_from_utc",
)


@dataclass(frozen=True, slots=True)
class VerifiedRecurringAuthority:
    """One authenticated standing probe scope; never a live-transport grant by itself."""

    authority_id: str
    key_id: str
    approver: str
    allowed_domains: tuple[str, ...]
    raw_store_root: Path
    dataset_root: Path
    calls_per_minute: int
    max_spend_micros: int
    valid_from_utc: datetime
    key_valid_until_utc: datetime
    payload_sha256: str
    signature_sha256: str
    authority_artifact_sha256: str

    def require_request(self, now: datetime) -> None:
        if now.utcoffset() != UTC.utcoffset(None):
            raise RecurringAuthorityError("recurring authority clock must be UTC")
        if now < self.valid_from_utc:
            raise RecurringAuthorityError("recurring authority is not active yet")
        if now >= self.key_valid_until_utc:
            raise RecurringAuthorityError("recurring authority signing key is expired")
        require_not_revoked(self, now)

    def max_spend_usd(self) -> Decimal:
        return Decimal(self.max_spend_micros) / USD_MICROS


@dataclass(frozen=True, slots=True)
class BoundRecurringApproval:
    """Carry verified standing scope plus a current clock to each request."""

    authority: VerifiedRecurringAuthority
    clock: Callable[[], datetime]

    def require_request(self) -> None:
        self.authority.require_request(self.clock())


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise RecurringAuthorityError(f"recurring authority {label} is invalid")
    return value


def _timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise RecurringAuthorityError(f"recurring authority {label} must be UTC text")
    try:
        return datetime.strptime(value, _UTC_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        raise RecurringAuthorityError(
            f"recurring authority {label} must use canonical UTC text"
        ) from None


def _absolute_path(value: object, label: str) -> Path:
    if not isinstance(value, str):
        raise RecurringAuthorityError(f"recurring authority {label} must be a path")
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts or str(path) != value:
        raise RecurringAuthorityError(f"recurring authority {label} must be canonical absolute")
    return path


def _document(payload: bytes) -> Mapping[str, object]:
    if len(payload) > _MAX_AUTHORITY_BYTES:
        raise RecurringAuthorityError("recurring authority exceeds 4096 bytes")

    def pairs(values: Sequence[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in values:
            if key in result:
                raise RecurringAuthorityError("recurring authority contains duplicate fields")
            result[key] = value
        return result

    try:
        parsed = json.loads(payload, object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise RecurringAuthorityError("recurring authority is not valid JSON") from None
    if not isinstance(parsed, Mapping):
        raise RecurringAuthorityError("recurring authority must be a JSON object")
    document = cast("Mapping[str, object]", parsed)
    if set(document) != set(_FIELDS):
        raise RecurringAuthorityError("recurring authority fields do not match the contract")
    if payload != canonical_json_bytes(document):
        raise RecurringAuthorityError("recurring authority bytes are not canonical")
    return document


def _allowed_domains(document: Mapping[str, object]) -> tuple[str, ...]:
    raw = document["allowed_domains"]
    if not isinstance(raw, list) or any(type(domain) is not str for domain in raw):
        raise RecurringAuthorityError("recurring authority allowed domains must be text")
    if tuple(raw) != _ALLOWED_DOMAINS:
        raise RecurringAuthorityError(
            "recurring authority allowed domains must be exactly api.finimpulse.com"
        )
    return _ALLOWED_DOMAINS


def _call_and_spend_bounds(document: Mapping[str, object]) -> tuple[int, int]:
    calls_per_minute = document["calls_per_minute"]
    if type(calls_per_minute) is not int or not 1 <= calls_per_minute <= MAX_CALLS_PER_MINUTE:
        raise RecurringAuthorityError(
            f"recurring authority calls_per_minute exceeds {MAX_CALLS_PER_MINUTE}"
        )
    max_spend_micros = document["max_spend_micros"]
    if type(max_spend_micros) is not int or max_spend_micros < 1:
        raise RecurringAuthorityError(
            "recurring authority max_spend_micros must be a positive integer"
        )
    return calls_per_minute, max_spend_micros


def usd_to_micros(amount: Decimal) -> int:
    """Convert a finite USD Decimal into integer microdollars, fail-closed on residue."""

    if not amount.is_finite() or amount <= 0:
        raise RecurringAuthorityError("spend amount must be a positive finite USD value")
    scaled = amount * USD_MICROS
    try:
        micros = int(scaled)
    except (InvalidOperation, OverflowError, ValueError):
        raise RecurringAuthorityError("spend amount cannot be expressed in microdollars") from None
    if Decimal(micros) != scaled:
        raise RecurringAuthorityError("spend amount must be an exact number of microdollars")
    return micros


def recurring_authority_issued_at(payload: bytes) -> datetime:
    """Read the canonical signing instant before detached-signature verification."""

    return _timestamp(_document(payload)["issued_at_utc"], "issued_at_utc")


def verify_recurring_authority(
    payload: bytes,
    signature: bytes,
    authority: OwnerAuthority,
    *,
    now: datetime,
) -> VerifiedRecurringAuthority:
    """Authenticate one standing FinImpulse probe scope without opening any live path."""

    if type(signature) is not bytes or len(signature) != _SIGNATURE_BYTES:
        raise RecurringAuthorityError("recurring authority signature must contain 64 bytes")
    if now.utcoffset() != UTC.utcoffset(None):
        raise RecurringAuthorityError("recurring authority verification clock must be UTC")
    document = _document(payload)
    if (
        document["contract"] != RECURRING_AUTHORITY_CONTRACT
        or type(document["version"]) is not int
        or document["version"] != RECURRING_AUTHORITY_VERSION
        or document["policy_id"] != POLICY_ID
    ):
        raise RecurringAuthorityError("recurring authority contract is unsupported")
    authority_id = _identifier(document["authority_id"], "authority_id")
    key_id = _identifier(document["key_id"], "key_id")
    approver = _identifier(document["approver"], "approver")
    allowed_domains = _allowed_domains(document)
    calls_per_minute, max_spend_micros = _call_and_spend_bounds(document)
    issued = _timestamp(document["issued_at_utc"], "issued_at_utc")
    valid_from = _timestamp(document["valid_from_utc"], "valid_from_utc")
    if issued > valid_from or valid_from > now:
        raise RecurringAuthorityError("recurring authority is not currently valid")
    if issued < authority.valid_from_utc or valid_from > authority.valid_until_utc:
        raise RecurringAuthorityError("recurring authority exceeds key validity")
    if (authority_id, key_id) != (authority.authority_id, authority.key_id):
        raise RecurringAuthorityError("recurring authority trust artifact does not match")
    try:
        authority.keyring.public_key(authority_id, key_id, issued).verify(signature, payload)
    except (InvalidSignature, StaleVerificationKeyError, UnknownVerificationKeyError):
        raise RecurringAuthorityError("recurring authority signature verification failed") from None
    verified = VerifiedRecurringAuthority(
        authority_id=authority_id,
        key_id=key_id,
        approver=approver,
        allowed_domains=allowed_domains,
        raw_store_root=_absolute_path(document["raw_store_root"], "raw_store_root"),
        dataset_root=_absolute_path(document["dataset_root"], "dataset_root"),
        calls_per_minute=calls_per_minute,
        max_spend_micros=max_spend_micros,
        valid_from_utc=valid_from,
        key_valid_until_utc=authority.valid_until_utc,
        payload_sha256=hashlib.sha256(payload).hexdigest(),
        signature_sha256=hashlib.sha256(signature).hexdigest(),
        authority_artifact_sha256=authority.artifact_sha256,
    )
    verified.require_request(now)
    return verified


def load_recurring_authority(path: Path, signature_path: Path) -> tuple[bytes, bytes]:
    """Read bounded detached authority artifacts without accepting non-files."""

    try:
        authority_metadata = path.lstat()
        signature_metadata = signature_path.lstat()
        if not stat.S_ISREG(authority_metadata.st_mode) or not stat.S_ISREG(
            signature_metadata.st_mode
        ):
            raise RecurringAuthorityError("recurring authority artifacts must be regular files")
        if authority_metadata.st_size > _MAX_AUTHORITY_BYTES:
            raise RecurringAuthorityError("recurring authority exceeds 4096 bytes")
        if signature_metadata.st_size != _SIGNATURE_BYTES:
            raise RecurringAuthorityError("recurring authority signature must contain 64 bytes")
        return path.read_bytes(), signature_path.read_bytes()
    except OSError as error:
        raise RecurringAuthorityError("cannot read recurring authority artifacts") from error
