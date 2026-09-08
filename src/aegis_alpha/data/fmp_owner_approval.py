"""Canonical detached-signature contract for one FMP live operation."""

from __future__ import annotations

import hashlib
import json
import re
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Final, cast

from cryptography.exceptions import InvalidSignature

from aegis_alpha.collection.usage_checkpoint_crypto import Ed25519PublicKeyring
from aegis_alpha.collection.usage_checkpoint_errors import (
    StaleVerificationKeyError,
    UnknownVerificationKeyError,
)
from aegis_alpha.data.fmp_cli_artifacts import (
    AUTHORIZED_MAX_CALLS,
    FMP_POLICY_ID,
    PreconditionError,
)
from aegis_alpha.data.serialization import canonical_json_bytes

APPROVAL_CONTRACT: Final = "aegis-alpha/fmp-owner-approval/v1"
APPROVAL_VERSION: Final = 1
CONTRACT_REVISION: Final = "004b-revision-2-owner-decision-2026-08-18"
SCOPE_CONTRACT: Final = "aegis-alpha/fmp-live-operation-scope/v1"
_MAX_APPROVAL_BYTES: Final = 4_096
_SIGNATURE_BYTES: Final = 64
_RUN_ID = re.compile(r"fmp-run-[0-9a-f]{32}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._:/-]{0,254}")
_UTC_TEXT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z")
_FIELDS: Final = {
    "contract",
    "version",
    "authority_id",
    "key_id",
    "approver",
    "contract_revision",
    "policy_id",
    "authorization_kind",
    "run_identity",
    "operation",
    "scope_sha256",
    "max_calls",
    "issued_at_utc",
    "expires_at_utc",
}


class OwnerApprovalError(PreconditionError):
    """Signed owner approval is absent, malformed, stale, or inapplicable."""


class FmpOperation(StrEnum):
    COLLECT = "collect"
    BUILD_UNIVERSE = "build-universe"


@dataclass(frozen=True, slots=True)
class OwnerApprovalAuthority:
    authority_id: str
    key_id: str
    valid_from_utc: datetime
    valid_until_utc: datetime
    artifact_sha256: str
    keyring: Ed25519PublicKeyring


@dataclass(frozen=True, slots=True)
class VerifiedOwnerApproval:
    authority_id: str
    key_id: str
    approver: str
    run_identity: str
    operation: FmpOperation
    scope_sha256: str
    max_calls: int
    issued_at_utc: datetime
    expires_at_utc: datetime
    payload_sha256: str
    signature_sha256: str
    authority_artifact_sha256: str

    def require_unexpired(self, now: datetime) -> None:
        if now >= self.expires_at_utc:
            raise OwnerApprovalError("signed owner approval expired during collection")


def _strict_document(payload: bytes) -> Mapping[str, object]:
    if len(payload) > _MAX_APPROVAL_BYTES:
        raise OwnerApprovalError("owner approval exceeds 4096 bytes")

    def from_pairs(pairs: Sequence[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise OwnerApprovalError("owner approval contains duplicate object keys")
            result[key] = value
        return result

    try:
        raw = json.loads(payload, object_pairs_hook=from_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise OwnerApprovalError("owner approval is not valid JSON") from None
    if not isinstance(raw, Mapping):
        raise OwnerApprovalError("owner approval must be a JSON object")
    document = cast("Mapping[str, object]", raw)
    if set(document) != _FIELDS:
        raise OwnerApprovalError("owner approval fields are not exact")
    if payload != canonical_json_bytes(document):
        raise OwnerApprovalError("owner approval bytes are not canonical JSON")
    return document


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str) or _UTC_TEXT.fullmatch(value) is None:
        raise OwnerApprovalError("owner approval timestamp is not canonical UTC")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError:
        raise OwnerApprovalError("owner approval timestamp is not canonical UTC") from None
    if parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ") != value:
        raise OwnerApprovalError("owner approval timestamp is not canonical UTC")
    return parsed


def _identifier(value: object) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise OwnerApprovalError("owner approval identity is not canonical")
    return value


def claimed_run_identity(payload: bytes) -> str:
    """Read the untrusted run claim only to compute the scope later authenticated."""

    run_identity = _strict_document(payload)["run_identity"]
    if not isinstance(run_identity, str) or _RUN_ID.fullmatch(run_identity) is None:
        raise OwnerApprovalError("owner approval run identity is not canonical")
    return run_identity


def verify_owner_approval(  # noqa: C901, PLR0912, PLR0913 - explicit contract checks
    payload: bytes,
    signature: bytes,
    authority: OwnerApprovalAuthority,
    *,
    now: datetime,
    expected_operation: str,
    expected_scope_sha256: str,
    expected_max_calls: int,
) -> VerifiedOwnerApproval:
    """Verify exact canonical bytes and bind the grant to one computed operation scope."""

    if type(signature) is not bytes or len(signature) != _SIGNATURE_BYTES:
        raise OwnerApprovalError("owner approval signature must contain exactly 64 bytes")
    if now.utcoffset() != UTC.utcoffset(None):
        raise OwnerApprovalError("owner approval verification clock must be UTC")
    document = _strict_document(payload)
    if (
        document["contract"] != APPROVAL_CONTRACT
        or type(document["version"]) is not int
        or document["version"] != APPROVAL_VERSION
    ):
        raise OwnerApprovalError("owner approval contract is unsupported")
    if document["contract_revision"] != CONTRACT_REVISION or document["policy_id"] != FMP_POLICY_ID:
        raise OwnerApprovalError("owner approval policy contract does not match")
    if document["authorization_kind"] != "manual_one_shot":
        raise OwnerApprovalError("owner approval must authorize one manual invocation")
    authority_id = _identifier(document["authority_id"])
    key_id = _identifier(document["key_id"])
    approver = _identifier(document["approver"])
    run_identity = document["run_identity"]
    if not isinstance(run_identity, str) or _RUN_ID.fullmatch(run_identity) is None:
        raise OwnerApprovalError("owner approval run identity is not canonical")
    try:
        operation = FmpOperation(document["operation"])
    except (TypeError, ValueError):
        raise OwnerApprovalError("owner approval operation is unsupported") from None
    scope = document["scope_sha256"]
    if not isinstance(scope, str) or _SHA256.fullmatch(scope) is None:
        raise OwnerApprovalError("owner approval scope digest is malformed")
    max_calls = document["max_calls"]
    if type(max_calls) is not int or not 1 <= max_calls <= AUTHORIZED_MAX_CALLS:
        raise OwnerApprovalError("owner approval max_calls must be an integer from 1 through 25")
    issued = _timestamp(document["issued_at_utc"])
    expires = _timestamp(document["expires_at_utc"])
    if issued > now or now >= expires or issued >= expires:
        raise OwnerApprovalError("owner approval is not currently valid")
    if issued < authority.valid_from_utc or expires > authority.valid_until_utc:
        raise OwnerApprovalError("owner approval exceeds the authority key validity interval")
    if (authority_id, key_id) != (authority.authority_id, authority.key_id):
        raise OwnerApprovalError("owner approval authority does not match the trust artifact")
    if operation.value != expected_operation or scope != expected_scope_sha256:
        raise OwnerApprovalError("owner approval does not match the requested operation scope")
    if max_calls != expected_max_calls:
        raise OwnerApprovalError("owner approval max_calls does not match the requested cap")
    try:
        authority.keyring.public_key(authority_id, key_id, issued).verify(signature, payload)
    except (InvalidSignature, StaleVerificationKeyError, UnknownVerificationKeyError):
        raise OwnerApprovalError("owner approval signature verification failed") from None
    return VerifiedOwnerApproval(
        authority_id=authority_id,
        key_id=key_id,
        approver=approver,
        run_identity=run_identity,
        operation=operation,
        scope_sha256=scope,
        max_calls=max_calls,
        issued_at_utc=issued,
        expires_at_utc=expires,
        payload_sha256=hashlib.sha256(payload).hexdigest(),
        signature_sha256=hashlib.sha256(signature).hexdigest(),
        authority_artifact_sha256=authority.artifact_sha256,
    )


def load_detached_approval(path: Path, signature_path: Path) -> tuple[bytes, bytes]:
    """Read bounded regular files; authenticity is established only by verification."""

    try:
        approval_metadata = path.lstat()
        signature_metadata = signature_path.lstat()
        if not stat.S_ISREG(approval_metadata.st_mode) or not stat.S_ISREG(
            signature_metadata.st_mode
        ):
            raise OwnerApprovalError("owner approval artifacts must be regular files")
        if approval_metadata.st_size > _MAX_APPROVAL_BYTES:
            raise OwnerApprovalError("owner approval exceeds 4096 bytes")
        if signature_metadata.st_size != _SIGNATURE_BYTES:
            raise OwnerApprovalError("owner approval signature must contain exactly 64 bytes")
        return path.read_bytes(), signature_path.read_bytes()
    except OSError:
        raise OwnerApprovalError("owner approval or detached signature is unavailable") from None
