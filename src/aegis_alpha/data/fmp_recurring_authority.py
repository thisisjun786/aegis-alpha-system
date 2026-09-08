"""Detached-signature contract for standing daily FMP authority."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final, cast

from cryptography.exceptions import InvalidSignature

from aegis_alpha.collection.usage_checkpoint_errors import (
    StaleVerificationKeyError,
    UnknownVerificationKeyError,
)
from aegis_alpha.data.fmp_cli_artifacts import FMP_POLICY_ID
from aegis_alpha.data.fmp_owner_approval import OwnerApprovalAuthority
from aegis_alpha.data.fmp_recurring_errors import RecurringAuthorityError
from aegis_alpha.data.fmp_recurring_fields import (
    absolute_path,
    identifier,
    sha256,
    snapshot_range,
    timestamp,
)
from aegis_alpha.data.fmp_recurring_revocation import require_not_revoked
from aegis_alpha.data.serialization import canonical_json_bytes

RECURRING_AUTHORITY_CONTRACT: Final = "aegis-alpha/fmp-recurring-authority"
RECURRING_AUTHORITY_VERSION: Final = 3
MAX_PROVIDER_CALLS_PER_MINUTE: Final = 3000
_MAX_AUTHORITY_BYTES: Final = 4096
_SIGNATURE_BYTES: Final = 64
_OPERATIONS: Final = frozenset({"build-universe", "collect"})
_FIELDS: Final = (
    "contract",
    "version",
    "authority_id",
    "key_id",
    "approver",
    "policy_id",
    "schedule_id",
    "cadence",
    "dataset_selection",
    "raw_store_root",
    "dataset_root",
    "output_root",
    "norgate_security_master",
    "norgate_security_master_sha256",
    "norgate_security_master_row_count",
    "norgate_snapshot_date",
    "backfill_from",
    "calls_per_minute",
    "calls_per_day",
    "registry_sha256",
    "notification_sha256",
    "tier_sha256",
    "issued_at_utc",
    "valid_from_utc",
)


@dataclass(frozen=True, slots=True)
class VerifiedRecurringAuthority:
    authority_id: str
    key_id: str
    approver: str
    schedule_id: str
    dataset_selection: str
    raw_store_root: Path
    dataset_root: Path
    output_root: Path
    norgate_security_master: Path
    norgate_security_master_sha256: str
    norgate_security_master_row_count: int
    norgate_snapshot_date: date
    backfill_from: date
    calls_per_minute: int
    calls_per_day: int
    registry_sha256: str
    notification_sha256: str
    tier_sha256: str
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

    def run_identity(
        self,
        operation: str,
        service_day: date,
        *,
        attempt_index: int = 0,
        shard: tuple[int, int] | None = None,
    ) -> str:
        if operation not in _OPERATIONS:
            raise RecurringAuthorityError("recurring authority operation is unsupported")
        if attempt_index < 0:
            raise RecurringAuthorityError("recurring attempt index cannot be negative")
        if shard is not None and (shard[1] < 1 or shard[0] < 1 or shard[0] > shard[1]):
            raise RecurringAuthorityError("recurring shard coordinates are out of range")
        identity: dict[str, object] = {
            "operation": operation,
            "payload_sha256": self.payload_sha256,
            "schedule_id": self.schedule_id,
            "service_day": service_day.isoformat(),
        }
        if attempt_index:
            identity["attempt_index"] = attempt_index
        if shard is not None:
            identity["shard"] = [shard[0], shard[1]]
        digest = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
        return f"fmp-run-{digest[:32]}"


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


def recurring_authority_issued_at(payload: bytes) -> datetime:
    """Read the canonical signing instant before detached-signature verification."""

    return timestamp(_document(payload)["issued_at_utc"], "issued_at_utc")


def _daily_call_budget(value: object) -> int:
    if type(value) is not int or value < 1:
        raise RecurringAuthorityError(
            "recurring authority calls_per_day must be a positive integer"
        )
    return value


def verify_recurring_authority(
    payload: bytes,
    signature: bytes,
    authority: OwnerApprovalAuthority,
    *,
    now: datetime,
) -> VerifiedRecurringAuthority:
    """Authenticate one standing daily scope without granting canonical use."""

    if type(signature) is not bytes or len(signature) != _SIGNATURE_BYTES:
        raise RecurringAuthorityError("recurring authority signature must contain 64 bytes")
    if now.utcoffset() != UTC.utcoffset(None):
        raise RecurringAuthorityError("recurring authority verification clock must be UTC")
    document = _document(payload)
    if (
        document["contract"] != RECURRING_AUTHORITY_CONTRACT
        or type(document["version"]) is not int
        or document["version"] != RECURRING_AUTHORITY_VERSION
        or document["policy_id"] != FMP_POLICY_ID
        or document["cadence"] != "daily"
        or document["dataset_selection"] != "all"
    ):
        raise RecurringAuthorityError("recurring authority contract is unsupported")
    authority_id = identifier(document["authority_id"], "authority_id")
    key_id = identifier(document["key_id"], "key_id")
    approver = identifier(document["approver"], "approver")
    schedule_id = identifier(document["schedule_id"], "schedule_id")
    calls_per_minute = document["calls_per_minute"]
    if (
        type(calls_per_minute) is not int
        or not 1 <= calls_per_minute <= MAX_PROVIDER_CALLS_PER_MINUTE
    ):
        raise RecurringAuthorityError("recurring authority calls_per_minute exceeds 3000")
    calls_per_day = _daily_call_budget(document["calls_per_day"])
    registry_sha256 = sha256(document["registry_sha256"], "registry digest")
    notification_sha256 = sha256(document["notification_sha256"], "notification digest")
    tier_sha256 = sha256(document["tier_sha256"], "tier digest")
    issued = timestamp(document["issued_at_utc"], "issued_at_utc")
    valid_from = timestamp(document["valid_from_utc"], "valid_from_utc")
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
    snapshot_date, backfill_from = snapshot_range(
        document["norgate_snapshot_date"], document["backfill_from"]
    )
    security_master_sha256 = sha256(
        document["norgate_security_master_sha256"], "Norgate security-master digest"
    )
    security_master_row_count = document["norgate_security_master_row_count"]
    if type(security_master_row_count) is not int or security_master_row_count < 1:
        raise RecurringAuthorityError("Norgate security-master row count is invalid")
    verified = VerifiedRecurringAuthority(
        authority_id=authority_id,
        key_id=key_id,
        approver=approver,
        schedule_id=schedule_id,
        dataset_selection="all",
        raw_store_root=absolute_path(document["raw_store_root"], "raw_store_root"),
        dataset_root=absolute_path(document["dataset_root"], "dataset_root"),
        output_root=absolute_path(document["output_root"], "output_root"),
        norgate_security_master=absolute_path(
            document["norgate_security_master"], "norgate_security_master"
        ),
        norgate_security_master_sha256=security_master_sha256,
        norgate_security_master_row_count=security_master_row_count,
        norgate_snapshot_date=snapshot_date,
        backfill_from=backfill_from,
        calls_per_minute=calls_per_minute,
        calls_per_day=calls_per_day,
        registry_sha256=registry_sha256,
        notification_sha256=notification_sha256,
        tier_sha256=tier_sha256,
        valid_from_utc=valid_from,
        key_valid_until_utc=authority.valid_until_utc,
        payload_sha256=hashlib.sha256(payload).hexdigest(),
        signature_sha256=hashlib.sha256(signature).hexdigest(),
        authority_artifact_sha256=authority.artifact_sha256,
    )
    verified.require_request(now)
    return verified
