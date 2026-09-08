"""Synthetic standing-authority builders for FinImpulse CLI tests. Zero network."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Final

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from aegis_alpha.collection.usage_checkpoint_crypto import (
    Ed25519PublicKeyring,
    TrustedEd25519PublicKey,
)
from aegis_alpha.data.finimpulse_owner_authority import OwnerAuthority
from aegis_alpha.data.finimpulse_recurring_authority import (
    MAX_CALLS_PER_MINUTE,
    POLICY_ID,
    RECURRING_AUTHORITY_CONTRACT,
    RECURRING_AUTHORITY_VERSION,
    USD_MICROS,
    VerifiedRecurringAuthority,
)
from aegis_alpha.data.finimpulse_recurring_revocation import (
    REVOCATION_CONTRACT,
    revocation_path,
)
from aegis_alpha.data.serialization import canonical_json_bytes

AUTHORITY_NOW: Final = datetime(2026, 8, 18, 12, 0, tzinfo=UTC)
AUTHORITY_ID: Final = "synthetic-finimpulse-owner-authority"
KEY_ID: Final = "ed25519:synthetic-finimpulse-2026-08"
DEFAULT_BUDGET_USD: Final = Decimal("0.10")
DEFAULT_MAX_SPEND_MICROS: Final = int(DEFAULT_BUDGET_USD * USD_MICROS)
_UTC_FORMAT: Final = "%Y-%m-%dT%H:%M:%S.%fZ"


@dataclass(frozen=True, slots=True)
class StandingAuthorityFixture:
    payload: bytes
    signature: bytes
    authority_path: Path
    signature_path: Path
    owner_authority_path: Path
    owner_authority: OwnerAuthority


def _utc_text(moment: datetime) -> str:
    return moment.strftime(_UTC_FORMAT)


def make_standing_authority(
    root: Path,
    *,
    destinations: Path | None = None,
    document_changes: Mapping[str, object] | None = None,
    owner_changes: Mapping[str, object] | None = None,
) -> StandingAuthorityFixture:
    """Sign one canonical standing probe scope rooted at this test's destinations."""

    dest = root if destinations is None else destinations
    key = Ed25519PrivateKey.generate()
    key_valid_from = AUTHORITY_NOW - timedelta(days=1)
    key_valid_until = AUTHORITY_NOW + timedelta(days=370)
    document: dict[str, object] = {
        "contract": RECURRING_AUTHORITY_CONTRACT,
        "version": RECURRING_AUTHORITY_VERSION,
        "authority_id": AUTHORITY_ID,
        "key_id": KEY_ID,
        "approver": "synthetic-owner",
        "policy_id": POLICY_ID,
        "allowed_domains": ["api.finimpulse.com"],
        "raw_store_root": str((dest / "raw").resolve()),
        "dataset_root": str((dest / "dataset").resolve()),
        "calls_per_minute": MAX_CALLS_PER_MINUTE,
        "max_spend_micros": DEFAULT_MAX_SPEND_MICROS,
        "issued_at_utc": "2026-08-18T11:59:00.000000Z",
        "valid_from_utc": "2026-08-18T12:00:00.000000Z",
    }
    if document_changes:
        document.update(document_changes)
    payload = canonical_json_bytes(document)
    signature = key.sign(payload)
    owner_document: dict[str, object] = {
        "schema": "aegis-alpha/finimpulse-owner-approval-authority",
        "version": 1,
        "authority_id": AUTHORITY_ID,
        "key_id": KEY_ID,
        "public_key_encoding": "raw-ed25519-hex",
        "public_key": key.public_key().public_bytes_raw().hex(),
        "valid_from_utc": _utc_text(key_valid_from),
        "valid_until_utc": _utc_text(key_valid_until),
    }
    if owner_changes:
        owner_document.update(owner_changes)
    owner_payload = canonical_json_bytes(owner_document)
    root.mkdir(parents=True, exist_ok=True)
    authority_path = root / "standing_authority.json"
    signature_path = root / "standing_authority.sig"
    owner_authority_path = root / "owner_authority.json"
    authority_path.write_bytes(payload)
    signature_path.write_bytes(signature)
    owner_authority_path.write_bytes(owner_payload)
    owner_authority_path.chmod(0o600)
    return StandingAuthorityFixture(
        payload=payload,
        signature=signature,
        authority_path=authority_path,
        signature_path=signature_path,
        owner_authority_path=owner_authority_path,
        owner_authority=OwnerAuthority(
            authority_id=AUTHORITY_ID,
            key_id=KEY_ID,
            valid_from_utc=key_valid_from,
            valid_until_utc=key_valid_until,
            artifact_sha256=hashlib.sha256(owner_payload).hexdigest(),
            keyring=Ed25519PublicKeyring(
                {
                    (AUTHORITY_ID, KEY_ID): TrustedEd25519PublicKey(
                        key.public_key().public_bytes_raw(),
                        valid_from_utc=key_valid_from,
                        valid_until_utc=key_valid_until,
                    )
                }
            ),
        ),
    )


def authority_argv(fixture: StandingAuthorityFixture) -> list[str]:
    return [
        "--recurring-authority",
        str(fixture.authority_path),
        "--recurring-authority-signature",
        str(fixture.signature_path),
    ]


def write_revocation(authority: VerifiedRecurringAuthority, revoked_at: datetime) -> Path:
    """Publish one canonical revocation marker under the signed raw store root."""

    destination = revocation_path(authority)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(
        canonical_json_bytes(
            {
                "authority_id": authority.authority_id,
                "contract": REVOCATION_CONTRACT,
                "payload_sha256": authority.payload_sha256,
                "revoked_at_utc": _utc_text(revoked_at),
                "version": 1,
            }
        )
    )
    return destination
