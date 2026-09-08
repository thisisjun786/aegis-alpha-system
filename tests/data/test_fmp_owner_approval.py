from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime, timedelta

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from aegis_alpha.collection.usage_checkpoint_crypto import (
    Ed25519PublicKeyring,
    TrustedEd25519PublicKey,
)
from aegis_alpha.data.fmp_collector_command import run_live_command
from aegis_alpha.data.fmp_owner_approval import (
    APPROVAL_CONTRACT,
    APPROVAL_VERSION,
    CONTRACT_REVISION,
    OwnerApprovalAuthority,
    OwnerApprovalError,
    verify_owner_approval,
)
from aegis_alpha.data.serialization import canonical_json_bytes

NOW = datetime(2026, 8, 19, 12, 0, tzinfo=UTC)
RUN_ID = "fmp-run-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
SCOPE = "b" * 64
AUTHORITY_ID = "synthetic-owner-authority"
KEY_ID = "ed25519:synthetic-2026-08"


def _document(**changes: object) -> dict[str, object]:
    document: dict[str, object] = {
        "contract": APPROVAL_CONTRACT,
        "version": APPROVAL_VERSION,
        "authority_id": AUTHORITY_ID,
        "key_id": KEY_ID,
        "approver": "synthetic-owner",
        "contract_revision": CONTRACT_REVISION,
        "policy_id": "fmp-operational-candidate-v1",
        "authorization_kind": "manual_one_shot",
        "run_identity": RUN_ID,
        "operation": "collect",
        "scope_sha256": SCOPE,
        "max_calls": 6,
        "issued_at_utc": "2026-08-19T11:59:00.000000Z",
        "expires_at_utc": "2026-08-19T12:30:00.000000Z",
    }
    document.update(changes)
    return document


def _authority(private_key: Ed25519PrivateKey) -> OwnerApprovalAuthority:
    valid_from = NOW - timedelta(days=1)
    valid_until = NOW + timedelta(days=1)
    return OwnerApprovalAuthority(
        authority_id=AUTHORITY_ID,
        key_id=KEY_ID,
        valid_from_utc=valid_from,
        valid_until_utc=valid_until,
        artifact_sha256="c" * 64,
        keyring=Ed25519PublicKeyring(
            {
                (AUTHORITY_ID, KEY_ID): TrustedEd25519PublicKey(
                    private_key.public_key().public_bytes_raw(),
                    valid_from_utc=valid_from,
                    valid_until_utc=valid_until,
                )
            }
        ),
    )


def _verify(document: dict[str, object]) -> None:
    private_key = Ed25519PrivateKey.generate()
    payload = canonical_json_bytes(document)
    verify_owner_approval(
        payload,
        private_key.sign(payload),
        _authority(private_key),
        now=NOW,
        expected_operation="collect",
        expected_scope_sha256=SCOPE,
        expected_max_calls=6,
    )


def test_direct_live_api_accepts_no_caller_constructed_authority() -> None:
    forbidden = {
        "approval",
        "artifact_hashes",
        "usage_baseline",
        "manifest",
        "manifest_bytes",
    }
    assert forbidden.isdisjoint(inspect.signature(run_live_command).parameters)


def test_valid_canonical_detached_signature_authorizes_exact_scope() -> None:
    _verify(_document())


@pytest.mark.parametrize(
    "mutation",
    [
        {"operation": "build-universe"},
        {"scope_sha256": "d" * 64},
        {"max_calls": 7},
        {"run_identity": "fmp-run-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
        {"authorization_kind": "scheduled"},
        {"policy_id": "other-policy"},
        {"contract_revision": "other-revision"},
        {"version": 2},
    ],
)
def test_signed_field_mismatch_fails_closed(mutation: dict[str, object]) -> None:
    with pytest.raises(OwnerApprovalError):
        _verify(_document(**mutation))


def test_tampered_signature_wrong_key_and_noncanonical_bytes_fail() -> None:
    private_key = Ed25519PrivateKey.generate()
    document = _document()
    payload = canonical_json_bytes(document)
    authority = _authority(private_key)
    signature = private_key.sign(payload)
    for candidate_payload, candidate_signature, candidate_authority in (
        (payload, bytes([signature[0] ^ 1]) + signature[1:], authority),
        (payload, signature, _authority(Ed25519PrivateKey.generate())),
        (json.dumps(document, indent=2).encode(), signature, authority),
    ):
        with pytest.raises(OwnerApprovalError):
            verify_owner_approval(
                candidate_payload,
                candidate_signature,
                candidate_authority,
                now=NOW,
                expected_operation="collect",
                expected_scope_sha256=SCOPE,
                expected_max_calls=6,
            )


@pytest.mark.parametrize(
    "payload",
    [
        b'{"contract":"x","contract":"y"}',
        canonical_json_bytes({**_document(), "unknown": True}),
        canonical_json_bytes({key: value for key, value in _document().items() if key != "key_id"}),
        b"{" + b" " * 4096 + b"}",
    ],
)
def test_duplicate_unknown_missing_and_oversized_payloads_fail(payload: bytes) -> None:
    with pytest.raises(OwnerApprovalError):
        verify_owner_approval(
            payload,
            b"0" * 64,
            _authority(Ed25519PrivateKey.generate()),
            now=NOW,
            expected_operation="collect",
            expected_scope_sha256=SCOPE,
            expected_max_calls=6,
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"issued_at_utc": "2026-08-19T12:00:00Z"},
        {"issued_at_utc": "2026-08-19T12:00:00.000000+00:00"},
        {"issued_at_utc": "2026-08-19T12:01:00.000000Z"},
        {"expires_at_utc": "2026-08-19T12:00:00.000000Z"},
        {"issued_at_utc": "2026-08-20T12:00:00.000000Z"},
        {"expires_at_utc": "2026-08-18T12:00:00.000000Z"},
    ],
)
def test_timestamp_contract_is_canonical_current_and_increasing(changes: dict[str, object]) -> None:
    with pytest.raises(OwnerApprovalError):
        _verify(_document(**changes))


def test_signature_must_be_exactly_64_bytes() -> None:
    private_key = Ed25519PrivateKey.generate()
    payload = canonical_json_bytes(_document())
    with pytest.raises(OwnerApprovalError):
        verify_owner_approval(
            payload,
            b"short",
            _authority(private_key),
            now=NOW,
            expected_operation="collect",
            expected_scope_sha256=SCOPE,
            expected_max_calls=6,
        )
