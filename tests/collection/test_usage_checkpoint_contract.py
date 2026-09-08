from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from aegis_alpha.collection.usage_checkpoint import (
    CheckpointContractError,
    UsageCheckpoint,
    UsageRecordLeaf,
    usage_records_root,
)
from aegis_alpha.collection.usage_checkpoint_crypto import (
    Ed25519PublicKeyring,
    MalformedEncodingError,
    SignatureVerificationError,
    UnknownVerificationKeyError,
    UnsupportedContractError,
    verify_usage_checkpoint,
)
from aegis_alpha.collection.usage_checkpoint_schema import SignedUsageCheckpoint

_START = datetime(2026, 8, 17, tzinfo=UTC)
_END = datetime(2026, 8, 18, tzinfo=UTC)
_GENERATED = _END + timedelta(minutes=1)


def _leaf(  # noqa: PLR0913 - compact fixture exposes independent signed fields
    sequence: int = 1,
    *,
    run_id: str = "run-fmp-20260817",
    plan_sha256: str = "1" * 64,
    provider: str = "fmp",
    quantity: Decimal = Decimal(3),
    recorded_at_utc: datetime | None = None,
) -> UsageRecordLeaf:
    return UsageRecordLeaf(
        run_id=run_id,
        usage_seq=sequence,
        plan_id="plan-fmp-daily-v1",
        plan_sha256=plan_sha256,
        provider=provider,
        metric="provider_requests",
        quantity=quantity,
        unit="count",
        recorded_at_utc=(
            _START + timedelta(hours=sequence) if recorded_at_utc is None else recorded_at_utc
        ),
    )


def _checkpoint(leaves: tuple[UsageRecordLeaf, ...]) -> UsageCheckpoint:
    return UsageCheckpoint.build(
        checkpoint_id="usage-checkpoint-20260817",
        provider="fmp",
        coverage_start_utc=_START,
        coverage_end_utc=_END,
        authority_id="aas-usage-authority-v1",
        key_id="ed25519:2026-08-18",
        generated_at_utc=_GENERATED,
        leaves=leaves,
    )


def _signed_fixture() -> tuple[
    tuple[UsageRecordLeaf, ...], SignedUsageCheckpoint, Ed25519PublicKeyring
]:
    leaves = (_leaf(), _leaf(2, quantity=Decimal("4.125")))
    checkpoint = _checkpoint(leaves)
    private_key = Ed25519PrivateKey.generate()
    public_bytes = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    signature = private_key.sign(checkpoint.canonical_bytes())
    signed = SignedUsageCheckpoint(checkpoint=checkpoint, signature=signature)
    keyring = Ed25519PublicKeyring({(checkpoint.authority_id, checkpoint.key_id): public_bytes})
    return leaves, signed, keyring


def test_canonical_vector_is_stable() -> None:
    leaves = (_leaf(), _leaf(2, quantity=Decimal("4.125")))
    checkpoint = _checkpoint(leaves)

    assert usage_records_root(leaves) == (
        "78b5ce50ca7a03461291a6cd7031ea5ed06351c4f378cc9dca69666257eb8a18"
    )
    assert checkpoint.canonical_bytes() == (
        b'{"authority_id":"aas-usage-authority-v1","checkpoint_id":"usage-checkpoint-'
        b'20260817","contract":"aegis-alpha/collection/usage-checkpoint/v1","coverage_end_utc"'
        b':"2026-08-18T00:00:00.000000Z","coverage_start_utc":"2026-08-17T00:00:00.000000Z"'
        b',"generated_at_utc":"2026-08-18T00:01:00.000000Z","key_id":"ed25519:2026-08-18"'
        b',"ordering_version":1,"provider":"fmp","schema_version":1,"signature_algorithm"'
        b':"ed25519","signature_version":1,"usage_record_count":2,"usage_record_ordering"'
        b':"recorded_at_utc,run_id,usage_seq,plan_id,plan_sha256,provider,metric,quantity,unit"'
        b',"usage_records_root_sha256":"78b5ce50ca7a03461291a6cd7031ea5ed06351c4f378cc9dca'
        b'69666257eb8a18"}'
    )


def test_ephemeral_ed25519_signature_is_deterministic_and_verifies() -> None:
    leaves = (_leaf(), _leaf(2, quantity=Decimal("4.125")))
    checkpoint = _checkpoint(leaves)
    private_key = Ed25519PrivateKey.generate()
    signature = private_key.sign(checkpoint.canonical_bytes())
    public_bytes = private_key.public_key().public_bytes_raw()
    keyring = Ed25519PublicKeyring({(checkpoint.authority_id, checkpoint.key_id): public_bytes})

    assert private_key.sign(checkpoint.canonical_bytes()) == signature
    verify_usage_checkpoint(
        SignedUsageCheckpoint(checkpoint=checkpoint, signature=signature), leaves, keyring
    )


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda leaf: replace(leaf, quantity=Decimal("3.000001")), id="quantity"),
        pytest.param(lambda leaf: replace(leaf, run_id="run-fmp-20260816"), id="run"),
        pytest.param(lambda leaf: replace(leaf, usage_seq=9), id="sequence"),
        pytest.param(lambda leaf: replace(leaf, plan_sha256="2" * 64), id="plan"),
        pytest.param(lambda leaf: replace(leaf, provider="fnp"), id="provider"),
    ],
)
def test_leaf_change_fails_verification(
    mutate: Callable[[UsageRecordLeaf], UsageRecordLeaf],
) -> None:
    leaves, signed, keyring = _signed_fixture()
    changed = (mutate(leaves[0]), leaves[1])

    with pytest.raises(SignatureVerificationError):
        verify_usage_checkpoint(signed, changed, keyring)


@pytest.mark.parametrize(
    ("mutate", "error"),
    [
        pytest.param(
            lambda value: replace(value, coverage_start_utc=_START - timedelta(seconds=1)),
            SignatureVerificationError,
            id="interval",
        ),
        pytest.param(
            lambda value: replace(value, provider="fnp"),
            SignatureVerificationError,
            id="checkpoint-provider",
        ),
        pytest.param(
            lambda value: replace(value, usage_records_root_sha256="2" * 64),
            SignatureVerificationError,
            id="root",
        ),
        pytest.param(
            lambda value: replace(value, usage_record_count=3),
            SignatureVerificationError,
            id="count",
        ),
        pytest.param(
            lambda value: replace(value, key_id="ed25519:2026-08-19"),
            UnknownVerificationKeyError,
            id="key",
        ),
        pytest.param(
            lambda value: replace(value, authority_id="aas-usage-authority-v2"),
            UnknownVerificationKeyError,
            id="authority",
        ),
        pytest.param(
            lambda value: replace(value, generated_at_utc=_GENERATED + timedelta(seconds=1)),
            SignatureVerificationError,
            id="generated",
        ),
    ],
)
def test_checkpoint_change_fails_verification(
    mutate: Callable[[UsageCheckpoint], UsageCheckpoint], error: type[BaseException]
) -> None:
    leaves, signed, keyring = _signed_fixture()
    changed = SignedUsageCheckpoint(
        checkpoint=mutate(signed.checkpoint), signature=signed.signature
    )

    with pytest.raises(error):
        verify_usage_checkpoint(changed, leaves, keyring)


def test_signature_and_public_key_changes_fail() -> None:
    leaves, signed, keyring = _signed_fixture()
    changed_signature = bytes([signed.signature[0] ^ 1]) + signed.signature[1:]
    trusted_public = keyring.public_key(
        signed.checkpoint.authority_id, signed.checkpoint.key_id
    ).public_bytes_raw()
    changed_public = bytes([trusted_public[0] ^ 1]) + trusted_public[1:]
    wrong_keyring = Ed25519PublicKeyring(
        {(signed.checkpoint.authority_id, signed.checkpoint.key_id): changed_public}
    )

    with pytest.raises(SignatureVerificationError):
        verify_usage_checkpoint(
            SignedUsageCheckpoint(signed.checkpoint, changed_signature), leaves, keyring
        )
    with pytest.raises(SignatureVerificationError) as captured:
        verify_usage_checkpoint(signed, leaves, wrong_keyring)
    assert signed.signature.hex() not in str(captured.value)
    assert changed_public.hex() not in str(captured.value)


def test_schema_fields_round_trip() -> None:
    leaves, signed, _ = _signed_fixture()

    restored = SignedUsageCheckpoint.from_schema_fields(signed.to_schema_fields(), leaves)

    assert restored == signed
    assert restored.checkpoint.usage_record_count == len(leaves)


def test_leaves_reject_unsorted_duplicate_and_malformed_values() -> None:
    first = _leaf()
    second = _leaf(2)
    with pytest.raises(CheckpointContractError, match="ordered"):
        usage_records_root((second, first))
    with pytest.raises(CheckpointContractError, match="duplicate"):
        usage_records_root((first, replace(first, quantity=Decimal(4))))
    with pytest.raises(CheckpointContractError, match="quantity"):
        _leaf(quantity=Decimal("0.0000001"))
    with pytest.raises(CheckpointContractError, match="UTC"):
        _leaf(recorded_at_utc=_START.replace(tzinfo=None))
    with pytest.raises(CheckpointContractError, match="run_id"):
        _leaf(run_id="run id")
    with pytest.raises(CheckpointContractError, match="plan_sha256"):
        _leaf(plan_sha256="A" * 64)


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda value: replace(value, checkpoint_id="bad id"), id="checkpoint-id"),
        pytest.param(lambda value: replace(value, provider="FMP"), id="provider"),
        pytest.param(lambda value: replace(value, authority_id="bad authority"), id="authority"),
        pytest.param(lambda value: replace(value, key_id="bad key"), id="key"),
        pytest.param(lambda value: replace(value, usage_records_root_sha256="A" * 64), id="root"),
        pytest.param(
            lambda value: replace(value, coverage_start_utc=_START.replace(tzinfo=None)),
            id="coverage-utc",
        ),
    ],
)
def test_checkpoint_rejects_malformed_schema_fields(
    mutate: Callable[[UsageCheckpoint], UsageCheckpoint],
) -> None:
    checkpoint = _checkpoint((_leaf(),))

    with pytest.raises(CheckpointContractError):
        mutate(checkpoint)


def test_wrong_versions_algorithm_and_malformed_encoding_are_typed() -> None:
    leaves = (_leaf(),)
    checkpoint = _checkpoint(leaves)
    with pytest.raises(UnsupportedContractError):
        replace(checkpoint, schema_version=2)
    with pytest.raises(UnsupportedContractError):
        replace(checkpoint, signature_version=2)
    with pytest.raises(UnsupportedContractError):
        replace(checkpoint, signature_algorithm="rsa")
    with pytest.raises(UnsupportedContractError):
        replace(checkpoint, ordering_version=2)
    with pytest.raises(MalformedEncodingError):
        SignedUsageCheckpoint(checkpoint, b"short")
    with pytest.raises(MalformedEncodingError):
        Ed25519PublicKeyring({("authority", "key"): b"short"})
