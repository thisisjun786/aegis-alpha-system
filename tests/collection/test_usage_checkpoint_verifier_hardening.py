from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from aegis_alpha.collection.usage_checkpoint import (
    CheckpointContractError,
    UsageCheckpoint,
    UsageRecordLeaf,
    usage_records_root,
)
from aegis_alpha.collection.usage_checkpoint_crypto import (
    Ed25519PublicKeyring,
    SignatureVerificationError,
    verify_usage_checkpoint,
)
from aegis_alpha.collection.usage_checkpoint_errors import UnsupportedContractError
from aegis_alpha.collection.usage_checkpoint_schema import SignedUsageCheckpoint

_START = datetime(2026, 8, 17, tzinfo=UTC)
_END = datetime(2026, 8, 18, tzinfo=UTC)


class _IntSubclass(int):
    pass


def _leaf(*, recorded_at_utc: datetime = _START, provider: str = "fmp") -> UsageRecordLeaf:
    return UsageRecordLeaf(
        run_id="run-bypass",
        usage_seq=1,
        plan_id="plan-bypass",
        plan_sha256="a" * 64,
        provider=provider,
        metric="provider_requests",
        quantity=Decimal(1),
        unit="count",
        recorded_at_utc=recorded_at_utc,
    )


def _resigned_bypass(
    leaves: tuple[UsageRecordLeaf, ...],
) -> tuple[SignedUsageCheckpoint, Ed25519PublicKeyring]:
    checkpoint = UsageCheckpoint(
        checkpoint_id="checkpoint-bypass",
        provider="fmp",
        coverage_start_utc=_START,
        coverage_end_utc=_END,
        usage_record_count=len(leaves),
        usage_records_root_sha256=usage_records_root(leaves),
        authority_id="authority-bypass",
        key_id="key-bypass",
        generated_at_utc=_END,
    )
    authority = Ed25519PrivateKey.generate()
    signed = SignedUsageCheckpoint(checkpoint, authority.sign(checkpoint.canonical_bytes()))
    keyring = Ed25519PublicKeyring(
        {(checkpoint.authority_id, checkpoint.key_id): authority.public_key().public_bytes_raw()}
    )
    return signed, keyring


_INVALID_SCOPES = [
    pytest.param((_leaf(recorded_at_utc=_START - timedelta(microseconds=1)),), id="before-start"),
    pytest.param((_leaf(recorded_at_utc=_END),), id="exactly-end"),
    pytest.param((_leaf(recorded_at_utc=_END + timedelta(microseconds=1)),), id="after-end"),
    pytest.param((_leaf(provider="fnp"),), id="provider-mismatch"),
]


@pytest.mark.parametrize("leaves", _INVALID_SCOPES)
def test_validly_resigned_semantic_bypass_fails_verification(
    leaves: tuple[UsageRecordLeaf, ...],
) -> None:
    signed, keyring = _resigned_bypass(leaves)

    with pytest.raises(SignatureVerificationError):
        verify_usage_checkpoint(signed, leaves, keyring)


@pytest.mark.parametrize("leaves", _INVALID_SCOPES)
def test_schema_reconstruction_rejects_semantic_bypass(
    leaves: tuple[UsageRecordLeaf, ...],
) -> None:
    signed, _ = _resigned_bypass(leaves)

    with pytest.raises(CheckpointContractError):
        SignedUsageCheckpoint.from_schema_fields(signed.to_schema_fields(), leaves)


def test_leaf_at_coverage_start_is_accepted() -> None:
    leaves = (_leaf(recorded_at_utc=_START),)
    signed, keyring = _resigned_bypass(leaves)

    restored = SignedUsageCheckpoint.from_schema_fields(signed.to_schema_fields(), leaves)
    verify_usage_checkpoint(restored, leaves, keyring)


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda value: replace(value, schema_version=True), id="schema-version"),
        pytest.param(lambda value: replace(value, signature_version=True), id="signature-version"),
        pytest.param(lambda value: replace(value, ordering_version=True), id="ordering-version"),
        pytest.param(
            lambda value: replace(value, schema_version=_IntSubclass(1)),
            id="schema-version-subclass",
        ),
        pytest.param(
            lambda value: replace(value, signature_version=_IntSubclass(1)),
            id="signature-version-subclass",
        ),
        pytest.param(
            lambda value: replace(value, ordering_version=_IntSubclass(1)),
            id="ordering-version-subclass",
        ),
    ],
)
def test_non_exact_integer_versions_are_rejected(
    mutate: Callable[[UsageCheckpoint], UsageCheckpoint],
) -> None:
    checkpoint = _resigned_bypass((_leaf(),))[0].checkpoint

    with pytest.raises(UnsupportedContractError):
        mutate(checkpoint)
