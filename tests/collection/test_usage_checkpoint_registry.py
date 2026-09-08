from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import Connection, Engine, func, select
from sqlalchemy.exc import DBAPIError

from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import collection_usage_checkpoints
from aegis_alpha.collection.usage_checkpoint import UsageCheckpoint, UsageRecordLeaf
from aegis_alpha.collection.usage_checkpoint_crypto import (
    Ed25519PublicKeyring,
    StaleVerificationKeyError,
    TrustedEd25519PublicKey,
    UnknownVerificationKeyError,
)
from aegis_alpha.collection.usage_checkpoint_errors import SignatureVerificationError
from aegis_alpha.collection.usage_checkpoint_repository import (
    AuthenticatedUsageCheckpointMetadata,
    UsageCheckpointConflictError,
    UsageCheckpointNotFoundError,
)
from aegis_alpha.collection.usage_checkpoint_schema import SignedUsageCheckpoint

_START = datetime(2026, 8, 17, tzinfo=UTC)
_END = datetime(2026, 8, 18, tzinfo=UTC)
_GENERATED = _END + timedelta(minutes=1)


def _leaf(**overrides: object) -> UsageRecordLeaf:
    values: dict[str, object] = {
        "run_id": "run-fmp-20260817",
        "usage_seq": 1,
        "plan_id": "plan-fmp-daily-v1",
        "plan_sha256": "1" * 64,
        "provider": "fmp",
        "metric": "provider_requests",
        "quantity": Decimal(3),
        "unit": "count",
        "recorded_at_utc": _START + timedelta(hours=1),
    }
    values.update(overrides)
    return UsageRecordLeaf(**values)  # type: ignore[arg-type]


def _signed(
    private_key: Ed25519PrivateKey,
    leaves: tuple[UsageRecordLeaf, ...],
    **overrides: object,
) -> SignedUsageCheckpoint:
    values: dict[str, object] = {
        "checkpoint_id": "usage-checkpoint-20260817",
        "provider": "fmp",
        "coverage_start_utc": _START,
        "coverage_end_utc": _END,
        "authority_id": "aas-usage-authority-v1",
        "key_id": "ed25519:2026-08-18",
        "generated_at_utc": _GENERATED,
        "leaves": leaves,
    }
    values.update(overrides)
    checkpoint = UsageCheckpoint.build(**values)  # type: ignore[arg-type]
    return SignedUsageCheckpoint(checkpoint, private_key.sign(checkpoint.canonical_bytes()))


@pytest.fixture
def checkpoint_material() -> tuple[
    tuple[UsageRecordLeaf, ...],
    SignedUsageCheckpoint,
    Ed25519PublicKeyring,
    Ed25519PrivateKey,
]:
    private_key = Ed25519PrivateKey.generate()
    leaves = (_leaf(),)
    signed = _signed(private_key, leaves)
    keyring = Ed25519PublicKeyring(
        {
            (
                signed.checkpoint.authority_id,
                signed.checkpoint.key_id,
            ): private_key.public_key().public_bytes_raw()
        }
    )
    return leaves, signed, keyring, private_key


@pytest.fixture
def checkpoint_connection(clean_postgres: Engine) -> Iterator[Connection]:
    with clean_postgres.connect() as connection:
        transaction = connection.begin()
        try:
            yield connection
        finally:
            transaction.rollback()


def _count(connection: Connection) -> int:
    return connection.execute(
        select(func.count()).select_from(collection_usage_checkpoints)
    ).scalar_one()


def test_valid_insert_replay_retrieve_and_reverify(
    collection_registry: CollectionRegistry,
    checkpoint_connection: Connection,
    checkpoint_material: tuple[
        tuple[UsageRecordLeaf, ...],
        SignedUsageCheckpoint,
        Ed25519PublicKeyring,
        Ed25519PrivateKey,
    ],
) -> None:
    leaves, signed, keyring, _ = checkpoint_material

    collection_registry.register_usage_checkpoint(
        signed, leaves, keyring, connection=checkpoint_connection
    )
    collection_registry.register_usage_checkpoint(
        signed, leaves, keyring, connection=checkpoint_connection
    )
    restored = collection_registry.usage_checkpoint_metadata(
        signed.checkpoint.checkpoint_id, keyring, connection=checkpoint_connection
    )

    assert isinstance(restored, AuthenticatedUsageCheckpointMetadata)
    assert restored.signed_checkpoint == signed
    assert restored.registered_at_utc.tzinfo is not None
    assert _count(checkpoint_connection) == 1


def test_conflicting_checkpoint_identity_is_typed(
    collection_registry: CollectionRegistry,
    checkpoint_connection: Connection,
    checkpoint_material: tuple[
        tuple[UsageRecordLeaf, ...],
        SignedUsageCheckpoint,
        Ed25519PublicKeyring,
        Ed25519PrivateKey,
    ],
) -> None:
    leaves, signed, keyring, private_key = checkpoint_material
    changed_leaves = (_leaf(quantity=Decimal(4)),)
    conflicting = _signed(private_key, changed_leaves)
    collection_registry.register_usage_checkpoint(
        signed, leaves, keyring, connection=checkpoint_connection
    )

    with pytest.raises(UsageCheckpointConflictError):
        collection_registry.register_usage_checkpoint(
            conflicting, changed_leaves, keyring, connection=checkpoint_connection
        )


@pytest.mark.parametrize("invalid", ["signature", "root", "count", "coverage", "provider"])
def test_invalid_checkpoint_is_rejected_before_insert(
    collection_registry: CollectionRegistry,
    checkpoint_connection: Connection,
    checkpoint_material: tuple[
        tuple[UsageRecordLeaf, ...],
        SignedUsageCheckpoint,
        Ed25519PublicKeyring,
        Ed25519PrivateKey,
    ],
    invalid: str,
) -> None:
    leaves, signed, keyring, private_key = checkpoint_material
    supplied_leaves = leaves
    if invalid == "signature":
        candidate = SignedUsageCheckpoint(signed.checkpoint, bytes(64))
    elif invalid == "root":
        candidate = SignedUsageCheckpoint(
            replace(signed.checkpoint, usage_records_root_sha256="2" * 64), signed.signature
        )
    elif invalid == "count":
        candidate = SignedUsageCheckpoint(
            replace(signed.checkpoint, usage_record_count=2), signed.signature
        )
    elif invalid == "coverage":
        supplied_leaves = (_leaf(recorded_at_utc=_END),)
        candidate = _signed(private_key, leaves)
    else:
        supplied_leaves = (_leaf(provider="sec"),)
        candidate = _signed(private_key, leaves)

    with pytest.raises(SignatureVerificationError):
        collection_registry.register_usage_checkpoint(
            candidate, supplied_leaves, keyring, connection=checkpoint_connection
        )
    assert _count(checkpoint_connection) == 0


def test_unknown_and_stale_keys_are_rejected_before_insert(
    collection_registry: CollectionRegistry,
    checkpoint_connection: Connection,
    checkpoint_material: tuple[
        tuple[UsageRecordLeaf, ...],
        SignedUsageCheckpoint,
        Ed25519PublicKeyring,
        Ed25519PrivateKey,
    ],
) -> None:
    leaves, signed, _, private_key = checkpoint_material
    with pytest.raises(UnknownVerificationKeyError):
        collection_registry.register_usage_checkpoint(
            signed, leaves, Ed25519PublicKeyring({}), connection=checkpoint_connection
        )
    stale = Ed25519PublicKeyring(
        {
            (signed.checkpoint.authority_id, signed.checkpoint.key_id): TrustedEd25519PublicKey(
                encoded=private_key.public_key().public_bytes_raw(),
                valid_from_utc=_GENERATED + timedelta(seconds=1),
            )
        }
    )
    with pytest.raises(StaleVerificationKeyError):
        collection_registry.register_usage_checkpoint(
            signed, leaves, stale, connection=checkpoint_connection
        )
    assert _count(checkpoint_connection) == 0


@pytest.mark.parametrize("column", ["provider", "signature"])
def test_direct_database_tamper_fails_metadata_retrieval(
    collection_registry: CollectionRegistry,
    checkpoint_connection: Connection,
    checkpoint_material: tuple[
        tuple[UsageRecordLeaf, ...],
        SignedUsageCheckpoint,
        Ed25519PublicKeyring,
        Ed25519PrivateKey,
    ],
    column: str,
) -> None:
    leaves, signed, keyring, _ = checkpoint_material
    collection_registry.register_usage_checkpoint(
        signed, leaves, keyring, connection=checkpoint_connection
    )
    checkpoint_connection.exec_driver_sql(
        "ALTER TABLE collection_usage_checkpoints DISABLE TRIGGER "
        "collection_usage_checkpoints_append_only"
    )
    values = {"provider": "sec"} if column == "provider" else {"signature": bytes(64)}
    checkpoint_connection.execute(
        collection_usage_checkpoints.update()
        .where(collection_usage_checkpoints.c.checkpoint_id == signed.checkpoint.checkpoint_id)
        .values(**values)
    )
    checkpoint_connection.exec_driver_sql(
        "ALTER TABLE collection_usage_checkpoints ENABLE TRIGGER "
        "collection_usage_checkpoints_append_only"
    )

    with pytest.raises(SignatureVerificationError):
        collection_registry.usage_checkpoint_metadata(
            signed.checkpoint.checkpoint_id, keyring, connection=checkpoint_connection
        )


def test_missing_checkpoint_and_untrusted_retrieval_are_typed(
    collection_registry: CollectionRegistry,
    checkpoint_connection: Connection,
    checkpoint_material: tuple[
        tuple[UsageRecordLeaf, ...],
        SignedUsageCheckpoint,
        Ed25519PublicKeyring,
        Ed25519PrivateKey,
    ],
) -> None:
    leaves, signed, keyring, private_key = checkpoint_material
    with pytest.raises(UsageCheckpointNotFoundError):
        collection_registry.usage_checkpoint_metadata(
            "missing-checkpoint", keyring, connection=checkpoint_connection
        )
    collection_registry.register_usage_checkpoint(
        signed, leaves, keyring, connection=checkpoint_connection
    )
    with pytest.raises(UnknownVerificationKeyError):
        collection_registry.usage_checkpoint_metadata(
            signed.checkpoint.checkpoint_id,
            Ed25519PublicKeyring({}),
            connection=checkpoint_connection,
        )
    stale = Ed25519PublicKeyring(
        {
            (signed.checkpoint.authority_id, signed.checkpoint.key_id): TrustedEd25519PublicKey(
                encoded=private_key.public_key().public_bytes_raw(),
                valid_until_utc=_GENERATED,
            )
        }
    )
    with pytest.raises(StaleVerificationKeyError):
        collection_registry.usage_checkpoint_metadata(
            signed.checkpoint.checkpoint_id, stale, connection=checkpoint_connection
        )


def test_checkpoint_table_refuses_update_delete_and_registry_exposes_neither(
    collection_registry: CollectionRegistry,
    checkpoint_connection: Connection,
    checkpoint_material: tuple[
        tuple[UsageRecordLeaf, ...],
        SignedUsageCheckpoint,
        Ed25519PublicKeyring,
        Ed25519PrivateKey,
    ],
) -> None:
    leaves, signed, keyring, _ = checkpoint_material
    collection_registry.register_usage_checkpoint(
        signed, leaves, keyring, connection=checkpoint_connection
    )
    where = collection_usage_checkpoints.c.checkpoint_id == signed.checkpoint.checkpoint_id
    for statement in (
        collection_usage_checkpoints.update().where(where).values(provider="sec"),
        collection_usage_checkpoints.delete().where(where),
    ):
        nested = checkpoint_connection.begin_nested()
        with pytest.raises(DBAPIError, match="append-only"):
            checkpoint_connection.execute(statement)
        nested.rollback()
    assert not hasattr(collection_registry, "update_usage_checkpoint")
    assert not hasattr(collection_registry, "delete_usage_checkpoint")
