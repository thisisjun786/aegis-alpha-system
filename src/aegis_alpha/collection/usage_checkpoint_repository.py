from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, cast

from sqlalchemy import Connection, Engine, select
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.exc import IntegrityError

from aegis_alpha.collection.schema import collection_usage_checkpoints
from aegis_alpha.collection.usage_checkpoint import UsageRecordLeaf
from aegis_alpha.collection.usage_checkpoint_crypto import (
    Ed25519PublicKeyring,
    verify_usage_checkpoint,
    verify_usage_checkpoint_signature,
)
from aegis_alpha.collection.usage_checkpoint_schema import (
    CheckpointSchemaFields,
    SignedUsageCheckpoint,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import RowMapping


class UsageCheckpointConflictError(RuntimeError):
    """A checkpoint identity already has a different immutable projection."""


class UsageCheckpointNotFoundError(LookupError):
    """No checkpoint exists for the exact requested checkpoint identity."""


class UsageCheckpointSelectionError(LookupError):
    """Exact provider coverage does not identify one persisted checkpoint."""


@dataclass(frozen=True, slots=True)
class AuthenticatedUsageCheckpointMetadata:
    """Authority-authenticated envelope; live usage rows have not been recomputed."""

    signed_checkpoint: SignedUsageCheckpoint
    registered_at_utc: datetime


class UsageCheckpointRepository:
    """Append-only persistence for independently signed usage commitments."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    @contextmanager
    def _transaction(self, connection: Connection | None) -> Iterator[Connection]:
        if connection is not None:
            if connection.engine is not self._engine:
                raise ValueError("checkpoint transaction belongs to another PostgreSQL engine")
            yield connection
            return
        with self._engine.begin() as owned:
            yield owned

    def register(
        self,
        signed: SignedUsageCheckpoint,
        leaves: Sequence[UsageRecordLeaf],
        keyring: Ed25519PublicKeyring,
        *,
        connection: Connection | None = None,
    ) -> None:
        """Verify the complete commitment before attempting its immutable insert."""

        leaf_tuple = tuple(leaves)
        verify_usage_checkpoint(signed, leaf_tuple, keyring)
        values = signed.to_schema_fields()
        try:
            with self._transaction(connection) as active:
                active.execute(
                    postgres_insert(collection_usage_checkpoints)
                    .values(values)
                    .on_conflict_do_nothing(
                        index_elements=[collection_usage_checkpoints.c.checkpoint_id]
                    )
                )
                observed = (
                    active.execute(
                        select(collection_usage_checkpoints)
                        .where(
                            collection_usage_checkpoints.c.checkpoint_id
                            == signed.checkpoint.checkpoint_id
                        )
                        .with_for_update()
                    )
                    .mappings()
                    .one()
                )
                if not _matches(observed, values):
                    raise UsageCheckpointConflictError(
                        "checkpoint identity has a different immutable projection"
                    )
        except IntegrityError as error:
            raise UsageCheckpointConflictError(
                "checkpoint identity conflicts with immutable evidence"
            ) from error

    def exact_checkpoint_id(
        self,
        *,
        provider: str,
        coverage_start_utc: datetime,
        coverage_end_utc: datetime,
        connection: Connection,
    ) -> str:
        """Return the sole checkpoint for exact provider coverage, or fail closed."""

        rows = (
            connection.execute(
                select(collection_usage_checkpoints.c.checkpoint_id)
                .where(
                    collection_usage_checkpoints.c.provider == provider,
                    collection_usage_checkpoints.c.coverage_start_utc == coverage_start_utc,
                    collection_usage_checkpoints.c.coverage_end_utc == coverage_end_utc,
                )
                .order_by(collection_usage_checkpoints.c.checkpoint_id)
                .limit(2)
            )
            .scalars()
            .all()
        )
        if len(rows) != 1:
            raise UsageCheckpointSelectionError(
                "exact provider coverage must identify one usage checkpoint"
            )
        return rows[0]

    def coverage_windows_at_or_before(
        self,
        *,
        provider: str,
        at_or_before_utc: datetime,
        connection: Connection,
    ) -> tuple[tuple[datetime, datetime], ...]:
        """List registered provider windows through one runtime instant."""

        rows = (
            connection.execute(
                select(
                    collection_usage_checkpoints.c.coverage_start_utc,
                    collection_usage_checkpoints.c.coverage_end_utc,
                )
                .where(
                    collection_usage_checkpoints.c.provider == provider,
                    collection_usage_checkpoints.c.coverage_end_utc <= at_or_before_utc,
                )
                .order_by(
                    collection_usage_checkpoints.c.coverage_end_utc.desc(),
                    collection_usage_checkpoints.c.coverage_start_utc,
                )
            )
            .tuples()
            .all()
        )
        return tuple((row[0], row[1]) for row in rows)

    def get_authenticated_metadata(
        self,
        checkpoint_id: str,
        keyring: Ed25519PublicKeyring,
        *,
        connection: Connection | None = None,
    ) -> AuthenticatedUsageCheckpointMetadata:
        """Return only metadata whose persisted detached signature still verifies."""

        with self._transaction(connection) as active:
            row = (
                active.execute(
                    select(collection_usage_checkpoints).where(
                        collection_usage_checkpoints.c.checkpoint_id == checkpoint_id
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            raise UsageCheckpointNotFoundError(checkpoint_id)
        signed = SignedUsageCheckpoint.from_schema_metadata(_schema_fields(row))
        verify_usage_checkpoint_signature(signed, keyring)
        return AuthenticatedUsageCheckpointMetadata(
            signed_checkpoint=signed,
            registered_at_utc=cast("datetime", row["registered_at_utc"]),
        )


def _matches(observed: RowMapping, expected: Mapping[str, object]) -> bool:
    return all(observed[field] == value for field, value in expected.items())


def _schema_fields(row: RowMapping) -> CheckpointSchemaFields:
    return cast(
        "CheckpointSchemaFields",
        cast(
            "object",
            {
                "checkpoint_id": row["checkpoint_id"],
                "schema_version": row["schema_version"],
                "provider": row["provider"],
                "coverage_start_utc": row["coverage_start_utc"],
                "coverage_end_utc": row["coverage_end_utc"],
                "usage_record_count": row["usage_record_count"],
                "usage_records_root_sha256": row["usage_records_root_sha256"],
                "authority_id": row["authority_id"],
                "key_id": row["key_id"],
                "generated_at_utc": row["generated_at_utc"],
                "signature_algorithm": row["signature_algorithm"],
                "signature_version": row["signature_version"],
                "signature": row["signature"],
            },
        ),
    )
