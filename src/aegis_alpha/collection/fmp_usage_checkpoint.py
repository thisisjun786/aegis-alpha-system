from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import TYPE_CHECKING

from sqlalchemy import text

from aegis_alpha.collection.provider_usage_repository import ProviderUsageRepository
from aegis_alpha.collection.usage_checkpoint import UsageCheckpoint, UsageRecordLeaf
from aegis_alpha.collection.usage_checkpoint_crypto import Ed25519PublicKeyring
from aegis_alpha.collection.usage_checkpoint_errors import (
    CheckpointContractError,
    ProviderUsageIntegrityError,
)
from aegis_alpha.collection.usage_checkpoint_repository import (
    UsageCheckpointRepository,
    UsageCheckpointSelectionError,
)
from aegis_alpha.collection.usage_checkpoint_schema import validate_checkpoint_leaves

if TYPE_CHECKING:
    from sqlalchemy import Connection, Engine

_FMP_PROVIDER = "fmp"
_USAGE_CHECKPOINT_LOCK_KEY = "aegis_alpha.collection.fmp_usage_checkpoint"


@dataclass(frozen=True, slots=True)
class UnsignedUsageCheckpointCandidate:
    """Complete canonical checkpoint material intended for an external signer."""

    checkpoint: UsageCheckpoint
    leaves: tuple[UsageRecordLeaf, ...]
    payload_bytes: bytes


@dataclass(frozen=True, slots=True)
class VerifiedProviderUsageAggregate:
    metric: str
    unit: str
    quantity: Decimal


@dataclass(frozen=True, slots=True)
class VerifiedProviderUsageSnapshot:
    """Authenticated provider usage; deliberately not a TrustedUsageSnapshot."""

    checkpoint_id: str
    provider: str
    coverage_start_utc: datetime
    coverage_end_utc: datetime
    generated_at_utc: datetime
    usage_record_count: int
    usage_records_root_sha256: str
    aggregates: tuple[VerifiedProviderUsageAggregate, ...]

    @property
    def totals(self) -> Mapping[tuple[str, str], Decimal]:
        return MappingProxyType(
            {(item.metric, item.unit): item.quantity for item in self.aggregates}
        )


class FmpUsageCheckpointService:
    """Prepare and reverify FMP-only usage commitments from PostgreSQL."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._usage = ProviderUsageRepository()
        self._checkpoints = UsageCheckpointRepository(engine)

    def prepare_candidate(  # noqa: PLR0913 - fields mirror the checkpoint contract
        self,
        *,
        checkpoint_id: str,
        coverage_start_utc: datetime,
        coverage_end_utc: datetime,
        authority_id: str,
        key_id: str,
        generated_at_utc: datetime,
    ) -> UnsignedUsageCheckpointCandidate:
        with self._engine.begin() as connection:
            acquire_fmp_usage_checkpoint_lock(connection)
            leaves = tuple(
                self._usage.leaves(
                    connection,
                    provider=_FMP_PROVIDER,
                    coverage_start_utc=coverage_start_utc,
                    coverage_end_utc=coverage_end_utc,
                )
            )
            checkpoint = UsageCheckpoint.build(
                checkpoint_id=checkpoint_id,
                provider=_FMP_PROVIDER,
                coverage_start_utc=coverage_start_utc,
                coverage_end_utc=coverage_end_utc,
                authority_id=authority_id,
                key_id=key_id,
                generated_at_utc=generated_at_utc,
                leaves=leaves,
            )
            return UnsignedUsageCheckpointCandidate(
                checkpoint=checkpoint,
                leaves=leaves,
                payload_bytes=checkpoint.canonical_bytes(),
            )

    def verify_current(
        self,
        *,
        checkpoint_id: str,
        coverage_start_utc: datetime,
        coverage_end_utc: datetime,
        keyring: Ed25519PublicKeyring,
    ) -> VerifiedProviderUsageSnapshot:
        with self._repeatable_read() as connection:
            return self._verify(
                connection,
                checkpoint_id=checkpoint_id,
                coverage_start_utc=coverage_start_utc,
                coverage_end_utc=coverage_end_utc,
                keyring=keyring,
            )

    def verify_exact_windows(
        self,
        *,
        windows: Sequence[tuple[datetime, datetime]],
        keyring: Ed25519PublicKeyring,
    ) -> tuple[VerifiedProviderUsageSnapshot, ...]:
        """Select and verify exact FMP windows in one read-only database snapshot."""

        with self._repeatable_read() as connection:
            snapshots = []
            for coverage_start_utc, coverage_end_utc in windows:
                checkpoint_id = self._checkpoints.exact_checkpoint_id(
                    provider=_FMP_PROVIDER,
                    coverage_start_utc=coverage_start_utc,
                    coverage_end_utc=coverage_end_utc,
                    connection=connection,
                )
                snapshots.append(
                    self._verify(
                        connection,
                        checkpoint_id=checkpoint_id,
                        coverage_start_utc=coverage_start_utc,
                        coverage_end_utc=coverage_end_utc,
                        keyring=keyring,
                    )
                )
            return tuple(snapshots)

    def verify_latest_available(
        self,
        *,
        at_or_before_utc: datetime,
        keyring: Ed25519PublicKeyring,
    ) -> tuple[VerifiedProviderUsageSnapshot, VerifiedProviderUsageSnapshot]:
        """Verify the latest complete rolling/daily pair available by runtime."""

        if at_or_before_utc.utcoffset() is None:
            raise ValueError("checkpoint selection instant must be timezone-aware")
        moment = at_or_before_utc.astimezone(UTC)
        with self._repeatable_read() as connection:
            windows = self._checkpoints.coverage_windows_at_or_before(
                provider=_FMP_PROVIDER,
                at_or_before_utc=moment,
                connection=connection,
            )
            selected = _latest_complete_windows(windows)
            snapshots = []
            for coverage_start_utc, coverage_end_utc in selected:
                snapshots.append(
                    self._verify(
                        connection,
                        checkpoint_id=self._checkpoints.exact_checkpoint_id(
                            provider=_FMP_PROVIDER,
                            coverage_start_utc=coverage_start_utc,
                            coverage_end_utc=coverage_end_utc,
                            connection=connection,
                        ),
                        coverage_start_utc=coverage_start_utc,
                        coverage_end_utc=coverage_end_utc,
                        keyring=keyring,
                    )
                )
            return snapshots[0], snapshots[1]

    def _verify(
        self,
        connection: Connection,
        *,
        checkpoint_id: str,
        coverage_start_utc: datetime,
        coverage_end_utc: datetime,
        keyring: Ed25519PublicKeyring,
    ) -> VerifiedProviderUsageSnapshot:
        authenticated = self._checkpoints.get_authenticated_metadata(
            checkpoint_id, keyring, connection=connection
        )
        checkpoint = authenticated.signed_checkpoint.checkpoint
        if (
            checkpoint.provider != _FMP_PROVIDER
            or checkpoint.coverage_start_utc != coverage_start_utc
            or checkpoint.coverage_end_utc != coverage_end_utc
        ):
            raise ProviderUsageIntegrityError(
                "checkpoint provider or coverage does not match the requested FMP snapshot"
            )
        leaves = tuple(
            self._usage.leaves(
                connection,
                provider=_FMP_PROVIDER,
                coverage_start_utc=coverage_start_utc,
                coverage_end_utc=coverage_end_utc,
            )
        )
        try:
            validate_checkpoint_leaves(checkpoint, leaves)
        except CheckpointContractError as error:
            raise ProviderUsageIntegrityError(
                "current FMP usage rows do not match the signed checkpoint"
            ) from error
        return _snapshot(checkpoint, leaves)

    @contextmanager
    def _repeatable_read(self) -> Iterator[Connection]:
        with (
            self._engine.connect().execution_options(
                isolation_level="REPEATABLE READ",
                postgresql_readonly=True,
            ) as connection,
            connection.begin(),
        ):
            yield connection


def acquire_fmp_usage_checkpoint_lock(connection: Connection) -> None:
    """Serialize usage inserts with FMP checkpoint snapshot preparation."""

    connection.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": _USAGE_CHECKPOINT_LOCK_KEY},
    )


def _latest_complete_windows(
    windows: Sequence[tuple[datetime, datetime]],
) -> tuple[tuple[datetime, datetime], tuple[datetime, datetime]]:
    for coverage_end in dict.fromkeys(end for _, end in windows):
        rolling_start = coverage_end - timedelta(days=30)
        daily_start = coverage_end.replace(hour=0, minute=0, second=0, microsecond=0)
        expected = ((rolling_start, coverage_end), (daily_start, coverage_end))
        if all(windows.count(window) == 1 for window in expected):
            return expected
    raise UsageCheckpointSelectionError(
        "no complete FMP rolling and daily checkpoint pair is available"
    )


def _snapshot(
    checkpoint: UsageCheckpoint,
    leaves: tuple[UsageRecordLeaf, ...],
) -> VerifiedProviderUsageSnapshot:
    totals: dict[tuple[str, str], Decimal] = {}
    for leaf in leaves:
        identity = (leaf.metric, leaf.unit)
        totals[identity] = totals.get(identity, Decimal(0)) + leaf.quantity
    aggregates = tuple(
        VerifiedProviderUsageAggregate(metric, unit, quantity)
        for (metric, unit), quantity in sorted(totals.items())
    )
    return VerifiedProviderUsageSnapshot(
        checkpoint_id=checkpoint.checkpoint_id,
        provider=checkpoint.provider,
        coverage_start_utc=checkpoint.coverage_start_utc,
        coverage_end_utc=checkpoint.coverage_end_utc,
        generated_at_utc=checkpoint.generated_at_utc,
        usage_record_count=checkpoint.usage_record_count,
        usage_records_root_sha256=checkpoint.usage_records_root_sha256,
        aggregates=aggregates,
    )
