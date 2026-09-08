from __future__ import annotations

from collections.abc import Callable, Hashable, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, assert_never

from sqlalchemy import Connection, Engine, func, select, text
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.exc import IntegrityError

from aegis_alpha.collection.fmp_usage_checkpoint import (
    FmpUsageCheckpointService,
    UnsignedUsageCheckpointCandidate,
    VerifiedProviderUsageSnapshot,
    acquire_fmp_usage_checkpoint_lock,
)
from aegis_alpha.collection.records import (
    CollectionReceipt,
    CollectionRun,
    CollectionRunEvent,
    CollectionRunPlan,
    CollectionRunState,
    CollectionUsageRecord,
    CurrentWatermark,
    RunEventType,
    WatermarkAdvance,
)
from aegis_alpha.collection.schema import (
    collection_run_events,
    collection_run_plans,
    collection_run_receipts,
    collection_runs,
    collection_usage_records,
    collection_watermarks,
)
from aegis_alpha.collection.usage_checkpoint import UsageRecordLeaf
from aegis_alpha.collection.usage_checkpoint_crypto import Ed25519PublicKeyring
from aegis_alpha.collection.usage_checkpoint_repository import (
    AuthenticatedUsageCheckpointMetadata,
    UsageCheckpointRepository,
)
from aegis_alpha.collection.usage_checkpoint_schema import SignedUsageCheckpoint
from aegis_alpha.metadata.records import reject_credential_metadata, validated_json_copy
from aegis_alpha.metadata.schema import source_snapshots

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.engine import RowMapping


class CollectionConflictError(RuntimeError):
    """An immutable collection identity already has a different projection."""


class CollectionStateError(RuntimeError):
    """A lifecycle transition, sequence, or watermark advance is not legal."""


#: A caller-supplied invariant re-checked inside the writing transaction.
#:
#: The callable runs after the rows are written but before COMMIT, so raising
#: from it rolls the whole unit of work back. This lets a caller bind an
#: external precondition, such as published-artifact identity, to the durable
#: write itself instead of checking it around an already-committed transaction.
PrecommitGuard = Callable[[], None]


def _require_same_engine(owner: Engine, connection: Connection) -> None:
    """Fail closed when a caller's transaction belongs to another engine."""

    if connection.engine is not owner:
        raise ValueError(
            "registry operations in one unit of work must share a single PostgreSQL engine"
        )


def _event_values(event: CollectionRunEvent) -> dict[str, object]:
    values: dict[str, object] = {
        "run_id": event.run_id,
        "event_type": event.event_type.value,
        "attempt_number": event.attempt_number,
        "retry_of_attempt": event.retry_of_attempt,
        "error_class": event.error_class,
        "error_message": event.error_message,
        "occurred_at_utc": event.occurred_at_utc,
        "details_json": _json_write_copy(event.details),
    }
    if event.error_message is not None:
        reject_credential_metadata(event.error_message)
    return values


class CollectionRegistry:
    def __init__(self, engine: Engine) -> None:
        if engine.dialect.name != "postgresql":
            raise ValueError("collection registry requires PostgreSQL")
        self._engine = engine

    @property
    def engine(self) -> Engine:
        return self._engine

    def begin_registration(self) -> AbstractContextManager[Connection]:
        """Open one transaction that several registry writes can share.

        The caller passes the yielded connection to each participating write, so
        the whole sequence commits once or rolls back together.
        """

        return self._engine.begin()

    @contextmanager
    def _transaction(self, connection: Connection | None) -> Iterator[Connection]:
        """Join a caller's transaction, or own one when none was supplied."""

        if connection is not None:
            _require_same_engine(self._engine, connection)
            # Deliberately no commit: the owning unit of work decides.
            yield connection
            return
        with self._engine.begin() as owned:
            yield owned

    def register_plan(
        self,
        plan: CollectionRunPlan,
        *,
        connection: Connection | None = None,
    ) -> None:
        thawed_parameters = _json_write_copy(plan.parameters)
        revalidated = replace(plan, parameters=thawed_parameters)
        if revalidated.plan_sha256 != plan.plan_sha256:
            raise CollectionConflictError("plan digest does not match its revalidated projection")
        parent = {
            "plan_id": revalidated.plan_id,
            "schema_version": revalidated.schema_version,
            "provider": revalidated.provider,
            "dataset": revalidated.dataset,
            "mode": revalidated.mode.value,
            "requested_window_start": revalidated.requested_window_start,
            "requested_window_end": revalidated.requested_window_end,
            "parameters_json": thawed_parameters,
            "plan_sha256": revalidated.plan_sha256,
            "created_at_utc": revalidated.created_at_utc,
        }
        try:
            with self._transaction(connection) as active:
                active.execute(
                    postgres_insert(collection_run_plans)
                    .values(parent)
                    .on_conflict_do_nothing(index_elements=[collection_run_plans.c.plan_id])
                    .returning(collection_run_plans.c.plan_id)
                ).scalar_one_or_none()
                observed = (
                    active.execute(
                        select(collection_run_plans)
                        .where(collection_run_plans.c.plan_id == plan.plan_id)
                        .with_for_update()
                    )
                    .mappings()
                    .one()
                )
                if not _projection_matches(observed, parent):
                    raise CollectionConflictError(
                        "plan identity has a different immutable projection"
                    )
        except IntegrityError as error:
            raise CollectionConflictError(
                "plan identity conflicts with immutable evidence"
            ) from error

    def start_run(self, run: CollectionRun) -> None:
        """Idempotently create one immutable run in its own transaction."""

        try:
            with self._engine.begin() as connection:
                self.create_or_observe_run(run, connection=connection)
        except IntegrityError as error:
            raise CollectionConflictError(
                "run identity conflicts with immutable evidence"
            ) from error

    def create_or_observe_run(
        self,
        run: CollectionRun,
        *,
        connection: Connection,
    ) -> bool:
        """Atomically create ``run`` or prove that its immutable row existed.

        The transaction-scoped advisory lock serializes cooperating run
        creators even while the row is absent.  The locked read then protects
        subsequent event writers when it already exists.  The boolean is true
        only when *this* transaction inserted the row, so callers must not use
        an earlier absence observation to classify a damaged existing run as a
        fresh one.
        """

        parent = {
            "run_id": run.run_id,
            "plan_id": run.plan_id,
            "created_at_utc": run.created_at_utc,
        }
        _require_same_engine(self._engine, connection)
        self._lock_run_creation(connection, run.run_id)
        plan_exists = connection.scalar(
            select(func.count())
            .select_from(collection_run_plans)
            .where(collection_run_plans.c.plan_id == run.plan_id)
        )
        if not plan_exists:
            raise ValueError("unknown plan_id for collection run")
        created = (
            connection.execute(
                postgres_insert(collection_runs)
                .values(parent)
                .on_conflict_do_nothing(index_elements=[collection_runs.c.run_id])
                .returning(collection_runs.c.run_id)
            ).scalar_one_or_none()
            is not None
        )
        observed = (
            connection.execute(
                select(collection_runs)
                .where(collection_runs.c.run_id == run.run_id)
                .with_for_update()
            )
            .mappings()
            .one()
        )
        if not _projection_matches(observed, parent):
            raise CollectionConflictError("run identity has a different immutable projection")
        return created

    def append_event(
        self,
        event: CollectionRunEvent,
        *,
        precommit_guard: PrecommitGuard | None = None,
        connection: Connection | None = None,
    ) -> int:
        values = _event_values(event)
        try:
            with self._transaction(connection) as active:
                run_row = (
                    active.execute(
                        select(collection_runs.c.run_id)
                        .where(collection_runs.c.run_id == event.run_id)
                        .with_for_update()
                    )
                    .mappings()
                    .one_or_none()
                )
                if run_row is None:
                    raise ValueError("unknown run_id for collection event")
                last = (
                    active.execute(
                        select(collection_run_events)
                        .where(collection_run_events.c.run_id == event.run_id)
                        .order_by(collection_run_events.c.event_seq.desc())
                        .limit(1)
                    )
                    .mappings()
                    .one_or_none()
                )
                if last is not None and _projection_matches(last, values):
                    if precommit_guard is not None:
                        precommit_guard()
                    return int(last["event_seq"])
                _validate_transition(last, event)
                if last is not None and event.occurred_at_utc < last["occurred_at_utc"]:
                    raise CollectionStateError("events must not move backwards in time")
                next_seq = 1 if last is None else int(last["event_seq"]) + 1
                active.execute(
                    collection_run_events.insert().values({**values, "event_seq": next_seq})
                )
                if precommit_guard is not None:
                    precommit_guard()
                return next_seq
        except IntegrityError as error:
            raise CollectionConflictError(
                "event identity conflicts with immutable evidence"
            ) from error

    def require_event_sequence(
        self,
        events: tuple[CollectionRunEvent, ...],
        *,
        connection: Connection | None = None,
    ) -> None:
        """Fail closed unless one terminal run has exactly these immutable events.

        A current-state projection proves only the final transition.  Terminal
        replay must also prove the original attempt number, timestamps, and
        event details that bind success to the attested publication.
        """

        if not events or len({event.run_id for event in events}) != 1:
            raise ValueError("event sequence must be nonempty and belong to one run")
        run_id = events[0].run_id
        expected = tuple(
            {**_event_values(event), "event_seq": sequence}
            for sequence, event in enumerate(events, start=1)
        )
        with self._transaction(connection) as active:
            observed = tuple(
                active.execute(
                    select(collection_run_events)
                    .where(collection_run_events.c.run_id == run_id)
                    .order_by(collection_run_events.c.event_seq)
                    .with_for_update()
                ).mappings()
            )
            if len(observed) != len(expected) or any(
                not _projection_matches(row, projection)
                for row, projection in zip(observed, expected, strict=True)
            ):
                raise CollectionConflictError("run has a different immutable event sequence")

    def lock_run_lineage(self, run_id: str, *, connection: Connection) -> None:
        """Lock the run/plan lineage in a caller-owned transaction."""

        if _locked_run_lineage(connection, run_id) is None:
            raise ValueError("unknown run_id for collection lineage")

    def current_run_state(
        self,
        run_id: str,
        *,
        connection: Connection | None = None,
    ) -> CollectionRunState | None:
        with self._transaction(connection) as active:
            row = (
                active.execute(
                    text(
                        "SELECT run_id, plan_id, state, terminal, attempt_count, "
                        "last_event_seq, last_occurred_at_utc "
                        "FROM public.collection_run_states WHERE run_id = :run_id"
                    ),
                    {"run_id": run_id},
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        return CollectionRunState(
            run_id=row["run_id"],
            plan_id=row["plan_id"],
            state=None if row["state"] is None else RunEventType(row["state"]),
            terminal=bool(row["terminal"]),
            attempt_count=int(row["attempt_count"]),
            last_event_seq=row["last_event_seq"],
            last_occurred_at_utc=row["last_occurred_at_utc"],
        )

    def run_plan_dataset(self, run_id: str) -> str | None:
        with self._transaction(None) as connection:
            return connection.execute(
                select(collection_run_plans.c.dataset)
                .select_from(
                    collection_runs.join(
                        collection_run_plans,
                        collection_run_plans.c.plan_id == collection_runs.c.plan_id,
                    )
                )
                .where(collection_runs.c.run_id == run_id)
            ).scalar_one_or_none()

    def _lock_run_creation(self, connection: Connection, run_id: str) -> None:
        """Serialize run creation, including the no-row case, for one transaction."""

        connection.execute(
            text(
                "SELECT pg_advisory_xact_lock("
                "hashtextextended('aegis_alpha.collection.run_creation:' || :run_id, 0)"
                ")"
            ),
            {"run_id": run_id},
        )
        # Row locks complement the advisory lock for callers that append or
        # inspect lifecycle events through the normal registry API.
        connection.execute(
            select(collection_runs.c.run_id)
            .where(collection_runs.c.run_id == run_id)
            .with_for_update()
        ).one_or_none()

    def record_receipt(
        self,
        receipt: CollectionReceipt,
        *,
        precommit_guard: PrecommitGuard | None = None,
        connection: Connection | None = None,
    ) -> None:
        values: dict[str, object] = {
            "run_id": receipt.run_id,
            "attempt_number": receipt.attempt_number,
            "source_snapshot_id": receipt.source_snapshot_id,
            "observed_window_start": receipt.observed_window_start,
            "observed_window_end": receipt.observed_window_end,
            "row_count": receipt.row_count,
            "byte_count": receipt.byte_count,
            "receipt_sha256": receipt.receipt_sha256,
        }
        try:
            with self._transaction(connection) as active:
                run_lineage = _locked_run_lineage(active, receipt.run_id)
                if run_lineage is None:
                    raise ValueError("unknown run_id for collection receipt")
                snapshot_lineage = (
                    active.execute(
                        select(source_snapshots.c.provider, source_snapshots.c.dataset)
                        .where(source_snapshots.c.snapshot_id == receipt.source_snapshot_id)
                        .with_for_update()
                    )
                    .mappings()
                    .one_or_none()
                )
                if snapshot_lineage is None:
                    raise ValueError("unknown source_snapshot_id for collection receipt")
                if (
                    run_lineage["provider"] != snapshot_lineage["provider"]
                    or run_lineage["dataset"] != snapshot_lineage["dataset"]
                ):
                    raise CollectionStateError(
                        "receipt source_snapshot provider/dataset does not match the run plan"
                    )
                attempt_started = active.scalar(
                    select(func.count())
                    .select_from(collection_run_events)
                    .where(
                        (collection_run_events.c.run_id == receipt.run_id)
                        & (collection_run_events.c.attempt_number == receipt.attempt_number)
                        & (collection_run_events.c.event_type == "attempt_started")
                    )
                )
                if not attempt_started:
                    raise CollectionStateError("receipt references an attempt that has not started")
                receipt_where = (
                    (collection_run_receipts.c.run_id == receipt.run_id)
                    & (collection_run_receipts.c.attempt_number == receipt.attempt_number)
                    & (collection_run_receipts.c.source_snapshot_id == receipt.source_snapshot_id)
                )
                active.execute(
                    postgres_insert(collection_run_receipts)
                    .values(values)
                    .on_conflict_do_nothing(
                        index_elements=[
                            collection_run_receipts.c.run_id,
                            collection_run_receipts.c.attempt_number,
                            collection_run_receipts.c.source_snapshot_id,
                        ]
                    )
                )
                observed = (
                    active.execute(
                        select(collection_run_receipts).where(receipt_where).with_for_update()
                    )
                    .mappings()
                    .one()
                )
                if not _projection_matches(observed, values):
                    raise CollectionConflictError(
                        "receipt identity has a different immutable projection"
                    )
                if precommit_guard is not None:
                    precommit_guard()
        except IntegrityError as error:
            raise CollectionConflictError(
                "receipt identity conflicts with immutable evidence"
            ) from error

    def advance_watermark(
        self,
        advance: WatermarkAdvance,
        *,
        connection: Connection | None = None,
    ) -> int:
        """Advance only after the run's terminal ``run_succeeded`` event.

        A checkpoint is a published success boundary, so never-started, active,
        failed, and cancelled runs cannot change it.
        """
        values: dict[str, object] = {
            "provider": advance.provider,
            "dataset": advance.dataset,
            "stream": advance.stream,
            "run_id": advance.run_id,
            "watermark_value": advance.watermark_value,
            "watermark_position": advance.watermark_position,
        }
        try:
            with self._transaction(connection) as active:
                run_lineage = _locked_run_lineage(active, advance.run_id)
                if run_lineage is None:
                    raise ValueError("unknown run_id for watermark advance")
                if run_lineage["provider"] != advance.provider or not _lineage_allows_dataset(
                    run_lineage, advance.dataset
                ):
                    raise CollectionStateError(
                        "watermark provider/dataset does not match the run plan"
                    )
                # All registry paths lock the run row before any secondary
                # stream lock.  Keeping this order prevents a committer that
                # already owns the run row from deadlocking with an
                # independent watermark advance on the same stream.
                self._lock_watermark_stream(
                    active,
                    provider=advance.provider,
                    dataset=advance.dataset,
                    stream=advance.stream,
                )
                latest_event = active.execute(
                    select(collection_run_events.c.event_type)
                    .where(collection_run_events.c.run_id == advance.run_id)
                    .order_by(collection_run_events.c.event_seq.desc())
                    .limit(1)
                ).scalar_one_or_none()
                if latest_event != RunEventType.RUN_SUCCEEDED.value:
                    raise CollectionStateError(
                        "watermark advance requires a terminal run_succeeded event"
                    )
                stream_where = (
                    (collection_watermarks.c.provider == advance.provider)
                    & (collection_watermarks.c.dataset == advance.dataset)
                    & (collection_watermarks.c.stream == advance.stream)
                )
                own = (
                    active.execute(
                        select(collection_watermarks).where(
                            stream_where & (collection_watermarks.c.run_id == advance.run_id)
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if own is not None:
                    if _projection_matches(own, values):
                        return int(own["watermark_seq"])
                    raise CollectionConflictError(
                        "run already advanced this stream with a different projection"
                    )
                latest = (
                    active.execute(
                        select(collection_watermarks)
                        .where(stream_where)
                        .order_by(
                            collection_watermarks.c.watermark_position.desc(),
                            collection_watermarks.c.watermark_seq.desc(),
                        )
                        .limit(1)
                        .with_for_update()
                    )
                    .mappings()
                    .one_or_none()
                )
                if (
                    latest is not None
                    and advance.watermark_position <= (latest["watermark_position"])
                ):
                    raise CollectionStateError(
                        "watermark must advance beyond the current stream position"
                    )
                next_seq = 1 if latest is None else int(latest["watermark_seq"]) + 1
                active.execute(
                    collection_watermarks.insert().values({**values, "watermark_seq": next_seq})
                )
                return next_seq
        except IntegrityError as error:
            raise CollectionConflictError(
                "watermark identity conflicts with immutable evidence"
            ) from error

    def _lock_watermark_stream(
        self,
        connection: Connection,
        *,
        provider: str,
        dataset: str,
        stream: str,
    ) -> None:
        """Serialize writes for one exact provider/dataset/stream tuple.

        Row locks cannot protect the first watermark because no row exists yet.
        A transaction-scoped advisory lock closes that absent-stream race while
        leaving unrelated streams independent.
        """

        key = "aegis_alpha.collection.watermark_stream:" + "".join(
            f"{len(part)}:{part}" for part in (provider, dataset, stream)
        )
        connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": key},
        )

    def latest_watermark(self, provider: str, dataset: str, stream: str) -> CurrentWatermark | None:
        with self._engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT provider, dataset, stream, watermark_seq, run_id, "
                        "watermark_value, watermark_position, recorded_at_utc "
                        "FROM public.collection_watermark_current "
                        "WHERE provider = :provider AND dataset = :dataset AND stream = :stream"
                    ),
                    {"provider": provider, "dataset": dataset, "stream": stream},
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        return CurrentWatermark(
            provider=row["provider"],
            dataset=row["dataset"],
            stream=row["stream"],
            watermark_seq=int(row["watermark_seq"]),
            run_id=row["run_id"],
            watermark_value=row["watermark_value"],
            watermark_position=row["watermark_position"],
            recorded_at_utc=row["recorded_at_utc"],
        )

    def record_usage(
        self,
        record: CollectionUsageRecord,
        *,
        recorded_at_clock: Callable[[], datetime] | None = None,
    ) -> None:
        values: dict[str, object] = {
            "run_id": record.run_id,
            "usage_seq": record.usage_seq,
            "metric": record.metric,
            "quantity": record.quantity,
            "unit": record.unit,
            "evidence_json": _json_write_copy(record.evidence),
            "recorded_at_utc": record.recorded_at_utc,
        }
        try:
            with self._engine.begin() as connection:
                acquire_fmp_usage_checkpoint_lock(connection)
                if recorded_at_clock is not None:
                    values["recorded_at_utc"] = recorded_at_clock().astimezone(UTC)
                run_exists = connection.scalar(
                    select(func.count())
                    .select_from(collection_runs)
                    .where(collection_runs.c.run_id == record.run_id)
                )
                if not run_exists:
                    raise ValueError("unknown run_id for collection usage record")
                usage_where = (collection_usage_records.c.run_id == record.run_id) & (
                    collection_usage_records.c.usage_seq == record.usage_seq
                )
                existing = (
                    connection.execute(
                        select(collection_usage_records).where(usage_where).with_for_update()
                    )
                    .mappings()
                    .one_or_none()
                )
                if existing is None:
                    connection.execute(postgres_insert(collection_usage_records).values(values))
                    existing = (
                        connection.execute(
                            select(collection_usage_records).where(usage_where).with_for_update()
                        )
                        .mappings()
                        .one()
                    )
                comparable = dict(values)
                comparable["recorded_at_utc"] = existing["recorded_at_utc"]
                if not _projection_matches(existing, comparable):
                    raise CollectionConflictError(
                        "usage identity has a different immutable projection"
                    )
                if recorded_at_clock is None and not _projection_matches(existing, values):
                    raise CollectionConflictError(
                        "usage identity has a different immutable projection"
                    )
        except IntegrityError as error:
            raise CollectionConflictError(
                "usage identity conflicts with immutable evidence"
            ) from error

    def run_usage_records(self, run_id: str) -> tuple[CollectionUsageRecord, ...]:
        with self._engine.connect() as connection:
            rows = (
                connection.execute(
                    select(collection_usage_records)
                    .where(collection_usage_records.c.run_id == run_id)
                    .order_by(collection_usage_records.c.usage_seq)
                )
                .mappings()
                .all()
            )
        return tuple(
            CollectionUsageRecord(
                run_id=row["run_id"],
                usage_seq=int(row["usage_seq"]),
                metric=row["metric"],
                quantity=row["quantity"],
                unit=row["unit"],
                evidence=_json_write_copy(row["evidence_json"]),
                recorded_at_utc=row["recorded_at_utc"],
            )
            for row in rows
        )

    def prepare_fmp_usage_checkpoint_candidate(  # noqa: PLR0913 - signed contract fields
        self,
        *,
        checkpoint_id: str,
        coverage_start_utc: datetime,
        coverage_end_utc: datetime,
        authority_id: str,
        key_id: str,
        generated_at_utc: datetime,
    ) -> UnsignedUsageCheckpointCandidate:
        return FmpUsageCheckpointService(self._engine).prepare_candidate(
            checkpoint_id=checkpoint_id,
            coverage_start_utc=coverage_start_utc,
            coverage_end_utc=coverage_end_utc,
            authority_id=authority_id,
            key_id=key_id,
            generated_at_utc=generated_at_utc,
        )

    def verify_fmp_usage_checkpoint(
        self,
        *,
        checkpoint_id: str,
        coverage_start_utc: datetime,
        coverage_end_utc: datetime,
        keyring: Ed25519PublicKeyring,
    ) -> VerifiedProviderUsageSnapshot:
        return FmpUsageCheckpointService(self._engine).verify_current(
            checkpoint_id=checkpoint_id,
            coverage_start_utc=coverage_start_utc,
            coverage_end_utc=coverage_end_utc,
            keyring=keyring,
        )

    def verify_exact_fmp_usage_checkpoints(
        self,
        *,
        windows: Sequence[tuple[datetime, datetime]],
        keyring: Ed25519PublicKeyring,
    ) -> tuple[VerifiedProviderUsageSnapshot, ...]:
        return FmpUsageCheckpointService(self._engine).verify_exact_windows(
            windows=windows,
            keyring=keyring,
        )

    def register_usage_checkpoint(
        self,
        signed: SignedUsageCheckpoint,
        leaves: Sequence[UsageRecordLeaf],
        keyring: Ed25519PublicKeyring,
        *,
        connection: Connection | None = None,
    ) -> None:
        UsageCheckpointRepository(self._engine).register(
            signed, leaves, keyring, connection=connection
        )

    def usage_checkpoint_metadata(
        self,
        checkpoint_id: str,
        keyring: Ed25519PublicKeyring,
        *,
        connection: Connection | None = None,
    ) -> AuthenticatedUsageCheckpointMetadata:
        return UsageCheckpointRepository(self._engine).get_authenticated_metadata(
            checkpoint_id, keyring, connection=connection
        )

    def aggregate_usage(
        self,
        *,
        since_utc: datetime,
        until_utc: datetime,
        run_ids: Sequence[str] | None = None,
    ) -> Mapping[tuple[str, str], Decimal]:
        """Sum usage quantities by ``(metric, unit)`` over ``[since_utc, until_utc)``.

        Read-only: a pure SELECT over ``collection_usage_records``.  The window
        is half-open on ``recorded_at_utc`` so adjacent windows never double
        count.  ``run_ids=None`` aggregates every run; an empty sequence
        matches nothing.
        """

        if since_utc.tzinfo is None or until_utc.tzinfo is None:
            raise ValueError("usage window bounds must be timezone-aware")
        conditions = [
            collection_usage_records.c.recorded_at_utc >= since_utc,
            collection_usage_records.c.recorded_at_utc < until_utc,
        ]
        if run_ids is not None:
            conditions.append(collection_usage_records.c.run_id.in_(run_ids))
        statement = (
            select(
                collection_usage_records.c.metric,
                collection_usage_records.c.unit,
                func.sum(collection_usage_records.c.quantity).label("total_quantity"),
            )
            .where(*conditions)
            .group_by(collection_usage_records.c.metric, collection_usage_records.c.unit)
        )
        with self._engine.connect() as connection:
            rows = connection.execute(statement).mappings().all()
        return {(row["metric"], row["unit"]): row["total_quantity"] for row in rows}


def _projection_matches(observed: RowMapping, expected: Mapping[str, object]) -> bool:
    return _normalize({key: observed[key] for key in expected}) == _normalize(dict(expected))


def _locked_run_lineage(connection: Connection, run_id: str) -> RowMapping | None:
    return (
        connection.execute(
            select(
                collection_runs.c.run_id,
                collection_run_plans.c.provider,
                collection_run_plans.c.dataset,
                collection_run_plans.c.parameters_json.label("parameters"),
            )
            .join(
                collection_run_plans,
                collection_run_plans.c.plan_id == collection_runs.c.plan_id,
            )
            .where(collection_runs.c.run_id == run_id)
            .with_for_update()
        )
        .mappings()
        .one_or_none()
    )


def _lineage_allows_dataset(lineage: RowMapping, dataset: str) -> bool:
    if lineage["dataset"] == dataset:
        return True
    parameters = lineage["parameters"]
    if not isinstance(parameters, Mapping):
        return False
    declared = parameters.get("datasets")
    return (
        isinstance(declared, list)
        and all(isinstance(value, str) for value in declared)
        and dataset in declared
    )


def _json_write_copy(value: object) -> dict[str, object]:
    thawed = validated_json_copy(value)
    if not isinstance(thawed, dict):
        raise TypeError("collection JSON metadata must be an object")
    return {str(key): child for key, child in thawed.items()}


def _normalize(value: object) -> Hashable:
    if isinstance(value, Mapping):
        return tuple(sorted((key, _normalize(child)) for key, child in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_normalize(child) for child in value)
    if isinstance(value, Decimal):
        return value.normalize()
    if not isinstance(value, Hashable):
        raise TypeError("collection projection contains an unhashable value")
    return value


def _validate_transition(last: RowMapping | None, event: CollectionRunEvent) -> None:
    event_type = event.event_type
    if last is None:
        if (
            event_type is RunEventType.ATTEMPT_STARTED
            and event.attempt_number == 1
            and event.retry_of_attempt is None
        ):
            return
        raise CollectionStateError("the first event must start attempt 1")
    last_type = RunEventType(last["event_type"])
    match last_type:
        case RunEventType.ATTEMPT_STARTED:
            if (
                event_type in {RunEventType.ATTEMPT_SUCCEEDED, RunEventType.ATTEMPT_FAILED}
                and event.attempt_number == last["attempt_number"]
            ) or event_type is RunEventType.RUN_CANCELLED:
                return
        case RunEventType.ATTEMPT_FAILED:
            if (
                event_type is RunEventType.ATTEMPT_STARTED
                and event.attempt_number == last["attempt_number"] + 1
                and event.retry_of_attempt == last["attempt_number"]
            ) or event_type in {RunEventType.RUN_FAILED, RunEventType.RUN_CANCELLED}:
                return
        case RunEventType.ATTEMPT_SUCCEEDED:
            if event_type is RunEventType.RUN_SUCCEEDED:
                return
        case RunEventType.RUN_SUCCEEDED | RunEventType.RUN_FAILED | RunEventType.RUN_CANCELLED:
            raise CollectionStateError("a terminal run rejects every further event")
        case _:
            assert_never(last_type)
    raise CollectionStateError(f"illegal lifecycle transition from {last_type} to {event_type}")
