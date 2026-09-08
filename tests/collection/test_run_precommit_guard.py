"""AAS-DATA-006C: precommit guards must roll back real PostgreSQL transactions.

These tests exercise the actual registries against a disposable PostgreSQL
database. A synthetic port double cannot prove transaction semantics, because
the whole point of the finding is that each registry method commits its own
transaction; only a real `COMMIT`/`ROLLBACK` shows whether a guard that raises
inside the unit of work leaves durable state behind.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine, func, select

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionReceipt,
    CollectionRun,
    CollectionRunEvent,
    CollectionRunPlan,
    RunEventType,
)
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import collection_run_events, collection_run_receipts
from aegis_alpha.data.contracts import SourceSnapshot, ValidationStatus
from aegis_alpha.metadata.records import (
    SourceSnapshotFile,
    SourceSnapshotRegistration,
    source_tree_digest,
)
from aegis_alpha.metadata.registry import MetadataRegistry

_CREATED_AT = datetime(2026, 7, 29, 10, 0, tzinfo=UTC)
_OBSERVED_START = datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
_OBSERVED_END = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
_SNAPSHOT_ID = "snapshot-precommit-guard"


class _GuardRejectedError(RuntimeError):
    """Raised by a precommit guard to abort the writing transaction."""


def _register_source_snapshot(clean_postgres: Engine) -> None:
    captured_at = datetime(2026, 7, 29, 8, 1, 10, tzinfo=UTC)
    snapshot = SourceSnapshot(
        snapshot_id=_SNAPSHOT_ID,
        schema_version=1,
        provider="fmp",
        dataset="eod_prices",
        source_uri=f"https://provider.invalid/v1/prices/{_SNAPSHOT_ID}",
        request_fingerprint="sha256:" + "b" * 64,
        parameters={"adjustment": "unadjusted"},
        requested_at_utc=captured_at,
        retrieved_at_utc=captured_at,
        content_type="application/json",
        encoding="utf-8",
        compression=None,
        raw_byte_length=100,
        content_sha256="a" * 64,
        parser_name="fmp-eod-json",
        parser_version="1.0.0",
        validation_status=ValidationStatus.PASS,
    )
    files = (SourceSnapshotFile("payload.json", 100, "1" * 64),)
    MetadataRegistry(clean_postgres).register_source_snapshot(
        SourceSnapshotRegistration(
            snapshot=snapshot,
            tree_sha256=source_tree_digest(files),
            files=files,
            manifest={"endpoint": "historical-price-eod/full"},
        )
    )


def _started_run(collection_registry: CollectionRegistry, run_id: str) -> CollectionRun:
    plan = CollectionRunPlan(
        plan_id=f"plan-{run_id}",
        schema_version=1,
        provider="fmp",
        dataset="eod_prices",
        mode=CollectionMode.BACKFILL,
        requested_window_start=_OBSERVED_START,
        requested_window_end=_OBSERVED_END,
        parameters={},
        created_at_utc=_CREATED_AT,
    )
    collection_registry.register_plan(plan)
    run = CollectionRun(run_id=run_id, plan_id=plan.plan_id, created_at_utc=_CREATED_AT)
    collection_registry.start_run(run)
    collection_registry.append_event(
        CollectionRunEvent(
            run_id=run_id,
            event_type=RunEventType.ATTEMPT_STARTED,
            occurred_at_utc=_CREATED_AT,
            attempt_number=1,
        )
    )
    return run


def _receipt(run_id: str) -> CollectionReceipt:
    return CollectionReceipt(
        run_id=run_id,
        attempt_number=1,
        source_snapshot_id=_SNAPSHOT_ID,
        observed_window_start=_OBSERVED_START,
        observed_window_end=_OBSERVED_END,
        row_count=18_672,
        byte_count=593_482,
        receipt_sha256="7" * 64,
    )


def _reject() -> None:
    raise _GuardRejectedError("publication changed after verification")


def _receipt_count(engine: Engine, run_id: str) -> int:
    with engine.connect() as connection:
        return int(
            connection.execute(
                select(func.count())
                .select_from(collection_run_receipts)
                .where(collection_run_receipts.c.run_id == run_id)
            ).scalar_one()
        )


def _event_types(engine: Engine, run_id: str) -> list[str]:
    with engine.connect() as connection:
        return [
            str(value)
            for value in connection.execute(
                select(collection_run_events.c.event_type)
                .where(collection_run_events.c.run_id == run_id)
                .order_by(collection_run_events.c.event_seq)
            )
            .scalars()
            .all()
        ]


def test_receipt_precommit_guard_rolls_back_the_receipt(
    clean_postgres: Engine,
    collection_registry: CollectionRegistry,
) -> None:
    _register_source_snapshot(clean_postgres)
    run = _started_run(collection_registry, "run-guard-receipt")

    with pytest.raises(_GuardRejectedError):
        collection_registry.record_receipt(_receipt(run.run_id), precommit_guard=_reject)

    assert _receipt_count(clean_postgres, run.run_id) == 0


def test_receipt_precommit_guard_commits_when_it_passes(
    clean_postgres: Engine,
    collection_registry: CollectionRegistry,
) -> None:
    _register_source_snapshot(clean_postgres)
    run = _started_run(collection_registry, "run-guard-receipt-ok")

    collection_registry.record_receipt(_receipt(run.run_id), precommit_guard=lambda: None)

    assert _receipt_count(clean_postgres, run.run_id) == 1


@pytest.mark.parametrize(
    "event_type",
    [RunEventType.ATTEMPT_SUCCEEDED, RunEventType.RUN_SUCCEEDED],
)
def test_event_precommit_guard_rolls_back_success_events(
    clean_postgres: Engine,
    collection_registry: CollectionRegistry,
    event_type: RunEventType,
) -> None:
    """A rejected guard must leave no success event durable, terminal included."""

    _register_source_snapshot(clean_postgres)
    run = _started_run(collection_registry, f"run-guard-{event_type.value}")
    if event_type is RunEventType.RUN_SUCCEEDED:
        collection_registry.append_event(
            CollectionRunEvent(
                run_id=run.run_id,
                event_type=RunEventType.ATTEMPT_SUCCEEDED,
                occurred_at_utc=_CREATED_AT,
                attempt_number=1,
            )
        )
    before = _event_types(clean_postgres, run.run_id)

    with pytest.raises(_GuardRejectedError):
        collection_registry.append_event(
            CollectionRunEvent(
                run_id=run.run_id,
                event_type=event_type,
                occurred_at_utc=_CREATED_AT,
                attempt_number=1 if event_type is RunEventType.ATTEMPT_SUCCEEDED else None,
            ),
            precommit_guard=_reject,
        )

    assert _event_types(clean_postgres, run.run_id) == before
    assert event_type.value not in _event_types(clean_postgres, run.run_id)
    state = collection_registry.current_run_state(run.run_id)
    assert state is not None
    assert not state.terminal


def test_replay_precommit_guard_rolls_back_idempotent_terminal_success(
    clean_postgres: Engine,
    collection_registry: CollectionRegistry,
) -> None:
    """The ATTEMPT_SUCCEEDED replay window is guarded as well.

    Replay re-appends `run_succeeded` for an already-successful attempt, so the
    guard must be able to abort that promotion too.
    """

    _register_source_snapshot(clean_postgres)
    run = _started_run(collection_registry, "run-guard-replay")
    collection_registry.append_event(
        CollectionRunEvent(
            run_id=run.run_id,
            event_type=RunEventType.ATTEMPT_SUCCEEDED,
            occurred_at_utc=_CREATED_AT,
            attempt_number=1,
        )
    )

    with pytest.raises(_GuardRejectedError):
        collection_registry.append_event(
            CollectionRunEvent(
                run_id=run.run_id,
                event_type=RunEventType.RUN_SUCCEEDED,
                occurred_at_utc=_CREATED_AT,
            ),
            precommit_guard=_reject,
        )

    assert RunEventType.RUN_SUCCEEDED.value not in _event_types(clean_postgres, run.run_id)
    state = collection_registry.current_run_state(run.run_id)
    assert state is not None
    assert state.state is RunEventType.ATTEMPT_SUCCEEDED
    assert not state.terminal


def test_duplicate_event_replay_still_honors_the_precommit_guard(
    clean_postgres: Engine,
    collection_registry: CollectionRegistry,
) -> None:
    """An idempotent no-op replay must not bypass the invariant."""

    _register_source_snapshot(clean_postgres)
    run = _started_run(collection_registry, "run-guard-duplicate")
    event = CollectionRunEvent(
        run_id=run.run_id,
        event_type=RunEventType.ATTEMPT_SUCCEEDED,
        occurred_at_utc=_CREATED_AT,
        attempt_number=1,
    )
    collection_registry.append_event(event)

    with pytest.raises(_GuardRejectedError):
        collection_registry.append_event(event, precommit_guard=_reject)

    assert _event_types(clean_postgres, run.run_id) == ["attempt_started", "attempt_succeeded"]
