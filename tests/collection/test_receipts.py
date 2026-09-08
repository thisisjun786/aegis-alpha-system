from __future__ import annotations

from datetime import UTC, datetime, timedelta

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
from aegis_alpha.collection.registry import (
    CollectionConflictError,
    CollectionRegistry,
    CollectionStateError,
)
from aegis_alpha.collection.schema import collection_run_receipts
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
_EXPECTED_ROW_COUNT = 18_672
_EXPECTED_BYTE_COUNT = 593_482


def _register_source_snapshot(
    clean_postgres: Engine,
    snapshot_id: str,
    *,
    provider: str = "fmp",
    dataset: str = "eod_prices",
) -> None:
    captured_at = datetime(2026, 7, 29, 8, 1, 10, tzinfo=UTC)
    snapshot = SourceSnapshot(
        snapshot_id=snapshot_id,
        schema_version=1,
        provider=provider,
        dataset=dataset,
        source_uri=f"https://provider.invalid/v1/prices/{snapshot_id}",
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


def _started_run(
    collection_registry: CollectionRegistry,
    run_id: str,
    *,
    provider: str = "fmp",
    dataset: str = "eod_prices",
) -> CollectionRun:
    plan = CollectionRunPlan(
        plan_id=f"plan-{run_id}",
        schema_version=1,
        provider=provider,
        dataset=dataset,
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


def _receipt(run_id: str, **overrides: object) -> CollectionReceipt:
    values: dict[str, object] = {
        "run_id": run_id,
        "attempt_number": 1,
        "source_snapshot_id": "snapshot-2026-07-29-a",
        "observed_window_start": _OBSERVED_START,
        "observed_window_end": _OBSERVED_END,
        "row_count": _EXPECTED_ROW_COUNT,
        "byte_count": _EXPECTED_BYTE_COUNT,
        "receipt_sha256": "7" * 64,
    }
    values.update(overrides)
    return CollectionReceipt(**values)


def test_record_receipt_links_raw_evidence_to_the_attempt(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    _register_source_snapshot(clean_postgres, "snapshot-2026-07-29-a")
    run = _started_run(collection_registry, "run-rc-1")

    collection_registry.record_receipt(_receipt(run.run_id))

    with clean_postgres.connect() as connection:
        row = connection.execute(select(collection_run_receipts)).mappings().one()
    assert row["source_snapshot_id"] == "snapshot-2026-07-29-a"
    assert row["observed_window_end"] == _OBSERVED_END
    assert row["row_count"] == _EXPECTED_ROW_COUNT
    assert row["byte_count"] == _EXPECTED_BYTE_COUNT


def test_receipt_rejects_unregistered_source_snapshot(
    collection_registry: CollectionRegistry,
) -> None:
    run = _started_run(collection_registry, "run-rc-1")

    with pytest.raises(ValueError, match="unknown source_snapshot"):
        collection_registry.record_receipt(_receipt(run.run_id))


def test_receipt_rejects_source_snapshot_from_a_different_provider_or_dataset(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    _register_source_snapshot(
        clean_postgres,
        "snapshot-other-lineage",
        provider="other-provider",
        dataset="other-dataset",
    )
    run = _started_run(collection_registry, "run-rc-lineage")

    with pytest.raises(CollectionStateError, match="provider/dataset"):
        collection_registry.record_receipt(
            _receipt(run.run_id, source_snapshot_id="snapshot-other-lineage")
        )


def test_receipt_requires_the_referenced_attempt_to_have_started(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    _register_source_snapshot(clean_postgres, "snapshot-2026-07-29-a")
    plan = CollectionRunPlan(
        plan_id="plan-run-rc-2",
        schema_version=1,
        provider="fmp",
        dataset="eod_prices_run_rc_2",
        mode=CollectionMode.PROBE,
        requested_window_start=None,
        requested_window_end=None,
        parameters={},
        created_at_utc=_CREATED_AT,
    )
    collection_registry.register_plan(plan)
    collection_registry.start_run(
        CollectionRun(run_id="run-rc-2", plan_id=plan.plan_id, created_at_utc=_CREATED_AT)
    )

    with pytest.raises(CollectionStateError):
        collection_registry.record_receipt(_receipt("run-rc-2"))


def test_receipt_replay_is_idempotent_and_differences_conflict(
    collection_registry: CollectionRegistry,
    clean_postgres: Engine,
) -> None:
    _register_source_snapshot(clean_postgres, "snapshot-2026-07-29-a")
    run = _started_run(collection_registry, "run-rc-1")
    receipt = _receipt(run.run_id)

    collection_registry.record_receipt(receipt)
    collection_registry.record_receipt(receipt)

    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(collection_run_receipts)) == 1
    with pytest.raises(CollectionConflictError):
        collection_registry.record_receipt(_receipt(run.run_id, row_count=1))


def test_receipt_record_validation() -> None:
    with pytest.raises(ValueError, match="attempt_number"):
        _receipt("run-x", attempt_number=0)
    with pytest.raises(ValueError, match="source_snapshot_id"):
        _receipt("run-x", source_snapshot_id=" ")
    with pytest.raises(ValueError, match="row_count"):
        _receipt("run-x", row_count=-1)
    with pytest.raises(ValueError, match="byte_count"):
        _receipt("run-x", byte_count=-5)
    with pytest.raises(ValueError, match="window"):
        _receipt("run-x", observed_window_start=_OBSERVED_END + timedelta(days=1))
    with pytest.raises(ValueError, match="window"):
        _receipt("run-x", observed_window_end=None)
    with pytest.raises(ValueError, match="receipt_sha256"):
        _receipt("run-x", receipt_sha256="not-a-sha")
