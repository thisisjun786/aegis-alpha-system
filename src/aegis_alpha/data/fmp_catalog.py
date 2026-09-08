"""Atomic FMP catalog completion and credential-free recovery of successful runs."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import cast

from sqlalchemy import Connection, Engine, exists, func, literal, select, tuple_

from aegis_alpha.collection.records import CollectionMode, CollectionRunPlan, RunEventType
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.collection.schema import (
    collection_run_events,
    collection_run_plans,
    collection_run_receipts,
    collection_runs,
)
from aegis_alpha.data.fmp_catalog_evidence import completed_evidence
from aegis_alpha.data.fmp_catalog_io import (
    CatalogFiles,
    FmpCatalogError,
    identifier,
    object_value,
    require,
)
from aegis_alpha.data.fmp_catalog_records import (
    CONTRACT,
    collection_receipt,
    dataset_registrations,
    source_registration,
)
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.metadata.registry import MetadataRegistry

COLLECTION_DATASETS = frozenset(selection.plan_dataset for selection in DatasetSelection)
MAX_RECOVERY_RUNS = 1000


class FmpCatalogPendingError(RuntimeError):
    """Collection is successful; catalog completion must be retried without HTTP."""

    def __init__(self, run_id: str, cause: Exception) -> None:
        super().__init__("FMP collection succeeded; catalog registration is pending")
        self.run_id = run_id
        self.error_type = type(cause).__name__

    def report(self) -> dict[str, object]:
        return {
            "status": "catalog_pending",
            "collection_status": "collection_succeeded",
            "catalog_status": "catalog_pending",
            "run_id": self.run_id,
            "terminal_event": "run_succeeded",
            "error_class": self.error_type,
        }


def _run(
    registry: CollectionRegistry,
    connection: Connection,
    run_id: str,
) -> tuple[CollectionRunPlan, int, datetime]:
    state = registry.current_run_state(run_id, connection=connection)
    require(
        state is not None and state.state is RunEventType.RUN_SUCCEEDED,
        "FMP catalog requires a successful collection run",
    )
    row = (
        connection.execute(
            select(collection_run_plans)
            .join(
                collection_runs,
                collection_runs.c.plan_id == collection_run_plans.c.plan_id,
            )
            .where(collection_runs.c.run_id == run_id)
        )
        .mappings()
        .one()
    )
    require(
        row["provider"] == "fmp" and row["dataset"] in COLLECTION_DATASETS,
        "FMP catalog refuses foreign provider or non-collection lineage",
    )
    plan = CollectionRunPlan(
        plan_id=row["plan_id"],
        schema_version=row["schema_version"],
        provider=row["provider"],
        dataset=row["dataset"],
        mode=CollectionMode(row["mode"]),
        requested_window_start=row["requested_window_start"],
        requested_window_end=row["requested_window_end"],
        parameters=object_value(row["parameters_json"]),
        created_at_utc=row["created_at_utc"],
    )
    require(plan.plan_sha256 == row["plan_sha256"], "FMP persisted plan hash mismatch")
    attempt = connection.scalar(
        select(collection_run_events.c.attempt_number)
        .where(
            collection_run_events.c.run_id == run_id,
            collection_run_events.c.event_type == "attempt_succeeded",
        )
        .order_by(collection_run_events.c.event_seq.desc())
        .limit(1)
    )
    finished_at = connection.scalar(
        select(collection_run_events.c.occurred_at_utc)
        .where(
            collection_run_events.c.run_id == run_id,
            collection_run_events.c.event_type == "run_succeeded",
        )
        .order_by(collection_run_events.c.event_seq.desc())
        .limit(1)
    )
    require(
        type(attempt) is int and attempt > 0 and isinstance(finished_at, datetime),
        "FMP successful run lacks its completed attempt",
    )
    return plan, cast("int", attempt), cast("datetime", finished_at)


def register_completed_collection(
    engine: Engine,
    raw_root: Path,
    dataset_root: Path,
    run_id: str,
) -> dict[str, object]:
    """Verify immutable evidence, then atomically register its exact catalog projection.

    No credential, current owner authority, transport, usage write, event or watermark
    update is involved. Replay verifies existing projections instead of replacing them.
    """
    identifier(run_id)
    registry = CollectionRegistry(engine)
    metadata = MetadataRegistry(engine)
    with engine.connect() as connection:
        plan, attempt_number, finished_at = _run(registry, connection, run_id)
    with CatalogFiles(raw_root, dataset_root) as files:
        evidence = completed_evidence(files, plan, run_id)
        source = source_registration(files, evidence, plan, finished_at)
        datasets = dataset_registrations(files, evidence, plan, finished_at)
        receipt = collection_receipt(evidence, attempt_number)
        with registry.begin_registration() as connection:
            registry.lock_run_lineage(run_id, connection=connection)
            require(
                _run(registry, connection, run_id) == (plan, attempt_number, finished_at),
                "FMP run changed during catalog verification",
            )
            metadata.register_source_snapshot(source, connection=connection)
            for dataset in datasets:
                metadata.register_dataset(dataset, connection=connection)
            registry.record_receipt(receipt, connection=connection)
            files.revalidate()
    return {
        "provider": "fmp",
        "run_id": run_id,
        "status": "catalog_complete",
        "collection_status": "collection_succeeded",
        "catalog_status": "catalog_complete",
        "provider_calls": 0,
        "source_snapshot_id": source.snapshot.snapshot_id,
        "datasets": [
            {
                "dataset_id": item.manifest.dataset_id,
                "dataset_version": item.manifest.dataset_version,
                "row_count": item.manifest.row_count,
                "content_sha256": item.manifest.content_sha256,
            }
            for item in datasets
        ],
        "data_eligibility_changed": False,
    }


def recover_completed_collections(
    engine: Engine,
    raw_root: Path,
    dataset_root: Path,
    limit: int = 100,
    *,
    after_run_id: str | None = None,
) -> dict[str, object]:
    """Recover one keyset page; skips advance the cursor, conflicts remain retryable."""
    if type(limit) is not int or not 1 <= limit <= MAX_RECOVERY_RUNS:
        raise FmpCatalogError("FMP catalog recovery limit must be an integer from 1 through 1000")
    if after_run_id is not None:
        require(isinstance(after_run_id, str), "FMP recovery cursor must be a run ID")
        identifier(after_run_id)
    # SQL counterpart of source_id; built-in pg_catalog functions need no extension.
    expected_source = literal(CONTRACT + "-") + func.pg_catalog.encode(
        func.pg_catalog.sha256(func.pg_catalog.convert_to(collection_runs.c.run_id, "UTF8")),
        "hex",
    )
    successful = exists(
        select(collection_run_events.c.run_id).where(
            collection_run_events.c.run_id == collection_runs.c.run_id,
            collection_run_events.c.event_type == "run_succeeded",
        )
    )
    registered = exists(
        select(collection_run_receipts.c.run_id).where(
            collection_run_receipts.c.run_id == collection_runs.c.run_id,
            collection_run_receipts.c.source_snapshot_id == expected_source,
        )
    )
    statement = (
        select(collection_runs.c.run_id)
        .join(
            collection_run_plans,
            collection_run_plans.c.plan_id == collection_runs.c.plan_id,
        )
        .where(
            collection_run_plans.c.provider == "fmp",
            collection_run_plans.c.dataset.in_(tuple(COLLECTION_DATASETS)),
            successful,
            ~registered,
        )
        .order_by(collection_runs.c.created_at_utc, collection_runs.c.run_id.collate("C"))
        .limit(limit + 1)
    )
    with engine.connect() as connection:
        if after_run_id is not None:
            cursor = connection.execute(
                select(collection_runs.c.created_at_utc, collection_runs.c.run_id)
                .join(
                    collection_run_plans,
                    collection_run_plans.c.plan_id == collection_runs.c.plan_id,
                )
                .where(
                    collection_runs.c.run_id == after_run_id,
                    collection_run_plans.c.provider == "fmp",
                    collection_run_plans.c.dataset.in_(tuple(COLLECTION_DATASETS)),
                )
            ).one_or_none()
            if cursor is None:
                raise FmpCatalogError(
                    "FMP recovery cursor must identify an existing FMP collection run"
                )
            statement = statement.where(
                tuple_(collection_runs.c.created_at_utc, collection_runs.c.run_id.collate("C"))
                > tuple_(cursor.created_at_utc, cursor.run_id)
            )
        candidates = tuple(connection.scalars(statement))
    recovered: list[str] = []
    skipped: list[str] = []
    failures: list[dict[str, str]] = []
    next_after_run_id = after_run_id
    with CatalogFiles(raw_root, dataset_root) as files:
        for run_id in candidates[:limit]:
            try:
                identifier(run_id)
                completion = Path("fmp/runs") / run_id / "completion.json"
                if not files.tree(raw_root).exists(completion):
                    skipped.append(run_id)
                    next_after_run_id = run_id
                    continue
                register_completed_collection(engine, raw_root, dataset_root, run_id)
                recovered.append(run_id)
                next_after_run_id = run_id
            except Exception as error:  # noqa: BLE001 -- safe structured boundary; no credential-bearing errors
                failures.append({"run_id": run_id, "error_type": type(error).__name__})
                break
    return {
        "provider": "fmp",
        "provider_calls": 0,
        "status": "catalog_pending" if failures or skipped else "catalog_complete",
        "recovered_run_ids": recovered,
        "skipped_run_ids": skipped,
        "failures": failures,
        "limit": limit,
        "next_after_run_id": next_after_run_id,
        "has_more": bool(failures) or len(candidates) > limit,
        "data_eligibility_changed": False,
    }
