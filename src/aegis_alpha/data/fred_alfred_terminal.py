"""Read-only comparison of terminal FRED evidence to its immutable DB projections."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import Connection, Table, select

from aegis_alpha.collection.records import CollectionRunPlan, CollectionUsageRecord
from aegis_alpha.collection.schema import collection_run_receipts, collection_usage_records
from aegis_alpha.data.contracts import SourceSnapshot
from aegis_alpha.data.fred_alfred_collector import CollectorError, CollectorOutcome
from aegis_alpha.data.fred_alfred_evidence import (
    dataset_registration,
    source_registration,
)
from aegis_alpha.data.fred_alfred_rate_limit import UsageLedger
from aegis_alpha.data.fred_alfred_series import PLAN_DATASET
from aegis_alpha.data.fred_alfred_usage_budget import settled_consumption_matches
from aegis_alpha.metadata.records import DatasetArtifact, DatasetRegistration
from aegis_alpha.metadata.registry import (
    _dataset_parent_values,
    _normalized_rows,
    _projection_matches,
    _source_parent_values,
)
from aegis_alpha.metadata.schema import (
    dataset_artifacts,
    dataset_input_files,
    dataset_sources,
    dataset_versions,
    quality_results,
    source_snapshot_files,
    source_snapshots,
)

if TYPE_CHECKING:
    from aegis_alpha.data.fred_alfred_registration import FredRuntime


@dataclass(frozen=True, slots=True)
class TerminalEvidence:
    """Projections of the exact bytes decoded into the replay result."""

    receipt_sha256: str
    recorded_at: datetime
    artifacts: tuple[DatasetArtifact, ...]


def _rows_match(
    connection: Connection,
    table: Table,
    key: Mapping[str, object],
    expected: Sequence[dict[str, object]],
) -> None:
    columns = [table.c[name] for name in expected[0]] if expected else list(table.c)
    statement = select(*columns).where(*(table.c[name] == value for name, value in key.items()))
    rows = connection.execute(statement).mappings().all()
    if _normalized_rows(rows) != _normalized_rows(expected):
        raise CollectorError(f"FRED persisted {table.name} projection differs")


def verify_persisted_source(
    connection: Connection, root: Path, snapshot: SourceSnapshot, path: Path
) -> None:
    registration = source_registration(root, snapshot, path)
    expected = _source_parent_values(registration)
    row = (
        connection.execute(
            select(source_snapshots).where(source_snapshots.c.snapshot_id == snapshot.snapshot_id)
        )
        .mappings()
        .one_or_none()
    )
    if row is None or not _projection_matches(row, expected):
        raise CollectorError("FRED persisted source snapshot projection differs")
    _rows_match(
        connection,
        source_snapshot_files,
        {"snapshot_id": snapshot.snapshot_id},
        [
            {
                "snapshot_id": snapshot.snapshot_id,
                "relative_path": item.relative_path,
                "size_bytes": item.size_bytes,
                "content_sha256": item.content_sha256,
            }
            for item in registration.files
        ],
    )


def _verify_catalog(
    connection: Connection, catalog: DatasetRegistration | None, run_id: str
) -> None:
    key = {
        "dataset_id": PLAN_DATASET,
        "dataset_version": hashlib.sha256(run_id.encode()).hexdigest(),
    }
    if catalog is None:
        _rows_match(connection, dataset_versions, key, [])
        return
    expected = {**_dataset_parent_values(catalog), "source_run_id": None}
    row = (
        connection.execute(
            select(dataset_versions).where(
                *(dataset_versions.c[name] == value for name, value in key.items())
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None or not _projection_matches(row, expected):
        raise CollectorError("FRED persisted dataset projection differs")
    _rows_match(
        connection,
        dataset_sources,
        key,
        [{**key, "source_snapshot_id": source} for source in catalog.manifest.source_snapshot_ids],
    )
    _rows_match(
        connection,
        dataset_input_files,
        key,
        [
            {
                **key,
                "source_snapshot_id": item.source_snapshot_id,
                "relative_path": item.relative_path,
            }
            for item in catalog.input_files
        ],
    )
    _rows_match(
        connection,
        dataset_artifacts,
        key,
        [
            {
                **key,
                "relative_path": item.relative_path,
                "media_type": item.media_type,
                "size_bytes": item.size_bytes,
                "row_count": item.row_count,
                "content_sha256": item.content_sha256,
                "partition_values_json": dict(item.partition_values),
            }
            for item in catalog.artifacts
        ],
    )
    _rows_match(connection, quality_results, key, [])


def verify_terminal_replay(
    runtime: FredRuntime,
    plan: CollectionRunPlan,
    outcome: CollectorOutcome,
    usage: UsageLedger,
    evidence: TerminalEvidence,
) -> None:
    if runtime.reservation is None or outcome.receipt_path is None:
        raise CollectorError("FRED persisted replay requires reservation and receipt")
    source = runtime.aggregate_source(plan, outcome.run_id, publish=False)
    catalog = dataset_registration(
        runtime.config,
        outcome,
        (*runtime.snapshots, source),
        evidence.recorded_at,
        verified_artifacts=evidence.artifacts,
    )
    with runtime.engine.connect().execution_options(
        isolation_level="REPEATABLE READ", postgresql_readonly=True
    ) as connection:
        state = runtime.registry.current_run_state(outcome.run_id, connection=connection)
        if (
            state is None
            or state.plan_id != plan.plan_id
            or not state.terminal
            or state.state is not outcome.terminal_event
        ):
            raise CollectorError("FRED persisted terminal state differs")
        _verify_catalog(connection, catalog, outcome.run_id)
        _rows_match(
            connection,
            collection_run_receipts,
            {"run_id": outcome.run_id},
            [
                {
                    "run_id": outcome.run_id,
                    "attempt_number": 1,
                    "source_snapshot_id": source.snapshot_id,
                    "observed_window_start": None,
                    "observed_window_end": None,
                    "row_count": len(outcome.rows),
                    "byte_count": usage.bytes_received,
                    "receipt_sha256": evidence.receipt_sha256,
                }
            ],
        )
        source_path = (
            runtime.config.raw_store_root
            / "blobs"
            / "sha256"
            / source.content_sha256[:2]
            / f"{source.content_sha256}.raw"
        )
        verify_persisted_source(connection, runtime.config.raw_store_root, source, source_path)
        record = CollectionUsageRecord(
            outcome.run_id,
            1,
            "calls_attempted",
            Decimal(usage.calls_attempted),
            "call",
            evidence.recorded_at,
        )
        if not settled_consumption_matches(
            runtime.engine, record, runtime.reservation, connection=connection
        ):
            raise CollectorError("FRED persisted settlement projection differs")
        _rows_match(
            connection,
            collection_usage_records,
            {"run_id": outcome.run_id},
            [
                {
                    "run_id": outcome.run_id,
                    "usage_seq": 1,
                    "metric": "calls_attempted",
                    "quantity": Decimal(usage.calls_attempted),
                    "unit": "call",
                    "recorded_at_utc": evidence.recorded_at,
                    "evidence_json": {
                        "source": "fred-alfred-actual-consumption",
                        "reconciles_reservation": runtime.reservation.reservation_run_id,
                    },
                },
                {
                    "run_id": outcome.run_id,
                    "usage_seq": 2,
                    "metric": "bytes_received",
                    "quantity": Decimal(usage.bytes_received),
                    "unit": "byte",
                    "recorded_at_utc": evidence.recorded_at,
                    "evidence_json": {},
                },
            ],
        )
