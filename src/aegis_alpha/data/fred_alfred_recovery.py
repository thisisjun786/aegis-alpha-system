"""Verify immutable FRED completion evidence before zero-HTTP finalization."""

from __future__ import annotations

import hashlib
import io
import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pyarrow.parquet as pq

from aegis_alpha.collection.records import RunEventType
from aegis_alpha.data.contracts import SourceSnapshot, ValidationStatus
from aegis_alpha.data.fred_alfred_collector import CollectorError, CollectorOutcome, SeriesOutcome
from aegis_alpha.data.fred_alfred_evidence import read_evidence, source_provenance_path
from aegis_alpha.data.fred_alfred_rate_limit import UsageLedger
from aegis_alpha.data.fred_alfred_series import PROVIDER
from aegis_alpha.data.fred_alfred_terminal import (
    TerminalEvidence,
    verify_persisted_source,
    verify_terminal_replay,
)
from aegis_alpha.data.fred_alfred_usage_budget import DailyUsageBudget
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.metadata.records import DatasetArtifact

if TYPE_CHECKING:
    from aegis_alpha.data.fred_alfred_registration import FredRuntime


def recover_runtime(runtime: FredRuntime) -> CollectorOutcome:
    admission = json.loads(read_evidence(runtime.admission_path))
    ready = json.loads(read_evidence(runtime.ready_path))
    receipt_bytes = read_evidence(runtime.config.receipt_path)
    if (
        admission["config_sha256"] != runtime.config_digest
        or ready["config_sha256"] != runtime.config_digest
    ):
        raise CollectorError("FRED recovery configuration differs")
    if hashlib.sha256(receipt_bytes).hexdigest() != ready["receipt_sha256"]:
        raise CollectorError("FRED recovery receipt bytes differ")
    receipt = json.loads(receipt_bytes)
    plan = runtime.plan(datetime.fromisoformat(admission["created_at_utc"]))
    if (
        receipt["run_id"] != runtime.config.run_identity
        or admission["run_id"] != runtime.config.run_identity
        or receipt["plan_id"] != plan.plan_id
        or admission["plan_id"] != plan.plan_id
    ):
        raise CollectorError("FRED recovery identity differs")
    reservation = admission["reservation"]
    runtime.reservation = DailyUsageBudget(
        day_start_utc=datetime.fromisoformat(reservation["day_start_utc"]),
        day_end_utc=datetime.fromisoformat(reservation["day_end_utc"]),
        calls_used=reservation["calls_used"],
        calls_per_day=reservation["calls_per_day"],
        remaining_calls=reservation["remaining_calls"],
        reservation_run_id=reservation["reservation_run_id"],
        reserved_calls=reservation["reserved_calls"],
    )
    for value in receipt["source_snapshots"]:
        snapshot = _decode_snapshot(value)
        if snapshot.provider != PROVIDER or snapshot.dataset not in {
            f"series:{series}" for series in runtime.config.series_ids
        }:
            raise CollectorError("FRED recovery source scope differs")
        path = (
            runtime.config.raw_store_root
            / "blobs"
            / "sha256"
            / snapshot.content_sha256[:2]
            / f"{snapshot.content_sha256}.raw"
        )
        if read_evidence(
            source_provenance_path(runtime.config.raw_store_root, snapshot.snapshot_id)
        ) != canonical_json_bytes(snapshot):
            raise CollectorError("FRED recovery source provenance differs")
        with runtime.engine.connect().execution_options(postgresql_readonly=True) as connection:
            verify_persisted_source(connection, runtime.config.raw_store_root, snapshot, path)
        runtime.snapshots.append(snapshot)
    rows, paths, verified_artifacts = _artifact_projection(runtime.config.dataset_root, receipt)
    outcomes = tuple(
        SeriesOutcome(
            item["series_id"],
            item["succeeded"],
            None
            if item["vintage_watermark"] is None
            else date.fromisoformat(item["vintage_watermark"]),
            item["row_count"],
            item["error_class"],
        )
        for item in receipt["series_outcomes"]
    )
    outcome = CollectorOutcome(
        runtime.config.run_identity,
        plan.plan_id,
        RunEventType.RUN_SUCCEEDED
        if any(item.succeeded for item in outcomes)
        else RunEventType.RUN_FAILED,
        (*paths, runtime.config.receipt_path),
        runtime.config.receipt_path,
        tuple(rows),
        outcomes,
        (),
    )
    usage = UsageLedger(**receipt["usage"])
    state = runtime.registry.current_run_state(outcome.run_id)
    if state is not None and state.terminal:
        verify_terminal_replay(
            runtime,
            plan,
            outcome,
            usage,
            TerminalEvidence(
                hashlib.sha256(receipt_bytes).hexdigest(),
                datetime.fromisoformat(receipt["collected_at_utc"]),
                verified_artifacts,
            ),
        )
        return replace(outcome, calls_attempted=usage.calls_attempted, recovered=True)
    return replace(runtime.finalize(plan, outcome, usage), recovered=True)


def _artifact_projection(
    root: Path, receipt: Mapping[str, Any]
) -> tuple[list[dict[str, object]], list[Path], tuple[DatasetArtifact, ...]]:
    rows = []
    paths = []
    verified_artifacts: list[DatasetArtifact] = []
    declared_paths = receipt["published_paths"]
    artifacts = receipt["published_artifacts"]
    if len(set(declared_paths)) != len(declared_paths) or set(declared_paths) != set(artifacts):
        raise CollectorError("FRED recovery artifact inventory differs")
    for value in declared_paths:
        digest = artifacts[value]
        path = Path(value)
        if not path.is_relative_to(root) or ".." in path.parts:
            raise CollectorError("FRED recovery artifact escapes configured root")
        payload = read_evidence(path)
        if hashlib.sha256(payload).hexdigest() != digest:
            raise CollectorError("FRED recovery artifact bytes differ")
        parquet = pq.ParquetFile(io.BytesIO(payload))
        verified_artifacts.append(
            DatasetArtifact(
                relative_path=str(path.relative_to(root)),
                media_type="application/vnd.apache.parquet",
                size_bytes=len(payload),
                row_count=parquet.metadata.num_rows,
                content_sha256=digest,
                partition_values={
                    key: value
                    for part in path.parts
                    if "=" in part
                    for key, value in [part.split("=", 1)]
                },
            )
        )
        if "series_dimension" not in path.parts:
            rows.extend(parquet.read().to_pylist())
        paths.append(path)
    return rows, paths, tuple(verified_artifacts)


def _decode_snapshot(document: Mapping[str, object]) -> SourceSnapshot:
    # SourceSnapshot validates the exact persisted value contract after JSON timestamp decoding.
    values = dict(document)
    for key in ("requested_at_utc", "retrieved_at_utc", "provider_published_at"):
        if values.get(key) is not None:
            values[key] = datetime.fromisoformat(str(values[key]))
    values["validation_status"] = ValidationStatus(str(values["validation_status"]))
    snapshot = SourceSnapshot(**cast("dict[str, Any]", values))
    if canonical_json_bytes(snapshot) != canonical_json_bytes(document):
        raise CollectorError("FRED recovery source schema differs")
    return snapshot
