"""Durable FMP state and consistency checks within the local-owner boundary."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from datetime import date, datetime
from pathlib import Path
from typing import cast

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRunPlan,
    CollectionRunState,
    RunEventType,
)
from aegis_alpha.data.fmp_collector import FmpCollector, PlanResumeRecord, read_plan_resume_record
from aegis_alpha.data.fmp_file_hash import sha256_file as _sha256_file
from aegis_alpha.data.fmp_rate_limit import UsageLedger
from aegis_alpha.data.fmp_receipt_metadata import validate_receipt_metadata_chunks
from aegis_alpha.data.serialization import canonical_json_bytes


def _attempt_digest(collector: FmpCollector, run_id: str) -> str:
    root = collector.config.raw_store_root / "fmp" / "runs" / run_id / "attempts"
    previous = "0" * 64
    paths = sorted(root.glob("*.json")) if root.is_dir() else ()
    for expected, path in enumerate(paths, start=1):
        payload = path.read_bytes()
        record = json.loads(payload)
        if (
            payload != canonical_json_bytes(record)
            or record.get("attempt_seq") != expected
            or record.get("run_identity") != run_id
            or record.get("previous_attempt_sha256") != previous
        ):
            raise ValueError("published run attempt ledger is invalid")
        previous = hashlib.sha256(payload).hexdigest()
    return previous


def _validate_artifact_consistency(
    collector: FmpCollector, path: Path, run_id: str, plan_id: str
) -> None:
    artifacts = _validate_complete_artifact_inventory(collector, run_id, plan_id)
    expected = artifacts.get(str(path))
    if expected is None:
        raise ValueError("normalized artifact is absent from its publication marker")


def _validate_complete_artifact_inventory(
    collector: FmpCollector,
    run_id: str,
    plan_id: str,
) -> Mapping[str, str]:
    artifacts = _validated_run_artifacts(collector, run_id, plan_id)
    if run_id in collector._historical_validated_runs:  # noqa: SLF001
        return artifacts
    for value, expected in artifacts.items():
        path = Path(value)
        if not path.is_file() or _sha256_file(path) != expected:
            raise ValueError("successful run publication marker inventory is incomplete")
    collector._historical_validated_runs.add(run_id)  # noqa: SLF001
    return artifacts


def _validated_run_artifacts(
    collector: FmpCollector,
    run_id: str,
    plan_id: str,
) -> Mapping[str, str]:
    cached = collector._historical_run_validation_cache.get(run_id)  # noqa: SLF001
    if cached is not None:
        cached_plan_id, artifacts = cached
        if cached_plan_id != plan_id:
            raise ValueError("cached normalized run plan identity changed")
        return artifacts
    marker = marker_path(collector, run_id)
    completion = marker.parent / "completion.json"
    if not marker.is_file() or not completion.is_file():
        raise ValueError("successful normalized run lacks durable publication binding")
    marker_bytes = marker.read_bytes()
    marker_document = json.loads(marker_bytes)
    completion_bytes = completion.read_bytes()
    completion_document = json.loads(completion_bytes)
    if completion_bytes != canonical_json_bytes(completion_document):
        raise ValueError("run completion binding is not canonical")
    if (
        marker_document.get("attempt_ledger_sha256") != _attempt_digest(collector, run_id)
        or marker_document.get("plan_id") != plan_id
        or completion_document.get("plan_id") != plan_id
        or completion_document.get("run_id") != run_id
        or completion_document.get("marker_sha256") != hashlib.sha256(marker_bytes).hexdigest()
    ):
        raise ValueError("run completion binding is invalid")
    receipt_path = Path(str(completion_document["receipt_path"]))
    receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes)
    if (
        receipt.get("run_id") != run_id
        or receipt.get("plan_id") != plan_id
        or receipt.get("publication_marker_sha256") != completion_document.get("marker_sha256")
        or hashlib.sha256(receipt_bytes).hexdigest() != completion_document.get("receipt_sha256")
    ):
        raise ValueError("run receipt binding is invalid")
    validate_receipt_metadata_chunks(
        collector.config.raw_store_root,
        str(receipt["run_identity"]),
        receipt.get("request_metadata_chunk_addresses"),
    )
    artifact_rows = cast("list[dict[str, str]]", marker_document["artifacts"])
    artifacts = {item["path"]: item["sha256"] for item in artifact_rows}
    if len(artifacts) != len(artifact_rows):
        raise ValueError("normalized publication marker has duplicate artifact paths")
    collector._historical_run_validation_cache[run_id] = (  # noqa: SLF001
        plan_id,
        artifacts,
    )
    return artifacts


def marker_path(collector: FmpCollector, run_id: str) -> Path:
    return collector.config.raw_store_root / "fmp" / "runs" / run_id / "publication.json"


def normalized_parts_exist(collector: FmpCollector, run_id: str) -> bool:
    root = collector.config.dataset_root / "normalized" / "fmp"
    return root.is_dir() and any(root.glob(f".runs/run_id={run_id}/*/part-*.parquet"))


def read_marker(collector: FmpCollector, run_id: str) -> dict[str, object] | None:
    path = marker_path(collector, run_id)
    if not path.is_file():
        return None
    document = json.loads(path.read_bytes())
    if (
        document.get("run_id") != run_id
        or document.get("attempt_ledger_sha256") != collector.attempt_ledger_sha256
        or UsageLedger(**cast("Mapping[str, int]", document["usage"]))
        != collector._limiter.ledger()  # noqa: SLF001
    ):
        raise ValueError("publication marker identity, attempts, or quantities are invalid")
    for artifact in cast("list[dict[str, str]]", document["artifacts"]):
        artifact_path = Path(artifact["path"])
        if _sha256_file(artifact_path) != artifact["sha256"]:
            raise ValueError("published normalized artifact does not match its marker")
    return cast("dict[str, object]", document)


def marker_ledger(document: Mapping[str, object]) -> UsageLedger:
    return UsageLedger(**cast("Mapping[str, int]", document["usage"]))


def marker_advances(document: Mapping[str, object]) -> tuple[tuple[str, str, date], ...]:
    return tuple(
        (str(item["dataset"]), str(item["stream"]), date.fromisoformat(str(item["value"])))
        for item in cast("list[dict[str, str]]", document["advances"])
    )


def _validate_resume(record: PlanResumeRecord, plan: CollectionRunPlan) -> str:
    if (
        record.plan_id != plan.plan_id
        or record.plan_sha256 != plan.plan_sha256
        or record.provider != plan.provider
        or record.dataset != plan.dataset
        or record.mode != plan.mode.value
        or canonical_json_bytes(dict(record.parameters))
        != canonical_json_bytes(dict(plan.parameters))
        or record.requested_window_start
        != (
            None if plan.requested_window_start is None else plan.requested_window_start.isoformat()
        )
        or record.requested_window_end
        != (None if plan.requested_window_end is None else plan.requested_window_end.isoformat())
        or record.run_id is None
    ):
        raise ValueError("plan resume record does not match immutable run inputs")
    return record.run_id


def watermarks_complete(collector: FmpCollector, marker: Path, run_id: str) -> bool:
    document = cast("Mapping[str, object]", json.loads(marker.read_bytes()))
    for dataset, stream, _value in marker_advances(document):
        current = collector._control_plane.latest_watermark(  # noqa: SLF001
            "fmp", dataset, stream
        )
        if current is None or current.run_id != run_id:
            return False
    return True


def _record_plan(record: PlanResumeRecord) -> CollectionRunPlan:
    return CollectionRunPlan(
        plan_id=record.plan_id,
        schema_version=record.schema_version,
        provider=record.provider,
        dataset=record.dataset,
        mode=CollectionMode(record.mode),
        requested_window_start=(
            None
            if record.requested_window_start is None
            else datetime.fromisoformat(record.requested_window_start)
        ),
        requested_window_end=(
            None
            if record.requested_window_end is None
            else datetime.fromisoformat(record.requested_window_end)
        ),
        parameters=record.parameters,
        created_at_utc=datetime.fromisoformat(record.created_at_utc),
    )


def resolve_resume(
    collector: FmpCollector,
    plan: CollectionRunPlan,
    candidate: str,
    *,
    successful_recovery_validator: Callable[
        [FmpCollector, CollectionRunPlan, str, CollectionRunState], bool
    ]
    | None = None,
    reuse_successful_candidate: bool = False,
) -> tuple[str, CollectionRunState | None, CollectionRunPlan]:
    root = collector.config.raw_store_root / "fmp" / "plans"
    paths = sorted(root.glob(f"{plan.plan_id}-*.json")) if root.is_dir() else ()
    for path in paths:
        record = read_plan_resume_record(path)
        if record is None:
            continue
        run_id = _validate_resume(record, plan)
        state = collector._control_plane.current_run_state(run_id)  # noqa: SLF001
        if state is None:
            return run_id, None, _record_plan(record)
        if not state.terminal:
            return run_id, state, _record_plan(record)
        if state is not None and state.state is RunEventType.RUN_SUCCEEDED:
            marker = marker_path(collector, run_id)
            recovery_pending = (
                False
                if successful_recovery_validator is None
                else successful_recovery_validator(collector, _record_plan(record), run_id, state)
            )
            if marker.is_file() and (
                (reuse_successful_candidate and run_id == candidate)
                or recovery_pending
                or not watermarks_complete(collector, marker, run_id)
            ):
                return run_id, state, _record_plan(record)
    return candidate, None, plan
