"""Verify SEC finalization evidence and atomically admit its terminal DB state."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import select

from aegis_alpha.collection.records import (
    CollectionReceipt,
    CollectionRunEvent,
    CollectionUsageRecord,
    RunEventType,
    WatermarkAdvance,
)
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.data.sec_collector import (
    DATASET,
    CollectorConfig,
    CollectorError,
    CollectorOutcome,
    SkipReceipt,
)
from aegis_alpha.data.sec_evidence import read_bytes, read_document, verify_pin
from aegis_alpha.data.sec_identity import Admission, AdmissionState
from aegis_alpha.data.sec_registration import (
    dataset_registration,
    instant,
    object_list,
    object_mapping,
    verify_sources,
)
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.metadata.records import SourceSnapshotFile
from aegis_alpha.metadata.registry import MetadataRegistry
from aegis_alpha.metadata.schema import source_snapshots

_MARKER_FIELDS = {
    "version",
    "run_id",
    "request_sha256",
    "plan_id",
    "started_at",
    "finished_at",
    "sources",
    "outputs",
    "receipt",
    "usage",
    "watermarks",
    "admissions",
    "skips",
    "calls_attempted",
    "disagreement_count",
    "attempts",
}


@dataclass(frozen=True, slots=True)
class VerifiedMarker:
    """Original canonical bytes checked against the registered run-source hash."""

    payload: bytes
    content_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.payload, bytes):
            raise TypeError("verified SEC marker payload must be immutable bytes")
        if hashlib.sha256(self.payload).hexdigest() != self.content_sha256:
            raise CollectorError("verified SEC marker bytes and hash disagree")

    @property
    def document(self) -> dict[str, object]:
        # A fresh projection cannot mutate the evidence retained across finalization.
        return object_mapping(json.loads(self.payload))

    @property
    def file(self) -> SourceSnapshotFile:
        return SourceSnapshotFile("finalization.json", len(self.payload), self.content_sha256)


def verified_marker(
    registry: CollectionRegistry,
    config: CollectorConfig,
    *,
    run_id: str,
    request_sha256: str,
) -> VerifiedMarker:
    path = config.dataset_root / "finalization.json"
    marker = read_document(path)
    if (
        set(marker) != _MARKER_FIELDS
        or type(marker["version"]) is not int
        or marker["version"] != 1
        or marker["run_id"] != run_id
        or marker["request_sha256"] != request_sha256
    ):
        raise CollectorError("SEC finalization identity/schema differs from this invocation")
    payload = canonical_json_bytes(marker)
    if read_bytes(path) != payload:
        raise CollectorError("SEC finalization changed while reading")
    digest = hashlib.sha256(payload).hexdigest()
    with registry.engine.connect() as connection:
        trusted = connection.execute(
            select(source_snapshots.c.content_sha256).where(
                source_snapshots.c.snapshot_id == f"{run_id}-source",
                source_snapshots.c.provider == "sec",
                source_snapshots.c.dataset == DATASET,
            )
        ).scalar_one_or_none()
    if trusted != digest:
        raise CollectorError("SEC finalization lacks its registered source hash")
    verify_files(config, marker)
    return VerifiedMarker(payload, digest)


def verify_files(config: CollectorConfig, marker: Mapping[str, object]) -> None:
    if (
        hashlib.sha256(read_bytes(config.dataset_root / "request.json")).hexdigest()
        != marker["request_sha256"]
    ):
        raise CollectorError("SEC durable request hash mismatch")
    verify_sources(config, object_list(marker["sources"]))
    for pin in object_list(marker["outputs"]):
        verify_pin(config.dataset_root, pin)
    for pin in object_list(marker["attempts"]):
        verify_pin(config.dataset_root, pin)
    verify_pin(config.receipt_path.parent, object_mapping(marker["receipt"]))


def _events(marker: Mapping[str, object], digest: str) -> tuple[CollectionRunEvent, ...]:
    run_id = str(marker["run_id"])
    return (
        CollectionRunEvent(
            run_id, RunEventType.ATTEMPT_STARTED, instant(marker["started_at"]), attempt_number=1
        ),
        CollectionRunEvent(
            run_id,
            RunEventType.ATTEMPT_SUCCEEDED,
            instant(marker["finished_at"]),
            attempt_number=1,
            details={"sec_finalization_sha256": digest},
        ),
        CollectionRunEvent(
            run_id,
            RunEventType.RUN_SUCCEEDED,
            instant(marker["finished_at"]),
            details={"sec_finalization_sha256": digest},
        ),
    )


def settle_usage(registry: CollectionRegistry, marker: Mapping[str, object]) -> None:
    for item in object_list(marker["usage"]):
        registry.record_usage(
            CollectionUsageRecord(
                run_id=str(marker["run_id"]),
                usage_seq=int(str(item["usage_seq"])),
                metric=str(item["metric"]),
                quantity=Decimal(str(item["quantity"])),
                unit=str(item["unit"]),
                recorded_at_utc=instant(item["recorded_at_utc"]),
            )
        )


def finish_run(
    registry: CollectionRegistry,
    metadata: MetadataRegistry,
    config: CollectorConfig,
    verified: VerifiedMarker,
) -> tuple[tuple[str, str, str], ...]:
    if not isinstance(verified, VerifiedMarker):
        raise TypeError("SEC finalization requires verified immutable marker bytes")
    marker = verified.document
    run_id = str(marker["run_id"])
    if (
        verified_marker(
            registry, config, run_id=run_id, request_sha256=str(marker["request_sha256"])
        )
        != verified
    ):
        raise CollectorError("SEC finalization differs from registered evidence")
    payload = verified.payload
    digest = verified.content_sha256
    events = _events(marker, digest)
    registration = dataset_registration(config, marker, marker_file=verified.file)
    settle_usage(registry, marker)
    advances = tuple(
        WatermarkAdvance(
            provider="sec",
            dataset=DATASET,
            stream=str(item["stream"]),
            run_id=run_id,
            watermark_value=str(item["watermark_value"]),
            watermark_position=instant(item["watermark_position"]),
        )
        for item in object_list(marker["watermarks"])
    )
    with registry.begin_registration() as connection:
        registry.lock_run_lineage(run_id, connection=connection)
        state = registry.current_run_state(run_id, connection=connection)
        if state is None or state.plan_id != marker["plan_id"]:
            raise CollectorError("SEC finalization has a foreign run/plan binding")
        registry.require_event_sequence(
            events if state.terminal else events[:1], connection=connection
        )
        metadata.register_dataset(registration, connection=connection)
        registry.record_receipt(
            CollectionReceipt(
                run_id=run_id,
                attempt_number=1,
                source_snapshot_id=f"{run_id}-source",
                row_count=registration.manifest.row_count,
                byte_count=len(payload),
                receipt_sha256=digest,
            ),
            connection=connection,
        )
        if not state.terminal:
            for event in events[1:]:
                registry.append_event(event, connection=connection)
        for advance in advances:
            registry.advance_watermark(advance, connection=connection)
        verify_files(config, marker)
        if read_bytes(config.dataset_root / "finalization.json") != payload:
            raise CollectorError("SEC finalization changed before commit")
    return tuple((item.dataset, item.stream, item.watermark_value) for item in advances)


def outcome_from_marker(config: CollectorConfig, marker: Mapping[str, object]) -> CollectorOutcome:
    admissions = tuple(
        Admission(
            instrument_id=str(item["instrument_id"]),
            state=AdmissionState(str(item["state"])),
            issuer_id=None if item["issuer_id"] is None else str(item["issuer_id"]),
            cik=None if item["cik"] is None else str(item["cik"]),
            cik_source=None if item["cik_source"] is None else str(item["cik_source"]),
            reason=None if item["reason"] is None else str(item["reason"]),
        )
        for item in object_list(marker["admissions"])
    )
    skips = tuple(
        SkipReceipt(
            str(item["instrument_id"]),
            AdmissionState(str(item["state"])),
            None if item["reason"] is None else str(item["reason"]),
            None if item["cik"] is None else str(item["cik"]),
        )
        for item in object_list(marker["skips"])
    )
    return CollectorOutcome(
        run_id=str(marker["run_id"]),
        plan_id=str(marker["plan_id"]),
        terminal_event=RunEventType.RUN_SUCCEEDED,
        published_paths=(
            *(
                config.dataset_root / str(item["relative_path"])
                for item in object_list(marker["outputs"])
            ),
            config.receipt_path,
        ),
        receipt_path=config.receipt_path,
        admissions=admissions,
        skips=skips,
        watermarks_advanced=tuple(
            (DATASET, str(item["stream"]), str(item["watermark_value"]))
            for item in object_list(marker["watermarks"])
        ),
        disagreement_count=int(str(marker["disagreement_count"])),
        calls_attempted=int(str(marker["calls_attempted"])),
    )
