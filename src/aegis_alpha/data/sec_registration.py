"""SEC response and run publications translated into existing metadata contracts."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import cast

import pyarrow.parquet as pq

from aegis_alpha.data.contracts import (
    DatasetManifest,
    Eligibility,
    QualityStatus,
    SourceSnapshot,
    ValidationStatus,
)
from aegis_alpha.data.sec_collector import (
    DATASET,
    CollectorConfig,
    raw_blob_path,
    raw_provenance_path,
)
from aegis_alpha.data.sec_evidence import file_pin, open_directory, verify_pin
from aegis_alpha.data.sec_normalize import NORMALIZED_VERSION, PROVIDER
from aegis_alpha.data.sec_transport import CollectorRequest, CollectorResponse
from aegis_alpha.metadata.records import (
    DatasetArtifact,
    DatasetInputFile,
    DatasetRegistration,
    SourceSnapshotFile,
    SourceSnapshotRegistration,
    dataset_artifact_digest,
    source_tree_digest,
)
from aegis_alpha.metadata.registry import MetadataRegistry


def instant(value: object) -> datetime:
    if not isinstance(value, str):
        raise TypeError("SEC timestamp must be text")
    result = datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("SEC timestamp must be timezone-aware")
    return result


def object_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("SEC evidence must contain an object")
    return cast("dict[str, object]", value)


def object_list(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise TypeError("SEC evidence must contain a list")
    return [object_mapping(item) for item in value]


def _source_registration(entry: Mapping[str, object]) -> SourceSnapshotRegistration:
    pin = object_mapping(entry["raw"])
    file = SourceSnapshotFile(
        str(pin["relative_path"]), int(str(pin["size_bytes"])), str(pin["content_sha256"])
    )
    snapshot = SourceSnapshot(
        snapshot_id=str(entry["snapshot_id"]),
        schema_version=1,
        provider=PROVIDER,
        dataset=str(entry["dataset"]),
        source_uri=str(entry["source_uri"]),
        request_fingerprint=str(entry["request_fingerprint"]),
        parameters={},
        requested_at_utc=instant(entry["requested_at_utc"]),
        retrieved_at_utc=instant(entry["retrieved_at_utc"]),
        content_type="application/json",
        encoding="utf-8",
        compression=None,
        raw_byte_length=file.size_bytes,
        content_sha256=file.content_sha256,
        parser_name="sec-runtime",
        parser_version=NORMALIZED_VERSION,
        validation_status=ValidationStatus.WARN,
    )
    return SourceSnapshotRegistration(snapshot, source_tree_digest((file,)), (file,), dict(entry))


def register_response(  # noqa: PLR0913 - exact response and published provenance bindings
    metadata: MetadataRegistry,
    config: CollectorConfig,
    request: CollectorRequest,
    response: CollectorResponse,
    *,
    snapshot_id: str,
    provenance: bytes,
) -> dict[str, object]:
    entry: dict[str, object] = {
        "snapshot_id": snapshot_id,
        "dataset": request.dataset.value,
        "source_uri": request.source_uri,
        "request_fingerprint": request.request_fingerprint,
        "requested_at_utc": response.requested_at_utc.isoformat(),
        "retrieved_at_utc": response.retrieved_at_utc.isoformat(),
        "status_code": response.status_code,
        "raw": file_pin(
            config.raw_store_root,
            raw_blob_path(config.raw_store_root, request.dataset, response.body),
        ),
        "provenance": file_pin(
            config.raw_store_root, raw_provenance_path(config.raw_store_root, provenance)
        ),
    }
    raw_pin = object_mapping(entry["raw"])
    provenance_pin = object_mapping(entry["provenance"])
    if (
        raw_pin["content_sha256"] != response.content_sha256
        or raw_pin["size_bytes"] != len(response.body)
        or provenance_pin["content_sha256"] != hashlib.sha256(provenance).hexdigest()
    ):
        raise ValueError("SEC response bytes changed before registration")
    metadata.register_source_snapshot(_source_registration(entry))
    return entry


def verify_sources(config: CollectorConfig, sources: Sequence[Mapping[str, object]]) -> None:
    for source in sources:
        verify_pin(config.raw_store_root, object_mapping(source["raw"]))
        verify_pin(config.raw_store_root, object_mapping(source["provenance"]))


def register_run_source(
    metadata: MetadataRegistry,
    config: CollectorConfig,
    run_id: str,
    payload: bytes,
    *,
    finished_at: datetime,
) -> str:
    snapshot_id = f"{run_id}-source"
    file = SourceSnapshotFile(
        "finalization.json", len(payload), hashlib.sha256(payload).hexdigest()
    )
    snapshot = SourceSnapshot(
        snapshot_id=snapshot_id,
        schema_version=1,
        provider=PROVIDER,
        dataset=DATASET,
        source_uri="urn:aas:sec:run:" + run_id,
        request_fingerprint="sha256:" + file.content_sha256,
        parameters={"run_id": run_id},
        requested_at_utc=finished_at,
        retrieved_at_utc=finished_at,
        content_type="application/json",
        encoding="utf-8",
        compression=None,
        raw_byte_length=len(payload),
        content_sha256=file.content_sha256,
        parser_name="sec-runtime-manifest",
        parser_version=NORMALIZED_VERSION,
        validation_status=ValidationStatus.WARN,
    )
    verify_pin(
        config.dataset_root,
        {
            "relative_path": file.relative_path,
            "size_bytes": file.size_bytes,
            "content_sha256": file.content_sha256,
        },
    )
    metadata.register_source_snapshot(
        SourceSnapshotRegistration(
            snapshot, source_tree_digest((file,)), (file,), {"run_id": run_id}
        )
    )
    return snapshot_id


def dataset_registration(
    config: CollectorConfig, marker: Mapping[str, object], *, marker_file: SourceSnapshotFile
) -> DatasetRegistration:
    run_id = str(marker["run_id"])
    sources = object_list(marker["sources"])
    outputs = object_list(marker["outputs"])
    artifacts: list[DatasetArtifact] = []
    with open_directory(config.dataset_root) as tree:
        for pin in outputs:
            relative = str(pin["relative_path"])
            with tree.binary_reader(relative) as handle:
                count = pq.ParquetFile(handle).metadata.num_rows
            artifacts.append(
                DatasetArtifact(
                    relative,
                    "application/vnd.apache.parquet",
                    int(str(pin["size_bytes"])),
                    count,
                    str(pin["content_sha256"]),
                    {},
                )
            )
    artifacts.append(
        DatasetArtifact(
            "finalization.json",
            "application/json",
            marker_file.size_bytes,
            None,
            marker_file.content_sha256,
            {},
        )
    )
    inputs = tuple(
        DatasetInputFile(
            str(source["snapshot_id"]), str(object_mapping(source["raw"])["relative_path"])
        )
        for source in sources
    )
    inputs += (DatasetInputFile(f"{run_id}-source", "finalization.json"),)
    manifest = DatasetManifest(
        dataset_id=DATASET,
        dataset_version=run_id,
        schema_version=1,
        source_snapshot_ids=tuple(sorted({item.source_snapshot_id for item in inputs})),
        row_count=sum(item.row_count or 0 for item in artifacts),
        coverage_start=None,
        coverage_end=None,
        identity_coverage=0.0,
        freshness_status=QualityStatus.WARN,
        quality_result_ids=(),
        transformation_version=NORMALIZED_VERSION,
        content_sha256=dataset_artifact_digest(artifacts),
        created_at_utc=instant(marker["finished_at"]),
        eligibility=Eligibility.blocked(),
    )
    return DatasetRegistration(manifest, inputs, tuple(artifacts), ())
