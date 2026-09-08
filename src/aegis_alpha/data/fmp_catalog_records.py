"""Typed catalog projections derived only from verified completed FMP files."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime

import pyarrow.parquet as pq

from aegis_alpha.collection.records import CollectionReceipt, CollectionRunPlan
from aegis_alpha.data.contracts import (
    DataQualityResult,
    DatasetManifest,
    Eligibility,
    QualityStatus,
    SourceSnapshot,
    ValidationStatus,
)
from aegis_alpha.data.fmp_catalog_evidence import CompletedEvidence
from aegis_alpha.data.fmp_catalog_io import CatalogFiles, identifier, list_value, require
from aegis_alpha.data.fmp_normalize import normalized_columns
from aegis_alpha.metadata.records import (
    DatasetArtifact,
    DatasetInputFile,
    DatasetRegistration,
    SourceSnapshotFile,
    SourceSnapshotRegistration,
    dataset_artifact_digest,
    source_tree_digest,
)

CONTRACT = "fmp-catalog-v1"


def source_id(run_id: str) -> str:
    return CONTRACT + "-" + hashlib.sha256(identifier(run_id).encode()).hexdigest()


def source_registration(
    files: CatalogFiles,
    evidence: CompletedEvidence,
    plan: CollectionRunPlan,
    finished_at: datetime,
) -> SourceSnapshotRegistration:
    file = SourceSnapshotFile(
        files.relative(files.home, evidence.receipt_path),
        len(evidence.receipt_bytes),
        evidence.receipt_sha256,
    )
    snapshot = SourceSnapshot(
        snapshot_id=source_id(evidence.run_id),
        schema_version=1,
        provider="fmp",
        dataset=plan.dataset,
        source_uri=evidence.receipt_path.as_uri(),
        request_fingerprint="sha256:" + plan.plan_sha256,
        parameters={"kind": "receipt-backed-run-aggregate", "run_id": evidence.run_id},
        requested_at_utc=plan.created_at_utc,
        retrieved_at_utc=finished_at,
        content_type="application/json",
        encoding="utf-8",
        compression=None,
        raw_byte_length=len(evidence.receipt_bytes),
        content_sha256=evidence.receipt_sha256,
        parser_name="fmp_receipt_aggregate",
        parser_version=CONTRACT,
        validation_status=ValidationStatus.PASS,
        license_classification="PROPRIETARY_SUBSCRIPTION",
        retention_classification="OWNER_CONTROLLED",
    )
    return SourceSnapshotRegistration(
        snapshot,
        source_tree_digest((file,)),
        (file,),
        {
            "kind": "receipt-backed-run-aggregate",
            "not_http_response": True,
            "raw_root": str(files.raw_root),
            "dataset_root": str(files.dataset_root),
            "receipt_path": str(evidence.receipt_path),
            "receipt": evidence.receipt,
            "publication": evidence.marker,
            "verified_provenance_addresses": list(evidence.provenance_addresses),
        },
    )


def _coverage(metadata: pq.FileMetaData) -> tuple[date | None, date | None]:
    """Use exact Parquet statistics only; absent statistics remain unknown."""
    starts: list[date] = []
    ends: list[date] = []
    for index in range(metadata.num_row_groups):
        group = metadata.row_group(index)
        for column in range(group.num_columns):
            chunk = group.column(column)
            if chunk.path_in_schema != "date":
                continue
            stats = chunk.statistics
            if stats is None or not stats.has_min_max:
                return None, None
            if not isinstance(stats.min, date) or not isinstance(stats.max, date):
                return None, None
            starts.append(stats.min)
            ends.append(stats.max)
    return (min(starts), max(ends)) if starts else (None, None)


def dataset_registrations(
    files: CatalogFiles,
    evidence: CompletedEvidence,
    plan: CollectionRunPlan,
    finished_at: datetime,
) -> tuple[DatasetRegistration, ...]:
    grouped: dict[str, list[DatasetArtifact]] = {}
    coverage: dict[str, list[tuple[date | None, date | None]]] = {}
    for dataset, path, size, sha in evidence.artifacts:
        rows = None
        if path.suffix == ".parquet":
            with files.tree(files.dataset_root).binary_reader(
                files.relative(files.dataset_root, path)
            ) as handle:
                metadata = pq.ParquetFile(handle).metadata
                require(
                    tuple(metadata.schema.names) == normalized_columns(dataset),
                    "FMP Parquet columns differ from their declared dataset",
                )
                rows = metadata.num_rows
                coverage.setdefault(dataset, []).append(_coverage(metadata))
        grouped.setdefault(dataset, []).append(
            DatasetArtifact(
                relative_path=files.relative(files.dataset_root, path),
                media_type="application/vnd.apache.parquet"
                if rows is not None
                else "application/json",
                size_bytes=size,
                row_count=rows,
                content_sha256=sha,
                partition_values={
                    "dataset": dataset,
                    "run_id": evidence.run_id,
                    "artifact_kind": "normalized" if rows is not None else "history_index",
                },
            )
        )
    registrations = []
    version = CONTRACT + "-" + hashlib.sha256(evidence.run_id.encode()).hexdigest()
    for dataset, artifacts in sorted(grouped.items()):
        # A history-only update is evidence, not a newly collected data partition.
        if not any(item.row_count is not None for item in artifacts):
            continue
        pairs = coverage.get(dataset, [])
        starts = [start for start, _end in pairs if start is not None]
        ends = [end for _start, end in pairs if end is not None]
        known = bool(pairs) and len(starts) == len(ends) == len(pairs)
        quality_id = f"{dataset}-{version}-quality"
        quality = DataQualityResult(
            result_id=quality_id,
            check_id="fmp-catalog-evidence",
            check_version=CONTRACT,
            subject_id=f"{dataset}@{version}",
            status=QualityStatus.BLOCKED,
            dimensions=("coverage", "eligibility"),
            details=(
                json.dumps(
                    {
                        "blocked_symbols": list_value(evidence.receipt["blocked_symbols"]),
                        "quality_results": list_value(evidence.receipt["quality_results"]),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
            safe_next_action="Review provider coverage and eligibility before downstream use",
            checked_at_utc=finished_at,
        )
        manifest = DatasetManifest(
            dataset_id=dataset,
            dataset_version=version,
            schema_version=1,
            source_snapshot_ids=(source_id(evidence.run_id),),
            row_count=sum(item.row_count or 0 for item in artifacts),
            coverage_start=min(starts) if known else None,
            coverage_end=max(ends) if known else None,
            identity_coverage=0.0,
            freshness_status=QualityStatus.BLOCKED,
            quality_result_ids=(quality_id,),
            transformation_version=CONTRACT,
            content_sha256=dataset_artifact_digest(artifacts),
            created_at_utc=plan.created_at_utc,
            eligibility=Eligibility.blocked(),
        )
        registrations.append(
            DatasetRegistration(
                manifest,
                (
                    DatasetInputFile(
                        source_id(evidence.run_id),
                        files.relative(files.home, evidence.receipt_path),
                    ),
                ),
                tuple(artifacts),
                (quality,),
            )
        )
    return tuple(registrations)


def collection_receipt(evidence: CompletedEvidence, attempt_number: int) -> CollectionReceipt:
    return CollectionReceipt(
        run_id=evidence.run_id,
        attempt_number=attempt_number,
        source_snapshot_id=source_id(evidence.run_id),
        receipt_sha256=evidence.receipt_sha256,
        byte_count=len(evidence.receipt_bytes),
    )
