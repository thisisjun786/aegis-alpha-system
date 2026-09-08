from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, date, datetime
from typing import cast

import pytest
from sqlalchemy import Engine, func, select

from aegis_alpha.data.contracts import (
    DataQualityResult,
    DatasetManifest,
    Eligibility,
    QualityStatus,
    SourceSnapshot,
    ValidationStatus,
)
from aegis_alpha.metadata.records import (
    DatasetArtifact,
    DatasetInputFile,
    DatasetRegistration,
    SourceSnapshotFile,
    SourceSnapshotRegistration,
    dataset_artifact_digest,
    source_tree_digest,
)
from aegis_alpha.metadata.registry import MetadataConflictError, MetadataRegistry
from aegis_alpha.metadata.schema import (
    dataset_artifacts,
    dataset_input_files,
    dataset_versions,
    quality_results,
    source_snapshot_files,
    source_snapshots,
)

_EXPECTED_SOURCE_FILES = 2
_EXPECTED_DATASET_ARTIFACTS = 2


def _source_snapshot(*, parameters: Mapping[str, str] | None = None) -> SourceSnapshot:
    captured_at = datetime(2026, 7, 29, 8, 1, 10, tzinfo=UTC)
    return SourceSnapshot(
        snapshot_id="norgate-2026-07-29-us-platinum",
        schema_version=1,
        provider="norgate",
        dataset="us_platinum_frozen_export",
        source_uri="file:///external/norgate/2026-07-29-us-platinum",
        request_fingerprint="sha256:" + "b" * 64,
        parameters=parameters or {"adjustments": "CAPITAL,TOTALRETURN"},
        requested_at_utc=captured_at,
        retrieved_at_utc=captured_at,
        content_type="application/x-directory",
        encoding=None,
        compression=None,
        raw_byte_length=300,
        content_sha256="a" * 64,
        parser_name="norgate-frozen-export",
        parser_version="1.0.0",
        validation_status=ValidationStatus.WARN,
        row_count=150_562_426,
        coverage_start="1990-01-02",
        coverage_end="2026-07-28",
        license_classification="PROPRIETARY_SUBSCRIPTION",
        retention_classification="OWNER_CONTROLLED",
    )


def _source_registration() -> SourceSnapshotRegistration:
    files = (
        SourceSnapshotFile("manifest.json", 100, "1" * 64),
        SourceSnapshotFile("prices/PRICE_CAPITAL_0000.fth", 200, "2" * 64),
    )
    return SourceSnapshotRegistration(
        snapshot=_source_snapshot(),
        tree_sha256=source_tree_digest(files),
        files=files,
        manifest={"export_id": "2026-07-29-us-platinum", "file_count": 2},
    )


def _dataset_registration(
    *,
    artifacts: tuple[DatasetArtifact, ...] | None = None,
) -> DatasetRegistration:
    selected_artifacts = artifacts or (
        DatasetArtifact(
            relative_path="security_master.parquet",
            media_type="application/vnd.apache.parquet",
            size_bytes=10_146,
            row_count=2,
            content_sha256="3" * 64,
            partition_values={},
        ),
        DatasetArtifact(
            relative_path="prices.parquet",
            media_type="application/vnd.apache.parquet",
            size_bytes=593_482,
            row_count=18_672,
            content_sha256="4" * 64,
            partition_values={"adjustment_type": ["CAPITAL", "TOTALRETURN"]},
        ),
    )
    checked_at = datetime(2026, 7, 29, 9, 0, tzinfo=UTC)
    quality = (
        DataQualityResult(
            result_id="sample-key-uniqueness-v1",
            check_id="key_uniqueness",
            check_version="1.0.0",
            subject_id="norgate-sample@sample-v1",
            status=QualityStatus.PASS,
            dimensions=("uniqueness",),
            details=("18,672 unique keys",),
            safe_next_action="keep eligibility blocked pending full-data validation",
            checked_at_utc=checked_at,
        ),
    )
    manifest = DatasetManifest(
        dataset_id="norgate-sample",
        dataset_version="sample-v1",
        schema_version=1,
        source_snapshot_ids=("norgate-2026-07-29-us-platinum",),
        row_count=18_674,
        coverage_start=date(1990, 1, 2),
        coverage_end=date(2026, 7, 28),
        identity_coverage=1.0,
        freshness_status=QualityStatus.WARN,
        quality_result_ids=("sample-key-uniqueness-v1",),
        transformation_version="aas-data-002.sample-v1",
        content_sha256=dataset_artifact_digest(selected_artifacts),
        created_at_utc=checked_at,
        eligibility=Eligibility.blocked(),
    )
    return DatasetRegistration(
        manifest=manifest,
        input_files=(
            DatasetInputFile(
                "norgate-2026-07-29-us-platinum",
                "prices/PRICE_CAPITAL_0000.fth",
            ),
        ),
        artifacts=selected_artifacts,
        quality_results=quality,
    )


def test_source_registration_is_exactly_idempotent(clean_postgres: Engine) -> None:
    registry = MetadataRegistry(clean_postgres)
    registration = _source_registration()

    registry.register_source_snapshot(registration)
    registry.register_source_snapshot(registration)

    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(source_snapshots)) == 1
        assert (
            connection.scalar(select(func.count()).select_from(source_snapshot_files))
            == _EXPECTED_SOURCE_FILES
        )
        row = connection.execute(select(source_snapshots)).mappings().one()
    assert row["snapshot_id"] == registration.snapshot.snapshot_id
    assert row["tree_sha256"] == registration.tree_sha256
    assert row["manifest_json"] == dict(registration.manifest)
    assert row["requested_at_utc"] == registration.snapshot.requested_at_utc


def test_source_registration_rejects_parent_or_child_rebinding(clean_postgres: Engine) -> None:
    registry = MetadataRegistry(clean_postgres)
    registration = _source_registration()
    registry.register_source_snapshot(registration)

    changed_parent = replace(registration, manifest={"file_count": 2, "changed": True})
    with pytest.raises(MetadataConflictError, match="snapshot identity"):
        registry.register_source_snapshot(changed_parent)

    changed_files = replace(
        registration,
        files=(registration.files[0], replace(registration.files[1], content_sha256="5" * 64)),
        tree_sha256=source_tree_digest(
            (registration.files[0], replace(registration.files[1], content_sha256="5" * 64))
        ),
    )
    with pytest.raises(MetadataConflictError, match="snapshot identity") as error:
        registry.register_source_snapshot(changed_files)
    assert "file:///" not in str(error.value)


def test_registration_rejects_duplicate_child_identities_before_sql() -> None:
    source = _source_registration()
    duplicate_case_path = replace(source.files[0], relative_path="MANIFEST.JSON")
    with pytest.raises(ValueError, match="case-insensitive source file identities"):
        SourceSnapshotRegistration(
            snapshot=replace(source.snapshot, raw_byte_length=400),
            tree_sha256=source_tree_digest((source.files[0], duplicate_case_path)),
            files=(source.files[0], duplicate_case_path),
            manifest=source.manifest,
        )

    dataset = _dataset_registration()
    duplicated_sources = replace(
        dataset.manifest,
        source_snapshot_ids=(
            "norgate-2026-07-29-us-platinum",
            "norgate-2026-07-29-us-platinum",
        ),
    )
    with pytest.raises(ValueError, match="duplicate dataset source identities"):
        replace(dataset, manifest=duplicated_sources)


def test_source_registration_rejects_recursive_credentials_without_leaking_value(
    clean_postgres: Engine,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sentinel = "DO_NOT_LEAK_SENTINEL"
    files = (SourceSnapshotFile("manifest.json", 300, "1" * 64),)
    with pytest.raises(ValueError, match="credential-like metadata is forbidden") as error:
        SourceSnapshotRegistration(
            snapshot=_source_snapshot(),
            tree_sha256=source_tree_digest(files),
            files=files,
            manifest={"nested": {"api_token": sentinel}},
        )

    captured = capsys.readouterr()
    assert sentinel not in str(error.value)
    assert sentinel not in captured.out
    assert sentinel not in captured.err
    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(source_snapshots)) == 0


def test_nested_metadata_is_detached_and_deeply_frozen() -> None:
    mutable_manifest = {"nested": {"labels": ["verified"]}}
    mutable_partitions = {"nested": {"adjustments": ["CAPITAL"]}}
    source = _source_registration()

    registration = replace(source, manifest=mutable_manifest)
    artifact = DatasetArtifact(
        relative_path="prices.parquet",
        media_type="application/vnd.apache.parquet",
        size_bytes=1,
        row_count=1,
        content_sha256="8" * 64,
        partition_values=mutable_partitions,
    )
    mutable_manifest["nested"]["labels"].append("mutated")
    mutable_partitions["nested"]["adjustments"].append("TOTALRETURN")

    assert registration.manifest["nested"] == {"labels": ("verified",)}
    assert artifact.partition_values["nested"] == {"adjustments": ("CAPITAL",)}
    credential_value = "sentinel"
    with pytest.raises(TypeError):
        cast("dict[str, object]", registration.manifest["nested"])["api_token"] = credential_value


def test_registry_revalidates_metadata_at_write_boundary(clean_postgres: Engine) -> None:
    registration = _source_registration()
    object.__setattr__(registration, "manifest", {"nested": {"api_token": "sentinel"}})

    with pytest.raises(ValueError, match="credential-like metadata is forbidden"):
        MetadataRegistry(clean_postgres).register_source_snapshot(registration)

    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(source_snapshots)) == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dataset_id", ""),
        ("dataset_version", "  "),
        ("transformation_version", ""),
    ],
)
def test_dataset_registration_rejects_empty_permanent_identity_fields(
    field: str,
    value: str,
) -> None:
    registration = _dataset_registration()
    manifest = replace(registration.manifest, **{field: value})

    with pytest.raises(ValueError, match="must be nonempty"):
        replace(registration, manifest=manifest)


@pytest.mark.parametrize("field", ["result_id", "check_id", "check_version"])
def test_dataset_registration_rejects_empty_quality_identity_fields(field: str) -> None:
    registration = _dataset_registration()
    quality = replace(registration.quality_results[0], **{field: " "})
    manifest = replace(registration.manifest, quality_result_ids=(quality.result_id,))

    with pytest.raises(ValueError, match="must be nonempty"):
        replace(registration, manifest=manifest, quality_results=(quality,))


def test_dataset_aggregate_is_atomic_and_exactly_idempotent(clean_postgres: Engine) -> None:
    registry = MetadataRegistry(clean_postgres)
    registry.register_source_snapshot(_source_registration())
    registration = _dataset_registration()

    registry.register_dataset(registration)
    registry.register_dataset(registration)

    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(dataset_versions)) == 1
        assert connection.scalar(select(func.count()).select_from(dataset_input_files)) == 1
        assert (
            connection.scalar(select(func.count()).select_from(dataset_artifacts))
            == _EXPECTED_DATASET_ARTIFACTS
        )
        assert connection.scalar(select(func.count()).select_from(quality_results)) == 1
        row = connection.execute(select(dataset_versions)).mappings().one()
    assert row["aggregate_content_sha256"] == dataset_artifact_digest(registration.artifacts)
    assert not any(
        row[name]
        for name in (
            "canonical_eligible",
            "backtest_eligible",
            "paper_eligible",
            "order_eligible",
        )
    )


def test_dataset_registration_rejects_partial_or_changed_child_set(clean_postgres: Engine) -> None:
    registry = MetadataRegistry(clean_postgres)
    registry.register_source_snapshot(_source_registration())
    registration = _dataset_registration()
    registry.register_dataset(registration)

    changed = _dataset_registration(artifacts=(registration.artifacts[0],))
    with pytest.raises(MetadataConflictError, match="dataset identity"):
        registry.register_dataset(changed)

    with clean_postgres.begin() as connection:
        connection.execute(
            dataset_artifacts.delete().where(dataset_artifacts.c.relative_path == "prices.parquet")
        )
    with pytest.raises(MetadataConflictError, match="dataset identity"):
        registry.register_dataset(registration)
    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(dataset_artifacts)) == 1


def test_dataset_registration_rolls_back_unknown_input_lineage(clean_postgres: Engine) -> None:
    registry = MetadataRegistry(clean_postgres)
    registry.register_source_snapshot(_source_registration())
    registration = _dataset_registration()
    invalid = replace(
        registration,
        input_files=(
            DatasetInputFile(
                "norgate-2026-07-29-us-platinum",
                "prices/PRICE_TOTALRETURN_9999.fth",
            ),
        ),
    )

    with pytest.raises(ValueError, match="input file lineage"):
        registry.register_dataset(invalid)

    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(dataset_versions)) == 0
        assert connection.scalar(select(func.count()).select_from(dataset_artifacts)) == 0


def test_concurrent_dataset_rebinding_has_one_winner_and_no_child_union(
    clean_postgres: Engine,
) -> None:
    registry = MetadataRegistry(clean_postgres)
    registry.register_source_snapshot(_source_registration())
    first = _dataset_registration()
    second_artifact = replace(first.artifacts[1], content_sha256="9" * 64)
    second = _dataset_registration(artifacts=(first.artifacts[0], second_artifact))

    def register(item: DatasetRegistration) -> str:
        try:
            registry.register_dataset(item)
        except MetadataConflictError:
            return "conflict"
        return "success"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = sorted(executor.map(register, (first, second)))

    assert outcomes == ["conflict", "success"]
    with clean_postgres.connect() as connection:
        rows = connection.execute(select(dataset_artifacts)).mappings().all()
        dataset = connection.execute(select(dataset_versions)).mappings().one()
    assert len(rows) == _EXPECTED_DATASET_ARTIFACTS
    assert dataset["aggregate_content_sha256"] in {
        first.manifest.content_sha256,
        second.manifest.content_sha256,
    }


def test_concurrent_identical_source_retry_is_idempotent(clean_postgres: Engine) -> None:
    registry = MetadataRegistry(clean_postgres)
    registration = _source_registration()

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(
            executor.map(registry.register_source_snapshot, (registration, registration))
        )

    assert outcomes == [None, None]
    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(source_snapshots)) == 1
        assert (
            connection.scalar(select(func.count()).select_from(source_snapshot_files))
            == _EXPECTED_SOURCE_FILES
        )


def test_concurrent_source_rebinding_has_one_winner(clean_postgres: Engine) -> None:
    registry = MetadataRegistry(clean_postgres)
    first = _source_registration()
    second = replace(first, manifest={"export_id": "different"})

    def register(item: SourceSnapshotRegistration) -> str:
        try:
            registry.register_source_snapshot(item)
        except MetadataConflictError:
            return "conflict"
        return "success"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = sorted(executor.map(register, (first, second)))

    assert outcomes == ["conflict", "success"]
    with clean_postgres.connect() as connection:
        assert connection.scalar(select(func.count()).select_from(source_snapshots)) == 1
