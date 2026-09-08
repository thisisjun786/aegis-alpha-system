"""Descriptor-bound immutable FRED evidence publication and verification."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import secrets
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from aegis_alpha.data.contracts import DatasetManifest, Eligibility, QualityStatus, SourceSnapshot
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.fred_alfred_normalize import DATASET_VERSION
from aegis_alpha.data.fred_alfred_series import PLAN_DATASET
from aegis_alpha.data.serialization import canonical_json_bytes
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

if TYPE_CHECKING:
    from sqlalchemy import Connection

    from aegis_alpha.data.fred_alfred_collector import CollectorConfig, CollectorOutcome


def read_evidence(path: Path) -> bytes:
    with DescriptorTree.open_path(path.parent) as tree:
        return tree.read_bytes(path.name)


def source_provenance_path(root: Path, snapshot_id: str) -> Path:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", snapshot_id) is None:
        raise ValueError("FRED snapshot id must be a safe leaf name")
    return root / "snapshots" / f"{snapshot_id}.json"


def publish_evidence(destination: Path, content: bytes) -> None:
    if not destination.is_absolute() or ".." in destination.parts:
        raise ValueError("evidence destination must be absolute without traversal")
    with DescriptorTree.open_path(Path(destination.anchor)) as root:
        for index in range(1, len(destination.parts) - 1):
            relative = Path(*destination.parts[1 : index + 1])
            root.mkdir(relative, exist_ok=True)
            root.fsync_directory(relative.parent)
    with DescriptorTree.open_path(destination.parent) as tree:
        if tree.exists(destination.name):
            if tree.read_bytes(destination.name) != content:
                raise ValueError("refusing to clobber immutable FRED evidence")
            return
        temporary = f".fred-{secrets.token_hex(16)}.tmp"
        try:
            with tree.binary_writer(temporary, exclusive=True) as handle:
                handle.write(content)
                handle.flush()
                os.fchmod(handle.fileno(), 0o444)
                os.fsync(handle.fileno())
            try:
                os.link(
                    temporary,
                    destination.name,
                    src_dir_fd=tree.descriptor,
                    dst_dir_fd=tree.descriptor,
                    follow_symlinks=False,
                )
            except FileExistsError:
                if tree.read_bytes(destination.name) != content:
                    raise ValueError("refusing to clobber immutable FRED evidence") from None
            tree.fsync_directory()
        finally:
            tree.unlink(temporary, missing_ok=True)
            tree.fsync_directory()


def capture_source(root: Path, snapshot: SourceSnapshot, body: bytes) -> Path:
    provenance_path = source_provenance_path(root, snapshot.snapshot_id)
    if (
        hashlib.sha256(body).hexdigest() != snapshot.content_sha256
        or len(body) != snapshot.raw_byte_length
    ):
        raise ValueError("FRED raw evidence does not match source snapshot")
    path = (
        root / "blobs" / "sha256" / snapshot.content_sha256[:2] / f"{snapshot.content_sha256}.raw"
    )
    publish_evidence(path, body)
    publish_evidence(provenance_path, canonical_json_bytes(snapshot))
    return path


def register_source(
    registry: MetadataRegistry,
    root: Path,
    snapshot: SourceSnapshot,
    path: Path,
    *,
    connection: Connection | None = None,
) -> None:
    registry.register_source_snapshot(
        source_registration(root, snapshot, path), connection=connection
    )


def source_registration(
    root: Path, snapshot: SourceSnapshot, path: Path
) -> SourceSnapshotRegistration:
    body = read_evidence(path)
    if (
        len(body) != snapshot.raw_byte_length
        or hashlib.sha256(body).hexdigest() != snapshot.content_sha256
    ):
        raise ValueError("FRED registered source bytes differ")
    files = (SourceSnapshotFile(str(path.relative_to(root)), len(body), snapshot.content_sha256),)
    return SourceSnapshotRegistration(
        snapshot=snapshot,
        files=files,
        tree_sha256=source_tree_digest(files),
        manifest={"source_snapshot_id": snapshot.snapshot_id},
    )


def dataset_registration(
    config: CollectorConfig,
    outcome: CollectorOutcome,
    sources: Sequence[SourceSnapshot],
    created: datetime,
    *,
    verified_artifacts: Sequence[DatasetArtifact] | None = None,
) -> DatasetRegistration | None:
    artifacts = list(verified_artifacts or ())
    for path in () if verified_artifacts is not None else outcome.published_paths:
        if path == outcome.receipt_path or path.suffix != ".parquet":
            continue
        payload = read_evidence(path)
        artifacts.append(
            DatasetArtifact(
                relative_path=str(path.relative_to(config.dataset_root)),
                media_type="application/vnd.apache.parquet",
                size_bytes=len(payload),
                row_count=pq.ParquetFile(io.BytesIO(payload)).metadata.num_rows,
                content_sha256=hashlib.sha256(payload).hexdigest(),
                partition_values={
                    key: value
                    for part in path.parts
                    if "=" in part
                    for key, value in [part.split("=", 1)]
                },
            )
        )
    if not artifacts:
        return None
    dates = [
        row["observation_date"] for row in outcome.rows if isinstance(row["observation_date"], date)
    ]
    inputs = tuple(
        DatasetInputFile(
            source_snapshot_id=source.snapshot_id,
            relative_path=f"blobs/sha256/{source.content_sha256[:2]}/{source.content_sha256}.raw",
        )
        for source in sources
    )
    manifest = DatasetManifest(
        dataset_id=PLAN_DATASET,
        dataset_version=hashlib.sha256(outcome.run_id.encode()).hexdigest(),
        schema_version=1,
        source_snapshot_ids=tuple(source.snapshot_id for source in sources),
        row_count=len(outcome.rows),
        coverage_start=min(dates) if dates else None,
        coverage_end=max(dates) if dates else None,
        identity_coverage=1.0,
        freshness_status=QualityStatus.WARN,
        quality_result_ids=(),
        transformation_version="fred-alfred-durable-v1",
        content_sha256=dataset_artifact_digest(artifacts),
        created_at_utc=created,
        eligibility=Eligibility.blocked(),
    )
    return DatasetRegistration(manifest, inputs, tuple(artifacts), ())


def verify_publication(config: CollectorConfig) -> None:
    receipt_bytes = read_evidence(config.receipt_path)
    ready = json.loads(
        read_evidence(config.receipt_path.with_name(config.receipt_path.name + ".ready.json"))
    )
    if hashlib.sha256(receipt_bytes).hexdigest() != ready["receipt_sha256"]:
        raise ValueError("FRED receipt bytes differ before registration commit")
    receipt = json.loads(receipt_bytes)
    run_root = (
        config.dataset_root
        / "fred_alfred"
        / DATASET_VERSION
        / hashlib.sha256(config.run_identity.encode()).hexdigest()
    )
    for value, digest in receipt["published_artifacts"].items():
        path = Path(value)
        if not path.is_relative_to(run_root) or ".." in path.parts:
            raise ValueError("FRED artifact escapes its run namespace")
        if hashlib.sha256(read_evidence(path)).hexdigest() != digest:
            raise ValueError("FRED artifact bytes differ before registration commit")
