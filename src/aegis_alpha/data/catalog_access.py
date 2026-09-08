"""Read-only, immutable views of the existing dataset catalog."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, cast

from sqlalchemy import select

from aegis_alpha.data.canonical_json import as_bool, as_int, as_mapping, as_str
from aegis_alpha.data.canonical_records import require_sha256
from aegis_alpha.data.contracts import Eligibility
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.metadata.records import DatasetArtifact, dataset_artifact_digest
from aegis_alpha.metadata.schema import dataset_artifacts, dataset_versions

if TYPE_CHECKING:
    from sqlalchemy import Engine


def _text(name: str, value: object) -> str:
    text = as_str(name, value)
    if not text or text != text.strip() or any(ord(char) < ord(" ") for char in text):
        raise ValueError(f"{name} must be nonempty exact text without control characters")
    return text


@dataclass(frozen=True, slots=True)
class ArtifactReference(DatasetArtifact):
    """Existing artifact contract with stricter read-boundary path/count checks."""

    def __post_init__(self) -> None:
        relative = _text("relative_path", self.relative_path)
        path = PurePosixPath(relative)
        if (
            path.is_absolute()
            or path.as_posix() != relative
            or any(part in {".", "..", "~"} for part in relative.split("/"))
            or any(char in relative for char in "\\*?[]")
            or relative.startswith("~")
        ):
            raise ValueError("artifact path must be an exact safe relative path")
        as_int("size_bytes", self.size_bytes)
        if self.row_count is not None:
            as_int("row_count", self.row_count)
        _text("media_type", self.media_type)
        _text("content_sha256", self.content_sha256)
        DatasetArtifact.__post_init__(self)


@dataclass(frozen=True, slots=True)
class DatasetView:
    """A detached dataset/version pin; no mutable registry or identity lookups."""

    dataset_id: str
    dataset_version: str
    schema_version: int
    transformation_version: str
    aggregate_content_sha256: str
    row_count: int
    eligibility: Eligibility
    artifacts: tuple[ArtifactReference, ...]

    def __post_init__(self) -> None:
        for name in ("dataset_id", "dataset_version", "transformation_version"):
            _text(name, getattr(self, name))
        if as_int("schema_version", self.schema_version) < 1:
            raise ValueError("schema_version must be positive")
        if as_int("row_count", self.row_count) < 0:
            raise ValueError("row_count must be nonnegative")
        require_sha256("aggregate_content_sha256", _text("hash", self.aggregate_content_sha256))
        if not isinstance(self.eligibility, Eligibility):
            raise TypeError("eligibility must be Eligibility")
        for name in ("canonical", "backtest", "paper", "order"):
            as_bool(name, getattr(self.eligibility, name))
        if not isinstance(self.artifacts, tuple) or not self.artifacts:
            raise ValueError("artifacts must be a nonempty immutable tuple")
        if any(not isinstance(item, ArtifactReference) for item in self.artifacts):
            raise TypeError("artifacts must contain ArtifactReference values")
        paths = [item.relative_path for item in self.artifacts]
        if len(paths) != len(set(paths)):
            raise ValueError("duplicate artifact paths")
        object.__setattr__(
            self, "artifacts", tuple(sorted(self.artifacts, key=lambda x: x.relative_path))
        )
        if dataset_artifact_digest(self.artifacts) != self.aggregate_content_sha256:
            raise ValueError("catalog artifact list does not match aggregate hash")

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe projection, including every artifact and flag."""
        return cast("dict[str, object]", json.loads(canonical_json_bytes(self)))


def _artifact(row: Mapping[str, object]) -> ArtifactReference:
    return ArtifactReference(
        relative_path=as_str("relative_path", row["relative_path"]),
        media_type=as_str("media_type", row["media_type"]),
        size_bytes=as_int("size_bytes", row["size_bytes"]),
        row_count=None if row["row_count"] is None else as_int("row_count", row["row_count"]),
        content_sha256=as_str("content_sha256", row["content_sha256"]),
        partition_values=as_mapping("partition_values_json", row["partition_values_json"]),
    )


def _view(row: Mapping[str, object], artifacts: tuple[ArtifactReference, ...]) -> DatasetView:
    return DatasetView(
        dataset_id=as_str("dataset_id", row["dataset_id"]),
        dataset_version=as_str("dataset_version", row["dataset_version"]),
        schema_version=as_int("schema_version", row["schema_version"]),
        transformation_version=as_str("transformation_version", row["transformation_version"]),
        aggregate_content_sha256=as_str(
            "aggregate_content_sha256", row["aggregate_content_sha256"]
        ),
        row_count=as_int("row_count", row["row_count"]),
        eligibility=Eligibility(
            canonical=as_bool("canonical_eligible", row["canonical_eligible"]),
            backtest=as_bool("backtest_eligible", row["backtest_eligible"]),
            paper=as_bool("paper_eligible", row["paper_eligible"]),
            order=as_bool("order_eligible", row["order_eligible"]),
        ),
        artifacts=artifacts,
    )


def load_dataset(engine: Engine, dataset_id: str, version: str) -> DatasetView:
    """Select exactly one registered version, validating its complete artifact pin."""
    _text("dataset_id", dataset_id)
    _text("version", version)
    with engine.connect() as connection:
        row = (
            connection.execute(
                select(dataset_versions).where(
                    dataset_versions.c.dataset_id == dataset_id,
                    dataset_versions.c.dataset_version == version,
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ValueError("dataset/version is not registered")
        artifacts = tuple(
            _artifact(dict(item))
            for item in connection.execute(
                select(dataset_artifacts)
                .where(
                    dataset_artifacts.c.dataset_id == dataset_id,
                    dataset_artifacts.c.dataset_version == version,
                )
                .order_by(dataset_artifacts.c.relative_path)
            ).mappings()
        )
    return _view(dict(row), artifacts)


def list_datasets(engine: Engine) -> list[dict[str, object]]:
    """List catalog metadata deterministically without reading files or inferring eligibility."""
    with engine.connect() as connection:
        rows = connection.execute(
            select(dataset_versions).order_by(
                dataset_versions.c.dataset_id, dataset_versions.c.dataset_version
            )
        ).mappings()
        return [
            cast("dict[str, object]", json.loads(canonical_json_bytes(dict(row)))) for row in rows
        ]
