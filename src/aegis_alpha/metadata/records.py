from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from hashlib import sha256
from types import MappingProxyType
from typing import cast
from urllib.parse import parse_qsl, urlsplit

from aegis_alpha.data.contracts import (
    DataQualityResult,
    DatasetManifest,
    Eligibility,
    SourceSnapshot,
)
from aegis_alpha.data.serialization import canonical_json_bytes

_SHA256_LENGTH = 64
_CREDENTIAL_NAMES = {
    "api_key",
    "apikey",
    "authorization",
    "bearer",
    "credential",
    "credentials",
    "password",
    "secret",
    "token",
}
_CREDENTIAL_VALUE_PATTERN = re.compile(
    r"(?i)\bbearer\s+[a-z0-9._~+/=-]{3,}"
    r"|(?:[a-z][a-z0-9+.-]*:)?//[^\s/?#]*@"
    r"|(?:[a-z][a-z0-9+.-]*:)?//[^\s/?#]*%40"
    r"|[?&](?:api_?key|access_?token|auth_?token|client_?secret|private_?key|access_?key"
    r"|secret|password|token|key)="
)


def _require_sha256(name: str, value: str) -> None:
    if len(value) != _SHA256_LENGTH or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{name} must be lowercase SHA-256 hexadecimal")


def _require_relative_path(value: str) -> None:
    parts = value.split("/")
    if not value.strip() or value != value.strip() or value.startswith("/") or ".." in parts:
        raise ValueError("artifact paths must be safe relative paths")


def _require_nonempty(field: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{field} must be nonempty")


def _normalized_key(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value)).strip()
    text = re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", text)
    return re.sub(r"[^a-z0-9]+", "_", text.casefold())


def _is_credential_key(value: object) -> bool:
    normalized = _normalized_key(value)
    segments = set(normalized.split("_"))
    return (
        normalized in _CREDENTIAL_NAMES
        or bool(segments & {"authorization", "bearer", "credential", "password", "secret", "token"})
        or {"api", "key"} <= segments
        or ("key" in segments and bool(segments & {"access", "private"}))
    )


def reject_credential_metadata(value: object) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if _is_credential_key(key):
                raise ValueError("credential-like metadata is forbidden")
            reject_credential_metadata(child)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for child in value:
            reject_credential_metadata(child)
        return
    if isinstance(value, str):
        _reject_credential_text(value)


def _reject_credential_text(value: str) -> None:
    if _CREDENTIAL_VALUE_PATTERN.search(value):
        raise ValueError("credential-like metadata is forbidden")
    if "://" not in value and not value.startswith("//"):
        return
    parsed = urlsplit(value)
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("credential-bearing URI metadata is forbidden")
    uri_keys = (
        key
        for component in (parsed.query, parsed.fragment)
        for key, _ in parse_qsl(component, keep_blank_values=True)
    )
    if any(_is_credential_key(key) for key in uri_keys):
        raise ValueError("credential-bearing URI metadata is forbidden")


def freeze_json_metadata(value: object) -> object:
    """Return a detached recursively immutable JSON-compatible projection."""

    if isinstance(value, Mapping):
        frozen: dict[str, object] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise TypeError("JSON metadata keys must be strings")
            frozen[key] = freeze_json_metadata(child)
        return MappingProxyType(frozen)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(freeze_json_metadata(child) for child in value)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON metadata numbers must be finite")
        return value
    raise TypeError("metadata must contain only JSON-compatible values")


def validated_json_copy(value: object) -> object:
    """Freeze once, reject credentials in that snapshot, then thaw the same snapshot."""

    frozen = freeze_json_metadata(value)
    reject_credential_metadata(frozen)
    return _thaw_json(frozen)


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(child) for child in value]
    return value


@dataclass(frozen=True, slots=True)
class SourceSnapshotFile:
    relative_path: str
    size_bytes: int
    content_sha256: str

    def __post_init__(self) -> None:
        _require_relative_path(self.relative_path)
        if self.size_bytes < 0:
            raise ValueError("source file size cannot be negative")
        _require_sha256("source file content_sha256", self.content_sha256)


def source_tree_digest(files: Sequence[SourceSnapshotFile]) -> str:
    rows = (
        f"{item.relative_path}\t{item.size_bytes}\t{item.content_sha256}"
        for item in sorted(files, key=lambda item: item.relative_path.casefold())
    )
    return hashlib.sha256("\n".join(rows).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class SourceSnapshotRegistration:
    snapshot: SourceSnapshot
    tree_sha256: str
    files: tuple[SourceSnapshotFile, ...]
    manifest: Mapping[str, object]

    def __post_init__(self) -> None:
        _require_sha256("tree_sha256", self.tree_sha256)
        if not self.files:
            raise ValueError("source snapshot requires typed file evidence")
        paths = [item.relative_path for item in self.files]
        if len(paths) != len(set(paths)):
            raise ValueError("duplicate source file identities are forbidden")
        if len(paths) != len({path.casefold() for path in paths}):
            raise ValueError("case-insensitive source file identities are forbidden")
        if source_tree_digest(self.files) != self.tree_sha256:
            raise ValueError("source file projection does not match tree_sha256")
        if sum(item.size_bytes for item in self.files) != self.snapshot.raw_byte_length:
            raise ValueError("source file sizes do not match snapshot raw_byte_length")
        frozen_parameters = freeze_json_metadata(self.snapshot.parameters)
        reject_credential_metadata(frozen_parameters)
        if not isinstance(frozen_parameters, Mapping):
            raise TypeError("source parameters must be a JSON object")
        object.__setattr__(
            self,
            "snapshot",
            replace(
                self.snapshot,
                parameters=cast("Mapping[str, str]", frozen_parameters),
            ),
        )
        frozen_manifest = freeze_json_metadata(self.manifest)
        reject_credential_metadata(frozen_manifest)
        if not isinstance(frozen_manifest, Mapping):
            raise TypeError("source manifest must be a JSON object")
        object.__setattr__(self, "manifest", frozen_manifest)


@dataclass(frozen=True, slots=True)
class DatasetInputFile:
    source_snapshot_id: str
    relative_path: str

    def __post_init__(self) -> None:
        _require_nonempty("source_snapshot_id", self.source_snapshot_id)
        _require_relative_path(self.relative_path)


@dataclass(frozen=True, slots=True)
class DatasetArtifact:
    relative_path: str
    media_type: str
    size_bytes: int
    row_count: int | None
    content_sha256: str
    partition_values: Mapping[str, object]

    def __post_init__(self) -> None:
        _require_relative_path(self.relative_path)
        _require_nonempty("artifact media_type", self.media_type)
        if self.size_bytes < 0 or (self.row_count is not None and self.row_count < 0):
            raise ValueError("artifact counts cannot be negative")
        _require_sha256("artifact content_sha256", self.content_sha256)
        frozen_partitions = freeze_json_metadata(self.partition_values)
        reject_credential_metadata(frozen_partitions)
        if not isinstance(frozen_partitions, Mapping):
            raise TypeError("partition_values must be a JSON object")
        object.__setattr__(self, "partition_values", frozen_partitions)


def dataset_artifact_digest(artifacts: Sequence[DatasetArtifact]) -> str:
    projections = [
        {
            "content_sha256": item.content_sha256,
            "relative_path": item.relative_path,
            "row_count": item.row_count,
            "size_bytes": item.size_bytes,
        }
        for item in sorted(artifacts, key=lambda item: item.relative_path)
    ]
    canonical = json.dumps(
        projections,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


@dataclass(frozen=True, slots=True)
class DatasetRegistration:
    manifest: DatasetManifest
    input_files: tuple[DatasetInputFile, ...]
    artifacts: tuple[DatasetArtifact, ...]
    quality_results: tuple[DataQualityResult, ...]

    def __post_init__(self) -> None:
        _require_nonempty("dataset_id", self.manifest.dataset_id)
        _require_nonempty("dataset_version", self.manifest.dataset_version)
        _require_nonempty("transformation_version", self.manifest.transformation_version)
        if self.manifest.eligibility != Eligibility.blocked():
            raise ValueError("schema v1 only accepts blocked dataset eligibility")
        if not self.input_files or not self.artifacts:
            raise ValueError("dataset requires input files and artifacts")
        quality_ids = _require_unique_dataset_children(self)
        for result in self.quality_results:
            _require_nonempty("quality result_id", result.result_id)
            _require_nonempty("quality check_id", result.check_id)
            _require_nonempty("quality check_version", result.check_version)
            _require_nonempty("quality safe_next_action", result.safe_next_action)
        if set(self.manifest.source_snapshot_ids) != {
            item.source_snapshot_id for item in self.input_files
        }:
            raise ValueError("dataset source set must exactly match input file lineage")
        if set(self.manifest.quality_result_ids) != set(quality_ids):
            raise ValueError("dataset quality result set does not match manifest")
        subject_id = f"{self.manifest.dataset_id}@{self.manifest.dataset_version}"
        if any(item.subject_id != subject_id for item in self.quality_results):
            raise ValueError("quality result subject does not match dataset identity")
        if dataset_artifact_digest(self.artifacts) != self.manifest.content_sha256:
            raise ValueError("dataset artifact projection does not match aggregate digest")


@dataclass(frozen=True, slots=True)
class FeatureContractRecordError(ValueError):
    reason: str

    def __str__(self) -> str:
        return f"invalid feature-contract registration: {self.reason}"


@dataclass(frozen=True, slots=True)
class FeatureContractInput:
    input_ordinal: int
    input_kind: str
    dataset_id: str | None
    dataset_version: str | None
    upstream_contract_name: str | None
    upstream_contract_version: str | None
    expected_digest_sha256: str

    def __post_init__(self) -> None:
        if self.input_ordinal <= 0:
            raise FeatureContractRecordError("input_ordinal must be positive")
        _require_sha256("expected_digest_sha256", self.expected_digest_sha256)
        allowed_kinds = {"dataset_version", "feature_contract"}
        if self.input_kind not in allowed_kinds:
            raise FeatureContractRecordError(
                "input_kind must be dataset_version or feature_contract"
            )
        is_dataset_input = self.input_kind == "dataset_version"
        if is_dataset_input:
            valid = (
                self.dataset_id is not None
                and bool(self.dataset_id.strip())
                and self.dataset_version is not None
                and bool(self.dataset_version.strip())
                and self.upstream_contract_name is None
                and self.upstream_contract_version is None
            )
        else:
            valid = (
                self.upstream_contract_name is not None
                and bool(self.upstream_contract_name.strip())
                and self.upstream_contract_version is not None
                and bool(self.upstream_contract_version.strip())
                and self.dataset_id is None
                and self.dataset_version is None
            )
        if not valid:
            raise FeatureContractRecordError(
                f"{self.input_kind} input must name exactly one complete subject"
            )


@dataclass(frozen=True, slots=True)
class FeatureContractRegistration:
    contract_name: str
    contract_version: str
    schema_version: int
    parameters: Mapping[str, object]
    definition_artifact_sha256: str
    canonical_serialization_sha256: str
    output_schema_ref: str
    consumes_capital: bool
    consumes_totalreturn: bool
    created_at_utc: datetime
    inputs: tuple[FeatureContractInput, ...]
    canonical_serialization_bytes: bytes | None = None

    def __post_init__(self) -> None:
        for field_name, value in (
            ("contract_name", self.contract_name),
            ("contract_version", self.contract_version),
            ("output_schema_ref", self.output_schema_ref),
        ):
            if not value.strip():
                raise FeatureContractRecordError(f"{field_name} must be nonempty")
        if self.schema_version != 1:
            raise FeatureContractRecordError("schema_version must equal 1")
        _require_sha256("definition_artifact_sha256", self.definition_artifact_sha256)
        _require_sha256("canonical_serialization_sha256", self.canonical_serialization_sha256)
        if self.created_at_utc.tzinfo is None or self.created_at_utc.utcoffset() is None:
            raise FeatureContractRecordError("created_at_utc must be timezone-aware")
        ordinals = tuple(item.input_ordinal for item in self.inputs)
        if len(ordinals) != len(set(ordinals)):
            raise FeatureContractRecordError("input ordinals must be unique")
        frozen_parameters = freeze_json_metadata(self.parameters)
        reject_credential_metadata(frozen_parameters)
        if not isinstance(frozen_parameters, Mapping):
            raise FeatureContractRecordError("parameters must be a JSON object")
        object.__setattr__(self, "parameters", frozen_parameters)
        series_id = frozen_parameters.get("series_id")
        is_derived_macro = self.contract_name in {
            "sp500_dividend_yield",
            "shareholder_yield",
        } or (
            isinstance(series_id, str)
            and series_id in {"sp500_dividend_yield", "shareholder_yield"}
        )
        if is_derived_macro and (
            not self.inputs or any(item.input_kind != "dataset_version" for item in self.inputs)
        ):
            raise FeatureContractRecordError(
                "derived macro contracts require named dataset-version inputs"
            )
        if is_derived_macro and (not self.consumes_capital or self.consumes_totalreturn):
            raise FeatureContractRecordError(
                "derived macro contracts must declare CAPITAL-only basis"
            )
        _bind_canonical_serialization(self)


def feature_contract_canonical_digest(fields: Mapping[str, object]) -> tuple[bytes, str]:
    payload = feature_contract_canonical_bytes(fields)
    return payload, sha256(payload).hexdigest()


def feature_contract_canonical_bytes(fields: Mapping[str, object]) -> bytes:
    """Serialize the admitted feature-contract definition projection."""
    raw_inputs = fields["inputs"]
    if not isinstance(raw_inputs, Sequence) or isinstance(raw_inputs, (str, bytes, bytearray)):
        raise FeatureContractRecordError("inputs must be a sequence")
    inputs = tuple(raw_inputs)
    if any(not isinstance(item, FeatureContractInput) for item in inputs):
        raise FeatureContractRecordError("inputs must be FeatureContractInput rows")
    typed_inputs = cast("tuple[FeatureContractInput, ...]", inputs)
    parameters = fields["parameters"]
    if not isinstance(parameters, Mapping):
        raise FeatureContractRecordError("parameters must be a JSON object")
    return canonical_json_bytes(
        {
            "consumes_capital": fields["consumes_capital"],
            "consumes_totalreturn": fields["consumes_totalreturn"],
            "contract_name": fields["contract_name"],
            "contract_version": fields["contract_version"],
            "definition_artifact_sha256": fields["definition_artifact_sha256"],
            "inputs": [
                {
                    "dataset_id": item.dataset_id,
                    "dataset_version": item.dataset_version,
                    "expected_digest_sha256": item.expected_digest_sha256,
                    "input_kind": item.input_kind,
                    "input_ordinal": item.input_ordinal,
                    "upstream_contract_name": item.upstream_contract_name,
                    "upstream_contract_version": item.upstream_contract_version,
                }
                for item in sorted(typed_inputs, key=lambda row: row.input_ordinal)
            ],
            "output_schema_ref": fields["output_schema_ref"],
            "parameters": dict(parameters),
            "schema_version": fields["schema_version"],
        }
    )


def _bind_canonical_serialization(registration: FeatureContractRegistration) -> None:
    expected_bytes = feature_contract_canonical_bytes(
        {
            "consumes_capital": registration.consumes_capital,
            "consumes_totalreturn": registration.consumes_totalreturn,
            "contract_name": registration.contract_name,
            "contract_version": registration.contract_version,
            "definition_artifact_sha256": registration.definition_artifact_sha256,
            "inputs": registration.inputs,
            "output_schema_ref": registration.output_schema_ref,
            "parameters": registration.parameters,
            "schema_version": registration.schema_version,
        }
    )
    if registration.canonical_serialization_bytes is None:
        raise FeatureContractRecordError(
            "canonical_serialization_bytes are required to bind the claimed digest"
        )
    if registration.canonical_serialization_bytes != expected_bytes:
        raise FeatureContractRecordError(
            "canonical_serialization_bytes do not serialize this registration projection"
        )
    if sha256(expected_bytes).hexdigest() != registration.canonical_serialization_sha256:
        raise FeatureContractRecordError(
            "canonical_serialization_sha256 does not match the registration projection"
        )


def _require_unique_dataset_children(registration: DatasetRegistration) -> list[str]:
    identities = {
        "source": list(registration.manifest.source_snapshot_ids),
        "input": [
            (item.source_snapshot_id, item.relative_path) for item in registration.input_files
        ],
        "artifact": [item.relative_path for item in registration.artifacts],
        "quality result": [item.result_id for item in registration.quality_results],
    }
    for kind, values in identities.items():
        if len(values) != len(set(values)):
            raise ValueError(f"duplicate dataset {kind} identities are forbidden")
    return [item.result_id for item in registration.quality_results]
