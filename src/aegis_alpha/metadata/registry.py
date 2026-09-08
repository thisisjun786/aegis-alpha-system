from __future__ import annotations

from collections.abc import Callable, Hashable, Iterable, Mapping
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Final

from sqlalchemy import Connection, Engine, Select, Table, select
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.exc import IntegrityError

from aegis_alpha.metadata.feature_contract_schema import (
    feature_contract_inputs,
    feature_contracts,
)
from aegis_alpha.metadata.records import (
    DatasetRegistration,
    FeatureContractInput,
    FeatureContractRegistration,
    SourceSnapshotRegistration,
    validated_json_copy,
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
    from collections.abc import Iterator

    from sqlalchemy.engine import RowMapping
    from sqlalchemy.sql.elements import ColumnElement


class MetadataConflictError(RuntimeError):
    """An immutable registry identity already has a different projection."""


#: A caller-supplied invariant re-checked inside the writing transaction.
#:
#: The callable runs after the rows are written but before COMMIT, so raising
#: from it rolls the whole unit of work back instead of leaving durable state
#: that a later check can only report on.
PrecommitGuard = Callable[[], None]

#: Feature contracts whose frozen definition consumes CAPITAL-basis inputs only.
#: Registering them with any other basis would admit a contract that the
#: derived-build admission can never match (ADR 0010, 010B derived-macro spec).
_CAPITAL_ONLY_CONTRACT_NAMES: Final = frozenset({"sp500_dividend_yield", "shareholder_yield"})


def dataset_input_lock_keys(
    inputs: tuple[FeatureContractInput, ...],
) -> tuple[tuple[str, str], ...]:
    """Stable dataset-id/version order so concurrent registrations cannot deadlock."""
    keys = {
        (item.dataset_id, item.dataset_version)
        for item in inputs
        if item.input_kind == "dataset_version"
        and item.dataset_id is not None
        and item.dataset_version is not None
    }
    return tuple(sorted(keys))


def _validate_feature_contract_input_digests(
    connection: Connection,
    registration: FeatureContractRegistration,
) -> None:
    """Fail registration unless every input digest matches its referenced subject."""
    locked_digests: dict[tuple[str, str], str | None] = {}
    for dataset_id, dataset_version in dataset_input_lock_keys(registration.inputs):
        locked_digests[(dataset_id, dataset_version)] = connection.execute(
            select(dataset_versions.c.aggregate_content_sha256)
            .where(
                (dataset_versions.c.dataset_id == dataset_id)
                & (dataset_versions.c.dataset_version == dataset_version)
            )
            .with_for_update()
        ).scalar_one_or_none()
    for item in registration.inputs:
        if item.input_kind == "dataset_version":
            dataset_id = item.dataset_id
            dataset_version = item.dataset_version
            if dataset_id is None or dataset_version is None:
                raise MetadataConflictError(
                    "feature contract input references an unknown upstream subject"
                )
            observed_digest = locked_digests[(dataset_id, dataset_version)]
        else:
            observed_digest = connection.execute(
                select(feature_contracts.c.canonical_serialization_sha256).where(
                    (feature_contracts.c.contract_name == item.upstream_contract_name)
                    & (feature_contracts.c.contract_version == item.upstream_contract_version)
                )
            ).scalar_one_or_none()
        if observed_digest is None:
            raise MetadataConflictError(
                "feature contract input references an unknown upstream subject"
            )
        if observed_digest != item.expected_digest_sha256:
            raise MetadataConflictError(
                "feature contract input digest does not match the referenced subject"
            )


class MetadataRegistry:
    def __init__(self, engine: Engine) -> None:
        if engine.dialect.name != "postgresql":
            raise ValueError("metadata registry requires PostgreSQL")
        self._engine = engine

    @property
    def engine(self) -> Engine:
        return self._engine

    @contextmanager
    def _transaction(self, connection: Connection | None) -> Iterator[Connection]:
        """Join a caller's transaction, or own one when none was supplied."""

        if connection is not None:
            if connection.engine is not self._engine:
                raise ValueError(
                    "registry operations in one unit of work must share a single PostgreSQL engine"
                )
            # Deliberately no commit: the owning unit of work decides.
            yield connection
            return
        with self._engine.begin() as owned:
            yield owned

    def register_source_snapshot(
        self, registration: SourceSnapshotRegistration, *, connection: Connection | None = None
    ) -> None:
        parent = _source_parent_values(registration)
        children = tuple(
            {
                "snapshot_id": registration.snapshot.snapshot_id,
                "relative_path": item.relative_path,
                "size_bytes": item.size_bytes,
                "content_sha256": item.content_sha256,
            }
            for item in registration.files
        )
        try:
            with self._transaction(connection) as active:
                inserted_snapshot_id = active.execute(
                    postgres_insert(source_snapshots)
                    .values(parent)
                    .on_conflict_do_nothing(index_elements=[source_snapshots.c.snapshot_id])
                    .returning(source_snapshots.c.snapshot_id)
                ).scalar_one_or_none()
                observed = (
                    active.execute(
                        select(source_snapshots)
                        .where(source_snapshots.c.snapshot_id == registration.snapshot.snapshot_id)
                        .with_for_update()
                    )
                    .mappings()
                    .one()
                )
                if not _projection_matches(observed, parent):
                    raise MetadataConflictError(
                        "snapshot identity has a different parent projection"
                    )
                if inserted_snapshot_id is not None:
                    active.execute(source_snapshot_files.insert(), children)
                    return
                observed_children = active.execute(
                    select(source_snapshot_files).where(
                        source_snapshot_files.c.snapshot_id == registration.snapshot.snapshot_id
                    )
                ).mappings()
                if _normalized_rows(observed_children) != _normalized_rows(children):
                    raise MetadataConflictError("snapshot identity has a different file set")
        except IntegrityError as error:
            raise MetadataConflictError(
                "snapshot identity conflicts with immutable evidence"
            ) from error

    def register_dataset(
        self,
        registration: DatasetRegistration,
        *,
        precommit_guard: PrecommitGuard | None = None,
        connection: Connection | None = None,
    ) -> None:
        parent = _dataset_parent_values(registration)
        dataset_key = (
            registration.manifest.dataset_id,
            registration.manifest.dataset_version,
        )
        source_rows = tuple(
            {
                "dataset_id": dataset_key[0],
                "dataset_version": dataset_key[1],
                "source_snapshot_id": source_id,
            }
            for source_id in registration.manifest.source_snapshot_ids
        )
        input_rows = tuple(
            {
                "dataset_id": dataset_key[0],
                "dataset_version": dataset_key[1],
                "source_snapshot_id": item.source_snapshot_id,
                "relative_path": item.relative_path,
            }
            for item in registration.input_files
        )
        artifact_rows = tuple(
            {
                "dataset_id": dataset_key[0],
                "dataset_version": dataset_key[1],
                "relative_path": item.relative_path,
                "media_type": item.media_type,
                "size_bytes": item.size_bytes,
                "row_count": item.row_count,
                "content_sha256": item.content_sha256,
                "partition_values_json": _json_mapping_copy(item.partition_values),
            }
            for item in registration.artifacts
        )
        quality_rows = tuple(
            {
                "result_id": item.result_id,
                "check_id": item.check_id,
                "check_version": item.check_version,
                "status": item.status.value,
                "dimensions_json": _json_sequence_copy(item.dimensions),
                "details_json": _json_sequence_copy(item.details),
                "safe_next_action": item.safe_next_action,
                "checked_at_utc": item.checked_at_utc,
                "source_snapshot_id": None,
                "dataset_id": dataset_key[0],
                "dataset_version": dataset_key[1],
            }
            for item in registration.quality_results
        )

        try:
            with self._transaction(connection) as active:
                existing_inputs = set(
                    active.execute(
                        select(
                            source_snapshot_files.c.snapshot_id,
                            source_snapshot_files.c.relative_path,
                        ).where(
                            source_snapshot_files.c.snapshot_id.in_(
                                registration.manifest.source_snapshot_ids
                            )
                        )
                    ).tuples()
                )
                requested_inputs = {
                    (item.source_snapshot_id, item.relative_path)
                    for item in registration.input_files
                }
                if existing_inputs & requested_inputs != requested_inputs:
                    raise ValueError("input file lineage is not registered")

                inserted_dataset_id = active.execute(
                    postgres_insert(dataset_versions)
                    .values(parent)
                    .on_conflict_do_nothing(
                        index_elements=[
                            dataset_versions.c.dataset_id,
                            dataset_versions.c.dataset_version,
                        ]
                    )
                    .returning(dataset_versions.c.dataset_id)
                ).scalar_one_or_none()
                observed = active.execute(_locked_dataset(dataset_key)).mappings().one()
                if not _projection_matches(observed, parent):
                    raise MetadataConflictError(
                        "dataset identity has a different parent projection"
                    )
                if inserted_dataset_id is not None:
                    active.execute(dataset_sources.insert(), source_rows)
                    active.execute(dataset_input_files.insert(), input_rows)
                    active.execute(dataset_artifacts.insert(), artifact_rows)
                    if quality_rows:
                        active.execute(quality_results.insert(), quality_rows)
                    if precommit_guard is not None:
                        precommit_guard()
                    return

                comparisons = (
                    (
                        "sources",
                        active.execute(
                            select(dataset_sources).where(
                                _dataset_where(dataset_sources, dataset_key)
                            )
                        ).mappings(),
                        source_rows,
                    ),
                    (
                        "inputs",
                        active.execute(
                            select(dataset_input_files).where(
                                _dataset_where(dataset_input_files, dataset_key)
                            )
                        ).mappings(),
                        input_rows,
                    ),
                    (
                        "artifacts",
                        active.execute(
                            select(dataset_artifacts).where(
                                _dataset_where(dataset_artifacts, dataset_key)
                            )
                        ).mappings(),
                        artifact_rows,
                    ),
                    (
                        "quality",
                        active.execute(
                            select(quality_results).where(
                                _dataset_where(quality_results, dataset_key)
                            )
                        ).mappings(),
                        quality_rows,
                    ),
                )
                for child_kind, observed_rows, expected_rows in comparisons:
                    if _normalized_rows(observed_rows) != _normalized_rows(expected_rows):
                        raise MetadataConflictError(
                            f"dataset identity has a different {child_kind} child set"
                        )
                if precommit_guard is not None:
                    precommit_guard()
        except IntegrityError as error:
            raise MetadataConflictError(
                "dataset identity conflicts with immutable evidence"
            ) from error

    def register_feature_contract(self, registration: FeatureContractRegistration) -> None:
        """Register one immutable feature contract and its exact typed input set."""
        if registration.consumes_capital and registration.consumes_totalreturn:
            raise MetadataConflictError("mixed basis feature contracts are forbidden")
        series_id = registration.parameters.get("series_id")
        is_derived_macro = registration.contract_name in _CAPITAL_ONLY_CONTRACT_NAMES or (
            isinstance(series_id, str) and series_id in _CAPITAL_ONLY_CONTRACT_NAMES
        )
        if is_derived_macro and (
            not registration.consumes_capital or registration.consumes_totalreturn
        ):
            raise MetadataConflictError("derived macro contracts must declare CAPITAL-only basis")
        parent = _feature_contract_parent_values(registration)
        contract_key = (registration.contract_name, registration.contract_version)
        for item in registration.inputs:
            if (item.upstream_contract_name, item.upstream_contract_version) == contract_key:
                raise MetadataConflictError(
                    "feature contract inputs cannot reference the contract being registered"
                )
        children = tuple(
            {
                "contract_name": contract_key[0],
                "contract_version": contract_key[1],
                "input_ordinal": item.input_ordinal,
                "input_kind": item.input_kind,
                "dataset_id": item.dataset_id,
                "dataset_version": item.dataset_version,
                "upstream_contract_name": item.upstream_contract_name,
                "upstream_contract_version": item.upstream_contract_version,
                "expected_digest_sha256": item.expected_digest_sha256,
            }
            for item in registration.inputs
        )
        try:
            with self._engine.begin() as connection:
                _validate_feature_contract_input_digests(connection, registration)
                inserted_contract_name = connection.execute(
                    postgres_insert(feature_contracts)
                    .values(parent)
                    .on_conflict_do_nothing(
                        index_elements=[
                            feature_contracts.c.contract_name,
                            feature_contracts.c.contract_version,
                        ]
                    )
                    .returning(feature_contracts.c.contract_name)
                ).scalar_one_or_none()
                observed = (
                    connection.execute(
                        select(feature_contracts)
                        .where(
                            (feature_contracts.c.contract_name == contract_key[0])
                            & (feature_contracts.c.contract_version == contract_key[1])
                        )
                        .with_for_update()
                    )
                    .mappings()
                    .one()
                )
                if not _projection_matches(observed, parent):
                    raise MetadataConflictError(
                        "feature contract identity has a different parent projection"
                    )
                if inserted_contract_name is not None:
                    if children:
                        connection.execute(feature_contract_inputs.insert(), children)
                    return
                observed_children = connection.execute(
                    select(feature_contract_inputs).where(
                        (feature_contract_inputs.c.contract_name == contract_key[0])
                        & (feature_contract_inputs.c.contract_version == contract_key[1])
                    )
                ).mappings()
                if _normalized_rows(observed_children) != _normalized_rows(children):
                    raise MetadataConflictError(
                        "feature contract identity has a different input child set"
                    )
        except IntegrityError as error:
            raise MetadataConflictError(
                "feature contract identity conflicts with immutable evidence"
            ) from error


def _feature_contract_parent_values(
    registration: FeatureContractRegistration,
) -> dict[str, object]:
    return {
        "contract_name": registration.contract_name,
        "contract_version": registration.contract_version,
        "schema_version": registration.schema_version,
        "parameters_json": _json_mapping_copy(registration.parameters),
        "definition_artifact_sha256": registration.definition_artifact_sha256,
        "canonical_serialization_sha256": registration.canonical_serialization_sha256,
        "output_schema_ref": registration.output_schema_ref,
        "consumes_capital": registration.consumes_capital,
        "consumes_totalreturn": registration.consumes_totalreturn,
        "created_at_utc": registration.created_at_utc,
    }


def _source_parent_values(registration: SourceSnapshotRegistration) -> dict[str, object]:
    snapshot = registration.snapshot
    return {
        "snapshot_id": snapshot.snapshot_id,
        "schema_version": snapshot.schema_version,
        "provider": snapshot.provider,
        "dataset": snapshot.dataset,
        "source_uri": snapshot.source_uri,
        "request_fingerprint": snapshot.request_fingerprint,
        "parameters_json": _json_mapping_copy(snapshot.parameters),
        "requested_at_utc": snapshot.requested_at_utc,
        "retrieved_at_utc": snapshot.retrieved_at_utc,
        "provider_published_at_utc": snapshot.provider_published_at,
        "provider_watermark": snapshot.provider_watermark,
        "observation_date": snapshot.observation_date,
        "content_type": snapshot.content_type,
        "encoding": snapshot.encoding,
        "compression": snapshot.compression,
        "raw_byte_length": snapshot.raw_byte_length,
        "content_sha256": snapshot.content_sha256,
        "tree_sha256": registration.tree_sha256,
        "parser_name": snapshot.parser_name,
        "parser_version": snapshot.parser_version,
        "validation_status": snapshot.validation_status.value,
        "row_count": snapshot.row_count,
        "coverage_start": _optional_date(snapshot.coverage_start),
        "coverage_end": _optional_date(snapshot.coverage_end),
        "license_classification": snapshot.license_classification,
        "retention_classification": snapshot.retention_classification,
        "manifest_json": _json_mapping_copy(registration.manifest),
    }


def _dataset_parent_values(registration: DatasetRegistration) -> dict[str, object]:
    manifest = registration.manifest
    return {
        "dataset_id": manifest.dataset_id,
        "dataset_version": manifest.dataset_version,
        "schema_version": manifest.schema_version,
        "row_count": manifest.row_count,
        "coverage_start": manifest.coverage_start,
        "coverage_end": manifest.coverage_end,
        "identity_coverage": Decimal(str(manifest.identity_coverage)),
        "freshness_status": manifest.freshness_status.value,
        "transformation_version": manifest.transformation_version,
        "aggregate_content_sha256": manifest.content_sha256,
        "canonical_eligible": manifest.eligibility.canonical,
        "backtest_eligible": manifest.eligibility.backtest,
        "paper_eligible": manifest.eligibility.paper,
        "order_eligible": manifest.eligibility.order,
        "created_at_utc": manifest.created_at_utc,
    }


def _optional_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value is not None else None


def _json_mapping_copy(value: object) -> dict[str, object]:
    validated = validated_json_copy(value)
    if not isinstance(validated, dict):
        raise TypeError("registry JSON metadata must be an object")
    return {str(key): child for key, child in validated.items()}


def _json_sequence_copy(value: object) -> list[object]:
    validated = validated_json_copy(value)
    if not isinstance(validated, list):
        raise TypeError("registry JSON metadata must be a sequence")
    return list(validated)


def _locked_dataset(dataset_key: tuple[str, str]) -> Select[tuple[object, ...]]:
    return (
        select(dataset_versions)
        .where(_dataset_where(dataset_versions, dataset_key))
        .with_for_update()
    )


def _dataset_where(table: Table, dataset_key: tuple[str, str]) -> ColumnElement[bool]:
    return (table.c.dataset_id == dataset_key[0]) & (table.c.dataset_version == dataset_key[1])


def _projection_matches(
    observed: RowMapping,
    expected: dict[str, object],
) -> bool:
    return _normalize({key: observed[key] for key in expected}) == _normalize(expected)


def _normalized_rows(rows: Iterable[object]) -> set[Hashable]:
    normalized: set[Hashable] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise TypeError("registry projection row must be a mapping")
        normalized.add(_normalize({str(key): value for key, value in row.items()}))
    return normalized


def _normalize(value: object) -> Hashable:
    if isinstance(value, dict):
        return tuple(sorted((key, _normalize(child)) for key, child in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_normalize(child) for child in value)
    if isinstance(value, Decimal):
        return value.normalize()
    if not isinstance(value, Hashable):
        raise TypeError("registry projection contains an unhashable value")
    return value
