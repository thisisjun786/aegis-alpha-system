from __future__ import annotations

import hashlib
from collections.abc import Hashable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Connection, Engine, Table, func, select, text
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.exc import IntegrityError

from aegis_alpha.identity.records import (
    ConflictClass,
    EntityType,
    IdentifierAssertion,
    IdentifierType,
    Instrument,
    Issuer,
    MappingConflict,
    ProviderMapping,
    ResolvedIdentifier,
    intervals_overlap,
)
from aegis_alpha.identity.schema import (
    ADMISSIBLE_SNAPSHOT_STATUSES,
    identity_identifier_assertions,
    identity_instruments,
    identity_issuers,
    identity_mapping_conflicts,
    identity_provider_mappings,
)
from aegis_alpha.metadata.records import validated_json_copy
from aegis_alpha.metadata.schema import source_snapshots

if TYPE_CHECKING:
    from sqlalchemy import Column, ColumnElement
    from sqlalchemy.engine import RowMapping


class IdentityConflictError(RuntimeError):
    """An immutable identity already has a different projection."""


class IdentityAmbiguityError(RuntimeError):
    """Stored identity evidence does not resolve to exactly one instrument."""


class IdentityEvidenceError(RuntimeError):
    """Source evidence does not support the identity fact being asserted."""


@dataclass(frozen=True, slots=True)
class _ImmutableWrite:
    """One idempotent insert-then-compare against an immutable identity row."""

    subject: str
    table: Table
    values: dict[str, object]
    index_elements: list[Column[str]]
    where: ColumnElement[bool]


class IdentityRegistry:
    """Deterministic, fail-closed security identity authority.

    Every write is idempotent on replay and rejects a divergent projection for
    an identity that already exists. No operation resolves a conflict, ranks a
    source, or rewrites source evidence.
    """

    def __init__(self, engine: Engine) -> None:
        if engine.dialect.name != "postgresql":
            raise ValueError("identity registry requires PostgreSQL")
        self._engine = engine

    def register_issuer(self, issuer: Issuer) -> None:
        values: dict[str, object] = {
            "issuer_id": issuer.issuer_id,
            "schema_version": issuer.schema_version,
            "display_name": issuer.display_name,
            "jurisdiction": issuer.jurisdiction,
            "created_at_utc": issuer.created_at_utc,
            "evidence_json": _json_write_copy(issuer.evidence),
        }
        try:
            with self._engine.begin() as connection:
                _insert_and_compare(
                    connection,
                    _ImmutableWrite(
                        subject="issuer",
                        table=identity_issuers,
                        values=values,
                        index_elements=[identity_issuers.c.issuer_id],
                        where=identity_issuers.c.issuer_id == issuer.issuer_id,
                    ),
                )
        except IntegrityError as error:
            raise IdentityConflictError(
                "issuer identity conflicts with immutable evidence"
            ) from error

    def register_instrument(self, instrument: Instrument) -> None:
        values: dict[str, object] = {
            "instrument_id": instrument.instrument_id,
            "issuer_id": instrument.issuer_id,
            "schema_version": instrument.schema_version,
            "instrument_kind": instrument.instrument_kind.value,
            "display_name": instrument.display_name,
            "created_at_utc": instrument.created_at_utc,
            "evidence_json": _json_write_copy(instrument.evidence),
        }
        try:
            with self._engine.begin() as connection:
                issuer_exists = connection.scalar(
                    select(func.count())
                    .select_from(identity_issuers)
                    .where(identity_issuers.c.issuer_id == instrument.issuer_id)
                )
                if not issuer_exists:
                    raise ValueError("unknown issuer_id for instrument")
                _insert_and_compare(
                    connection,
                    _ImmutableWrite(
                        subject="instrument",
                        table=identity_instruments,
                        values=values,
                        index_elements=[identity_instruments.c.instrument_id],
                        where=identity_instruments.c.instrument_id == instrument.instrument_id,
                    ),
                )
        except IntegrityError as error:
            raise IdentityConflictError(
                "instrument identity conflicts with immutable evidence"
            ) from error

    def assert_identifier(self, assertion: IdentifierAssertion) -> None:
        """Record one dated, source-backed identifier assertion.

        The referenced entity and source snapshot must already exist: an
        assertion without registered source evidence is never accepted.
        """
        values: dict[str, object] = {
            "assertion_id": assertion.assertion_id,
            "schema_version": assertion.schema_version,
            "entity_type": assertion.entity_type.value,
            "entity_id": assertion.entity_id,
            "identifier_type": assertion.identifier_type.value,
            "identifier_value": assertion.identifier_value,
            "source_value": assertion.source_value,
            "source_snapshot_id": assertion.source_snapshot_id,
            "effective_start": assertion.effective_start,
            "effective_end": assertion.effective_end,
            "asserted_at_utc": assertion.asserted_at_utc,
            "evidence_json": _json_write_copy(assertion.evidence),
            "assertion_sha256": assertion.assertion_sha256,
        }
        try:
            with self._engine.begin() as connection:
                _require_entity(connection, assertion.entity_type, assertion.entity_id)
                lineage = _require_source_snapshot(connection, assertion.source_snapshot_id)
                values["source_validation_status"] = lineage["validation_status"]
                _reject_contradictory_assertions(connection, assertion)
                natural_key = (
                    (identity_identifier_assertions.c.entity_type == assertion.entity_type.value)
                    & (identity_identifier_assertions.c.entity_id == assertion.entity_id)
                    & (
                        identity_identifier_assertions.c.identifier_type
                        == assertion.identifier_type.value
                    )
                    & (
                        identity_identifier_assertions.c.identifier_value
                        == assertion.identifier_value
                    )
                    & (
                        identity_identifier_assertions.c.effective_start
                        == assertion.effective_start
                    )
                )
                existing = (
                    connection.execute(
                        select(identity_identifier_assertions).where(natural_key).with_for_update()
                    )
                    .mappings()
                    .one_or_none()
                )
                if existing is not None:
                    if existing["assertion_sha256"] == assertion.assertion_sha256:
                        return
                    raise IdentityConflictError(
                        "identifier assertion has a different immutable projection"
                    )
                _insert_and_compare(
                    connection,
                    _ImmutableWrite(
                        subject="identifier assertion",
                        table=identity_identifier_assertions,
                        values=values,
                        index_elements=[identity_identifier_assertions.c.assertion_id],
                        where=(
                            identity_identifier_assertions.c.assertion_id == assertion.assertion_id
                        ),
                    ),
                )
        except IntegrityError as error:
            raise IdentityConflictError(
                "identifier assertion conflicts with immutable evidence"
            ) from error

    def map_provider_identifier(self, mapping: ProviderMapping) -> None:
        """Bind a provider identifier to one instrument over an effective interval.

        Any interval overlap on the same provider key is rejected. When the
        overlapping mapping points at a different instrument, the ambiguity is
        also persisted as durable blocked evidence in the same transaction, so
        the conflict survives the rejection instead of being lost.
        """
        values: dict[str, object] = {
            "mapping_id": mapping.mapping_id,
            "schema_version": mapping.schema_version,
            "provider": mapping.provider,
            "namespace": mapping.namespace,
            "provider_identifier": mapping.provider_identifier,
            "instrument_id": mapping.instrument_id,
            "source_snapshot_id": mapping.source_snapshot_id,
            "effective_start": mapping.effective_start,
            "effective_end": mapping.effective_end,
            "asserted_at_utc": mapping.asserted_at_utc,
            "evidence_json": _json_write_copy(mapping.evidence),
            "mapping_sha256": mapping.mapping_sha256,
        }
        conflict: MappingConflict | None = None
        try:
            with self._engine.begin() as connection:
                _lock_provider_key(connection, mapping)
                _require_instrument(connection, mapping.instrument_id)
                lineage = _require_source_snapshot(
                    connection, mapping.source_snapshot_id, expected_provider=mapping.provider
                )
                values["source_provider"] = lineage["provider"]
                values["source_validation_status"] = lineage["validation_status"]
                siblings = (
                    connection.execute(
                        select(identity_provider_mappings)
                        .where(_provider_key_where(mapping))
                        .order_by(identity_provider_mappings.c.effective_start)
                        .with_for_update()
                    )
                    .mappings()
                    .all()
                )
                for sibling in siblings:
                    if sibling["mapping_sha256"] == mapping.mapping_sha256:
                        return
                    if not intervals_overlap(
                        mapping.effective_start,
                        mapping.effective_end,
                        sibling["effective_start"],
                        sibling["effective_end"],
                    ):
                        continue
                    conflict = _build_conflict(mapping, sibling)
                    _record_conflict(connection, conflict)
                    break
                if conflict is None:
                    _insert_and_compare(
                        connection,
                        _ImmutableWrite(
                            subject="provider mapping",
                            table=identity_provider_mappings,
                            values=values,
                            index_elements=[identity_provider_mappings.c.mapping_id],
                            where=identity_provider_mappings.c.mapping_id == mapping.mapping_id,
                        ),
                    )
        except IntegrityError as error:
            raise IdentityConflictError(
                "provider mapping conflicts with immutable evidence"
            ) from error
        if conflict is not None:
            raise IdentityAmbiguityError(
                f"provider mapping overlaps an existing mapping ({conflict.conflict_class.value})"
            )

    def resolve_instrument(
        self,
        provider: str,
        namespace: str,
        provider_identifier: str,
        as_of: datetime,
    ) -> str | None:
        """Return the instrument a provider identifier meant at ``as_of``.

        ``None`` means unmapped, which is a legitimate state for a
        provider-normalized row. It is never an invitation to guess.
        """
        if as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")
        with self._engine.connect() as connection:
            rows = (
                connection.execute(
                    select(identity_provider_mappings.c.instrument_id).where(
                        (identity_provider_mappings.c.provider == provider)
                        & (identity_provider_mappings.c.namespace == namespace)
                        & (identity_provider_mappings.c.provider_identifier == provider_identifier)
                        & (identity_provider_mappings.c.effective_start <= as_of)
                        & (
                            identity_provider_mappings.c.effective_end.is_(None)
                            | (identity_provider_mappings.c.effective_end > as_of)
                        )
                    )
                )
                .scalars()
                .all()
            )
        distinct = set(rows)
        if not distinct:
            return None
        if len(distinct) > 1:
            raise IdentityAmbiguityError(
                "provider identifier resolves to multiple instruments at this instant"
            )
        return next(iter(distinct))

    def resolve_issuer(self, instrument_id: str) -> str | None:
        with self._engine.connect() as connection:
            return connection.execute(
                select(identity_instruments.c.issuer_id).where(
                    identity_instruments.c.instrument_id == instrument_id
                )
            ).scalar_one_or_none()

    def identifiers_for(
        self,
        entity_type: EntityType,
        entity_id: str,
        as_of: datetime,
        identifier_type: IdentifierType | None = None,
    ) -> tuple[ResolvedIdentifier, ...]:
        if as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")
        where = (
            (identity_identifier_assertions.c.entity_type == EntityType(entity_type).value)
            & (identity_identifier_assertions.c.entity_id == entity_id)
            & (identity_identifier_assertions.c.effective_start <= as_of)
            & (
                identity_identifier_assertions.c.effective_end.is_(None)
                | (identity_identifier_assertions.c.effective_end > as_of)
            )
        )
        if identifier_type is not None:
            where &= (
                identity_identifier_assertions.c.identifier_type
                == IdentifierType(identifier_type).value
            )
        with self._engine.connect() as connection:
            rows = (
                connection.execute(
                    select(identity_identifier_assertions)
                    .where(where)
                    .order_by(
                        identity_identifier_assertions.c.identifier_type,
                        identity_identifier_assertions.c.identifier_value,
                    )
                )
                .mappings()
                .all()
            )
        return tuple(
            ResolvedIdentifier(
                entity_type=EntityType(row["entity_type"]),
                entity_id=row["entity_id"],
                identifier_type=IdentifierType(row["identifier_type"]),
                identifier_value=row["identifier_value"],
                source_value=row["source_value"],
                source_snapshot_id=row["source_snapshot_id"],
                effective_start=row["effective_start"],
                effective_end=row["effective_end"],
            )
            for row in rows
        )

    def list_conflicts(
        self,
        provider: str | None = None,
        namespace: str | None = None,
        provider_identifier: str | None = None,
    ) -> tuple[MappingConflict, ...]:
        statement = select(identity_mapping_conflicts).order_by(
            identity_mapping_conflicts.c.detected_at_utc,
            identity_mapping_conflicts.c.conflict_id,
        )
        if provider is not None:
            statement = statement.where(identity_mapping_conflicts.c.provider == provider)
        if namespace is not None:
            statement = statement.where(identity_mapping_conflicts.c.namespace == namespace)
        if provider_identifier is not None:
            statement = statement.where(
                identity_mapping_conflicts.c.provider_identifier == provider_identifier
            )
        with self._engine.connect() as connection:
            rows = connection.execute(statement).mappings().all()
        return tuple(
            MappingConflict(
                conflict_id=row["conflict_id"],
                provider=row["provider"],
                namespace=row["namespace"],
                provider_identifier=row["provider_identifier"],
                conflict_class=ConflictClass(row["conflict_class"]),
                attempted_instrument_id=row["attempted_instrument_id"],
                attempted_source_snapshot_id=row["attempted_source_snapshot_id"],
                attempted_effective_start=row["attempted_effective_start"],
                attempted_effective_end=row["attempted_effective_end"],
                existing_mapping_id=row["existing_mapping_id"],
                existing_instrument_id=row["existing_instrument_id"],
                detected_at_utc=row["detected_at_utc"],
                details=dict(row["details_json"]),
            )
            for row in rows
        )


def _provider_key_where(mapping: ProviderMapping) -> ColumnElement[bool]:
    return (
        (identity_provider_mappings.c.provider == mapping.provider)
        & (identity_provider_mappings.c.namespace == mapping.namespace)
        & (identity_provider_mappings.c.provider_identifier == mapping.provider_identifier)
    )


def _build_conflict(mapping: ProviderMapping, sibling: RowMapping) -> MappingConflict:
    same_instrument = sibling["instrument_id"] == mapping.instrument_id
    conflict_class = (
        ConflictClass.INTERVAL_OVERLAP if same_instrument else ConflictClass.INSTRUMENT_DISAGREEMENT
    )
    return MappingConflict(
        conflict_id=_conflict_id(mapping, sibling, conflict_class),
        provider=mapping.provider,
        namespace=mapping.namespace,
        provider_identifier=mapping.provider_identifier,
        conflict_class=conflict_class,
        attempted_instrument_id=mapping.instrument_id,
        attempted_source_snapshot_id=mapping.source_snapshot_id,
        attempted_effective_start=mapping.effective_start,
        attempted_effective_end=mapping.effective_end,
        existing_mapping_id=sibling["mapping_id"],
        existing_instrument_id=sibling["instrument_id"],
        detected_at_utc=mapping.asserted_at_utc,
        details={
            "attempted_mapping_id": mapping.mapping_id,
            "attempted_mapping_sha256": mapping.mapping_sha256,
            "existing_effective_start": sibling["effective_start"].isoformat(),
            "existing_effective_end": (
                None if sibling["effective_end"] is None else sibling["effective_end"].isoformat()
            ),
            "existing_source_snapshot_id": sibling["source_snapshot_id"],
        },
    )


def _record_conflict(connection: Connection, conflict: MappingConflict) -> None:
    connection.execute(
        postgres_insert(identity_mapping_conflicts)
        .values(
            {
                "conflict_id": conflict.conflict_id,
                "provider": conflict.provider,
                "namespace": conflict.namespace,
                "provider_identifier": conflict.provider_identifier,
                "conflict_class": conflict.conflict_class.value,
                "attempted_instrument_id": conflict.attempted_instrument_id,
                "attempted_source_snapshot_id": conflict.attempted_source_snapshot_id,
                "attempted_effective_start": conflict.attempted_effective_start,
                "attempted_effective_end": conflict.attempted_effective_end,
                "existing_mapping_id": conflict.existing_mapping_id,
                "existing_instrument_id": conflict.existing_instrument_id,
                "detected_at_utc": conflict.detected_at_utc,
                "details_json": _json_write_copy(conflict.details),
            }
        )
        .on_conflict_do_nothing(index_elements=[identity_mapping_conflicts.c.conflict_id])
    )


def _require_entity(connection: Connection, entity_type: EntityType, entity_id: str) -> None:
    table = identity_issuers if entity_type is EntityType.ISSUER else identity_instruments
    column = next(iter(table.primary_key.columns))
    exists = connection.scalar(select(func.count()).select_from(table).where(column == entity_id))
    if not exists:
        raise ValueError(f"unknown {entity_type.value}_id for identifier assertion")


def _require_instrument(connection: Connection, instrument_id: str) -> None:
    exists = connection.scalar(
        select(func.count())
        .select_from(identity_instruments)
        .where(identity_instruments.c.instrument_id == instrument_id)
    )
    if not exists:
        raise ValueError("unknown instrument_id for provider mapping")


def _require_source_snapshot(
    connection: Connection,
    source_snapshot_id: str,
    expected_provider: str | None = None,
) -> RowMapping:
    """Return the snapshot lineage, refusing evidence that cannot support the fact.

    A snapshot must exist, must be admissible, and — for a provider mapping —
    must come from the same provider the mapping speaks for. Evidence collected
    from one provider is not testimony about another provider's identifier
    namespace, and a BLOCKED snapshot is retained for audit rather than promoted
    into an identity fact.
    """

    lineage = (
        connection.execute(
            select(
                source_snapshots.c.provider,
                source_snapshots.c.validation_status,
            ).where(source_snapshots.c.snapshot_id == source_snapshot_id)
        )
        .mappings()
        .one_or_none()
    )
    if lineage is None:
        raise ValueError("identity evidence requires a registered source snapshot")
    if lineage["validation_status"] not in ADMISSIBLE_SNAPSHOT_STATUSES:
        raise IdentityEvidenceError(
            "identity evidence requires a snapshot with an admissible validation status"
        )
    if expected_provider is not None and lineage["provider"] != expected_provider:
        raise IdentityEvidenceError("provider mapping evidence must come from the same provider")
    return lineage


def _insert_and_compare(connection: Connection, write: _ImmutableWrite) -> None:
    connection.execute(
        postgres_insert(write.table)
        .values(write.values)
        .on_conflict_do_nothing(index_elements=write.index_elements)
    )
    observed = (
        connection.execute(select(write.table).where(write.where).with_for_update())
        .mappings()
        .one()
    )
    if not _projection_matches(observed, write.values):
        raise IdentityConflictError(
            f"{write.subject} identity has a different immutable projection"
        )


def _json_write_copy(value: object) -> dict[str, object]:
    thawed = validated_json_copy(value)
    if not isinstance(thawed, dict):
        raise TypeError("identity JSON metadata must be an object")
    return {str(key): child for key, child in thawed.items()}


def _projection_matches(observed: RowMapping, expected: Mapping[str, object]) -> bool:
    return all(_comparable(observed[key]) == _comparable(value) for key, value in expected.items())


def _comparable(value: object) -> Hashable:
    if isinstance(value, Mapping):
        return tuple(sorted((key, _comparable(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_comparable(item) for item in value)
    return value  # type: ignore[return-value]


def _conflict_id(
    mapping: ProviderMapping,
    sibling: RowMapping,
    conflict_class: ConflictClass,
) -> str:
    """Return a fixed-length, collision-resistant conflict identity.

    Concatenating the two mapping IDs is neither injective (``a:b`` + ``c`` and
    ``a`` + ``b:c`` collide) nor length-bounded (two 255-character IDs overflow
    the column and would lose the evidence row entirely). Digesting a
    length-framed projection of the attempted and existing evidence keeps one
    row per distinct conflict instead.
    """

    parts = (
        mapping.mapping_sha256,
        str(sibling["mapping_sha256"]),
        conflict_class.value,
    )
    framed = "".join(f"{len(part)}:{part}" for part in parts)
    return f"conflict-{hashlib.sha256(framed.encode()).hexdigest()}"


def _lock_provider_key(connection: Connection, mapping: ProviderMapping) -> None:
    """Serialize writers of one provider key for the rest of this transaction.

    ``SELECT ... FOR UPDATE`` locks only rows that already exist, so two
    transactions writing the first (or a gap-filling) mapping for the same key
    would both see no sibling and both commit an overlap. A transaction-scoped
    advisory lock closes that phantom window; the ``EXCLUDE`` constraint remains
    the database-level backstop.
    """

    connection.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {
            "key": f"identity_provider_mapping:{mapping.provider}:{mapping.namespace}:"
            f"{mapping.provider_identifier}"
        },
    )


def _lock_identifier_key(connection: Connection, assertion: IdentifierAssertion) -> None:
    """Serialize writers that could create contradictory identifier history.

    Both contradiction directions are keyed on the identifier type, so one lock
    per type closes the phantom window for the same-entity and same-value checks
    alike without serializing unrelated identifier types.
    """

    connection.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"identity_identifier_assertion:{assertion.identifier_type.value}"},
    )


def _reject_contradictory_assertions(
    connection: Connection,
    assertion: IdentifierAssertion,
) -> None:
    """Refuse assertions that would make identity history self-contradictory.

    Two overlapping facts are contradictory in either direction: one entity
    holding two different values of the same identifier type at once, or one
    identifier value pointing at two different entities at once. Adjacent
    intervals are untouched, so a dated ticker change or a later reuse of a
    retired symbol both remain expressible.
    """

    _lock_identifier_key(connection, assertion)
    same_type = identity_identifier_assertions.c.identifier_type == assertion.identifier_type.value
    # An overlapping row must start before this interval ends. The remaining
    # half of the overlap test needs this row's end, so it is applied in Python
    # through the same helper the records layer documents.
    if assertion.effective_end is not None:
        same_type &= identity_identifier_assertions.c.effective_start < assertion.effective_end
    candidates = (
        connection.execute(
            select(
                identity_identifier_assertions.c.entity_type,
                identity_identifier_assertions.c.entity_id,
                identity_identifier_assertions.c.identifier_value,
                identity_identifier_assertions.c.effective_start,
                identity_identifier_assertions.c.effective_end,
            ).where(same_type)
        )
        .mappings()
        .all()
    )
    entity_key = (assertion.entity_type.value, assertion.entity_id)
    for row in candidates:
        if not intervals_overlap(
            assertion.effective_start,
            assertion.effective_end,
            row["effective_start"],
            row["effective_end"],
        ):
            continue
        row_entity = (row["entity_type"], row["entity_id"])
        same_entity = row_entity == entity_key
        same_value = row["identifier_value"] == assertion.identifier_value
        if same_entity and not same_value:
            raise IdentityAmbiguityError(
                "entity already holds a different value of this identifier type at that time"
            )
        if same_value and not same_entity:
            raise IdentityAmbiguityError(
                "identifier value already belongs to a different entity at that time"
            )
