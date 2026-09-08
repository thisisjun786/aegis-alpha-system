from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from aegis_alpha.identity.records import (
    EntityType,
    IdentifierAssertion,
    IdentifierType,
    Instrument,
    InstrumentKind,
    Issuer,
    ProviderMapping,
)
from aegis_alpha.identity.registry import (
    IdentityAmbiguityError,
    IdentityConflictError,
    IdentityRegistry,
)
from aegis_alpha.identity.schema import identity_mapping_conflicts, identity_provider_mappings

if TYPE_CHECKING:
    from sqlalchemy import Engine

_T0 = datetime(2026, 1, 1, tzinfo=UTC)
_T1 = datetime(2026, 2, 1, tzinfo=UTC)
_T2 = datetime(2026, 3, 1, tzinfo=UTC)
_ASSERTED_AT = datetime(2026, 7, 31, 9, 0, tzinfo=UTC)
_EXPECTED_CONFLICT_COUNT = 2


def _issuer(**overrides: object) -> Issuer:
    values: dict[str, object] = {
        "issuer_id": "issuer-apple",
        "created_at_utc": _ASSERTED_AT,
        "display_name": "Apple Inc.",
        "jurisdiction": "US",
        "evidence": {"fixture": "issuer"},
    }
    values.update(overrides)
    return Issuer(**values)  # ty: ignore[invalid-argument-type]


def _instrument(**overrides: object) -> Instrument:
    values: dict[str, object] = {
        "instrument_id": "instrument-apple-common",
        "issuer_id": "issuer-apple",
        "instrument_kind": InstrumentKind.EQUITY,
        "created_at_utc": _ASSERTED_AT,
        "display_name": "Apple common stock",
        "evidence": {"fixture": "instrument"},
    }
    values.update(overrides)
    return Instrument(**values)  # ty: ignore[invalid-argument-type]


def _assertion(**overrides: object) -> IdentifierAssertion:
    values: dict[str, object] = {
        "assertion_id": "assertion-isin",
        "entity_type": EntityType.INSTRUMENT,
        "entity_id": "instrument-apple-common",
        "identifier_type": IdentifierType.ISIN,
        "source_value": "US0378331005",
        "source_snapshot_id": "snapshot-1",
        "effective_start": _T0,
        "effective_end": None,
        "asserted_at_utc": _ASSERTED_AT,
        "evidence": {"fixture": "assertion"},
    }
    values.update(overrides)
    return IdentifierAssertion(**values)  # ty: ignore[invalid-argument-type]


def _mapping(**overrides: object) -> ProviderMapping:
    values: dict[str, object] = {
        "mapping_id": "mapping-1",
        "provider": "norgate",
        "namespace": "us-equities",
        "provider_identifier": "12345",
        "instrument_id": "instrument-apple-common",
        "source_snapshot_id": "snapshot-1",
        "effective_start": _T0,
        "effective_end": _T1,
        "asserted_at_utc": _ASSERTED_AT,
        "evidence": {"fixture": "mapping"},
    }
    values.update(overrides)
    return ProviderMapping(**values)  # ty: ignore[invalid-argument-type]


def _register_entities(registry: IdentityRegistry, *instrument_ids: str) -> None:
    registry.register_issuer(_issuer())
    for instrument_id in instrument_ids:
        registry.register_instrument(_instrument(instrument_id=instrument_id))


def test_register_entities_are_immutable_and_preserve_issuer_ownership(
    identity_registry: IdentityRegistry,
) -> None:
    identity_registry.register_issuer(_issuer())
    identity_registry.register_issuer(_issuer())
    with pytest.raises(IdentityConflictError):
        identity_registry.register_issuer(_issuer(display_name="Different issuer"))
    with pytest.raises(ValueError, match="unknown issuer_id"):
        identity_registry.register_instrument(_instrument(issuer_id="issuer-missing"))

    first = _instrument()
    second = _instrument(instrument_id="instrument-apple-adr", instrument_kind=InstrumentKind.ADR)
    identity_registry.register_instrument(first)
    identity_registry.register_instrument(first)
    with pytest.raises(IdentityConflictError):
        identity_registry.register_instrument(_instrument(display_name="Different instrument"))
    identity_registry.register_instrument(second)

    assert identity_registry.resolve_issuer(first.instrument_id) == "issuer-apple"
    assert identity_registry.resolve_issuer(second.instrument_id) == "issuer-apple"


def test_identifier_assertions_are_source_backed_immutable_and_normalized(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
) -> None:
    _register_entities(identity_registry, "instrument-apple-common")
    register_source_snapshot("snapshot-1")
    register_source_snapshot("snapshot-2")
    isin = _assertion()
    cik = _assertion(
        assertion_id="assertion-cik",
        entity_type=EntityType.ISSUER,
        entity_id="issuer-apple",
        identifier_type=IdentifierType.CIK,
        source_value="320193",
    )
    identity_registry.assert_identifier(isin)
    identity_registry.assert_identifier(isin)
    identity_registry.assert_identifier(cik)
    assert cik.identifier_value == "0000320193"
    with pytest.raises(IdentityConflictError):
        identity_registry.assert_identifier(_assertion(source_snapshot_id="snapshot-2"))
    with pytest.raises(ValueError, match="unknown instrument_id"):
        identity_registry.assert_identifier(
            _assertion(assertion_id="missing-entity", entity_id="missing")
        )
    with pytest.raises(ValueError, match="registered source snapshot"):
        identity_registry.assert_identifier(
            _assertion(assertion_id="missing-source", source_snapshot_id="missing")
        )


def test_provider_mapping_rejects_overlap_persists_evidence_and_accepts_adjacency(
    identity_registry: IdentityRegistry,
    clean_postgres: Engine,
    register_source_snapshot: Callable[..., None],
) -> None:
    _register_entities(identity_registry, "instrument-apple-common", "instrument-other")
    register_source_snapshot("snapshot-1")
    first = _mapping()
    adjacent = _mapping(
        mapping_id="mapping-2",
        effective_start=_T1,
        effective_end=_T2,
        instrument_id="instrument-other",
    )
    identity_registry.map_provider_identifier(first)
    identity_registry.map_provider_identifier(first)
    identity_registry.map_provider_identifier(adjacent)

    disagreement = _mapping(
        mapping_id="mapping-disagreement",
        instrument_id="instrument-other",
        effective_start=_T0 + timedelta(days=1),
        effective_end=_T1,
    )
    with pytest.raises(IdentityAmbiguityError, match="instrument_disagreement"):
        identity_registry.map_provider_identifier(disagreement)
    same_instrument = _mapping(
        mapping_id="mapping-overlap",
        effective_start=_T0 + timedelta(days=2),
        effective_end=_T1,
    )
    with pytest.raises(IdentityAmbiguityError, match="interval_overlap"):
        identity_registry.map_provider_identifier(same_instrument)

    conflicts = identity_registry.list_conflicts("norgate", "us-equities", "12345")
    assert [item.conflict_class.value for item in conflicts] == [
        "instrument_disagreement",
        "interval_overlap",
    ]
    with clean_postgres.connect() as connection:
        assert (
            connection.scalar(select(func.count()).select_from(identity_provider_mappings))
            == _EXPECTED_CONFLICT_COUNT
        )
        assert (
            connection.scalar(select(func.count()).select_from(identity_mapping_conflicts))
            == _EXPECTED_CONFLICT_COUNT
        )


def test_open_intervals_are_rejected_by_api_and_partial_unique_index(
    identity_registry: IdentityRegistry,
    clean_postgres: Engine,
    register_source_snapshot: Callable[..., None],
) -> None:
    _register_entities(identity_registry, "instrument-apple-common", "instrument-other")
    register_source_snapshot("snapshot-1")
    identity_registry.map_provider_identifier(_mapping(effective_end=None))
    with pytest.raises(IdentityAmbiguityError, match="instrument_disagreement"):
        identity_registry.map_provider_identifier(
            _mapping(mapping_id="second-open", instrument_id="instrument-other", effective_end=None)
        )
    with pytest.raises(IntegrityError), clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO identity_provider_mappings (mapping_id, schema_version, provider, "
                "namespace, provider_identifier, instrument_id, source_snapshot_id, "
                "effective_start, "
                "effective_end, asserted_at_utc, evidence_json, mapping_sha256) VALUES "
                "('raw-second-open', 1, 'norgate', 'us-equities', '12345', 'instrument-other', "
                "'snapshot-1', :start, NULL, :asserted, '{}', :sha)"
            ),
            {"start": _T1, "asserted": _ASSERTED_AT, "sha": "2" * 64},
        )


def test_resolve_instrument_uses_half_open_point_in_time_intervals(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
) -> None:
    _register_entities(identity_registry, "instrument-apple-common", "instrument-other")
    register_source_snapshot("snapshot-1")
    assert identity_registry.resolve_instrument("norgate", "us-equities", "unknown", _T0) is None
    identity_registry.map_provider_identifier(_mapping())
    identity_registry.map_provider_identifier(
        _mapping(
            mapping_id="mapping-2",
            instrument_id="instrument-other",
            effective_start=_T1,
            effective_end=_T2,
        )
    )
    assert identity_registry.resolve_instrument("norgate", "us-equities", "12345", _T0) == (
        "instrument-apple-common"
    )
    assert identity_registry.resolve_instrument("norgate", "us-equities", "12345", _T1) == (
        "instrument-other"
    )
    assert identity_registry.resolve_instrument("norgate", "us-equities", "12345", _T2) is None
    with pytest.raises(ValueError, match="timezone-aware"):
        identity_registry.resolve_instrument(
            "norgate", "us-equities", "12345", _T0.replace(tzinfo=None)
        )


def test_identifiers_for_filters_point_in_time_and_keeps_source_value(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
) -> None:
    _register_entities(identity_registry, "instrument-apple-common")
    register_source_snapshot("snapshot-1")
    identity_registry.assert_identifier(
        _assertion(source_value=" us0378331005 ", effective_end=_T1)
    )
    identity_registry.assert_identifier(
        _assertion(
            assertion_id="ticker",
            identifier_type=IdentifierType.TICKER,
            source_value="aapl",
            effective_end=None,
        )
    )
    resolved = identity_registry.identifiers_for(
        EntityType.INSTRUMENT, "instrument-apple-common", _T0, IdentifierType.ISIN
    )
    assert len(resolved) == 1
    assert resolved[0].source_value == " us0378331005 "
    assert resolved[0].identifier_value == "US0378331005"
    assert (
        identity_registry.identifiers_for(
            EntityType.INSTRUMENT, "instrument-apple-common", _T1, IdentifierType.ISIN
        )
        == ()
    )
    assert [
        item.identifier_type
        for item in identity_registry.identifiers_for(
            EntityType.INSTRUMENT, "instrument-apple-common", _T0
        )
    ] == [IdentifierType.ISIN, IdentifierType.TICKER]


def test_ticker_assertions_are_dated_and_independent_of_provider_mapping(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
) -> None:
    _register_entities(identity_registry, "instrument-apple-common", "instrument-other")
    register_source_snapshot("snapshot-1")
    identity_registry.assert_identifier(
        _assertion(identifier_type=IdentifierType.TICKER, source_value="AAPL", effective_end=_T1)
    )
    identity_registry.assert_identifier(
        _assertion(
            assertion_id="ticker-other",
            entity_id="instrument-other",
            identifier_type=IdentifierType.TICKER,
            source_value="AAPL",
            effective_start=_T1,
            effective_end=None,
        )
    )
    identity_registry.map_provider_identifier(
        _mapping(provider_identifier="AAPL", effective_end=None)
    )
    assert identity_registry.resolve_instrument("norgate", "us-equities", "AAPL", _T0) == (
        "instrument-apple-common"
    )


@pytest.mark.parametrize(
    "case",
    [
        (
            "identity_identifier_assertions",
            (
                "assertion_id",
                "schema_version",
                "entity_type",
                "entity_id",
                "identifier_type",
                "identifier_value",
                "source_value",
                "source_snapshot_id",
                "effective_start",
                "effective_end",
                "asserted_at_utc",
                "evidence_json",
                "assertion_sha256",
            ),
            (
                "'bad-level'",
                "1",
                "'instrument'",
                "'instrument-apple-common'",
                "'cik'",
                "'0000320193'",
                "'320193'",
                "'snapshot-1'",
                ":start",
                "NULL",
                ":asserted",
                "'{}'",
                ":sha",
            ),
            {"sha": "3" * 64},
        ),
        (
            "identity_identifier_assertions",
            (
                "assertion_id",
                "schema_version",
                "entity_type",
                "entity_id",
                "identifier_type",
                "identifier_value",
                "source_value",
                "source_snapshot_id",
                "effective_start",
                "effective_end",
                "asserted_at_utc",
                "evidence_json",
                "assertion_sha256",
            ),
            (
                "'bad-syntax'",
                "1",
                "'instrument'",
                "'instrument-apple-common'",
                "'isin'",
                "'INVALID'",
                "'INVALID'",
                "'snapshot-1'",
                ":start",
                "NULL",
                ":asserted",
                "'{}'",
                ":sha",
            ),
            {"sha": "4" * 64},
        ),
        (
            "identity_identifier_assertions",
            (
                "assertion_id",
                "schema_version",
                "entity_type",
                "entity_id",
                "identifier_type",
                "identifier_value",
                "source_value",
                "source_snapshot_id",
                "effective_start",
                "effective_end",
                "asserted_at_utc",
                "evidence_json",
                "assertion_sha256",
            ),
            (
                "'equal-ends'",
                "1",
                "'instrument'",
                "'instrument-apple-common'",
                "'ticker'",
                "'AAPL'",
                "'AAPL'",
                "'snapshot-1'",
                ":start",
                ":start",
                ":asserted",
                "'{}'",
                ":sha",
            ),
            {"sha": "5" * 64},
        ),
        (
            "identity_identifier_assertions",
            (
                "assertion_id",
                "schema_version",
                "entity_type",
                "entity_id",
                "identifier_type",
                "identifier_value",
                "source_value",
                "source_snapshot_id",
                "effective_start",
                "effective_end",
                "asserted_at_utc",
                "evidence_json",
                "assertion_sha256",
            ),
            (
                "'reverse-ends'",
                "1",
                "'instrument'",
                "'instrument-apple-common'",
                "'ticker'",
                "'AAPL'",
                "'AAPL'",
                "'snapshot-1'",
                ":end",
                ":start",
                ":asserted",
                "'{}'",
                ":sha",
            ),
            {"sha": "6" * 64},
        ),
        (
            "identity_instruments",
            (
                "instrument_id",
                "issuer_id",
                "schema_version",
                "instrument_kind",
                "created_at_utc",
                "evidence_json",
            ),
            ("'bad-kind'", "'issuer-apple'", "1", "'bond'", ":asserted", "'{}'"),
            {},
        ),
        (
            "identity_mapping_conflicts",
            (
                "conflict_id",
                "provider",
                "namespace",
                "provider_identifier",
                "conflict_class",
                "attempted_instrument_id",
                "attempted_source_snapshot_id",
                "attempted_effective_start",
                "existing_mapping_id",
                "existing_instrument_id",
                "detected_at_utc",
                "details_json",
            ),
            (
                "'bad-class'",
                "'norgate'",
                "'us-equities'",
                "'12345'",
                "'other'",
                "'instrument-apple-common'",
                "'snapshot-1'",
                ":start",
                "'mapping-valid'",
                "'instrument-apple-common'",
                ":asserted",
                "'{}'",
            ),
            {},
        ),
        (
            "identity_issuers",
            ("issuer_id", "schema_version", "jurisdiction", "created_at_utc", "evidence_json"),
            ("'bad-jurisdiction'", "1", "'1!'", ":asserted", "'{}'"),
            {},
        ),
    ],
)
def test_identity_database_constraints_reject_invalid_raw_rows(
    case: tuple[str, tuple[str, ...], tuple[str, ...], dict[str, object]],
    clean_postgres: Engine,
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
) -> None:
    _register_entities(identity_registry, "instrument-apple-common")
    register_source_snapshot("snapshot-1")
    identity_registry.map_provider_identifier(_mapping(mapping_id="mapping-valid"))
    table_name, columns, values, params = case
    statement = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)})"  # noqa: S608
    with pytest.raises(IntegrityError), clean_postgres.begin() as connection:
        connection.execute(
            text(statement), {"start": _T0, "end": _T1, "asserted": _ASSERTED_AT, **params}
        )
