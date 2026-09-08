"""Round-1 review findings: evidence admissibility, entity FKs, and contradictions.

Each test states the acceptance criterion it proves and exercises both the
registry API and a raw-SQL path that bypasses it, because a rule that only the
API enforces is a convention rather than a guarantee.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

from aegis_alpha.data.contracts import ValidationStatus
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
    IdentityEvidenceError,
    IdentityRegistry,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Engine

_NOW = datetime(2026, 7, 31, tzinfo=UTC)
_T2020 = datetime(2020, 1, 1, tzinfo=UTC)
_T2021 = datetime(2021, 1, 1, tzinfo=UTC)
_T2022 = datetime(2022, 1, 1, tzinfo=UTC)
_T2023 = datetime(2023, 1, 1, tzinfo=UTC)
_T2024 = datetime(2024, 1, 1, tzinfo=UTC)


def _seed(registry: IdentityRegistry) -> None:
    registry.register_issuer(Issuer("iss-r1", _NOW))
    registry.register_instrument(Instrument("ins-r1a", "iss-r1", InstrumentKind.EQUITY, _NOW))
    registry.register_instrument(Instrument("ins-r1b", "iss-r1", InstrumentKind.EQUITY, _NOW))


def _mapping(
    mapping_id: str,
    instrument_id: str,
    snapshot_id: str,
    *,
    window: tuple[datetime, datetime | None],
    provider: str = "norgate",
) -> ProviderMapping:
    start, end = window
    return ProviderMapping(
        mapping_id=mapping_id,
        provider=provider,
        namespace="us-equities",
        provider_identifier="55555",
        instrument_id=instrument_id,
        source_snapshot_id=snapshot_id,
        effective_start=start,
        asserted_at_utc=_NOW,
        effective_end=end,
    )


_INSTRUMENT_TICKER = (EntityType.INSTRUMENT, IdentifierType.TICKER)
_ISSUER_CIK = (EntityType.ISSUER, IdentifierType.CIK)


def _assertion(  # noqa: PLR0913 - a test factory reads better than a nested options object
    assertion_id: str,
    entity: str,
    source_value: str,
    window: tuple[datetime, datetime | None],
    *,
    level: tuple[EntityType, IdentifierType] = _INSTRUMENT_TICKER,
    snapshot_id: str = "snap-ok",
) -> IdentifierAssertion:
    start, end = window
    entity_type, identifier_type = level
    return IdentifierAssertion(
        assertion_id=assertion_id,
        entity_type=entity_type,
        entity_id=entity,
        identifier_type=identifier_type,
        source_value=source_value,
        source_snapshot_id=snapshot_id,
        effective_start=start,
        asserted_at_utc=_NOW,
        effective_end=end,
    )


def test_mapping_rejects_snapshot_from_a_different_provider(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
) -> None:
    """Evidence collected from one provider is not testimony about another.

    An FMP snapshot cannot establish what a Norgate identifier meant, so the
    mapping must fail closed rather than inherit unrelated provenance.
    """

    register_source_snapshot("snap-fmp", provider="fmp")
    _seed(identity_registry)
    with pytest.raises(IdentityEvidenceError, match="same provider"):
        identity_registry.map_provider_identifier(
            _mapping("m-cross", "ins-r1a", "snap-fmp", window=(_T2020, _T2022))
        )


def test_mapping_rejects_inadmissible_snapshot_status(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
) -> None:
    """A BLOCKED snapshot is retained as evidence but may not back an identity."""

    register_source_snapshot(
        "snap-blocked", provider="norgate", validation_status=ValidationStatus.BLOCKED
    )
    _seed(identity_registry)
    with pytest.raises(IdentityEvidenceError, match="admissible validation status"):
        identity_registry.map_provider_identifier(
            _mapping("m-blocked", "ins-r1a", "snap-blocked", window=(_T2020, _T2022))
        )


def test_assertion_rejects_inadmissible_snapshot_status(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
) -> None:
    register_source_snapshot(
        "snap-blocked", provider="norgate", validation_status=ValidationStatus.BLOCKED
    )
    _seed(identity_registry)
    with pytest.raises(IdentityEvidenceError, match="admissible validation status"):
        identity_registry.assert_identifier(
            _assertion("a-blocked", "ins-r1a", "AAPL", (_T2020, _T2022), snapshot_id="snap-blocked")
        )


def test_warn_snapshots_remain_admissible(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
) -> None:
    """WARN is a quality signal, not a disqualification."""

    register_source_snapshot(
        "snap-warn", provider="norgate", validation_status=ValidationStatus.WARN
    )
    _seed(identity_registry)
    identity_registry.map_provider_identifier(
        _mapping("m-warn", "ins-r1a", "snap-warn", window=(_T2020, _T2022))
    )
    assert (
        identity_registry.resolve_instrument("norgate", "us-equities", "55555", _T2021) == "ins-r1a"
    )


@pytest.mark.parametrize("snapshot", ["snap-fmp", "snap-blocked"])
def test_raw_sql_cannot_forge_mapping_snapshot_lineage(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
    clean_postgres: Engine,
    snapshot: str,
) -> None:
    """The denormalized lineage cannot be lied about behind the API's back.

    A trigger overwrites the claimed provider and status from the referenced
    snapshot, so the CHECK constraints then reject the row.
    """

    register_source_snapshot("snap-fmp", provider="fmp")
    register_source_snapshot(
        "snap-blocked", provider="norgate", validation_status=ValidationStatus.BLOCKED
    )
    _seed(identity_registry)
    with pytest.raises((IntegrityError, DBAPIError)), clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO identity_provider_mappings (mapping_id, schema_version, "
                "provider, namespace, provider_identifier, instrument_id, "
                "source_snapshot_id, effective_start, effective_end, asserted_at_utc, "
                "evidence_json, mapping_sha256, source_provider, source_validation_status) "
                "VALUES ('raw-lineage', 1, 'norgate', 'us-equities', '55555', 'ins-r1a', "
                ":snapshot, :start, :end, :now, '{}', :digest, 'norgate', 'PASS')"
            ),
            {
                "snapshot": snapshot,
                "start": _T2020,
                "end": _T2022,
                "now": _NOW,
                "digest": "c" * 64,
            },
        )


@pytest.mark.parametrize(
    "case",
    [
        ("issuer", "ins-r1a", "cik", "0000320193"),
        ("instrument", "iss-r1", "ticker", "AAPL"),
        ("issuer", "ghost-issuer", "cik", "0000320193"),
        ("instrument", "ghost-instrument", "ticker", "AAPL"),
    ],
)
def test_raw_sql_cannot_assert_against_missing_or_wrong_level_entity(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
    clean_postgres: Engine,
    case: tuple[str, str, str, str],
) -> None:
    """Generated columns carry real FKs, so the level is enforced by the database.

    Without them `entity_id` is polymorphic and unreferenced, letting raw SQL
    attach an issuer-level fact to an instrument or to nothing at all.
    """

    entity_type, entity_id, identifier_type, identifier_value = case
    register_source_snapshot("snap-ok", provider="norgate")
    _seed(identity_registry)
    with pytest.raises((IntegrityError, DBAPIError)), clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO identity_identifier_assertions (assertion_id, schema_version, "
                "entity_type, entity_id, identifier_type, identifier_value, source_value, "
                "source_snapshot_id, effective_start, effective_end, asserted_at_utc, "
                "evidence_json, assertion_sha256, source_validation_status) VALUES "
                "('raw-entity', 1, :entity_type, :entity_id, :identifier_type, :value, "
                ":value, 'snap-ok', :start, :end, :now, '{}', :digest, 'PASS')"
            ),
            {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "identifier_type": identifier_type,
                "value": identifier_value,
                "start": _T2020,
                "end": _T2022,
                "now": _NOW,
                "digest": "d" * 64,
            },
        )


def test_correct_level_assertions_still_succeed(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
) -> None:
    """The new foreign keys must not block legitimate issuer/instrument facts."""

    register_source_snapshot("snap-ok", provider="norgate")
    _seed(identity_registry)
    identity_registry.assert_identifier(
        _assertion("ok-instrument", "ins-r1a", "AAPL", (_T2020, _T2022))
    )
    identity_registry.assert_identifier(
        _assertion(
            "ok-issuer",
            "iss-r1",
            "320193",
            window=(_T2020, _T2022),
            level=_ISSUER_CIK,
        )
    )
    identifiers = identity_registry.identifiers_for(EntityType.ISSUER, "iss-r1", _T2020)
    assert [item.identifier_value for item in identifiers] == ["0000320193"]


def test_same_entity_cannot_hold_two_values_at_once(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
) -> None:
    register_source_snapshot("snap-ok", provider="norgate")
    _seed(identity_registry)
    identity_registry.assert_identifier(_assertion("t1", "ins-r1a", "AAPL", (_T2020, _T2022)))
    with pytest.raises(IdentityAmbiguityError, match="different value"):
        identity_registry.assert_identifier(_assertion("t2", "ins-r1a", "MSFT", (_T2021, _T2023)))


def test_same_value_cannot_name_two_entities_at_once(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
) -> None:
    register_source_snapshot("snap-ok", provider="norgate")
    _seed(identity_registry)
    identity_registry.assert_identifier(_assertion("t1", "ins-r1a", "AAPL", (_T2020, _T2022)))
    with pytest.raises(IdentityAmbiguityError, match="different entity"):
        identity_registry.assert_identifier(_assertion("t2", "ins-r1b", "AAPL", (_T2021, _T2023)))


def test_adjacent_assertions_remain_legal(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
) -> None:
    """A dated ticker change and a later reuse of a retired symbol must work.

    This is the case the contradiction rules must not break: half-open
    intervals make the handover instant unambiguous.
    """

    register_source_snapshot("snap-ok", provider="norgate")
    _seed(identity_registry)
    identity_registry.assert_identifier(_assertion("t1", "ins-r1a", "AAPL", (_T2020, _T2022)))
    identity_registry.assert_identifier(_assertion("t2", "ins-r1a", "MSFT", (_T2022, _T2023)))
    identity_registry.assert_identifier(_assertion("t3", "ins-r1b", "AAPL", (_T2023, _T2024)))
    assert [
        item.identifier_value
        for item in identity_registry.identifiers_for(EntityType.INSTRUMENT, "ins-r1a", _T2022)
    ] == ["MSFT"]


@pytest.mark.parametrize(
    ("second_entity", "second_value"),
    [("ins-r1a", "MSFT"), ("ins-r1b", "AAPL")],
)
def test_raw_sql_cannot_persist_contradictory_assertions(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
    clean_postgres: Engine,
    second_entity: str,
    second_value: str,
) -> None:
    """Exclusion constraints hold in both directions even bypassing the API."""

    register_source_snapshot("snap-ok", provider="norgate")
    _seed(identity_registry)
    identity_registry.assert_identifier(_assertion("t1", "ins-r1a", "AAPL", (_T2020, _T2022)))
    with pytest.raises((IntegrityError, DBAPIError)), clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO identity_identifier_assertions (assertion_id, schema_version, "
                "entity_type, entity_id, identifier_type, identifier_value, source_value, "
                "source_snapshot_id, effective_start, effective_end, asserted_at_utc, "
                "evidence_json, assertion_sha256, source_validation_status) VALUES "
                "('raw-contradiction', 1, 'instrument', :entity, 'ticker', :value, :value, "
                "'snap-ok', :start, :end, :now, '{}', :digest, 'PASS')"
            ),
            {
                "entity": second_entity,
                "value": second_value,
                "start": _T2021,
                "end": _T2023,
                "now": _NOW,
                "digest": "e" * 64,
            },
        )


def test_concurrent_writers_cannot_create_contradictory_assertions(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
    clean_postgres: Engine,
) -> None:
    """Two racing assertions on an empty identifier key must not both commit."""

    register_source_snapshot("snap-ok", provider="norgate")
    _seed(identity_registry)
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def attempt(assertion_id: str, entity_id: str) -> None:
        registry = IdentityRegistry(clean_postgres)
        assertion = _assertion(assertion_id, entity_id, "AAPL", (_T2020, _T2022))
        barrier.wait(timeout=10)
        try:
            registry.assert_identifier(assertion)
        except BaseException as error:  # noqa: BLE001
            errors.append(error)

    threads = [
        threading.Thread(target=attempt, args=("race-a", "ins-r1a")),
        threading.Thread(target=attempt, args=("race-b", "ins-r1b")),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    with clean_postgres.connect() as connection:
        persisted = connection.execute(
            text(
                "SELECT count(*) FROM identity_identifier_assertions "
                "WHERE identifier_value = 'AAPL'"
            )
        ).scalar_one()
    assert persisted == 1, "only one entity may hold a ticker at a given time"
    assert len(errors) == 1, "the losing writer must fail closed"
