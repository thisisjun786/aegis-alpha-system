from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from aegis_alpha.identity.records import (
    IdentifierType,
    IdentifierValueError,
    Instrument,
    InstrumentKind,
    Issuer,
    ProviderMapping,
    normalize_identifier,
)
from aegis_alpha.identity.registry import IdentityAmbiguityError, IdentityRegistry

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Engine

_NOW = datetime(2026, 7, 31, tzinfo=UTC)
_SNAPSHOT_ID = "identity-hardening-snapshot"
_CONFLICT_ID_MAX_LENGTH = 255


@pytest.mark.parametrize(
    ("identifier_type", "laundered_source"),
    [
        (IdentifierType.TICKER, "\uff21\uff21\uff30\uff2c"),
        (IdentifierType.TICKER, "\ufb03"),
        (IdentifierType.TICKER, "\u00df"),
        (IdentifierType.TICKER, "\u0131"),
        (IdentifierType.NORGATE_ASSETID, "\uff11\uff12\uff13"),
        (IdentifierType.NORGATE_ASSETID, "\u00b923"),
    ],
)
def test_compatibility_characters_are_never_laundered_into_identifiers(
    identifier_type: IdentifierType,
    laundered_source: str,
) -> None:
    """Unicode compatibility forms must not become clean-looking identifiers.

    Without an ASCII allowlist, NFKC would turn fullwidth latin letters into
    ``AAPL`` and a superscript digit sequence into ``123``, manufacturing a
    valid-looking identity from source evidence that was never valid.
    """

    with pytest.raises(IdentifierValueError, match="ASCII identifier characters"):
        normalize_identifier(identifier_type, laundered_source)


def test_ascii_sources_still_normalize() -> None:
    assert normalize_identifier(IdentifierType.TICKER, " brk.b ") == "BRK.B"
    assert normalize_identifier(IdentifierType.CIK, " 320193 ") == "0000320193"


def _seed_entities(registry: IdentityRegistry) -> None:
    registry.register_issuer(Issuer("iss-h", _NOW))
    registry.register_instrument(Instrument("ins-h1", "iss-h", InstrumentKind.EQUITY, _NOW))
    registry.register_instrument(Instrument("ins-h2", "iss-h", InstrumentKind.EQUITY, _NOW))


def _mapping(
    mapping_id: str,
    instrument_id: str,
    snapshot_id: str,
    start: datetime,
    end: datetime | None,
) -> ProviderMapping:
    return ProviderMapping(
        mapping_id=mapping_id,
        provider="norgate",
        namespace="us-equities",
        provider_identifier="98765",
        instrument_id=instrument_id,
        source_snapshot_id=snapshot_id,
        effective_start=start,
        asserted_at_utc=_NOW,
        effective_end=end,
    )


def test_conflict_identity_is_length_bounded_and_unambiguous(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
) -> None:
    """Conflict IDs must survive maximum-length mapping IDs.

    Concatenating two 255-character mapping IDs would overflow the column and
    lose the blocked evidence entirely, so the conflict identity is a digest.
    """

    register_source_snapshot(_SNAPSHOT_ID)
    _seed_entities(identity_registry)
    long_a = "a" * 200
    long_b = "b" * 200
    first = _mapping(
        long_a,
        "ins-h1",
        _SNAPSHOT_ID,
        datetime(2020, 1, 1, tzinfo=UTC),
        datetime(2022, 1, 1, tzinfo=UTC),
    )
    identity_registry.map_provider_identifier(first)

    overlapping = _mapping(
        long_b,
        "ins-h2",
        _SNAPSHOT_ID,
        datetime(2021, 1, 1, tzinfo=UTC),
        datetime(2023, 1, 1, tzinfo=UTC),
    )
    with pytest.raises(IdentityAmbiguityError):
        identity_registry.map_provider_identifier(overlapping)

    conflicts = identity_registry.list_conflicts(provider="norgate")
    assert len(conflicts) == 1
    assert len(conflicts[0].conflict_id) <= _CONFLICT_ID_MAX_LENGTH
    assert conflicts[0].attempted_instrument_id == "ins-h2"
    assert conflicts[0].existing_instrument_id == "ins-h1"


def test_database_rejects_overlap_inserted_behind_the_registry_api(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
    clean_postgres: Engine,
) -> None:
    """The EXCLUDE constraint holds even when a writer bypasses the API."""

    register_source_snapshot(_SNAPSHOT_ID)
    _seed_entities(identity_registry)
    identity_registry.map_provider_identifier(
        _mapping(
            "map-h1",
            "ins-h1",
            _SNAPSHOT_ID,
            datetime(2020, 1, 1, tzinfo=UTC),
            datetime(2022, 1, 1, tzinfo=UTC),
        )
    )
    raw = _mapping(
        "map-h2",
        "ins-h2",
        _SNAPSHOT_ID,
        datetime(2021, 1, 1, tzinfo=UTC),
        datetime(2023, 1, 1, tzinfo=UTC),
    )
    with pytest.raises(IntegrityError), clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO identity_provider_mappings (mapping_id, schema_version, "
                "provider, namespace, provider_identifier, instrument_id, "
                "source_snapshot_id, effective_start, effective_end, asserted_at_utc, "
                "evidence_json, mapping_sha256) VALUES (:mapping_id, 1, :provider, "
                ":namespace, :provider_identifier, :instrument_id, :snapshot, :start, "
                ":end, :asserted, '{}', :digest)"
            ),
            {
                "mapping_id": raw.mapping_id,
                "provider": raw.provider,
                "namespace": raw.namespace,
                "provider_identifier": raw.provider_identifier,
                "instrument_id": raw.instrument_id,
                "snapshot": raw.source_snapshot_id,
                "start": raw.effective_start,
                "end": raw.effective_end,
                "asserted": raw.asserted_at_utc,
                "digest": raw.mapping_sha256,
            },
        )


def test_concurrent_writers_cannot_persist_overlapping_mappings(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
    clean_postgres: Engine,
) -> None:
    """Two racing writers on an empty provider key must not both commit.

    ``SELECT ... FOR UPDATE`` locks only existing rows, so without the advisory
    lock and the EXCLUDE constraint both transactions would see no sibling and
    both insert an overlapping interval.
    """

    register_source_snapshot(_SNAPSHOT_ID)
    _seed_entities(identity_registry)
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def attempt(mapping_id: str, instrument_id: str, start: datetime, end: datetime) -> None:
        registry = IdentityRegistry(clean_postgres)
        mapping = _mapping(mapping_id, instrument_id, _SNAPSHOT_ID, start, end)
        barrier.wait(timeout=10)
        try:
            registry.map_provider_identifier(mapping)
        except BaseException as error:  # noqa: BLE001
            errors.append(error)

    threads = [
        threading.Thread(
            target=attempt,
            args=(
                "race-1",
                "ins-h1",
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 3, 1, tzinfo=UTC),
            ),
        ),
        threading.Thread(
            target=attempt,
            args=(
                "race-2",
                "ins-h2",
                datetime(2026, 2, 1, tzinfo=UTC),
                datetime(2026, 4, 1, tzinfo=UTC),
            ),
        ),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    with clean_postgres.connect() as connection:
        persisted = connection.execute(
            text(
                "SELECT count(*) FROM identity_provider_mappings "
                "WHERE provider_identifier = '98765'"
            )
        ).scalar_one()
    assert persisted == 1, "exactly one of two overlapping concurrent writers may win"
    assert len(errors) == 1, "the losing writer must fail closed"
