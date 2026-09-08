"""Round-2 finding: snapshot lineage cannot drift out from under identity evidence.

Child-side synchronization alone only fixes lineage at write time. These tests
cover the other half: a snapshot whose provider or validation status changes
*after* identity evidence already references it.

Deterministic lock-ordering coverage for the concurrent case lives in
``test_lineage_concurrency.py``.
"""

from __future__ import annotations

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
from aegis_alpha.identity.registry import IdentityRegistry

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Engine

_NOW = datetime(2026, 7, 31, tzinfo=UTC)
_START = datetime(2020, 1, 1, tzinfo=UTC)
_LINEAGE_FROZEN = "lineage is immutable"


def _seed(registry: IdentityRegistry) -> None:
    registry.register_issuer(Issuer("iss-r2", _NOW))
    registry.register_instrument(Instrument("ins-r2", "iss-r2", InstrumentKind.EQUITY, _NOW))


def _mapping(mapping_id: str, snapshot_id: str) -> ProviderMapping:
    return ProviderMapping(
        mapping_id=mapping_id,
        provider="norgate",
        namespace="us-equities",
        provider_identifier="77777",
        instrument_id="ins-r2",
        source_snapshot_id=snapshot_id,
        effective_start=_START,
        asserted_at_utc=_NOW,
    )


def _assertion(assertion_id: str, snapshot_id: str) -> IdentifierAssertion:
    return IdentifierAssertion(
        assertion_id=assertion_id,
        entity_type=EntityType.INSTRUMENT,
        entity_id="ins-r2",
        identifier_type=IdentifierType.TICKER,
        source_value="AAPL",
        source_snapshot_id=snapshot_id,
        effective_start=_START,
        asserted_at_utc=_NOW,
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE source_snapshots SET validation_status = 'BLOCKED' WHERE snapshot_id = :sid",
        "UPDATE source_snapshots SET provider = 'fmp' WHERE snapshot_id = :sid",
    ],
)
def test_parent_lineage_cannot_change_under_existing_mapping(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
    clean_postgres: Engine,
    mutation: str,
) -> None:
    """Flipping a referenced snapshot would strand a mapping on bad evidence.

    Without the parent-side guard the mapping keeps its clean stored lineage
    while the snapshot it cites has become BLOCKED or foreign-provider.
    """

    register_source_snapshot("snap-r2", provider="norgate")
    _seed(identity_registry)
    identity_registry.map_provider_identifier(_mapping("m-r2", "snap-r2"))

    with (
        pytest.raises((IntegrityError, DBAPIError), match=_LINEAGE_FROZEN),
        clean_postgres.begin() as connection,
    ):
        connection.execute(text(mutation), {"sid": "snap-r2"})


@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE source_snapshots SET validation_status = 'BLOCKED' WHERE snapshot_id = :sid",
        "UPDATE source_snapshots SET provider = 'fmp' WHERE snapshot_id = :sid",
    ],
)
def test_parent_lineage_cannot_change_under_existing_assertion(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
    clean_postgres: Engine,
    mutation: str,
) -> None:
    register_source_snapshot("snap-r2", provider="norgate")
    _seed(identity_registry)
    identity_registry.assert_identifier(_assertion("a-r2", "snap-r2"))

    with (
        pytest.raises((IntegrityError, DBAPIError), match=_LINEAGE_FROZEN),
        clean_postgres.begin() as connection,
    ):
        connection.execute(text(mutation), {"sid": "snap-r2"})


def test_unreferenced_snapshot_lineage_stays_editable(
    register_source_snapshot: Callable[..., None],
    clean_postgres: Engine,
) -> None:
    """The guard must freeze lineage only once identity evidence depends on it."""

    register_source_snapshot("snap-free", provider="norgate")
    with clean_postgres.begin() as connection:
        connection.execute(
            text(
                "UPDATE source_snapshots SET validation_status = 'BLOCKED' "
                "WHERE snapshot_id = 'snap-free'"
            )
        )
    with clean_postgres.connect() as connection:
        assert (
            connection.execute(
                text(
                    "SELECT validation_status FROM source_snapshots WHERE snapshot_id = 'snap-free'"
                )
            ).scalar_one()
            == "BLOCKED"
        )


def test_unrelated_snapshot_columns_remain_updatable(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
    clean_postgres: Engine,
) -> None:
    """Only provider and validation_status are frozen, not the whole row."""

    register_source_snapshot("snap-r2", provider="norgate")
    _seed(identity_registry)
    identity_registry.map_provider_identifier(_mapping("m-r2", "snap-r2"))

    with clean_postgres.begin() as connection:
        connection.execute(
            text(
                "UPDATE source_snapshots SET provider_watermark = 'later' "
                "WHERE snapshot_id = 'snap-r2'"
            )
        )


def test_forged_child_lineage_is_still_rejected(
    identity_registry: IdentityRegistry,
    register_source_snapshot: Callable[..., None],
    clean_postgres: Engine,
) -> None:
    """Round-1 behavior must survive the round-2 change."""

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
                "VALUES ('forged', 1, 'norgate', 'us-equities', '77777', 'ins-r2', "
                "'snap-blocked', :start, NULL, :now, '{}', :digest, 'norgate', 'PASS')"
            ),
            {"start": _START, "now": _NOW, "digest": "f" * 64},
        )
