from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from aegis_alpha.data.contracts import (
    AdjustmentBasis,
    DataObservation,
    SourcePolicy,
    SourceRole,
)


def test_observation_after_decision_cutoff_is_not_available() -> None:
    observation = DataObservation(
        observation_id="obs-001",
        schema_version=1,
        instrument_id="instrument-001",
        series_id=None,
        value=Decimal("101.25"),
        unit="USD",
        currency="USD",
        observed_at=datetime(2026, 7, 28, 20, 0, tzinfo=UTC),
        available_at=datetime(2026, 7, 29, 1, 0, tzinfo=UTC),
        retrieved_at_utc=datetime(2026, 7, 29, 1, 1, tzinfo=UTC),
        source_snapshot_id="snapshot-001",
        adjustment_basis=AdjustmentBasis.RAW,
        backtest_eligible=True,
    )

    assert not observation.is_available_at(datetime(2026, 7, 29, 0, 59, tzinfo=UTC))
    assert observation.is_available_at(datetime(2026, 7, 29, 1, 0, tzinfo=UTC))


def test_observation_requires_stable_identity_not_ticker() -> None:
    with pytest.raises(ValueError, match="stable instrument_id or series_id"):
        DataObservation(
            observation_id="obs-001",
            schema_version=1,
            instrument_id=None,
            series_id=None,
            value=Decimal("101.25"),
            unit="USD",
            currency="USD",
            observed_at=datetime(2026, 7, 28, 20, 0, tzinfo=UTC),
            available_at=datetime(2026, 7, 29, 1, 0, tzinfo=UTC),
            retrieved_at_utc=datetime(2026, 7, 29, 1, 1, tzinfo=UTC),
            source_snapshot_id="snapshot-001",
            adjustment_basis=AdjustmentBasis.RAW,
            backtest_eligible=True,
        )


def test_observation_cannot_be_available_before_it_occurs() -> None:
    with pytest.raises(ValueError, match="observed_at"):
        DataObservation(
            observation_id="obs-future-001",
            schema_version=1,
            instrument_id="instrument-001",
            series_id=None,
            value=Decimal("101.25"),
            unit="USD",
            currency="USD",
            observed_at=datetime(2026, 7, 29, 1, 0, tzinfo=UTC),
            available_at=datetime(2026, 7, 29, 0, 59, tzinfo=UTC),
            retrieved_at_utc=datetime(2026, 7, 29, 1, 1, tzinfo=UTC),
            source_snapshot_id="snapshot-001",
            adjustment_basis=AdjustmentBasis.RAW,
            backtest_eligible=True,
        )


def test_unknown_license_cannot_be_scheduled() -> None:
    with pytest.raises(ValueError, match="known license"):
        SourcePolicy(
            policy_id="policy-001",
            provider="unverified",
            role=SourceRole.REFERENCE_IDENTITY,
            domains=("identity",),
            fields=("ticker",),
            semantic_compatibility="reference only",
            scheduled_collection_allowed=True,
            canonical_write_allowed=False,
            backtest_eligible=False,
            paper_eligible=False,
            order_eligible=False,
            license_classification="UNKNOWN",
            retention_classification="UNKNOWN",
        )


def test_emergency_source_cannot_be_order_eligible() -> None:
    with pytest.raises(ValueError, match="cannot be order eligible"):
        SourcePolicy(
            policy_id="policy-002",
            provider="emergency-fixture",
            role=SourceRole.EMERGENCY_PAPER_ONLY,
            domains=("price",),
            fields=("close",),
            semantic_compatibility="diagnostic only",
            scheduled_collection_allowed=False,
            canonical_write_allowed=False,
            backtest_eligible=False,
            paper_eligible=True,
            order_eligible=True,
            license_classification="TEST_ONLY",
            retention_classification="EPHEMERAL",
        )


def test_historical_reference_cannot_feed_current_or_scheduled_paths() -> None:
    with pytest.raises(ValueError, match="historical backtest reference"):
        SourcePolicy(
            policy_id="policy-historical-001",
            provider="frozen-history",
            role=SourceRole.HISTORICAL_BACKTEST_REFERENCE,
            domains=("historical_price",),
            fields=("close",),
            semantic_compatibility="frozen historical baseline",
            scheduled_collection_allowed=False,
            canonical_write_allowed=True,
            backtest_eligible=True,
            paper_eligible=True,
            order_eligible=False,
            license_classification="OWNER_CONTROLLED",
            retention_classification="PERMANENT",
        )
