from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from aegis_alpha.data.contracts import (
    DataQualityResult,
    DatasetManifest,
    Eligibility,
    FallbackDecision,
    FallbackReceipt,
    MappingState,
    QualityStatus,
    SecurityIdentity,
    SourceRole,
    TickerMapping,
)


def test_dataset_manifest_requires_raw_lineage() -> None:
    with pytest.raises(ValueError, match="source snapshot"):
        DatasetManifest(
            dataset_id="canonical-prices",
            dataset_version="2026-07-29.1",
            schema_version=1,
            source_snapshot_ids=(),
            row_count=2,
            coverage_start=date(2026, 7, 28),
            coverage_end=date(2026, 7, 29),
            identity_coverage=1.0,
            freshness_status=QualityStatus.PASS,
            quality_result_ids=("quality-001",),
            transformation_version="1.0.0",
            content_sha256="a" * 64,
            created_at_utc=datetime(2026, 7, 29, 21, 1, tzinfo=UTC),
            eligibility=Eligibility(canonical=True, backtest=True, paper=True, order=False),
        )


def test_blocked_quality_cannot_be_eligible() -> None:
    result = DataQualityResult(
        result_id="quality-001",
        check_id="pit-availability",
        check_version="1.0.0",
        subject_id="canonical-prices",
        status=QualityStatus.BLOCKED,
        dimensions=("temporal_integrity",),
        details=("available_at missing",),
        safe_next_action="collect timestamped source evidence",
        checked_at_utc=datetime(2026, 7, 29, 21, 1, tzinfo=UTC),
    )

    assert result.eligibility() == Eligibility.blocked()


def test_single_passing_quality_check_does_not_grant_dataset_eligibility() -> None:
    result = DataQualityResult(
        result_id="quality-002",
        check_id="schema-integrity",
        check_version="1.0.0",
        subject_id="canonical-prices",
        status=QualityStatus.PASS,
        dimensions=("schema_integrity",),
        details=(),
        safe_next_action="evaluate the complete quality result set",
        checked_at_utc=datetime(2026, 7, 29, 21, 1, tzinfo=UTC),
    )

    assert result.eligibility() == Eligibility.blocked()


def test_eligible_manifest_requires_quality_evidence() -> None:
    with pytest.raises(ValueError, match="quality result"):
        DatasetManifest(
            dataset_id="canonical-prices",
            dataset_version="2026-07-29.1",
            schema_version=1,
            source_snapshot_ids=("snapshot-001",),
            row_count=2,
            coverage_start=date(2026, 7, 28),
            coverage_end=date(2026, 7, 29),
            identity_coverage=1.0,
            freshness_status=QualityStatus.PASS,
            quality_result_ids=(),
            transformation_version="1.0.0",
            content_sha256="a" * 64,
            created_at_utc=datetime(2026, 7, 29, 21, 1, tzinfo=UTC),
            eligibility=Eligibility(canonical=True, backtest=True, paper=False, order=False),
        )


def test_ambiguous_ticker_mapping_is_not_resolved_automatically() -> None:
    identity = SecurityIdentity(
        issuer_id="issuer-001",
        instrument_id="instrument-001",
        asset_id="norgate-123",
        ticker_history=(
            TickerMapping(
                ticker="ABC",
                mic="XNYS",
                effective_from=date(2020, 1, 1),
                effective_to=None,
                source_snapshot_id="snapshot-identity-001",
                state=MappingState.AMBIGUOUS,
            ),
        ),
    )

    with pytest.raises(ValueError, match="ambiguous"):
        identity.ticker_at(date(2026, 7, 29))


def test_fallback_without_semantic_equivalence_requires_owner_approval() -> None:
    decision = FallbackDecision(
        decision_id="fallback-001",
        primary_provider="fmp",
        primary_snapshot_id=None,
        primary_error_code="STALE",
        fallback_provider="official-source",
        fallback_role=SourceRole.OFFICIAL_VERIFIER,
        semantic_compatibility=False,
        reason="primary is stale",
        owner_approval_required=True,
    )

    receipt = FallbackReceipt.from_decision(
        decision,
        fallback_snapshot_id="snapshot-fallback-001",
        content_sha256="b" * 64,
        retrieved_at_utc=datetime(2026, 7, 29, 21, 1, tzinfo=UTC),
        freshness_status=QualityStatus.PASS,
    )

    assert receipt.eligibility == Eligibility.blocked()
    assert receipt.degraded


@pytest.mark.parametrize(
    "fallback_role",
    [
        SourceRole.NO_SAFE_FALLBACK,
        SourceRole.OFFICIAL_VERIFIER,
        SourceRole.HISTORICAL_BACKTEST_REFERENCE,
    ],
)
def test_non_operational_fallback_role_cannot_gain_eligibility(
    fallback_role: SourceRole,
) -> None:
    decision = FallbackDecision(
        decision_id=f"fallback-{fallback_role.value.lower()}",
        primary_provider="fmp",
        primary_snapshot_id="snapshot-primary-001",
        primary_error_code="UNAVAILABLE",
        fallback_provider="non-operational-source",
        fallback_role=fallback_role,
        semantic_compatibility=True,
        reason="primary is unavailable",
        owner_approval_required=False,
    )

    receipt = FallbackReceipt.from_decision(
        decision,
        fallback_snapshot_id="snapshot-fallback-001",
        content_sha256="b" * 64,
        retrieved_at_utc=datetime(2026, 7, 29, 21, 1, tzinfo=UTC),
        freshness_status=QualityStatus.PASS,
    )

    assert receipt.eligibility == Eligibility.blocked()
