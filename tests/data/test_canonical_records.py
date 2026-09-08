from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from aegis_alpha.data.canonical_records import (
    CANONICAL_SCHEMA_VERSION,
    CanonicalBuildError,
    CanonicalCorporateAction,
    CanonicalDisagreement,
    CanonicalErrorCode,
    CanonicalIdentityBinding,
    CanonicalPriceObservation,
    CanonicalQualityDiagnostic,
    CorporateActionType,
    IdentityResolutionState,
    SourceLineage,
    assert_eligibility_blocked,
    encode_float,
)
from aegis_alpha.data.contracts import AdjustmentBasis, Eligibility, QualityStatus

OBSERVED = datetime(2020, 1, 15, 21, 0, tzinfo=UTC)
AVAILABLE = OBSERVED + timedelta(hours=1)


def lineage(  # noqa: PLR0913 - a fixture builder mirrors the record's fields
    *,
    provider: str = "norgate",
    dataset_id: str = "norgate-us-platinum-provider-normalized",
    dataset_version: str = "v1",
    source_snapshot_id: str = "snap-1",
    artifact_relative_path: str = "prices/part-00000.parquet",
    artifact_sha256: str = "a" * 64,
    row_ordinal: int = 0,
) -> SourceLineage:
    return SourceLineage(
        provider=provider,
        dataset_id=dataset_id,
        dataset_version=dataset_version,
        source_snapshot_id=source_snapshot_id,
        artifact_relative_path=artifact_relative_path,
        artifact_sha256=artifact_sha256,
        row_ordinal=row_ordinal,
    )


def observation(  # noqa: PLR0913 - a fixture builder mirrors the record's fields
    *,
    instrument_id: str = "inst-1",
    observation_date: date = date(2020, 1, 15),
    adjustment_basis: AdjustmentBasis = AdjustmentBasis.SPLIT_ADJUSTED,
    close: float = 10.5,
    volume: float = 1000.0,
    available_at: datetime = AVAILABLE,
    quality_flags: tuple[str, ...] = (),
    schema_version: int = CANONICAL_SCHEMA_VERSION,
) -> CanonicalPriceObservation:
    return CanonicalPriceObservation(
        instrument_id=instrument_id,
        observation_date=observation_date,
        adjustment_basis=adjustment_basis,
        open=10.0,
        high=11.0,
        low=9.0,
        close=close,
        volume=volume,
        unadjusted_close=10.5,
        dividend=0.0,
        currency="USD",
        observed_at=OBSERVED,
        available_at=available_at,
        lineage=lineage(),
        quality_flags=quality_flags,
        schema_version=schema_version,
    )


def test_lineage_requires_every_provenance_field() -> None:
    with pytest.raises(CanonicalBuildError) as error:
        lineage(source_snapshot_id="  ")
    assert error.value.code is CanonicalErrorCode.MISSING_LINEAGE


def test_lineage_rejects_a_non_digest_artifact_hash() -> None:
    with pytest.raises(ValueError, match="SHA-256"):
        lineage(artifact_sha256="not-a-digest")


def test_observation_requires_a_resolved_instrument() -> None:
    with pytest.raises(CanonicalBuildError) as error:
        observation(instrument_id="")
    assert error.value.code is CanonicalErrorCode.UNRESOLVED_IDENTITY


def test_observation_rejects_a_basis_outside_the_canonical_set() -> None:
    with pytest.raises(CanonicalBuildError) as error:
        observation(adjustment_basis=AdjustmentBasis.RAW)
    assert error.value.code is CanonicalErrorCode.UNKNOWN_BASIS


def test_observation_rejects_availability_before_observation() -> None:
    with pytest.raises(ValueError, match="available_at cannot precede"):
        observation(available_at=OBSERVED - timedelta(seconds=1))


def test_observation_requires_timezone_aware_instants() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        observation(available_at=datetime(2020, 1, 15, 22, 0))  # noqa: DTZ001


def test_observation_id_is_stable_and_basis_specific() -> None:
    capital = observation()
    same = observation(close=10.5)
    total_return = observation(adjustment_basis=AdjustmentBasis.TOTAL_RETURN)
    assert capital.observation_id == same.observation_id
    assert capital.observation_id != total_return.observation_id


def test_observation_id_is_identity_while_the_digest_carries_content() -> None:
    baseline = observation()
    changed = observation(close=99.0)
    assert baseline.observation_id == changed.observation_id
    assert baseline.digest_projection() != changed.digest_projection()


def test_is_available_at_respects_the_decision_cutoff() -> None:
    row = observation()
    assert row.is_available_at(AVAILABLE)
    assert not row.is_available_at(AVAILABLE - timedelta(seconds=1))


def test_quality_flags_must_be_sorted_for_determinism() -> None:
    with pytest.raises(ValueError, match="sorted"):
        observation(quality_flags=("b_flag", "a_flag"))


def test_unknown_schema_version_is_rejected_not_coerced() -> None:
    with pytest.raises(ValueError, match="unsupported canonical schema_version"):
        observation(schema_version=CANONICAL_SCHEMA_VERSION + 1)


def test_digest_projection_normalizes_equivalent_instants() -> None:
    """The same instant written in another offset must digest identically."""

    shifted = AVAILABLE.astimezone(timezone(timedelta(hours=9)))
    assert shifted != AVAILABLE.replace(tzinfo=None)
    assert (
        observation(available_at=shifted).digest_projection()["available_at"]
        == observation().digest_projection()["available_at"]
    )


def test_float_encoding_round_trips_exactly() -> None:
    value = 0.1 + 0.2
    assert float.fromhex(encode_float(value)) == value


def test_corporate_action_must_name_its_source_field() -> None:
    with pytest.raises(CanonicalBuildError) as error:
        CanonicalCorporateAction(
            instrument_id="inst-1",
            action_type=CorporateActionType.DIVIDEND,
            effective_date=date(2020, 1, 15),
            value=0.25,
            currency="USD",
            observed_at=OBSERVED,
            available_at=AVAILABLE,
            lineage=lineage(),
            derived_from="",
        )
    assert error.value.code is CanonicalErrorCode.MISSING_LINEAGE


def test_dividend_requires_a_positive_value() -> None:
    with pytest.raises(ValueError, match="positive"):
        CanonicalCorporateAction(
            instrument_id="inst-1",
            action_type=CorporateActionType.DIVIDEND,
            effective_date=date(2020, 1, 15),
            value=0.0,
            currency="USD",
            observed_at=OBSERVED,
            available_at=AVAILABLE,
            lineage=lineage(),
            derived_from="dividend",
        )


def test_identity_binding_cannot_be_unresolved_and_carry_an_instrument() -> None:
    with pytest.raises(ValueError, match="cannot carry an instrument_id"):
        CanonicalIdentityBinding(
            provider="norgate",
            namespace="norgate_assetid",
            provider_identifier="101",
            as_of=OBSERVED,
            state=IdentityResolutionState.AMBIGUOUS,
            instrument_id="inst-1",
            issuer_id=None,
        )


def test_quality_diagnostic_cannot_grant_eligibility() -> None:
    diagnostic = CanonicalQualityDiagnostic(
        check_id="check",
        check_version="1",
        status=QualityStatus.PASS,
        subject="dataset@v1",
        details=("rows=1",),
        safe_next_action="none",
    )
    assert diagnostic.eligibility() == Eligibility.blocked()


def test_disagreement_is_always_diagnostic_only() -> None:
    disagreement = CanonicalDisagreement(
        instrument_id="inst-1",
        observation_date=date(2020, 1, 15),
        adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
        field_name="close",
        canonical_provider="norgate",
        canonical_value=10.0,
        comparison_provider="fmp",
        comparison_value=10.5,
        canonical_lineage=lineage(),
        comparison_lineage=lineage(provider="fmp"),
    )
    assert disagreement.resolution.value == "DIAGNOSTIC_ONLY"
    assert disagreement.absolute_difference == pytest.approx(0.5)
    assert disagreement.relative_difference == pytest.approx(0.05)


def test_disagreement_requires_two_distinct_providers() -> None:
    with pytest.raises(ValueError, match="two distinct providers"):
        CanonicalDisagreement(
            instrument_id="inst-1",
            observation_date=date(2020, 1, 15),
            adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
            field_name="close",
            canonical_provider="norgate",
            canonical_value=10.0,
            comparison_provider="norgate",
            comparison_value=10.5,
            canonical_lineage=lineage(),
            comparison_lineage=lineage(),
        )


def test_eligibility_promotion_is_refused() -> None:
    with pytest.raises(CanonicalBuildError) as error:
        assert_eligibility_blocked(
            Eligibility(canonical=True, backtest=True, paper=False, order=False)
        )
    assert error.value.code is CanonicalErrorCode.ELIGIBILITY_PROMOTION


def test_blocked_eligibility_is_accepted() -> None:
    assert_eligibility_blocked(Eligibility.blocked())


def test_no_adjustment_basis_converter_exists() -> None:
    """A converter would be an unproven semantic claim, so none may exist."""

    from aegis_alpha.data import canonical_records  # noqa: PLC0415

    for module in (canonical_records,):
        suspicious = [
            name
            for name in dir(module)
            if any(
                token in name.lower()
                for token in ("convert", "to_total_return", "back_adjust", "reconstruct")
            )
        ]
        assert suspicious == []
