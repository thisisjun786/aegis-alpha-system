"""Round-1 AAS009-NUMERIC-01: non-finite values can never become canonical."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta

import pytest

from aegis_alpha.data.canonical_json import as_float
from aegis_alpha.data.canonical_records import (
    CanonicalBuildError,
    CanonicalCorporateAction,
    CanonicalDisagreement,
    CanonicalErrorCode,
    CanonicalPriceObservation,
    CorporateActionType,
    SourceLineage,
    encode_float,
    require_finite,
)
from aegis_alpha.data.contracts import AdjustmentBasis

OBSERVED = datetime(2020, 1, 15, 21, 0, tzinfo=UTC)
AVAILABLE = OBSERVED + timedelta(hours=1)
NON_FINITE = (float("nan"), float("inf"), float("-inf"))


def lineage(provider: str = "norgate") -> SourceLineage:
    return SourceLineage(
        provider=provider,
        dataset_id="d",
        dataset_version="v",
        source_snapshot_id="snap-1",
        artifact_relative_path="p.parquet",
        artifact_sha256="a" * 64,
        row_ordinal=0,
    )


@pytest.mark.parametrize("value", NON_FINITE)
def test_float_hex_would_have_serialized_non_finite_values(value: float) -> None:
    """Establishes the hazard the guard exists to close."""

    assert not math.isfinite(value)
    assert isinstance(float(value).hex(), str)


@pytest.mark.parametrize("value", NON_FINITE)
def test_require_finite_rejects(value: float) -> None:
    with pytest.raises(CanonicalBuildError) as error:
        require_finite("value", value)
    assert error.value.code is CanonicalErrorCode.SCHEMA_DRIFT


@pytest.mark.parametrize("value", NON_FINITE)
def test_encode_float_refuses_to_digest_a_non_finite_value(value: float) -> None:
    with pytest.raises(CanonicalBuildError):
        encode_float(value)


@pytest.mark.parametrize("value", NON_FINITE)
def test_json_reader_rejects_non_finite_numbers(value: float) -> None:
    with pytest.raises(CanonicalBuildError):
        as_float("close", value)


@pytest.mark.parametrize("field_name", ["open", "high", "low", "close", "unadjusted_close"])
@pytest.mark.parametrize("value", NON_FINITE)
def test_price_observation_rejects_non_finite_fields(field_name: str, value: float) -> None:
    numbers = dict.fromkeys(("open", "high", "low", "close", "unadjusted_close"), 10.0)
    numbers[field_name] = value
    with pytest.raises(CanonicalBuildError) as error:
        CanonicalPriceObservation(
            instrument_id="inst-1",
            observation_date=date(2020, 1, 15),
            adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
            open=numbers["open"],
            high=numbers["high"],
            low=numbers["low"],
            close=numbers["close"],
            volume=1000.0,
            unadjusted_close=numbers["unadjusted_close"],
            dividend=0.0,
            currency="USD",
            observed_at=OBSERVED,
            available_at=AVAILABLE,
            lineage=lineage(),
        )
    assert error.value.code is CanonicalErrorCode.SCHEMA_DRIFT


@pytest.mark.parametrize("value", NON_FINITE)
def test_corporate_action_rejects_a_non_finite_value(value: float) -> None:
    with pytest.raises(CanonicalBuildError):
        CanonicalCorporateAction(
            instrument_id="inst-1",
            action_type=CorporateActionType.DIVIDEND,
            effective_date=date(2020, 1, 15),
            value=value,
            currency="USD",
            observed_at=OBSERVED,
            available_at=AVAILABLE,
            lineage=lineage(),
            derived_from="dividend",
        )


@pytest.mark.parametrize("value", NON_FINITE)
def test_disagreement_rejects_non_finite_comparison_values(value: float) -> None:
    with pytest.raises(CanonicalBuildError):
        CanonicalDisagreement(
            instrument_id="inst-1",
            observation_date=date(2020, 1, 15),
            adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
            field_name="close",
            canonical_provider="norgate",
            canonical_value=10.0,
            comparison_provider="fmp",
            comparison_value=value,
            canonical_lineage=lineage(),
            comparison_lineage=lineage("fmp"),
        )


def test_finite_values_still_round_trip_exactly() -> None:
    value = 0.1 + 0.2
    assert float.fromhex(encode_float(value)) == value
