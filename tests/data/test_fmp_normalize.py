from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from aegis_alpha.data.fmp_normalize import (
    DATASET_SPECS,
    PRICE_BASIS_DATASETS,
    PROVENANCE_FIELDS,
    NormalizationResult,
    Provenance,
    collected_dates,
    normalize_records,
    normalized_columns,
)
from aegis_alpha.data.fmp_windows import CollectorContractError, parse_universe_manifest

FIXTURE_ROOT = (
    Path(__file__).resolve().parents[1] / "fixtures" / "provider_neutral" / "fmp_collector"
)
RETRIEVED_AT = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
PROVENANCE = Provenance(
    source_receipt_id="synth-receipt-0001",
    raw_content_sha256="c" * 64,
    retrieved_at_utc=RETRIEVED_AT,
)
_PRICE_BASIS_COUNT = 3
_INTEGRAL_FLOAT_VOLUME = 125000


def _records(name: str) -> list[dict[str, Any]]:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _normalize(
    dataset: str,
    fixture: str,
    *,
    symbol: str | None = None,
) -> NormalizationResult:
    return normalize_records(
        dataset=dataset,
        records=_records(fixture),
        provenance=PROVENANCE,
        symbol=symbol,
    )


def test_exactly_six_datasets_are_defined() -> None:
    assert set(DATASET_SPECS) == {
        "fmp_price_eod_full",
        "fmp_price_eod_non_split_adjusted",
        "fmp_price_eod_dividend_adjusted",
        "fmp_splits",
        "fmp_dividends",
        "fmp_profile",
    }


def test_every_dataset_carries_the_four_provenance_columns() -> None:
    for dataset in DATASET_SPECS:
        columns = normalized_columns(dataset)
        for provenance_field in PROVENANCE_FIELDS:
            assert provenance_field.name in columns


def test_price_eod_full_preserves_fields_and_types() -> None:
    result = _normalize("fmp_price_eod_full", "price_eod_full.json", symbol="SYNTH.A")
    assert result.unknown_fields == ()
    first = result.rows[0]
    assert first["date"] == date(2026, 7, 27)
    assert isinstance(first["close"], float)
    assert isinstance(first["volume"], int)
    assert first["provider"] == "fmp"
    assert first["source_receipt_id"] == "synth-receipt-0001"
    assert first["raw_content_sha256"] == "c" * 64
    assert first["retrieved_at_utc"] == RETRIEVED_AT
    # A nullable field may be absent in value but keeps its column.
    assert result.rows[1]["vwap"] is None


def test_the_three_price_bases_stay_separate_and_are_never_derived() -> None:
    full = _normalize("fmp_price_eod_full", "price_eod_full.json")
    non_split = _normalize("fmp_price_eod_non_split_adjusted", "price_eod_non_split_adjusted.json")
    dividend = _normalize("fmp_price_eod_dividend_adjusted", "price_eod_dividend_adjusted.json")

    assert len(PRICE_BASIS_DATASETS) == _PRICE_BASIS_COUNT
    assert len(set(PRICE_BASIS_DATASETS)) == _PRICE_BASIS_COUNT
    # Each basis keeps only its own field vocabulary.
    assert "close" in full.rows[0]
    assert "close" not in non_split.rows[0]
    assert "adjClose" not in full.rows[0]
    # The adjusted bases carry independently collected values, not computations.
    assert non_split.rows[0]["adjClose"] != dividend.rows[0]["adjClose"]
    assert non_split.rows[0]["volume"] != full.rows[0]["volume"]


def test_blank_optional_dividend_dates_are_null() -> None:
    result = _normalize("fmp_dividends", "dividends_blank_declaration.json", symbol="CULL")
    assert result.rows[0]["declarationDate"] == date(2026, 1, 20)
    assert result.rows[1]["declarationDate"] is None


def test_collection_manifest_receipt_active_booleans_parse() -> None:
    document = {
        "generated_at_utc": "2026-08-28T18:07:08.169955+00:00",
        "sources": [
            {
                "endpoint": "/stable/actively-trading-list",
                "retrieved_at_utc": "2026-08-28T18:07:11.003121+00:00",
                "raw_content_sha256": "b" * 64,
            }
        ],
        "entries": [
            {"symbol": "VTSI", "ipoDate": None, "delistedDate": None, "active": True},
            {"symbol": "CULL", "ipoDate": None, "delistedDate": None, "active": True},
        ],
    }

    manifest = parse_universe_manifest(document)
    assert [entry.symbol for entry in manifest.entries] == ["VTSI", "CULL"]
    assert all(entry.active is True for entry in manifest.entries)


def test_splits_and_dividends_preserve_nullable_fields() -> None:
    splits = _normalize("fmp_splits", "splits.json")
    assert splits.rows[0]["splitType"] == "SYNTH_STOCK_SPLIT"
    assert splits.rows[1]["splitType"] is None
    assert isinstance(splits.rows[0]["numerator"], float)

    dividends = _normalize("fmp_dividends", "dividends.json")
    assert dividends.rows[0]["paymentDate"] == date(2026, 6, 1)
    assert dividends.rows[1]["declarationDate"] is None
    assert dividends.rows[1]["yield"] is None


def test_profile_uses_retrieved_at_utc_as_its_observation_date() -> None:
    profile = _normalize("fmp_profile", "profile.json")
    row = profile.rows[0]
    assert row["retrieved_at_utc"] == RETRIEVED_AT
    assert row["cik"] == "0000000001"
    assert row["isActivelyTrading"] is True
    assert row["ipoDate"] == date(2015, 1, 2)
    assert isinstance(row["marketCap"], float)


def test_identifiers_are_persisted_verbatim_as_attributes() -> None:
    profile = _normalize("fmp_profile", "profile.json")
    row = profile.rows[0]
    assert row["cusip"] == "SYNTH0001"
    assert row["isin"] == "SX0000000001"


def test_unknown_fields_are_ignored_in_rows_and_recorded_as_an_observation() -> None:
    result = _normalize("fmp_price_eod_full", "price_eod_full_unknown_field.json")
    assert result.unknown_fields == ("anotherSynthField", "synthUnknownField")
    assert "synthUnknownField" not in result.rows[0]


def test_missing_required_field_blocks_the_run() -> None:
    with pytest.raises(CollectorContractError, match="missing required field 'close'"):
        _normalize("fmp_price_eod_full", "price_eod_full_missing_required.json")


def test_null_required_field_blocks_the_run() -> None:
    with pytest.raises(CollectorContractError, match="cannot be null"):
        normalize_records(
            dataset="fmp_price_eod_full",
            records=[{**_records("price_eod_full.json")[0], "close": None}],
            provenance=PROVENANCE,
        )


def test_wrong_typed_nullable_field_blocks_the_run() -> None:
    with pytest.raises(CollectorContractError, match="vwap must be a float64"):
        _normalize("fmp_price_eod_full", "price_eod_full_wrong_type.json")


def test_wrong_typed_volume_is_not_silently_truncated() -> None:
    result = normalize_records(
        dataset="fmp_price_eod_full",
        records=[
            {**_records("price_eod_full.json")[0], "volume": 125000.7},
            _records("price_eod_full.json")[0],
        ],
        provenance=PROVENANCE,
    )
    assert result.skipped_records == 1
    assert len(result.rows) == 1
    assert isinstance(result.rows[0]["volume"], int)


def test_integral_float_volume_is_accepted() -> None:
    result = normalize_records(
        dataset="fmp_price_eod_full",
        records=[{**_records("price_eod_full.json")[0], "volume": float(_INTEGRAL_FLOAT_VOLUME)}],
        provenance=PROVENANCE,
    )
    assert result.rows[0]["volume"] == _INTEGRAL_FLOAT_VOLUME


def test_boolean_is_not_accepted_as_a_number() -> None:
    with pytest.raises(CollectorContractError, match="must be a float64"):
        normalize_records(
            dataset="fmp_price_eod_full",
            records=[{**_records("price_eod_full.json")[0], "close": True}],
            provenance=PROVENANCE,
        )


def test_a_record_for_another_symbol_blocks_the_run() -> None:
    with pytest.raises(CollectorContractError, match="another symbol"):
        _normalize("fmp_price_eod_full", "price_eod_full.json", symbol="SYNTH.B")


def test_malformed_trade_date_blocks_the_run() -> None:
    with pytest.raises(CollectorContractError, match="ISO calendar date"):
        normalize_records(
            dataset="fmp_price_eod_full",
            records=[{**_records("price_eod_full.json")[0], "date": "2026-13-45"}],
            provenance=PROVENANCE,
        )


def test_trade_dates_are_naive_and_retrieved_at_is_aware_utc() -> None:
    result = _normalize("fmp_price_eod_full", "price_eod_full.json")
    row = result.rows[0]
    trade_date = row["date"]
    retrieved = row["retrieved_at_utc"]
    assert isinstance(trade_date, date)
    assert not isinstance(trade_date, datetime)
    assert isinstance(retrieved, datetime)
    assert retrieved.tzinfo is not None


def test_naive_provenance_timestamp_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Provenance(
            source_receipt_id="synth-receipt-0002",
            raw_content_sha256="d" * 64,
            retrieved_at_utc=datetime(2026, 7, 29, 12, 0),  # noqa: DTZ001 - deliberate
        )


def test_unknown_dataset_is_rejected() -> None:
    with pytest.raises(CollectorContractError, match="unknown provider-normalized dataset"):
        normalize_records(dataset="fmp_unknown", records=[], provenance=PROVENANCE)


def test_collected_dates_are_sorted_and_deduplicated() -> None:
    result = _normalize("fmp_price_eod_full", "price_eod_full.json")
    assert collected_dates(result.rows) == (date(2026, 7, 27), date(2026, 7, 28))
