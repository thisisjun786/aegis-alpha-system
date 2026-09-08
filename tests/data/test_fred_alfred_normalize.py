from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from aegis_alpha.data.fred_alfred_normalize import (
    CURRENT_VINTAGE_END,
    OBSERVATION_KEY,
    NormalizeError,
    assert_unique_keys,
    normalize_observations,
    normalize_value,
    observation_key,
)
from aegis_alpha.data.fred_alfred_series import (
    ALLOWED_SERIES_IDS,
    DEFAULT_SERIES_IDS,
    DEFAULT_SERIES_UNIVERSE_SHA256,
    MACRO_SERIES_IDS,
    MACRO_SERIES_UNIVERSE_SHA256,
    refuse_derived_spread,
    series_universe_sha256,
    validate_series_universe,
    watermark_dataset,
    watermark_stream,
)

STAMP = datetime(2026, 8, 18, 12, 0, tzinfo=UTC)
MACRO_SERIES_COUNT = 35


def _row(
    *,
    series_id: str = "T10Y2Y",
    observation_date: date = date(2024, 1, 15),
    realtime_start: date = date(2024, 1, 16),
    realtime_end: date = date(2024, 3, 14),
    value: str = "0.45",
) -> dict[str, object]:
    rows = normalize_observations(
        [
            {
                "date": observation_date.isoformat(),
                "realtime_start": realtime_start.isoformat(),
                "realtime_end": realtime_end.isoformat(),
                "value": value,
            }
        ],
        series_id=series_id,
        source_snapshot_id="snap-synth",
        raw_content_sha256="a" * 64,
        retrieved_at_utc=STAMP,
        availability_time_utc=STAMP,
    )
    return dict(rows[0])


def test_default_series_universe_hash_is_frozen() -> None:
    assert DEFAULT_SERIES_IDS == ("T10Y2Y", "T10Y3M", "DGS10", "DGS2")
    assert series_universe_sha256(DEFAULT_SERIES_IDS) == DEFAULT_SERIES_UNIVERSE_SHA256
    assert DEFAULT_SERIES_UNIVERSE_SHA256 == (
        "73460d09a4c034abac9b316bd27a915e60720e9813242edda87a77ab2bdc79b8"
    )


def test_collector_rejects_invented_series() -> None:
    with pytest.raises(ValueError, match="must not invent extra series"):
        validate_series_universe(("T10Y2Y", "MULTPL"))


def test_macro_catalog_is_frozen_and_distinct_from_legacy_default() -> None:
    assert len(MACRO_SERIES_IDS) == MACRO_SERIES_COUNT
    assert len(set(MACRO_SERIES_IDS)) == MACRO_SERIES_COUNT
    assert MACRO_SERIES_IDS[:4] == DEFAULT_SERIES_IDS
    assert frozenset(MACRO_SERIES_IDS) == ALLOWED_SERIES_IDS
    assert series_universe_sha256(MACRO_SERIES_IDS) == MACRO_SERIES_UNIVERSE_SHA256
    assert MACRO_SERIES_UNIVERSE_SHA256 != DEFAULT_SERIES_UNIVERSE_SHA256
    # The legacy four hash is pinned regardless of how many series the catalog grows to
    assert DEFAULT_SERIES_UNIVERSE_SHA256 == (
        "73460d09a4c034abac9b316bd27a915e60720e9813242edda87a77ab2bdc79b8"
    )
    assert validate_series_universe(MACRO_SERIES_IDS) == MACRO_SERIES_IDS
    for series_id in MACRO_SERIES_IDS:
        assert series_id == series_id.strip().upper()
        assert watermark_dataset(series_id) == "fred_alfred_observations"
        assert watermark_stream(series_id) == series_id


def test_macro_selection_is_ordered_deterministically_by_catalog() -> None:
    selected = validate_series_universe(("USREC", "CPIAUCSL", "T10Y2Y", "UNRATE"))
    assert selected == ("T10Y2Y", "CPIAUCSL", "UNRATE", "USREC")
    reversed_selection = validate_series_universe(tuple(reversed(MACRO_SERIES_IDS)))
    assert reversed_selection == MACRO_SERIES_IDS
    assert series_universe_sha256(reversed_selection) == MACRO_SERIES_UNIVERSE_SHA256
    assert validate_series_universe(("DGS2", "DGS10", "T10Y3M", "T10Y2Y")) == DEFAULT_SERIES_IDS


def test_macro_selection_rejects_duplicates_unknown_ids_and_wrong_case() -> None:
    with pytest.raises(ValueError, match="duplicate series_id"):
        validate_series_universe(("CPIAUCSL", "UNRATE", "CPIAUCSL"))
    with pytest.raises(ValueError, match="must not invent extra series; rejected: GDPPOT"):
        validate_series_universe(("GDP", "GDPPOT"))
    with pytest.raises(ValueError, match="exact uppercase FRED ids"):
        validate_series_universe(("cpiaucsl",))
    with pytest.raises(ValueError, match="exact uppercase FRED ids"):
        validate_series_universe((" UNRATE",))
    with pytest.raises(ValueError, match="watermark stream is undefined"):
        watermark_stream("GDPPOT")


def test_natural_key_is_series_date_and_vintage_window() -> None:
    assert OBSERVATION_KEY == (
        "series_id",
        "observation_date",
        "realtime_start",
        "realtime_end",
    )
    row = _row()
    key = observation_key(row)
    assert key.as_tuple() == (
        "T10Y2Y",
        date(2024, 1, 15),
        date(2024, 1, 16),
        date(2024, 3, 14),
    )


def test_vintage_restatement_keeps_both_rows() -> None:
    old = _row(realtime_start=date(2024, 1, 16), realtime_end=date(2024, 3, 14), value="0.45")
    new = _row(
        realtime_start=date(2024, 3, 15),
        realtime_end=CURRENT_VINTAGE_END,
        value="0.41",
    )
    assert_unique_keys((old, new))
    assert old["observation_date"] == new["observation_date"]
    assert old["value"] == "0.45"
    assert new["value"] == "0.41"
    assert observation_key(old).as_tuple() != observation_key(new).as_tuple()


def test_duplicate_natural_key_is_rejected() -> None:
    first = _row()
    second = _row()
    with pytest.raises(NormalizeError, match="duplicate natural key"):
        assert_unique_keys((first, second))


def test_fred_dot_missing_token_becomes_null_but_raw_string_is_preserved() -> None:
    assert normalize_value(".") is None
    assert normalize_value("0.45") == "0.45"
    row = _row(value=".")
    assert row["value"] is None


def test_t10y2y_is_not_derived_from_dgs10_minus_dgs2() -> None:
    with pytest.raises(ValueError, match="refusing to derive T10Y2Y"):
        refuse_derived_spread(left="DGS10", right="DGS2")
    official = _row(series_id="T10Y2Y", value="0.45")
    ten_year = _row(series_id="DGS10", value="4.20")
    two_year = _row(series_id="DGS2", value="4.00")
    assert official["value"] != "0.20"
    assert {official["series_id"], ten_year["series_id"], two_year["series_id"]} == {
        "T10Y2Y",
        "DGS10",
        "DGS2",
    }
