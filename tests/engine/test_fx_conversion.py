"""The FX conversion rule over explicitly supplied fixings, with independent expectations."""

from __future__ import annotations

from datetime import date

import pytest

from aegis_alpha.engine.fx_conversion import (
    FX_CONVERSION_SCHEMA,
    FX_FIXING_RULE,
    FxConversion,
    FxFixing,
    FxRates,
)

USD_IN_KRW = FxConversion("USD", "KRW", "USD/KRW", 3, "account_currency")
FIXINGS = (
    FxFixing(date(2026, 1, 5), 1000.0),
    FxFixing(date(2026, 1, 6), 1100.0),
    FxFixing(date(2026, 1, 9), 1200.0),
)


def test_a_value_takes_the_latest_fixing_on_or_before_its_date() -> None:
    rates = FxRates(USD_IN_KRW, reversed(FIXINGS))
    assert rates.convert(2.0, date(2026, 1, 5)) == (2000.0, FIXINGS[0])
    assert rates.convert(2.0, date(2026, 1, 6)) == (2200.0, FIXINGS[1])
    # Two days after the last fixing before it, within the three-day age.
    assert rates.convert(2.0, date(2026, 1, 8)) == (2200.0, FIXINGS[1])
    assert rates.convert(2.0, date(2026, 1, 9)) == (2400.0, FIXINGS[2])


def test_no_later_fixing_and_no_fixing_past_its_age_converts_a_value() -> None:
    rates = FxRates(USD_IN_KRW, FIXINGS)
    # Before the first fixing: a later fixing never stands in for an earlier date.
    assert rates.convert(2.0, date(2026, 1, 4)) is None
    # Four days after the last fixing: older than the granted three days.
    assert rates.convert(2.0, date(2026, 1, 13)) is None
    assert rates.fixing_for(date(2026, 1, 12)) == FIXINGS[2]
    strict = FxRates(FxConversion("USD", "KRW", "USD/KRW", 0, "account_currency"), FIXINGS)
    assert strict.convert(2.0, date(2026, 1, 7)) is None


def test_a_series_quoted_the_other_way_divides() -> None:
    conversion = FxConversion("KRW", "USD", "USD/KRW", 0, "price_currency")
    rates = FxRates(conversion, FIXINGS)
    assert conversion.inverse
    assert rates.convert(5500.0, date(2026, 1, 6)) == (5.0, FIXINGS[1])
    assert conversion.document() == {
        "schema": FX_CONVERSION_SCHEMA,
        "rule": FX_FIXING_RULE,
        "currency": "KRW",
        "account_currency": "USD",
        "series_id": "USD/KRW",
        "direction": "divide",
        "max_fixing_age_days": 0,
        "signal_basis": "price_currency",
    }
    assert USD_IN_KRW.document()["direction"] == "multiply"


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (("usd", "KRW", "usd/KRW", 0, "account_currency"), "three-letter"),
        (("KRW", "KRW", "KRW/KRW", 0, "account_currency"), "other than the account"),
        (("USD", "KRW", "EUR/KRW", 0, "account_currency"), "USD/KRW or KRW/USD"),
        (("USD", "KRW", "USD/KRW", -1, "account_currency"), "nonnegative"),
        (("USD", "KRW", "USD/KRW", True, "account_currency"), "nonnegative"),
        (("USD", "KRW", "USD/KRW", 0, "native"), "signal_basis"),
    ],
)
def test_a_conversion_states_its_terms_exactly(arguments: tuple[object, ...], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        FxConversion(*arguments)  # ty: ignore[invalid-argument-type]


@pytest.mark.parametrize("rate", [0.0, -1.0, float("nan"), float("inf"), True])
def test_a_fixing_rate_is_finite_and_positive(rate: float) -> None:
    with pytest.raises(ValueError, match="finite and positive"):
        FxFixing(date(2026, 1, 5), rate)


def test_one_date_holds_one_fixing() -> None:
    with pytest.raises(ValueError, match="repeats one fixing date"):
        FxRates(USD_IN_KRW, (*FIXINGS, FxFixing(date(2026, 1, 6), 1.0)))
