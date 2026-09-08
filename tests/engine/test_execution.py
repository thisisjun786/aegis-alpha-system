"""Independent analytic cases for next-open accounting; no market or private recipes."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import cast

import pytest

from aegis_alpha.engine.execution import Fill, NavPoint, ReplayResult, replay_next_open

DAYS = [date(2024, 1, 31), date(2024, 2, 1), date(2024, 2, 2)]


def test_public_types_are_frozen_nav_fill_and_result() -> None:
    result = replay_next_open(
        DAYS,
        [{"SYN_A": 100}, {"SYN_A": 200}, {"SYN_A": 300}],
        [{"SYN_A": 100}, {"SYN_A": 250}, {"SYN_A": 400}],
        {DAYS[0]: {"SYN_A": 1}},
        1000,
        0,
    )
    assert isinstance(result, ReplayResult)
    assert isinstance(result.nav[0], NavPoint)
    assert isinstance(result.fills[0], Fill)
    with pytest.raises(AttributeError):
        result.nav[0].equity = 0  # ty: ignore[invalid-assignment]


def test_buy_after_gap_keeps_terminal_position_without_liquidation() -> None:
    # Independent: buy 1000/200=5 at next open; mark 5*250 then 5*400; cash residual 0.
    result = replay_next_open(
        DAYS,
        [{"SYN_A": 100}, {"SYN_A": 200}, {"SYN_A": 300}],
        [{"SYN_A": 100}, {"SYN_A": 250}, {"SYN_A": 400}],
        {DAYS[0]: {"SYN_A": 1}},
        1000,
        0,
    )
    assert [point.equity for point in result.nav] == pytest.approx([1000, 1250, 2000])
    assert [point.cash for point in result.nav] == pytest.approx([1000, 0, 0])
    assert [point.fee for point in result.nav] == pytest.approx([0, 0, 0])
    assert len(result.fills) == 1
    fill = result.fills[0]
    assert fill.decision_date == DAYS[0]
    assert fill.execution_date == DAYS[1]
    assert fill.symbol == "SYN_A"
    assert fill.price == pytest.approx(200)
    assert fill.shares == pytest.approx(5)
    assert fill.fee == pytest.approx(0)
    assert result.nav[-1].date == DAYS[-1]


def test_overnight_holding_marks_open_gap_before_sale() -> None:
    # Independent: 10 shares overnight; open 200 then sell to cash 2000; close 300 unused.
    result = replay_next_open(
        DAYS,
        [{"SYN_A": 100}, {"SYN_A": 100}, {"SYN_A": 200}],
        [{"SYN_A": 100}, {"SYN_A": 100}, {"SYN_A": 300}],
        {DAYS[0]: {"SYN_A": 1}, DAYS[1]: {}},
        1000,
        0,
    )
    assert result.nav[-1].equity == pytest.approx(2000)
    assert result.nav[-1].cash == pytest.approx(2000)
    assert result.fills[-1].shares == pytest.approx(-10)
    assert result.fills[-1].execution_date == DAYS[2]


def test_buy_and_sell_fees_are_independent_both_side_turnover() -> None:
    # Independent: post = 1000/1.01 after buy; then sale fee 1% of that notional.
    result = replay_next_open(
        DAYS,
        [{"SYN_A": 100}] * 3,
        [{"SYN_A": 100}] * 3,
        {DAYS[0]: {"SYN_A": 1}, DAYS[1]: {}},
        1000,
        0.01,
    )
    expected_after_buy = 1000 / 1.01
    expected_after_sale = expected_after_buy * 0.99
    assert result.nav[1].equity == pytest.approx(expected_after_buy)
    assert result.nav[-1].equity == pytest.approx(expected_after_sale)
    assert result.nav[-1].cash == pytest.approx(expected_after_sale)
    assert sum(fill.fee for fill in result.fills) == pytest.approx(1000 - expected_after_sale)
    assert result.fills[0].fee == pytest.approx(expected_after_buy * 0.01)
    assert result.fills[1].fee == pytest.approx(expected_after_buy * 0.01)


def test_unallocated_cash_residual_and_next_supplied_session() -> None:
    days = [date(2024, 5, 31), date(2024, 6, 3)]
    result = replay_next_open(
        days,
        [{"SYN_A": 100}] * 2,
        [{"SYN_A": 100}, {"SYN_A": 200}],
        {days[0]: {"SYN_A": 0.5}},
        1000,
        0,
    )
    assert result.nav[-1].equity == pytest.approx(1500)
    assert result.nav[-1].cash == pytest.approx(500)
    assert result.fills[0].shares == pytest.approx(5)


def test_costly_swap_charges_sale_and_purchase_without_phantom_fills() -> None:
    prices = [{"SYN_A": 100, "SYN_B": 100}] * 3
    result = replay_next_open(
        DAYS, prices, prices, {DAYS[0]: {"SYN_A": 1}, DAYS[1]: {"SYN_B": 1}}, 1000, 0.01
    )
    expected = 1000 / 1.01 * 0.99 / 1.01
    assert result.nav[-1].equity == pytest.approx(expected)
    assert [fill.symbol for fill in result.fills] == ["SYN_A", "SYN_A", "SYN_B"]
    unchanged = replay_next_open(
        DAYS, prices, prices, {DAYS[0]: {"SYN_A": 0.5}, DAYS[1]: {"SYN_A": 0.5}}, 1000, 0.01
    )
    expected_half = 1000 / 1.005
    assert len(unchanged.fills) == 1
    assert unchanged.nav[-1].fee == 0
    assert unchanged.nav[-1].equity == pytest.approx(expected_half)
    assert unchanged.nav[-1].cash == pytest.approx(0.5 * expected_half)


def test_late_listing_and_missing_open_or_close_never_forward_fills() -> None:
    result = replay_next_open(
        DAYS, [{}, {}, {"SYN_A": 100}], [{}, {}, {"SYN_A": 120}], {DAYS[1]: {"SYN_A": 1}}, 1000, 0
    )
    assert result.nav[-1].equity == pytest.approx(1200)
    with pytest.raises(ValueError, match="observed open"):
        replay_next_open(
            DAYS,
            [{}, {"SYN_A": 100}, {}],
            [{}, {"SYN_A": 100}, {}],
            {DAYS[0]: {"SYN_A": 1}},
            1000,
            0,
        )
    held = replay_next_open(
        DAYS,
        [{"SYN_A": 100}, {"SYN_A": 100}, {"SYN_A": 100}],
        [{"SYN_A": 100}, {"SYN_A": 100}, {"SYN_A": 100}],
        {DAYS[0]: {"SYN_A": 1}},
        1000,
        0,
    )
    assert held.nav[-1].equity == pytest.approx(1000)
    with pytest.raises(ValueError, match="observed open"):
        replay_next_open(
            DAYS,
            [{"SYN_A": 100}, {"SYN_A": 100}, {"SYN_A": 100}],
            [{"SYN_A": 100}, {"SYN_A": 100}, {}],
            {DAYS[0]: {"SYN_A": 1}},
            1000,
            0,
        )


@pytest.mark.parametrize("bad", [None, float("nan"), 0, -1, float("inf")])
def test_missing_or_invalid_open_never_falls_back_to_close(bad: float | None) -> None:
    opening = [{"SYN_A": 100}, {} if bad is None else {"SYN_A": bad}, {"SYN_A": 100}]
    with pytest.raises(ValueError, match="observed open"):
        replay_next_open(DAYS, opening, [{"SYN_A": 100}] * 3, {DAYS[0]: {"SYN_A": 1}}, 1000, 0)


def test_last_or_unknown_decision_has_no_execution_session() -> None:
    for day in [DAYS[-1], date(2024, 2, 3)]:
        with pytest.raises(ValueError, match="following"):
            replay_next_open(
                DAYS, [{"SYN_A": 100}] * 3, [{"SYN_A": 100}] * 3, {day: {"SYN_A": 1}}, 1000, 0
            )


@pytest.mark.parametrize(
    "weights",
    [{"SYN_A": -0.1}, {"SYN_A": 1.1}, {"SYN_A": float("nan")}, {"CASH": 1}, {"SYN_A": True}],
)
def test_invalid_weights_are_rejected(weights: dict[str, object]) -> None:
    with pytest.raises(ValueError, match=r"target weights|cash is"):
        replay_next_open(
            DAYS,
            [{"SYN_A": 100}] * 3,
            [{"SYN_A": 100}] * 3,
            {DAYS[0]: cast("dict[str, float]", weights)},
            1000,
            0,
        )


@pytest.mark.parametrize(
    "payload",
    [
        [datetime(2024, 1, 31, tzinfo=UTC), date(2024, 2, 1)],
        [True, date(2024, 2, 1)],
        [date(2024, 1, 31), date(2024, 1, 31)],
        [date(2024, 2, 1), date(2024, 1, 31)],
        [date(2024, 1, 31)],
    ],
)
def test_invalid_calendar_dates_are_rejected(payload: list[object]) -> None:
    count = max(len(payload), 2)
    prices = [{"SYN_A": 100}] * count
    with pytest.raises(ValueError, match=r"datetime.date|strictly increasing|two sessions"):
        replay_next_open(cast("list[date]", payload), prices, prices, {}, 1000, 0)


@pytest.mark.parametrize("cash", [0, -1, float("nan"), float("inf"), True])
def test_nonpositive_or_nonfinite_cash_is_rejected(cash: object) -> None:
    with pytest.raises(ValueError, match="initial cash"):
        replay_next_open(
            DAYS, [{"SYN_A": 100}] * 3, [{"SYN_A": 100}] * 3, {}, cast("float", cash), 0
        )


@pytest.mark.parametrize("cost", [-0.01, 1, 1.5, float("nan"), True])
def test_cost_must_be_finite_half_open_unit_interval(cost: object) -> None:
    with pytest.raises(ValueError, match="cost"):
        replay_next_open(
            DAYS, [{"SYN_A": 100}] * 3, [{"SYN_A": 100}] * 3, {}, 1000, cast("float", cost)
        )


@pytest.mark.parametrize("symbol", ["", " SYN_A", "SYN_A ", "SYN\nA", chr(0), 1])
def test_identity_strings_reject_blank_padded_and_non_string_symbols(symbol: object) -> None:
    with pytest.raises(ValueError, match="identity strings"):
        replay_next_open(
            DAYS,
            [{"SYN_A": 100}] * 3,
            [{"SYN_A": 100}] * 3,
            {DAYS[0]: cast("dict[str, float]", {symbol: 1})},
            1000,
            0,
        )


def test_nonfinite_portfolio_arithmetic_is_rejected() -> None:
    with pytest.raises(ArithmeticError, match="closing equity"):
        replay_next_open(
            DAYS,
            [{}, {"SYN_A": 1e-300}, {"SYN_A": 100}],
            [{}, {"SYN_A": 1e300}, {"SYN_A": 100}],
            {DAYS[0]: {"SYN_A": 1}},
            1000,
            0,
        )


def test_compensated_weight_sum_preserves_full_allocation() -> None:
    # Correctly rounded sum is one (exact rational sum exceeds it by 5.55e-17).
    weights = {"SYN_A": 1 / 3, **{f"SYN_{n}": 0.16666666666666669 for n in range(4)}}
    prices = dict.fromkeys(weights, 1.0)
    result = replay_next_open(DAYS, [prices] * 3, [prices] * 3, {DAYS[0]: weights}, 100.0, 0.0)
    assert result.nav[-1].equity == pytest.approx(100.0)
    assert result.nav[-1].cash == pytest.approx(0.0, abs=1e-12)


def test_extreme_inputs_keep_value_error_boundary() -> None:
    prices = [{"SYN_A": 1.0, "SYN_B": 1.0}] * 3
    with pytest.raises(ValueError, match="weights"):
        replay_next_open(
            DAYS, prices, prices, {DAYS[0]: {"SYN_A": 1e308, "SYN_B": 1e308}}, 100.0, 0.0
        )
    with pytest.raises(ValueError, match="initial cash"):
        replay_next_open(DAYS, prices, prices, {}, 10**1000, 0.0)
    with pytest.raises(ValueError, match="weights"):
        replay_next_open(DAYS, prices, prices, {DAYS[0]: {"SYN_A": 10**1000}}, 100.0, 0.0)
    with pytest.raises(ValueError, match="observed open/close"):
        replay_next_open(
            DAYS, [{"SYN_A": 10**1000}] * 3, prices, {DAYS[0]: {"SYN_A": 1.0}}, 100.0, 0.0
        )
