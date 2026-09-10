"""Synthetic, independently calculated cashflow accounting and numerical boundaries."""

from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from dataclasses import asdict
from datetime import UTC, date, datetime
from typing import cast

import pytest

from aegis_alpha.engine.execution import (
    CashFlow,
    CashFlowReplayResult,
    UnitNavPoint,
    replay_next_open,
    replay_next_open_cashflows,
)

DAYS = [date(2024, 1, 31), date(2024, 2, 1), date(2024, 2, 2)]


@pytest.mark.parametrize(
    "amount", [0, -0.0, True, False, None, "1", float("nan"), float("inf"), -float("inf"), 10**1000]
)
def test_cashflow_rejects_invalid_amount_at_construction(amount: object) -> None:
    with pytest.raises(ValueError, match="finite and nonzero"):
        CashFlow(DAYS[1], cast("float", amount))


@pytest.mark.parametrize("day", [True, None, "2024-02-01", datetime(2024, 2, 1, tzinfo=UTC)])
def test_cashflow_rejects_invalid_date_at_construction(day: object) -> None:
    with pytest.raises(ValueError, match=r"datetime\.date"):
        CashFlow(cast("date", day), 1)


@pytest.mark.parametrize(
    "flows", [None, {}, "", b"", iter(()), [None], [{"date": DAYS[1], "amount": 1}]]
)
def test_flows_require_a_sequence_of_cashflow_instances(flows: object) -> None:
    with pytest.raises(ValueError, match=r"sequence|CashFlow instances"):
        replay_next_open_cashflows(
            DAYS, [{}] * 3, [{}] * 3, {}, 100, 0, cast("Sequence[CashFlow]", flows)
        )


@pytest.mark.parametrize(
    "flows",
    [
        [CashFlow(DAYS[0], 1)],
        [CashFlow(date(2024, 2, 3), 1)],
        [CashFlow(DAYS[1], 1), CashFlow(DAYS[1], -1)],
        [CashFlow(DAYS[2], 1), CashFlow(DAYS[1], 1)],
    ],
)
def test_flow_dates_must_be_supplied_sorted_unique_and_after_baseline(
    flows: list[CashFlow],
) -> None:
    with pytest.raises(ValueError, match=r"supplied sessions|strictly increasing"):
        replay_next_open_cashflows(DAYS, [{}] * 3, [{}] * 3, {}, 100, 0, flows)


def test_flow_count_is_checked_before_reading_items() -> None:
    class Oversized(list[CashFlow]):
        def __iter__(self):  # noqa: ANN204 -- deliberately unreadable boundary sequence
            pytest.fail("oversized flow sequence must not be iterated")

    flows = Oversized([CashFlow(DAYS[1], 1)] * 3)
    with pytest.raises(ValueError, match="session count"):
        replay_next_open_cashflows(DAYS, [{}] * 3, [{}] * 3, {}, 100, 0, flows)


@pytest.mark.parametrize("amount", [-100, -101])
def test_cash_only_withdrawal_cannot_empty_or_overdraw_account(amount: float) -> None:
    with pytest.raises((ValueError, ArithmeticError), match=r"account equity|available cash"):
        replay_next_open_cashflows(
            DAYS, [{}] * 3, [{}] * 3, {}, 100, 0, [CashFlow(DAYS[1], amount)]
        )


def test_pending_liquidation_cannot_finance_a_withdrawal() -> None:
    prices = [{"SYN_A": 10}] * 3
    with pytest.raises(ValueError, match="available cash before rebalancing"):
        replay_next_open_cashflows(
            DAYS,
            prices,
            prices,
            {DAYS[0]: {"SYN_A": 1}, DAYS[1]: {}},
            100,
            0,
            [CashFlow(DAYS[2], -1)],
        )


@pytest.mark.parametrize("price", [None, 0, -1, True, float("nan"), float("inf"), 10**1000])
def test_held_flow_day_requires_actual_valid_open_even_without_a_target(price: object) -> None:
    last = {} if price is None else {"SYN_A": cast("float", price)}
    with pytest.raises(ValueError, match="observed open/close"):
        replay_next_open_cashflows(
            DAYS,
            [{}, {"SYN_A": 10}, last],
            [{}, {"SYN_A": 10}, {"SYN_A": 22}],
            {DAYS[0]: {"SYN_A": 1}},
            100,
            0,
            [CashFlow(DAYS[2], 100)],
        )


def test_postflow_account_overflow_is_rejected() -> None:
    with pytest.raises(ArithmeticError, match="postflow account equity"):
        replay_next_open_cashflows(
            DAYS, [{}] * 3, [{}] * 3, {}, 1e308, 0, [CashFlow(DAYS[1], 1e308)]
        )


@pytest.mark.parametrize("amount", [-1, 1])
def test_cashflow_that_rounds_away_in_cash_is_rejected(amount: float) -> None:
    with pytest.raises(ArithmeticError, match="cash change is not representable"):
        replay_next_open_cashflows(
            DAYS, [{}] * 3, [{}] * 3, {}, 1e300, 0, [CashFlow(DAYS[1], amount)]
        )


def test_cashflow_that_changes_cash_but_rounds_away_in_units_is_rejected() -> None:
    # The deposit changes zero cash to one, but cannot increase 1000 fund units
    # after the security's very large opening gain.
    with pytest.raises(ArithmeticError, match="unit change is not representable"):
        replay_next_open_cashflows(
            DAYS,
            [{}, {"SYN_A": 10}, {"SYN_A": 1e300}],
            [{}, {"SYN_A": 10}, {"SYN_A": 1e300}],
            {DAYS[0]: {"SYN_A": 1}},
            1000,
            0,
            [CashFlow(DAYS[2], 1)],
        )


def test_preflow_opening_equity_overflow_is_rejected_before_deposit() -> None:
    with pytest.raises(ArithmeticError, match="preflow opening equity"):
        replay_next_open_cashflows(
            DAYS,
            [{}, {"SYN_A": 10}, {"SYN_A": 1e308}],
            [{}, {"SYN_A": 10}, {"SYN_A": 10}],
            {DAYS[0]: {"SYN_A": 1}},
            100,
            0,
            [CashFlow(DAYS[2], 1)],
        )


@pytest.mark.parametrize(("opening", "amount"), [(1e-200, 1e200), (1e100, 1e-300)])
def test_unit_issuance_overflow_or_underflow_is_rejected(opening: float, amount: float) -> None:
    with pytest.raises(ArithmeticError, match="unit issuance"):
        replay_next_open_cashflows(
            DAYS,
            [{}, {"SYN_A": 10}, {"SYN_A": opening}],
            [{}, {"SYN_A": 10}, {"SYN_A": 10}],
            {DAYS[0]: {"SYN_A": 1}},
            100,
            0,
            [CashFlow(DAYS[2], amount)],
        )


def test_cumulative_fund_units_cannot_overflow() -> None:
    days = [*DAYS, date(2024, 2, 5)]
    # Buy one share for 1e300, then mark it at 1. Each deposit issues about
    # 1e308 units; the second overflows units while account equity stays finite.
    prices = [{}, {"SYN_A": 1e300}, {"SYN_A": 1}, {"SYN_A": 1}]
    with pytest.raises(ArithmeticError, match="postflow units"):
        replay_next_open_cashflows(
            days,
            prices,
            prices,
            {days[0]: {"SYN_A": 1}},
            1e300,
            0,
            [CashFlow(days[2], 1e8), CashFlow(days[3], 1e8)],
        )


def test_redemption_cannot_round_residual_units_to_zero() -> None:
    days = [*DAYS, date(2024, 2, 5)]
    # Buy one share for 3, sell for 1. Three fund units remain at unit value 1/3.
    # Cash retains 2**-53 after withdrawal, but unit subtraction rounds to zero.
    prices = [{}, {"SYN_A": 3}, {"SYN_A": 1}, {}]
    with pytest.raises(ArithmeticError, match="postflow units"):
        replay_next_open_cashflows(
            days,
            prices,
            prices,
            {days[0]: {"SYN_A": 1}, days[1]: {}},
            3,
            0,
            [CashFlow(days[3], -0.9999999999999999)],
        )


@pytest.mark.parametrize(
    ("initial", "opening", "closing"), [(1e-6, 1e-12, 1e300), (1e200, 1e200, 1e-200)]
)
def test_legacy_skips_unit_ratio_overflow_and_underflow_checks(
    initial: float, opening: float, closing: float
) -> None:
    opens = [{}, {"SYN_A": opening}, {}]
    closes = [{}, {"SYN_A": opening}, {"SYN_A": closing}]
    targets = {DAYS[0]: {"SYN_A": 1}}
    legacy = replay_next_open(DAYS, opens, closes, targets, initial, 0)
    assert legacy.nav[-1].equity == pytest.approx(initial / opening * closing)
    with pytest.raises(ArithmeticError, match="unit value"):
        replay_next_open_cashflows(DAYS, opens, closes, targets, initial, 0, [])


def test_public_cashflow_and_result_records_are_frozen_and_snapshotted() -> None:
    flows = [CashFlow(DAYS[1], 25)]
    result = replay_next_open_cashflows(DAYS, [{}] * 3, [{}] * 3, {}, 100, 0, flows)
    assert isinstance(result, CashFlowReplayResult)
    assert isinstance(result.unit_nav[0], UnitNavPoint)
    assert isinstance(result.unit_nav, tuple)
    assert result.unit_nav[0] == UnitNavPoint(DAYS[0], 1, 100, 0)
    assert result.cashflows == (CashFlow(DAYS[1], 25),)
    with pytest.raises(AttributeError):
        flows[0].amount = 0  # ty: ignore[invalid-assignment]
    with pytest.raises(AttributeError):
        result.unit_nav[0].units = 0  # ty: ignore[invalid-assignment]
    with pytest.raises(AttributeError):
        result.cashflows = ()  # ty: ignore[invalid-assignment]
    flows.clear()
    assert result.cashflows == (CashFlow(DAYS[1], 25),)


def test_flat_prices_deposit_increases_account_without_investment_return() -> None:
    prices = [{"SYN_A": 10}] * 3
    result = replay_next_open_cashflows(
        DAYS,
        prices,
        prices,
        {DAYS[0]: {"SYN_A": 1}, DAYS[1]: {"SYN_A": 1}},
        100,
        0,
        [CashFlow(DAYS[2], 100)],
    )
    assert [point.equity for point in result.account.nav] == pytest.approx([100, 100, 200])
    assert [point.units for point in result.unit_nav] == pytest.approx([100, 100, 200])
    assert [point.unit_value for point in result.unit_nav] == pytest.approx([1, 1, 1])
    assert [point.external_flow for point in result.unit_nav] == [0, 0, 100]


def test_gap_deposit_and_intraday_gain_use_preflow_open_value() -> None:
    # Ten shares worth 200 at open: issue 50 fund units for the 100 deposit.
    # Rebalance buys five shares; 15*22 = 330 and 330/150 = 2.2 at close.
    result = replay_next_open_cashflows(
        DAYS,
        [{}, {"SYN_A": 10}, {"SYN_A": 20}],
        [{}, {"SYN_A": 15}, {"SYN_A": 22}],
        {DAYS[0]: {"SYN_A": 1}, DAYS[1]: {"SYN_A": 1}},
        100,
        0,
        [CashFlow(DAYS[2], 100)],
    )
    assert [point.equity for point in result.account.nav] == pytest.approx([100, 150, 330])
    assert [point.unit_value for point in result.unit_nav] == pytest.approx([1, 1.5, 2.2])
    assert [point.units for point in result.unit_nav] == pytest.approx([100, 100, 150])
    assert [fill.shares for fill in result.account.fills] == pytest.approx([10, 5])
    assert result.account.fills[-1].decision_date == DAYS[1]
    assert result.account.fills[-1].execution_date == DAYS[2]
    assert result.account.fills[-1].price == pytest.approx(20)


def test_deposit_stays_cash_without_prior_close_rebalance() -> None:
    # Ten shares still worth 220 at close, plus 100 uninvested deposit.
    result = replay_next_open_cashflows(
        DAYS,
        [{}, {"SYN_A": 10}, {"SYN_A": 20}],
        [{}, {"SYN_A": 10}, {"SYN_A": 22}],
        {DAYS[0]: {"SYN_A": 1}},
        100,
        0,
        [CashFlow(DAYS[2], 100)],
    )
    assert result.account.nav[-1].equity == pytest.approx(320)
    assert result.account.nav[-1].cash == pytest.approx(100)
    assert result.unit_nav[-1].units == pytest.approx(150)
    assert result.unit_nav[-1].unit_value == pytest.approx(320 / 150)
    assert len(result.account.fills) == 1


def test_rebalancing_fee_after_deposit_reduces_unit_nav() -> None:
    # The 200 available buys notional 200/1.01; only that trade incurs a fee.
    prices = [{"SYN_A": 10}] * 3
    result = replay_next_open_cashflows(
        DAYS,
        prices,
        prices,
        {DAYS[0]: {"SYN_A": 1}},
        100,
        0.01,
        [CashFlow(DAYS[1], 100)],
    )
    assert result.account.nav[1].equity == pytest.approx(200 / 1.01)
    assert result.account.nav[1].fee == pytest.approx(2 / 1.01)
    assert result.account.fills[0].shares == pytest.approx(20 / 1.01)
    assert result.account.fills[0].fee == pytest.approx(2 / 1.01)
    assert result.unit_nav[1].units == pytest.approx(200)
    assert result.unit_nav[1].unit_value == pytest.approx(1 / 1.01)


def test_cash_only_deposit_and_withdrawal_need_no_prices_or_fills() -> None:
    flows = [CashFlow(DAYS[1], -40), CashFlow(DAYS[2], 15)]
    result = replay_next_open_cashflows(DAYS, [{}] * 3, [{}] * 3, {}, 100, 0.01, flows)
    assert [point.equity for point in result.account.nav] == [100, 60, 75]
    assert [point.cash for point in result.account.nav] == [100, 60, 75]
    assert [point.fee for point in result.account.nav] == [0, 0, 0]
    assert result.account.fills == ()
    assert result.unit_nav == (
        UnitNavPoint(DAYS[0], 1, 100, 0),
        UnitNavPoint(DAYS[1], 1, 60, -40),
        UnitNavPoint(DAYS[2], 1, 75, 15),
    )


def test_withdraw_all_remaining_cash_keeps_held_account_alive() -> None:
    # Fifty cash + five shares doubling to 100 = 150 at open, unit value 1.5.
    # Redeem 50/1.5 units; the five remaining shares close at 110.
    result = replay_next_open_cashflows(
        DAYS,
        [{}, {"SYN_A": 10}, {"SYN_A": 20}],
        [{}, {"SYN_A": 10}, {"SYN_A": 22}],
        {DAYS[0]: {"SYN_A": 0.5}},
        100,
        0,
        [CashFlow(DAYS[2], -50)],
    )
    assert result.account.nav[-1].cash == 0
    assert result.account.nav[-1].equity == pytest.approx(110)
    assert result.unit_nav[-1].units == pytest.approx(200 / 3)
    assert result.unit_nav[-1].unit_value == pytest.approx(1.65)
    assert len(result.account.fills) == 1


@pytest.mark.parametrize("cost", [0, 0.01])
def test_empty_flows_preserve_complete_legacy_account(cost: float) -> None:
    prices = [{"SYN_A": 10, "SYN_B": 20}, {"SYN_A": 15, "SYN_B": 18}, {"SYN_A": 20, "SYN_B": 22}]
    targets = {DAYS[0]: {"SYN_A": 0.5}, DAYS[1]: {"SYN_B": 1}}
    legacy = replay_next_open(DAYS, prices, prices, targets, 100, cost)
    result = replay_next_open_cashflows(DAYS, prices, prices, targets, 100, cost, [])
    assert result.account == legacy
    assert asdict(result.account) == asdict(legacy)
    assert set(asdict(legacy)) == {"nav", "fills"}
    assert result.cashflows == ()
    assert [point.unit_value for point in result.unit_nav] == [
        point.equity / 100 for point in legacy.nav
    ]


def test_nonflow_held_day_does_not_require_open_prices() -> None:
    opens = [{}, {"SYN_A": 10}, {}]
    closes = [{}, {"SYN_A": 10}, {"SYN_A": 12}]
    targets = {DAYS[0]: {"SYN_A": 1}}
    result = replay_next_open_cashflows(DAYS, opens, closes, targets, 100, 0, [])
    assert result.account == replay_next_open(DAYS, opens, closes, targets, 100, 0)
    assert result.unit_nav[-1].unit_value == pytest.approx(1.2)


def test_input_mutation_and_future_changes_cannot_rewrite_prior_results() -> None:
    opens = [{}, {"SYN_A": 10}, {"SYN_A": 20}]
    closes = [{}, {"SYN_A": 12}, {"SYN_A": 22}]
    targets = {DAYS[0]: {"SYN_A": 1}, DAYS[1]: {"SYN_A": 1}}
    flows = [CashFlow(DAYS[1], 50), CashFlow(DAYS[2], 100)]
    original = deepcopy((DAYS, opens, closes, targets, flows))
    before = replay_next_open_cashflows(DAYS, opens, closes, targets, 100, 0, flows)
    assert (DAYS, opens, closes, targets, flows) == original
    opens[2]["SYN_A"] = 30
    closes[2]["SYN_A"] = 40
    flows[1] = CashFlow(DAYS[2], 200)
    after = replay_next_open_cashflows(DAYS, opens, closes, targets, 100, 0, flows)
    assert before.account.nav[:2] == after.account.nav[:2]
    assert before.unit_nav[:2] == after.unit_nav[:2]
    assert before.account.fills[0] == after.account.fills[0]
    assert before.cashflows == (CashFlow(DAYS[1], 50), CashFlow(DAYS[2], 100))
    assert before.account.nav[-1] != after.account.nav[-1]
